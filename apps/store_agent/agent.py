"""Store assistant agent: an LLM that answers store questions by calling tools over the gold data, the documents and Genie.

The loop is hand-written so every step is visible: the model asks for a tool, we run it as the viewer, send the result
back, and repeat until it answers (at most MAX_STEPS tool rounds). Test without the UI: python agent.py (see bottom).
"""

import json
import os
import time

import grocery_data as data

LLM = os.environ.get("LLM_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct")
GENIE_SPACE_ID = os.environ["GENIE_SPACE_ID"]  # app.yaml: from the genie-space resource; locally: export it
MAX_STEPS = 5

TOOLS = [
    {"type": "function", "function": {
        "name": "get_critical_items",
        "description": "Ranked critical items for one store (or ALL): stockouts, waste and next week's shortfalls, "
                       "each with the dollars at stake and the reason. Use for 'what should I act on' questions.",
        "parameters": {"type": "object", "properties": {
            "store_id": {"type": "string", "description": "Store code like ST-07, or ALL"},
            "limit": {"type": "integer", "description": "How many items, default 5"}}, "required": ["store_id"]}}},
    {"type": "function", "function": {
        "name": "get_reorder",
        "description": "Next week's order for a store: forecast with its 80% range, stock on arrival, order units and "
                       "cases, and the reason (for example capped by shelf life). Optionally filter by product name.",
        "parameters": {"type": "object", "properties": {
            "store_id": {"type": "string", "description": "Store code like ST-12"},
            "product": {"type": "string", "description": "Product name or part of it, e.g. strawberries"}},
            "required": ["store_id"]}}},
    {"type": "function", "function": {
        "name": "search_documents",
        "description": "Search supplier contracts, store visit notes, customer feedback and delivery notes by meaning. "
                       "Use for WHY questions (causes, agreements, what was observed) and what can be done.",
        "parameters": {"type": "object", "properties": {
            "question": {"type": "string", "description": "What to look for, in plain words"},
            "store_id": {"type": "string", "description": "Optional store code to include that store's own notes"}},
            "required": ["question"]}}},
    {"type": "function", "function": {
        "name": "ask_genie",
        "description": "Ask the Genie analytics space any other question about sales, stock, waste, forecasts or "
                       "regions, for example comparisons across stores, regions or categories.",
        "parameters": {"type": "object", "properties": {
            "question": {"type": "string", "description": "The data question in plain English"}}, "required": ["question"]}}},
]

# RULE: grounding: numbers only from tool results, cite documents, refuse off-topic questions
SYSTEM = """You are the store assistant for a 20-store grocery chain. You help store managers and head office decide
what to order and what to fix this week.
Rules:
- Use the tools for every number. Never invent or estimate numbers that no tool returned.
- For why questions, combine data (get_critical_items, get_reorder) with documents (search_documents) and cite each
  document you use as [doc_name].
- Store codes look like ST-07. Stores visible to this user: {stores}.
- If a question is not about the stores, products, stock, sales, waste, orders or suppliers, say briefly that you can
  only help with store operations, and do not call tools.
- Answer in at most 120 words: the answer first, then 1-3 bullet points with the evidence, then one suggested action."""


def tool_get_critical_items(store_id, limit=5):
    items = data.critical_items(store_id.upper()).head(int(limit or 5))
    return items[["store_id", "product_name", "issue", "detail", "impact_usd", "severity", "next_week_order_cases"]]


def tool_get_reorder(store_id, product=None):
    return data.reorder_list(store_id.upper(), product).head(10)


def tool_search_documents(question, store_id=None):
    hits = data.search_documents(question, store_id.upper() if store_id else None)
    return hits[["doc_name", "section", "chunk_text", "score"]]


def tool_ask_genie(question):
    # Genie Conversation API as the viewer: start, poll until done, then fetch the query result of each attachment
    api = data.workspace_client().api_client
    base = f"/api/2.0/genie/spaces/{GENIE_SPACE_ID}"
    started = api.do("POST", f"{base}/start-conversation", body={"content": question})
    conv, msg = started["conversation_id"], started["message_id"]
    for _ in range(40):
        message = api.do("GET", f"{base}/conversations/{conv}/messages/{msg}")
        if message.get("status") in ("COMPLETED", "FAILED", "CANCELLED"):
            break
        time.sleep(3)
    answer = {"status": message.get("status")}
    for att in message.get("attachments", []):
        if att.get("text"):
            answer["text"] = att["text"].get("content", "")
        if att.get("query"):
            answer["sql"] = att["query"].get("query", "")
            res = api.do("GET", f"{base}/conversations/{conv}/messages/{msg}/attachments/{att['attachment_id']}/query-result")
            stmt = res.get("statement_response", {})
            cols = [c["name"] for c in stmt.get("manifest", {}).get("schema", {}).get("columns", [])]
            answer["rows"] = [dict(zip(cols, r)) for r in stmt.get("result", {}).get("data_array", [])[:15]]
    return answer


TOOL_FUNCS = {"get_critical_items": tool_get_critical_items, "get_reorder": tool_get_reorder,
              "search_documents": tool_search_documents, "ask_genie": tool_ask_genie}


def run_tool(name, args):
    try:
        result = TOOL_FUNCS[name](**args)
    except Exception as e:  # a failed tool goes back to the model as text, so it can explain or try another tool
        return {"error": str(e)[:300]}
    return result.to_dict(orient="records") if hasattr(result, "to_dict") else result


def chat_completion(messages, tools=True):
    # OpenAI-style chat on the Foundation Model endpoint, as the viewer; tools=False forces a plain answer
    body = {"messages": messages, "temperature": 0, "max_tokens": 700, **({"tools": TOOLS} if tools else {})}
    api = data.workspace_client().api_client
    return api.do("POST", f"/serving-endpoints/{LLM}/invocations", body=body)["choices"][0]["message"]


def answer(question, history=(), visible_stores=""):
    """Returns (final answer text, list of steps); each step = {tool, args, result}."""
    messages = [{"role": "system", "content": SYSTEM.format(stores=visible_stores or "all")}, *history,
                {"role": "user", "content": question}]
    steps = []
    for _ in range(MAX_STEPS):
        reply = chat_completion(messages)
        calls = reply.get("tool_calls") or []
        if not calls:
            return reply.get("content", ""), steps
        messages.append({"role": "assistant", "content": reply.get("content") or "", "tool_calls": calls})
        for call in calls:
            name, args = call["function"]["name"], json.loads(call["function"].get("arguments") or "{}")
            result = run_tool(name, args)
            steps.append({"tool": name, "args": args, "result": result})
            messages.append({"role": "tool", "tool_call_id": call["id"],
                             "content": json.dumps(result, default=str)[:6000]})
    # TRAP: cap the loop so a confused model can't call tools forever; ask for an answer with what it has
    messages.append({"role": "user", "content": "Answer now with the information you have."})
    return chat_completion(messages, tools=False).get("content", ""), steps


DEMO_QUESTIONS = [
    "What are the 3 most critical items at ST-07 this week and why?",
    "Why is coffee always out of stock at ST-07, and what can we do?",
    "How much should ST-12 order of strawberries next week?",
    "Which region has the highest waste?",
    "What's the weather tomorrow?",
]

if __name__ == "__main__":
    # Scripted test: each demo question, the tools the agent chose, and its answer
    for q in DEMO_QUESTIONS:
        text, steps = answer(q)
        print(f"\nQ: {q}\n   tools: {[(s['tool'], s['args']) for s in steps]}\n   A: {text}")
