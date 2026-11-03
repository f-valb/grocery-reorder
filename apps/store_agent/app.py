"""Store assistant: chat with an agent that uses the gold data, the documents and Genie, and shows every tool it used.

Runs as the signed-in viewer (user authorization): the agent sees only the stores that viewer may see.
"""

import pandas as pd
import streamlit as st

import agent
import grocery_data as data

st.set_page_config(page_title="Store assistant", page_icon="💬", layout="wide")


@st.cache_data(ttl=600, show_spinner=False)
def visible_stores(viewer):
    return ", ".join(data.stores().store_id)


viewer = data.current_user()
stores = visible_stores(viewer)

with st.sidebar:
    st.header("Store assistant")
    st.caption("Answers from the gold tables, the supplier contracts and visit notes, and the Genie space. "
               "Every number comes from a tool; expand a step to see it.")
    st.subheader("Try asking")
    for q in agent.DEMO_QUESTIONS:
        if st.button(q, use_container_width=True):
            st.session_state.pending = q
    if st.button("New conversation", type="primary"):
        st.session_state.turns = []
    st.caption(f"Signed in as {viewer} · model {agent.LLM}")

st.title("Store assistant")
st.session_state.setdefault("turns", [])


def show_steps(steps):
    # One expander per tool call: what the agent asked for and what came back
    for step in steps:
        with st.expander(f"🔧 {step['tool']}({', '.join(f'{k}={v}' for k, v in step['args'].items())})"):
            result = step["result"]
            if isinstance(result, list) and result:
                st.dataframe(pd.DataFrame(result), hide_index=True, use_container_width=True)
            elif isinstance(result, dict) and "sql" in result:
                st.write(result.get("text", ""))
                st.code(result["sql"], language="sql")
                st.dataframe(pd.DataFrame(result.get("rows", [])), hide_index=True)
            else:
                st.json(result)


for turn in st.session_state.turns:
    st.chat_message("user").write(turn["question"])
    with st.chat_message("assistant"):
        show_steps(turn["steps"])
        st.write(turn["answer"])

question = st.chat_input("Ask about a store, a product, an order or a supplier") or st.session_state.pop("pending", None)
if question:
    st.chat_message("user").write(question)
    # Keep only the question/answer text of earlier turns as history (tool results stay out to keep the prompt small)
    history = [m for t in st.session_state.turns[-3:]
               for m in ({"role": "user", "content": t["question"]}, {"role": "assistant", "content": t["answer"]})]
    with st.chat_message("assistant"):
        with st.spinner("Thinking and calling tools..."):
            text, steps = agent.answer(question, history, stores)
        show_steps(steps)
        st.write(text)
    st.session_state.turns.append({"question": question, "answer": text, "steps": steps})
