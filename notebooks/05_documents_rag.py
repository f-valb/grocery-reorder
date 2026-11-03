# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Documents: vector search and generated answers
# MAGIC **Purpose:** put the chain's documents to work. Supplier agreements, SOPs, store visit notes, promo briefs and customer feedback are chunked, embedded and indexed with Databricks Vector Search; an LLM answers "why" questions with the gold numbers plus the retrieved passages, citing its sources, and writes a weekly brief per store.
# MAGIC **Inputs:** `silver_*` and `gold_*` (to keep the documents consistent with the data), the documents written in section 1
# MAGIC **Outputs:** files in `/Volumes/workspace/grocery_intel/raw/documents/`, `bronze_documents`, `bronze_document_files`, `silver_document_parse_log`, `silver_doc_chunks`, vector index `doc_chunks_index`, `gold_doc_answers`, `gold_store_briefs`, `gold_rag_check`
# MAGIC
# MAGIC The documents are mocked (the brief allows it) but built from the same stores, products, promos and price changes as the structured data, so text and numbers tell one story.
# MAGIC Three of them arrive in real formats, uploaded to `raw/document_files/` (made by `scripts/make_source_files.py`): the Coffee contract as a **PDF**, the ST-07 visit note as a **Word** file and a photographed **delivery note** (PNG).

# COMMAND ----------

# MAGIC %pip install pypdf==5.1.0 --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 Write source documents (mock of a SharePoint / Drive export)

# COMMAND ----------

# Setup: environment, paths, model and index names
import hashlib
import json
import os
import re
import time

import pandas as pd
from pyspark.sql import functions as F

spark.sql("USE workspace.grocery_intel")
IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
DOC_DIR = os.path.join(os.environ.get("RAW_DIR", "/Volumes/workspace/grocery_intel/raw"), "documents")
FILES_DIR = os.path.join(os.environ.get("RAW_DIR", "/Volumes/workspace/grocery_intel/raw"), "document_files")
REAL_FORMAT_DOCS = {"supplier_coffee", "visit_st07_2026_09"}  # these arrive as PDF / Word files instead of markdown
VS_ENDPOINT = "grocery_docs_vs"
INDEX_NAME = "workspace.grocery_intel.doc_chunks_index"
EMBEDDING_MODEL = "databricks-gte-large-en"
LLM = "databricks-meta-llama-3-3-70b-instruct"
VISION_LLM = "databricks-llama-4-maverick"  # reads text from images (OCR); ai_parse_document is not available on Free Edition
TOP_K = 4

# COMMAND ----------

# Facts the documents must agree with, read from silver (store names, promos, price changes, new item)
stores = {r.store_id: r.store_name for r in spark.table("silver_stores").collect()}
promos = spark.sql("""
SELECT p.product_name, pr.week_start, pr.discount_pct FROM silver_promotions pr
JOIN (SELECT DISTINCT item_id, product_name FROM silver_products) p ON p.item_id = pr.item_id ORDER BY pr.week_start""").collect()
price_changes = spark.sql("""
SELECT product_name, MIN(valid_to) AS old_until, MIN(unit_cost) AS old_cost, MAX(unit_cost) AS new_cost
FROM silver_products GROUP BY product_name HAVING COUNT(*) > 1 ORDER BY product_name""").collect()


def doc(doc_type, name, title, meta, sections):
    # One markdown document: a small front-matter block, a title, and ## sections (the chunk boundaries)
    head = "\n".join(f"{k}: {v}" for k, v in {"doc_type": doc_type, **meta}.items())
    body = "\n\n".join(f"## {heading}\n{text.strip()}" for heading, text in sections)
    return f"{doc_type}/{name}.md", f"---\n{head}\n---\n# {title}\n\n{body}\n"


documents = []
# SIGNAL: supplier allocation caps explain the popular-item stockouts (the generator fills ~70% of demand)
for item, supplier in [("Coffee", "BeanCo Roasters"), ("Pasta", "Durum & Co"), ("Sparkling Water", "Clearwater Beverages"),
                       ("Eggs 12pk", "Valley Farms")]:
    documents.append(doc("supplier_agreement", f"supplier_{item.lower().replace(' ', '_')}", f"Supply agreement: {item} ({supplier})",
        {"product": item, "store_id": "ALL", "effective_date": "2026-01-01"}, [
        ("Parties and scope", f"{supplier} supplies {item} to all 20 stores of the chain. Deliveries arrive weekly, on Monday, for orders placed by Wednesday 18:00."),
        ("Allocation (clause 4.1)", f"Because of constrained capacity, {supplier} allocates each store at most 70% of that store's average weekly volume over the previous quarter. Orders above the allocation are cut back to the allocation without notice."),
        ("Emergency orders (clause 4.2)", f"A store may place one emergency order per week for {item} above its allocation, delivered within 48 hours, at a surcharge of 8% on the unit cost. Emergency orders are capped at 30% of the weekly allocation."),
        ("Review (clause 7)", "Allocation levels are reviewed quarterly. The buyer may request a higher allocation with 4 weeks of sales evidence, including days out of stock."),
    ]))
# Fresh suppliers: the option to deliver twice a week is what the shelf-life cap needs
documents.append(doc("supplier_agreement", "supplier_greenfield_produce", "Supply agreement: fresh produce (GreenField Produce)",
    {"product": "Produce", "store_id": "ALL", "effective_date": "2026-03-01"}, [
    ("Scope", "GreenField Produce supplies all produce lines (bananas, strawberries, salad mix, tomatoes, avocados, apples, spinach, grapes) to all stores. Standard delivery is once a week, on Monday."),
    ("Delivery frequency (clause 3.3)", "A second weekly delivery on Thursday is available per store for a flat fee of $45 per delivery. Orders for the Thursday delivery close on Tuesday 12:00. Stores on twice-weekly delivery can order to 4 days of sales per delivery."),
    ("Returns", "Unsold produce is not returnable. Quality claims must be raised within 24 hours of delivery with photos."),
]))
documents.append(doc("supplier_agreement", "supplier_hearth_bakery", "Supply agreement: bakery (Hearth Bakery)",
    {"product": "Bakery", "store_id": "ALL", "effective_date": "2026-02-01"}, [
    ("Scope", "Hearth Bakery supplies sourdough, bagels, croissants, baguettes and muffins. Shelf life is 3 to 5 days from delivery."),
    ("Delivery frequency (clause 2.4)", "Daily delivery (Monday to Saturday) is available at no extra cost for stores ordering at least 2 cases a day. Weekly delivery is the default."),
    ("Minimum order", "One case (8 units) per product per delivery."),
]))
# SOP: how fresh ordering should work (the shelf-life cap is the written policy)
documents.append(doc("sop", "sop_fresh_ordering", "Standard operating procedure: ordering and handling fresh products",
    {"product": "Produce, Bakery, Dairy", "store_id": "ALL", "effective_date": "2026-01-15"}, [
    ("Ordering rule", "Order fresh products to cover expected sales within the remaining shelf life only. Do not add a safety buffer above shelf-life cover for products with 7 days of shelf life or less: leftover stock expires."),
    ("Markdown", "Mark down fresh products by 30% on the last day of shelf life, and by 50% two hours before closing."),
    ("Donation and waste logging", "Expired products that are safe go to the food bank partner on the same day. Everything removed from the shelf is logged in the store-ops app with reason EXPIRED or DAMAGED before the end of the day, in store local time."),
    ("Damaged stock", "Damaged packaging is logged as DAMAGED. Repeated damage from one supplier is reported to the category buyer."),
]))
documents.append(doc("sop", "sop_stock_counts", "Standard operating procedure: weekly stock counts",
    {"product": "All", "store_id": "ALL", "effective_date": "2026-01-15"}, [
    ("When", "Stock is counted every Sunday after closing; the POS snapshot is taken at 23:00 UTC."),
    ("Corrections", "If a count is wrong, re-submit the corrected snapshot. The latest valid submission replaces the earlier one; negative counts are rejected by head office."),
]))
# SIGNAL: store visit notes explain ST-07's waste (habitual buffer ordering) and ST-12's one-off spike
documents.append(doc("store_visit", "visit_st07_2026_09", f"Area manager visit: {stores['ST-07']}",
    {"store_id": "ST-07", "visit_date": "2026-09-16"}, [
    ("Ordering practice", "The store manager orders roughly 50% above the system suggestion on every line, 'to be safe for the weekend'. This is applied to fresh lines too, including sourdough, muffins and strawberries."),
    ("Observations", "Waste bins were full of sourdough and muffins on Monday morning. Back room holds about two weeks of cover on several dry and plant-based lines. Availability on shelf is good."),
    ("Agreed actions", "Follow the system order for fresh lines for 4 weeks and review waste. Area manager to check again in October."),
]))
documents.append(doc("store_visit", "visit_st12_2026_09", f"Area manager visit: {stores['ST-12']}",
    {"store_id": "ST-12", "visit_date": "2026-09-14"}, [
    ("Bayside Summer Festival", "The Bayside Summer Festival ran next to the store in the week of 2026-09-07. Beverages sold about three times their normal volume and several lines sold out by Saturday."),
    ("Guidance", "This was a one-off event. Do not raise standing beverage orders because of it; plan next year's festival as a separate event order."),
]))
documents.append(doc("store_visit", "visit_st03_2026_10", f"Area manager visit: {stores['ST-03']}",
    {"store_id": "ST-03", "visit_date": "2026-10-07"}, [
    ("Summary", "Large store, ordering follows the system. Coffee and pasta gaps on shelf most weeks; the team says deliveries arrive short. Fresh waste in line with the chain average."),
]))
# Promo briefs, one per promoted product, from the promo calendar
by_product = {}
for p in promos:
    by_product.setdefault(p.product_name, []).append(p)
for product, rows in by_product.items():
    weeks = ", ".join(f"week of {r.week_start} ({r.discount_pct}% off)" for r in rows)
    documents.append(doc("promo_brief", f"promo_{product.lower().replace(' ', '_')}", f"Promotion brief: {product}",
        {"product": product, "store_id": "ALL", "owner": "Marketing"}, [
        ("Plan", f"Chain-wide price promotion on {product}: {weeks}."),
        ("Expected uplift and ordering", "Expect sales of about 1.8 times a normal week. Stores should order for the promotion one week ahead; the promo calendar is available to the ordering system in advance."),
    ]))
# Price change notice from the ERP
documents.append(doc("notice", "notice_cost_increase_2026_09", "Supplier cost increase notice",
    {"product": ", ".join(r.product_name for r in price_changes), "store_id": "ALL", "effective_date": "2026-09-14"}, [
    ("Change", "From 2026-09-14 unit costs rise by about 12% and shelf prices by about 10% on: "
     + "; ".join(f"{r.product_name} (cost {r.old_cost:.2f} to {r.new_cost:.2f})" for r in price_changes) + "."),
    ("Reporting", "Revenue and waste value for weeks before 2026-09-14 use the old prices."),
]))
# Category and launch memos
documents.append(doc("category_memo", "memo_plant_based_2026_10", "Category review: plant-based",
    {"product": "Plant-based", "store_id": "ALL", "author": "Category manager", "date": "2026-10-05"}, [
    ("Trend", "Plant-based demand has grown about 6% a week since August across all stores, led by oat milk, tofu and plant mince."),
    ("Recommendation", "Increase facings for oat milk and almond yogurt and let the forecast drive orders; do not cap plant-based orders at last quarter's volume."),
]))
documents.append(doc("launch_memo", "launch_protein_bar", "New product launch: Protein Bar",
    {"product": "Protein Bar", "store_id": "ALL", "date": "2026-09-28"}, [
    ("Launch", "Protein Bar (Dry Goods, case of 24) launched in all stores in the week of 2026-09-28."),
    ("Forecasting", "With fewer than 3 weeks of sales, forecasts use the average of the weeks available (or the category median before the first sale). Review after 6 weeks."),
]))
# SIGNAL: customer feedback digests that echo the stockouts, the fresh quality issue and plant-based demand
feedback = {
    "ST-07": ["Fresh bread is often a day old by the afternoon.", "Coffee shelf empty again on Thursday.", "Love the plant-based range, wish there was more almond yogurt."],
    "ST-03": ["Pasta has been missing for two weeks.", "Sparkling water sold out at the weekend."],
    "ST-12": ["During the festival the drinks fridge was empty.", "Oat milk sold out again."],
    "ST-01": ["Eggs out of stock on Friday evening.", "Great fruit and veg quality."],
}
for store_id, comments in feedback.items():
    documents.append(doc("customer_feedback", f"feedback_{store_id.lower()}_2026_q3", f"Customer feedback digest Q3: {stores[store_id]}",
        {"store_id": store_id, "period": "2026-08 to 2026-10"}, [
        ("Top comments", "\n".join(f"- {c}" for c in comments)),
    ]))

# Write every document as a markdown file into the volume (overwrite = idempotent); real-format documents are skipped
documents = [(p, txt) for p, txt in documents if os.path.basename(p).removesuffix(".md") not in REAL_FORMAT_DOCS]
for name in REAL_FORMAT_DOCS:  # remove markdown copies left by earlier runs
    for folder in ("supplier_agreement", "store_visit"):
        if os.path.exists(f"{DOC_DIR}/{folder}/{name}.md"):
            os.remove(f"{DOC_DIR}/{folder}/{name}.md")
for rel_path, text in documents:
    os.makedirs(os.path.dirname(f"{DOC_DIR}/{rel_path}"), exist_ok=True)
    with open(f"{DOC_DIR}/{rel_path}", "w") as fh:
        fh.write(text)
print(f"{len(documents)} documents written to {DOC_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 Bronze: land the documents as rows

# COMMAND ----------

# One row per file: full text, path and load time (lineage from the volume to the table)
# TRAP: wholetext must be an argument of .text(): its default (False) overrides .option("wholetext") and splits files into lines
bronze_docs = (spark.read.option("recursiveFileLookup", True).text(DOC_DIR, wholetext=True)
               .select(F.col("_metadata.file_path").alias("source_file"), F.col("value").alias("content"))
               .withColumn("_source_system", F.lit("document_store")).withColumn("_ingested_at", F.current_timestamp()))
bronze_docs.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("bronze_documents")

# COMMAND ----------

# Real-format files (PDF, Word, images) land as bytes: one row per file with its content, size and modified time
bronze_files = (spark.read.format("binaryFile").option("recursiveFileLookup", True).load(FILES_DIR)
                .select(F.col("path").alias("source_file"), "content", "length", "modificationTime")
                .withColumn("_source_system", F.lit("document_store")).withColumn("_ingested_at", F.current_timestamp()))
bronze_files.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("bronze_document_files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 Silver: chunk by section, keep metadata for filtering and citing
# MAGIC One chunk per `##` section, prefixed with the document title so each chunk makes sense on its own; short documents (up to 110 words) stay whole. Metadata (doc type, store, product) comes from the front matter.

# COMMAND ----------

MAX_WHOLE_DOC_WORDS = 110


# RULE: chunk = document title + one section (or one PDF page); chunk_id is a hash of file + position, so reruns keep ids
# TRAP: short documents stay one chunk: split into tiny sections, the cause ("orders 50% above the system") ranked 8th
def make_chunks(source_file, title, meta, sections, source_format):
    if sum(len(text.split()) for _, text in sections) <= MAX_WHOLE_DOC_WORDS:
        sections = [("Full document", " ".join(f"{h}: {txt}" for h, txt in sections))]
    name = os.path.basename(source_file)
    return [{"chunk_id": hashlib.sha1(f"{name}#{i}".encode()).hexdigest()[:16], "doc_name": name, "doc_title": title,
             "section": heading, "doc_type": meta["doc_type"], "store_id": meta.get("store_id", "ALL"),
             "product": meta.get("product", ""), "source_format": source_format,
             "chunk_text": f"{title}. {heading}: {' '.join(text.split())}", "source_file": source_file}
            for i, (heading, text) in enumerate(sections)]


def parse_markdown(content):
    # Front matter (metadata), "# " title, "## " sections
    front, body = re.match(r"---\n(.*?)\n---\n(.*)", content, re.S).groups()
    meta = dict(line.split(": ", 1) for line in front.splitlines())
    title = re.search(r"^# (.+)$", body, re.M).group(1)
    return title, meta, [tuple(s[3:].split("\n", 1)) for s in re.split(r"\n(?=## )", body)[1:]]


chunks = [r for d in spark.table("bronze_documents").collect()
          for r in make_chunks(d.source_file, *parse_markdown(d.content), "markdown")]

# COMMAND ----------

# MAGIC %md
# MAGIC **Real formats.** Each format is turned into text with its structure kept: a PDF page by page (so answers can cite the page), a Word file by heading, an image by reading its text with a vision model (OCR).
# MAGIC Metadata for these files (doc type, store, product) would come from the document store (SharePoint / Drive fields); here it is a small mapping.

# COMMAND ----------

# Parsers for PDF (pypdf, per page), Word (the .docx XML, per heading) and images (vision LLM transcription)
import base64
import io
import zipfile
from html import unescape

FILE_META = {
    "supplier_coffee.pdf": {"doc_type": "supplier_agreement", "store_id": "ALL", "product": "Coffee"},
    "visit_st07_2026_09.docx": {"doc_type": "store_visit", "store_id": "ST-07"},
    "delivery_note_st07_2026_10_12.png": {"doc_type": "delivery_note", "store_id": "ST-07", "product": "Coffee"},
}


def parse_pdf(data):
    from pypdf import PdfReader
    pages = [(n, page.extract_text() or "") for n, page in enumerate(PdfReader(io.BytesIO(data)).pages, start=1)]
    title = next(line for line in pages[0][1].splitlines() if line.strip())
    return title, [(f"Page {n}", text) for n, text in pages]


def parse_docx(data):
    xml = zipfile.ZipFile(io.BytesIO(data)).read("word/document.xml").decode()
    title, sections, heading, lines = "", [], "Summary", []
    for para in re.findall(r"<w:p>(.*?)</w:p>", xml, re.S):
        style = re.search(r'w:pStyle w:val="([^"]+)"', para)
        text = unescape("".join(re.findall(r"<w:t[^>]*>(.*?)</w:t>", para, re.S)))
        if style and style.group(1) == "Title":
            title = text
        elif style and style.group(1).startswith("Heading"):
            if lines:
                sections.append((heading, " ".join(lines)))
            heading, lines = text, []
        elif text.strip():
            lines.append(text)
    sections.append((heading, " ".join(lines)))
    return title, sections


def parse_image(data, mime="image/png"):
    if not IS_DATABRICKS:
        return None
    prompt = "Transcribe all text in this image exactly, keeping table rows on one line. Output only the text."
    resp = WorkspaceClient().api_client.do("POST", f"/serving-endpoints/{VISION_LLM}/invocations", body={
        "max_tokens": 600, "temperature": 0, "messages": [{"role": "user", "content": [
            {"type": "text", "text": prompt},
            {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{base64.b64encode(data).decode()}"}}]}]})
    text = resp["choices"][0]["message"]["content"].strip()
    title = text.splitlines()[0]
    return title, [("Transcribed image", "\n".join(text.splitlines()[1:]))]


if IS_DATABRICKS:
    from databricks.sdk import WorkspaceClient

# QUALITY: every file gets a parse log row; a file that yields too little text is not indexed and shows up here
parse_log = []
for f in spark.table("bronze_document_files").collect():
    name = os.path.basename(f.source_file)
    ext = name.rsplit(".", 1)[-1].lower()
    parser = {"pdf": ("pypdf", parse_pdf), "docx": ("docx_xml", parse_docx), "png": ("vision_llm_ocr", parse_image),
              "jpg": ("vision_llm_ocr", parse_image)}.get(ext)
    parsed = parser[1](bytes(f.content)) if parser else None
    words = sum(len(text.split()) for _, text in parsed[1]) if parsed else 0
    status = "parsed" if words >= 10 else ("skipped_locally" if parser and not IS_DATABRICKS else "too_little_text")
    parse_log.append({"source_file": f.source_file, "source_format": ext, "method": parser[0] if parser else "unsupported",
                      "words": words, "status": status})
    if status == "parsed":
        chunks += make_chunks(f.source_file, parsed[0], FILE_META.get(name, {"doc_type": "unknown"}), parsed[1], ext)
spark.createDataFrame(pd.DataFrame(parse_log)).write.mode("overwrite").option("overwriteSchema", True).saveAsTable("silver_document_parse_log")
display(spark.table("silver_document_parse_log"))

# COMMAND ----------

# Write the chunks from every format to one table
# SPARK: join back to both bronze tables in Spark so Unity Catalog lineage links chunks to the landed files
landed = (spark.table("bronze_documents").select("source_file", "_ingested_at")
          .unionByName(spark.table("bronze_document_files").select("source_file", "_ingested_at")))
(spark.createDataFrame(pd.DataFrame(chunks)).join(landed, "source_file")
 .write.mode("overwrite").option("overwriteSchema", True).saveAsTable("silver_doc_chunks"))
# Delta Sync indexes read the change feed of their source table
spark.sql("ALTER TABLE silver_doc_chunks SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
display(spark.sql("""SELECT source_format, doc_type, COUNT(DISTINCT doc_name) AS documents, COUNT(*) AS chunks FROM silver_doc_chunks
                       GROUP BY source_format, doc_type ORDER BY source_format, doc_type"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 Embed + index (Databricks Vector Search)
# MAGIC A Delta Sync index on `silver_doc_chunks`: Vector Search computes the embeddings itself with `databricks-gte-large-en` and re-syncs when the table changes. The index is a Unity Catalog object, governed like a table.

# COMMAND ----------

# Create the index the first time, re-sync it on later runs, and wait until every chunk is indexed
if IS_DATABRICKS:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.vectorsearch import (DeltaSyncVectorIndexSpecRequest, EmbeddingSourceColumn, PipelineType,
                                                     VectorIndexType)

    w = WorkspaceClient()
    SYNC_COLUMNS = ["chunk_id", "doc_name", "doc_title", "section", "doc_type", "store_id", "product", "source_format", "chunk_text"]
    existing = [i.name for i in w.vector_search_indexes.list_indexes(endpoint_name=VS_ENDPOINT)]
    # Synced columns are fixed when an index is created: after changing SYNC_COLUMNS, delete the index once and rerun
    if INDEX_NAME not in existing:
        w.vector_search_indexes.create_index(
            name=INDEX_NAME, endpoint_name=VS_ENDPOINT, primary_key="chunk_id", index_type=VectorIndexType.DELTA_SYNC,
            delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
                source_table="workspace.grocery_intel.silver_doc_chunks", pipeline_type=PipelineType.TRIGGERED,
                embedding_source_columns=[EmbeddingSourceColumn(name="chunk_text", embedding_model_endpoint_name=EMBEDDING_MODEL)],
                columns_to_sync=SYNC_COLUMNS))
    else:
        # TRAP: a TRIGGERED index only picks up a rewritten table when asked to sync
        for _ in range(30):
            try:
                w.vector_search_indexes.sync_index(INDEX_NAME)
                break
            except Exception:
                time.sleep(10)  # the index is still busy with an earlier sync
    expected = spark.table("silver_doc_chunks").count()
    for attempt in range(90):  # up to 15 minutes for the first build
        status = w.vector_search_indexes.get_index(INDEX_NAME).status
        if status.ready and (status.indexed_row_count or 0) == expected:
            break
        time.sleep(10)
    print(f"index ready={status.ready}, indexed {status.indexed_row_count} of {expected} chunks")

# At scale: documents would arrive through a connector (SharePoint, Google Drive) into the volume, be parsed with
# ai_parse_document (PDFs, scans), and the index would sync on a schedule; access would be filtered by store_id metadata.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 Retrieval: vector search, with metadata filters

# COMMAND ----------

# Vector search: the question is embedded with the same model and matched to the closest chunks
def retrieve(question, store_id=None, k=TOP_K):
    filters = json.dumps({"store_id": [store_id, "ALL"]}) if store_id else None
    res = w.vector_search_indexes.query_index(
        index_name=INDEX_NAME, columns=["chunk_id", "doc_name", "doc_title", "section", "store_id", "chunk_text"],
        query_text=question, num_results=k, filters_json=filters)
    cols = [c.name for c in res.manifest.columns]
    return pd.DataFrame(res.result.data_array or [], columns=cols)


if IS_DATABRICKS:
    display(spark.createDataFrame(retrieve("Why does coffee keep running out?")[["doc_name", "section", "score"]]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 Generated answers and store briefs (retrieval + gold numbers + LLM)
# MAGIC The model gets two kinds of context: numbers from the gold data product, and passages from the documents. It must answer only from that context and cite the documents it used.

# COMMAND ----------

# RULE: grounded answers only: the prompt allows nothing beyond the given numbers and passages, and asks for [doc_name] citations
SYSTEM_PROMPT = ("You are an assistant for grocery store managers. Answer only from the DATA and DOCUMENTS given. "
                 "Cite every document you use as [doc_name], adding the page for PDFs, e.g. [supplier_coffee.pdf, page 2]. If the context does not answer the question, say so. "
                 "Explain the cause from the DOCUMENTS and back it with numbers from the DATA. "
                 "Be concrete: numbers, product names, what to do next. At most 150 words.")


def recent_product_stats(store_id):
    # Last 4 full weeks per product for one store: stockouts, missed demand, waste
    return spark.sql(f"""
    WITH recent AS (SELECT * FROM gold_store_product_week WHERE store_id = '{store_id}' AND NOT is_partial_week
                    AND week_num > (SELECT MAX(week_num) FROM gold_store_product_week WHERE NOT is_partial_week) - 4)
    SELECT product_name, SUM(stockout_days) AS stockout_days, ROUND(SUM(est_demand - units_sold)) AS missed_units,
           ROUND(SUM(waste_value)) AS waste_usd, ROUND(100 * SUM(expired_units) / NULLIF(SUM(received), 0), 1) AS expired_pct
    FROM recent GROUP BY product_name""").toPandas()


def store_facts(store_id):
    # Gold numbers for one store: top 3 stockout and top 3 waste products (last 4 full weeks), plus next week's order
    stats = recent_product_stats(store_id)
    stockouts = stats[stats.stockout_days > 0].nlargest(3, "stockout_days")
    return ("Top stockout products:\n" + (stockouts.to_string(index=False) if len(stockouts) else "none") +
            "\n\nTop waste products:\n" + stats.nlargest(3, "waste_usd").to_string(index=False) +
            "\n\nNext week's order:\n" + spark.sql(f"""SELECT product_name, order_units, reason FROM gold_reorder_recommendation
            WHERE store_id = '{store_id}' ORDER BY order_value DESC LIMIT 6""").toPandas().to_string(index=False))


PRODUCT_NAMES = [r.product_name for r in spark.sql("SELECT DISTINCT product_name FROM silver_products").collect()]
CATEGORY_WORDS = {"beverage": "Beverages", "drink": "Beverages", "bread": "Bakery", "bakery": "Bakery", "produce": "Produce",
                  "plant-based": "Plant-based", "dairy": "Dairy", "dry goods": "Dry Goods"}


def question_facts(question, store_id):
    # Weekly numbers for any product or category the question names, plus how much the store receives vs its demand
    q = question.lower()
    products = [p for p in PRODUCT_NAMES if p.lower() in q or p.lower().split()[0] in q.split()]
    categories = sorted({c for word, c in CATEGORY_WORDS.items() if word in q})
    parts = [spark.sql(f"""
    SELECT ROUND(SUM(CASE WHEN store_id = '{store_id}' THEN received END) / SUM(CASE WHEN store_id = '{store_id}' THEN est_demand END), 2)
             AS this_store_received_vs_demand,
           ROUND(SUM(received) / SUM(est_demand), 2) AS chain_received_vs_demand
    FROM gold_store_product_week WHERE NOT is_partial_week""").toPandas().to_string(index=False)]
    for product in products:
        parts.append(f"Weekly {product} at {store_id}:\n" + spark.sql(f"""
        SELECT week_start, units_sold, est_demand, days_in_stock, received, expired_units FROM gold_store_product_week
        WHERE store_id = '{store_id}' AND product_name = '{product}' AND NOT is_partial_week ORDER BY week_start""").toPandas().to_string(index=False))
    for category in categories:
        parts.append(f"Weekly {category} at {store_id}:\n" + spark.sql(f"""
        SELECT week_start, SUM(units_sold) AS units_sold, ROUND(SUM(est_demand)) AS est_demand, SUM(stockout_days) AS stockout_days,
               SUM(expired_units) AS expired_units, ROUND(SUM(waste_value)) AS waste_usd FROM gold_store_product_week
        WHERE store_id = '{store_id}' AND category = '{category}' AND NOT is_partial_week GROUP BY week_start ORDER BY week_start""").toPandas().to_string(index=False))
    return "\n\n".join(parts)


def generate(question, facts, passages):
    from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
    context = "DATA (last 4 full weeks, from the gold tables):\n" + facts + "\n\nDOCUMENTS:\n" + "\n".join(
        f"[{p.doc_name}] {p.chunk_text}" for p in passages.itertuples())
    resp = w.serving_endpoints.query(name=LLM, max_tokens=400, temperature=0.0, messages=[
        ChatMessage(role=ChatMessageRole.SYSTEM, content=SYSTEM_PROMPT),
        ChatMessage(role=ChatMessageRole.USER, content=f"{context}\n\nQUESTION: {question}")])
    return resp.choices[0].message.content.strip()


DEMO_QUESTIONS = [  # question, store, the document a correct answer must retrieve
    ("Why does Coffee keep running out, and what can the store do about it?", "ST-07", "supplier_coffee.pdf"),
    ("Why is this store throwing away so much bread and fresh produce?", "ST-07", "visit_st07_2026_09.docx"),
    ("Did we receive the coffee we ordered for the week of 2026-10-12, and if not, why?", "ST-07", "delivery_note_st07_2026_10_12.png"),
    ("Can we get produce delivered twice a week, and what would it cost?", "ST-07", "supplier_greenfield_produce.md"),
    ("What happened to beverage sales in the week of 2026-09-07, and should we order more drinks from now on?", "ST-12", "visit_st12_2026_09.md"),
    ("What do customers say about plant-based products, and what should we do?", "ST-12", "memo_plant_based_2026_10.md"),
]
if IS_DATABRICKS:
    answers, checks = [], []
    for question, store_id, expected_doc in DEMO_QUESTIONS:
        passages = retrieve(question, store_id)
        facts = store_facts(store_id) + "\n\n" + question_facts(question, store_id)
        answers.append({"question": question, "store_id": store_id, "answer": generate(question, facts, passages),
                        "chunk_ids": passages.chunk_id.tolist(), "model": LLM})
        # QUALITY: retrieval check: the document that holds the answer must be in the top results
        checks.append({"question": question, "expected_doc": expected_doc, "retrieved_docs": passages.doc_name.tolist(),
                       "hit": expected_doc in passages.doc_name.tolist()})
    spark.createDataFrame(pd.DataFrame(checks)).write.mode("overwrite").option("overwriteSchema", True).saveAsTable("gold_rag_check")
    display(spark.table("gold_rag_check"))

# COMMAND ----------

# Weekly brief per store: the store's numbers plus the passages retrieved for its biggest issues
BRIEF_QUESTION = ("Write this store's weekly brief: the 2 biggest stockout problems and the 2 biggest waste problems, why they "
                  "happen according to the documents, and what to do next week.")
if IS_DATABRICKS:
    briefs = []
    for store_id in sorted(stores):
        stats = recent_product_stats(store_id)
        problems = list(stats[stats.stockout_days > 0].nlargest(2, "stockout_days").product_name) + list(stats.nlargest(2, "waste_usd").product_name)
        # One search per problem product, plus the store's own visit notes and feedback (filtered to this store only)
        found = [retrieve(f"Why does {p} run out or go to waste, and what can the store do?", store_id, k=2) for p in problems]
        found.append(w.vector_search_indexes.query_index(index_name=INDEX_NAME, num_results=2, query_text="store visit notes and customer feedback",
                     columns=["chunk_id", "doc_name", "doc_title", "section", "store_id", "chunk_text"],
                     filters_json=json.dumps({"store_id": store_id})).result.data_array or [])
        found[-1] = pd.DataFrame(found[-1], columns=["chunk_id", "doc_name", "doc_title", "section", "store_id", "chunk_text", "score"])
        passages = pd.concat(found, ignore_index=True).drop_duplicates("chunk_id")
        briefs.append({"store_id": store_id, "brief": generate(BRIEF_QUESTION, store_facts(store_id), passages),
                       "chunk_ids": passages.chunk_id.tolist(), "model": LLM})

# COMMAND ----------

# Write answers and briefs with their cited documents (joined to silver_doc_chunks in Spark, so lineage reaches the documents)
def with_sources(rows, keys):
    df = spark.createDataFrame(pd.DataFrame(rows))
    sources = (df.select(*keys, F.explode("chunk_ids").alias("chunk_id"))
               .join(spark.table("silver_doc_chunks").select("chunk_id", "doc_name"), "chunk_id")
               .groupBy(*keys).agg(F.sort_array(F.collect_set("doc_name")).alias("source_documents")))
    return df.join(sources, keys, "left").drop("chunk_ids").withColumn("generated_at", F.current_timestamp())


if IS_DATABRICKS:
    with_sources(answers, ["question", "store_id"]).write.mode("overwrite").option("overwriteSchema", True).saveAsTable("gold_doc_answers")
    (with_sources(briefs, ["store_id"]).join(spark.table("silver_stores").select("store_id", "store_name"), "store_id")
     .write.mode("overwrite").option("overwriteSchema", True).saveAsTable("gold_store_briefs"))
    display(spark.table("gold_doc_answers").select("store_id", "question", "answer", "source_documents"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 Checks and comments

# COMMAND ----------

# QUALITY: every document chunked, every chunk indexed, every demo question retrieved its source, every answer cites a document
if IS_DATABRICKS:
    display(spark.sql(f"""
    SELECT 'documents_chunked' AS check_name, CAST(COUNT(DISTINCT doc_name) AS DOUBLE) AS value, COUNT(DISTINCT doc_name) = {len(documents)} AS passed FROM silver_doc_chunks
    UNION ALL SELECT 'chunks_indexed', CAST({status.indexed_row_count or 0} AS DOUBLE), {status.indexed_row_count or 0} = (SELECT COUNT(*) FROM silver_doc_chunks)
    UNION ALL SELECT 'retrieval_hit_rate', AVG(CAST(hit AS DOUBLE)), AVG(CAST(hit AS DOUBLE)) = 1 FROM gold_rag_check
    UNION ALL SELECT 'answers_with_citation', AVG(CAST(answer RLIKE '\\\\[[A-Za-z0-9_.-]+' AS DOUBLE)), AVG(CAST(answer RLIKE '\\\\[[A-Za-z0-9_.-]+' AS DOUBLE)) = 1 FROM gold_doc_answers
    """))
for table, comment in {
    "silver_doc_chunks": "Grain: one section of one document. Text chunks with doc type, store and product metadata; source of the vector index.",
    "gold_doc_answers": "Grain: demo question x store. LLM answer grounded in gold numbers and retrieved document passages, with cited documents.",
    "gold_store_briefs": "Grain: store. Generated weekly brief from the store numbers and retrieved documents (regenerated each run, wording varies).",
}.items():
    spark.sql(f"COMMENT ON TABLE {table} IS '{comment}'")
