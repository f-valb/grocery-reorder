"""Critical items: what each store manager should act on this week, ranked by dollars at stake.

Reads the gold data product as the signed-in viewer (user authorization), so a store manager sees only their store.
Run locally: DATABRICKS_CONFIG_PROFILE=food streamlit run apps/critical_items/app.py (after deploy_apps.py --stage).
"""

import pandas as pd
import streamlit as st

import grocery_data as data

st.set_page_config(page_title="Critical items", page_icon="🛒", layout="wide")


# Cache per viewer and store for 10 minutes: gold changes once a week, and every rerun would hit the warehouse
@st.cache_data(ttl=600, show_spinner=False)
def load(viewer, store_id):
    return data.critical_items(store_id), data.kpis(store_id), data.reorder_list(store_id), data.data_health()


@st.cache_data(ttl=600, show_spinner=False)
def load_stores(viewer):
    return data.stores()


viewer = data.current_user()
stores = load_stores(viewer)
if stores.empty:
    st.error(f"No stores are visible to {viewer}. Ask head office to add you to the store manager list.")
    st.stop()

# Store picker: the options are only the stores the row filter lets this viewer see
options = (["ALL"] if len(stores) > 1 else []) + stores.store_id.tolist()
labels = {"ALL": "All stores", **dict(zip(stores.store_id, stores.store_name))}
with st.sidebar:
    st.header("Critical items")
    store_id = st.selectbox("Store", options, format_func=labels.get)
    severities = st.multiselect("Severity", ["High", "Medium"], default=["High", "Medium"])
    issues = st.multiselect("Problem", ["Stockouts", "Waste", "Short next week"],
                            default=["Stockouts", "Waste", "Short next week"])
    st.caption(f"Signed in as {viewer}")

with st.spinner("Loading the latest gold data..."):
    critical, kpi, reorder, health = load(viewer, store_id)

st.title(f"Critical items: {labels[store_id]}")
st.caption("Last 4 complete weeks, plus next week's order plan. An item is critical when at least $100 is at stake.")

# Headline KPIs for the last 4 complete weeks
c1, c2, c3, c4 = st.columns(4)
c1.metric("Shelf availability", f"{kpi.availability_pct:.1f}%")
c2.metric("Lost sales (stockouts)", f"${kpi.lost_sales_usd:,.0f}")
c3.metric("Waste at cost", f"${kpi.waste_usd:,.0f}")
c4.metric("Critical items", f"{len(critical)}", f"{(critical.severity == 'High').sum()} high", delta_color="inverse")

shown = critical[critical.severity.isin(severities) & critical.issue.isin(issues)]
tab_items, tab_chart, tab_orders = st.tabs(["Act on these", "Where the money is", "Next week's orders"])

with tab_items:
    st.dataframe(
        shown[["severity", "store_id", "product_name", "category", "issue", "detail", "impact_usd", "next_week_order_cases"]],
        hide_index=True, use_container_width=True,
        column_config={"impact_usd": st.column_config.NumberColumn("At stake", format="$%d"),
                       "next_week_order_cases": st.column_config.NumberColumn("Order next week (cases)"),
                       "product_name": "Product", "store_id": "Store", "issue": "Problem", "detail": "Why"})
    st.download_button("Download as CSV", shown.to_csv(index=False), "critical_items.csv", "text/csv")

with tab_chart:
    left, right = st.columns(2)
    by_category = shown.pivot_table(index="category", columns="issue", values="impact_usd", aggfunc="sum", fill_value=0)
    left.subheader("Dollars at stake by category")
    left.bar_chart(by_category, horizontal=True)
    if store_id == "ALL":
        right.subheader("Dollars at stake by store")
        right.bar_chart(shown.pivot_table(index="store_id", columns="issue", values="impact_usd", aggfunc="sum", fill_value=0))
    else:
        right.subheader("Top 5 items")
        right.bar_chart(shown.groupby("product_name").impact_usd.sum().nlargest(5), horizontal=True)

with tab_orders:
    st.caption("Model forecast with its 80% range; order = forecast + safety stock - stock on arrival, in whole cases.")
    st.dataframe(reorder, hide_index=True, use_container_width=True,
                 column_config={"order_value": st.column_config.NumberColumn("Order value", format="$%.2f")})

# Data health strip: can I trust these numbers?
st.divider()
h1, h2, h3, h4 = st.columns(4)
h1.metric("Data refreshed", pd.Timestamp(health.silver_refreshed_at).strftime("%d %b %H:%M UTC"))
h2.metric("Rows quarantined", f"{health.quarantined_rows:,}")
h3.metric("Forecast model", f"v{health.model_version} (champion)")
h4.metric("Quality checks passed", health.quality_checks)
st.caption(f"Model gate: {health.model_gate}")
