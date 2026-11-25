"""Ticket analytics: a small read-only Databricks App (Streamlit).   streamlit run app.py

Shows the pieces the pipeline built: earnings by sale type, closed sales by plan length, be-backs, conversion with and without a price
drop, identity-stitching quality, and the highest-scoring visitors from the purchase-propensity model. Every number comes from an
audited gold table or a score table; nothing is computed in the app.
"""
import streamlit as st

import data

st.set_page_config(page_title="Ticket analytics", layout="wide")
st.title("Ticket analytics")
st.caption("Source: audited gold tables. Synthetic data; Free Edition is for non-commercial use.")


@st.cache_data(ttl=600)
def load(name):
    return data.run_query(name, data.warehouse_runner())


def show(title, name, note=""):
    st.subheader(title)
    if note:
        st.caption(note)
    try:
        df = load(name)
        st.dataframe(df, use_container_width=True)
        return df
    except Exception as e:  # noqa: BLE001
        st.warning(f"Could not load {name}: {str(e)[:200]}")


earn = show("Earnings by sale type", "earnings_by_sale_type", "Fee revenue on every transfer, plus profit on tickets the company bought back and resold.")
if earn is not None and len(earn):
    k = data.kpis(earn)
    a, b, c = st.columns(3)
    a.metric("Fee revenue", f"{k['fee_revenue']:,.2f}")
    b.metric("Owned-inventory profit", f"{k['owned_inventory_profit']:,.2f}")
    c.metric("Total earnings", f"{k['total_earnings']:,.2f}")
left, right = st.columns(2)
with left:
    show("Closed sales by plan length", "closed_sales_by_plan_length", "A sale closes when net captured payments reach 80 percent of the order total.")
    show("Be-backs before purchase", "be_backs", "Return visits to the event page within 30 days before buying.")
with right:
    show("Conversion and price drops", "conversion_by_price_drop", "A signal, not proof: compare buyers with visitors who left.")
    show("How sessions were attributed to people", "identity_methods")
show("Most likely to buy (still undecided)", "top_propensity", "Visitors whose 30-day outcome is not known yet, ranked by model score.")
show("Model runs", "model_runs")
