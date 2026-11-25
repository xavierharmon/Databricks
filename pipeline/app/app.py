"""Ticket analytics: a read-only Databricks App (Streamlit) with charts.   streamlit run app.py      (preview without data: TIX_DEMO=1)

Four tabs: Sales, Installments, Visitors, Models. Every number comes from an audited gold table or a score table; nothing is computed here
except display shaping. Panels that cannot load show a short warning naming the query, never a crash.
"""
import json
import os

import altair as alt
import pandas as pd
import streamlit as st

import data

st.set_page_config(page_title="Ticket analytics", page_icon=":ticket:", layout="wide")
INK, RED, BLUE, TEAL, GOLD, GREY = "#1F2933", "#E4572E", "#2F6DB5", "#2A9D8F", "#E9B44C", "#9AA5B1"
st.markdown("""<style>
.block-container {padding-top: 1.4rem; max-width: 1250px;}
[data-testid="stMetric"] {background: #F3F5F9; border-radius: 12px; padding: 14px 18px;}
[data-testid="stMetricValue"] {font-size: 1.7rem;}
h1, h2, h3 {letter-spacing: -0.01em;}
.caption {color: #6B7785; font-size: 0.9rem; margin-top: -0.4rem; margin-bottom: 0.6rem;}
</style>""", unsafe_allow_html=True)


@st.cache_data(ttl=600)
def load(name):
    if os.environ.get("TIX_DEMO"):
        import demo
        return demo.FRAMES[name].copy()
    return data.run_query(name, data.warehouse_runner())


def get(name):
    try:
        df = load(name)
        return df if df is not None and len(df) else None
    except Exception as e:  # noqa: BLE001
        st.warning(f"Could not load **{name}**: {str(e)[:160]}")
        return None


def num(df, *cols):
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def money(v):
    return f"{v:,.0f}" if abs(v) >= 100000 else f"{v:,.2f}"


def show(chart):
    """Draw a chart at full width. Streamlit renamed the option (use_container_width became width='stretch'), so try the new one first."""
    try:
        st.altair_chart(chart, width="stretch")
    except TypeError:
        st.altair_chart(chart, use_container_width=True)


def title(text, note=""):
    st.markdown(f"### {text}")
    if note:
        st.markdown(f'<div class="caption">{note}</div>', unsafe_allow_html=True)


# ------------------------------------------------------------------------------------------------ header and headline numbers
st.title(":ticket: Ticket analytics")
st.markdown('<div class="caption">Audited gold tables on Databricks. Synthetic data, non-commercial use.</div>', unsafe_allow_html=True)
if os.environ.get("TIX_DEMO"):
    st.info("Demo mode: the numbers below are invented to preview the layout.")

earn, plans, journeys = get("earnings_by_sale_type"), get("closed_sales_by_plan_length"), get("conversion_by_price_drop")
k = st.columns(5)
if earn is not None:
    num(earn, "transfers", "gross_sales", "fee_revenue", "owned_inventory_profit")
    kp = data.kpis(earn)
    k[0].metric("Fee revenue", money(kp["fee_revenue"]))
    k[1].metric("Owned-inventory profit", money(kp["owned_inventory_profit"]))
    k[2].metric("Total earnings", money(kp["total_earnings"]))
if plans is not None:
    num(plans, "num_payments", "orders", "closed_now")
    k[3].metric("Plans closed (80% paid)", f"{int(plans['closed_now'].sum())} of {int(plans['orders'].sum())}")
if journeys is not None:
    num(journeys, "journeys", "conversion_rate")
    total = journeys["journeys"].sum()
    k[4].metric("Visitor conversion", f"{(journeys['conversion_rate'] * journeys['journeys']).sum() / total:.0%}" if total else "n/a")

tab_sales, tab_plans, tab_visitors, tab_models = st.tabs(["Sales", "Installments", "Visitors", "Models"])

# ------------------------------------------------------------------------------------------------ sales
with tab_sales:
    left, right = st.columns([3, 2])
    week = get("sales_by_week")
    with left:
        title("Weekly sales", "Gross ticket sales and the fees earned on them, by week.")
        if week is not None:
            num(week, "gross_sales", "fee_revenue", "transfers")
            week["week"] = pd.to_datetime(week["week"])
            long = week.melt("week", value_vars=["gross_sales", "fee_revenue"], var_name="measure", value_name="amount")
            long["measure"] = long["measure"].map({"gross_sales": "Gross sales", "fee_revenue": "Fee revenue"})
            show(alt.Chart(long).mark_area(opacity=0.35, line=True).encode(
                x=alt.X("week:T", title=None), y=alt.Y("amount:Q", title=None), color=alt.Color("measure:N", scale=alt.Scale(range=[BLUE, RED]), legend=alt.Legend(orient="top", title=None)),
                tooltip=["week:T", "measure:N", alt.Tooltip("amount:Q", format=",.0f")]).properties(height=300))
    with right:
        title("Where the money comes from", "Fee revenue and owned-inventory profit by sale type.")
        if earn is not None:
            mix = earn.melt("sale_type", value_vars=["fee_revenue", "owned_inventory_profit"], var_name="source", value_name="amount")
            mix["source"] = mix["source"].map({"fee_revenue": "Fees", "owned_inventory_profit": "Profit on bought-back tickets"})
            show(alt.Chart(mix[mix["amount"] > 0]).mark_bar(cornerRadiusEnd=4).encode(
                y=alt.Y("sale_type:N", sort="-x", title=None), x=alt.X("amount:Q", title=None), color=alt.Color("source:N", scale=alt.Scale(range=[RED, TEAL]), legend=alt.Legend(orient="top", title=None)),
                tooltip=["sale_type:N", "source:N", alt.Tooltip("amount:Q", format=",.2f")]).properties(height=300))
    a, b = st.columns(2)
    with a:
        title("Transfers by sale type")
        if earn is not None:
            show(alt.Chart(earn).mark_arc(innerRadius=70).encode(
                theta="transfers:Q", color=alt.Color("sale_type:N", scale=alt.Scale(range=[BLUE, RED, TEAL, GOLD]), legend=alt.Legend(orient="right", title=None)),
                tooltip=["sale_type:N", "transfers:Q"]).properties(height=260))
    with b:
        title("Sports and special events")
        cat = get("sales_by_category")
        if cat is not None:
            num(cat, "gross_sales", "fee_revenue", "transfers")
            show(alt.Chart(cat).mark_bar(cornerRadiusEnd=4, size=38).encode(
                y=alt.Y("event_category:N", title=None), x=alt.X("gross_sales:Q", title="Gross sales"), color=alt.Color("event_category:N", scale=alt.Scale(range=[BLUE, GOLD]), legend=None),
                tooltip=["event_category:N", alt.Tooltip("gross_sales:Q", format=",.0f"), alt.Tooltip("fee_revenue:Q", format=",.0f"), "transfers:Q"]).properties(height=260))

# ------------------------------------------------------------------------------------------------ installments
with tab_plans:
    title("Closed sales by plan length", "A sale closes once net captured payments reach 80 percent of its total. Longer plans close less often.")
    if plans is not None:
        plans["closed_rate"] = plans["closed_now"] / plans["orders"]
        base = alt.Chart(plans).encode(x=alt.X("num_payments:O", title="Payments in the plan"))
        bars = base.mark_bar(cornerRadiusEnd=4, color=BLUE).encode(y=alt.Y("orders:Q", title="Orders"), tooltip=["num_payments:O", "orders:Q", "closed_now:Q", alt.Tooltip("closed_rate:Q", format=".0%")])
        line = base.mark_line(color=RED, point=alt.OverlayMarkDef(color=RED, size=80)).encode(y=alt.Y("closed_rate:Q", title="Closed rate", axis=alt.Axis(format="%", orient="right")))
        show(alt.layer(bars, line).resolve_scale(y="independent").properties(height=340))
        st.caption("Bars: orders on that plan length. Line: share of them closed now, after refunds.")

# ------------------------------------------------------------------------------------------------ visitors
with tab_visitors:
    a, b = st.columns(2)
    with a:
        title("Return visits before buying", "Be-backs: earlier visits to the event page in the 30 days before the purchase.")
        bb = get("be_backs")
        if bb is not None:
            num(bb, "be_backs", "purchases")
            show(alt.Chart(bb).mark_bar(cornerRadiusEnd=4, color=TEAL).encode(
                x=alt.X("be_backs:O", title="Return visits"), y=alt.Y("purchases:Q", title="Purchases"), tooltip=["be_backs:O", "purchases:Q"]).properties(height=300))
    with b:
        title("Did a price drop matter?", "A signal, not proof: compare visitors who bought with those who left.")
        if journeys is not None:
            show(alt.Chart(journeys).mark_bar(cornerRadiusEnd=4, size=60).encode(
                x=alt.X("exposure:N", title=None, axis=alt.Axis(labelAngle=0)), y=alt.Y("conversion_rate:Q", title="Conversion rate", axis=alt.Axis(format="%")),
                color=alt.Color("exposure:N", scale=alt.Scale(range=[GREY, RED]), legend=None), tooltip=["exposure:N", "journeys:Q", alt.Tooltip("conversion_rate:Q", format=".1%")]).properties(height=300))
    title("How visits were tied to people", "Strongest evidence first. Shared devices and bots are never guessed.")
    ident = get("identity_methods")
    if ident is not None:
        num(ident, "sessions")
        show(alt.Chart(ident).mark_bar(cornerRadiusEnd=4, color=BLUE).encode(
            y=alt.Y("method:N", sort="-x", title=None), x=alt.X("sessions:Q", title="Sessions"), tooltip=["method:N", "sessions:Q"]).properties(height=300))

# ------------------------------------------------------------------------------------------------ models
with tab_models:
    runs = get("model_runs")
    title("How good are the models?", "AUC measures ranking quality: 0.5 is a coin flip, 1.0 is perfect. Each model must beat or match a one-line rule.")
    if runs is not None:
        cols = st.columns(max(1, len(runs)))
        for col, (_, r) in zip(cols, runs.iterrows()):
            try:
                m = json.loads(r["metrics"])
            except Exception:  # noqa: BLE001
                m = {}
            with col:
                st.markdown(f"**{str(r['model_name']).replace('_', ' ').title()}**")
                if "auc" in m:
                    st.metric("AUC", f"{m['auc']:.3f}", delta=f"{m['auc'] - m['baseline_auc']:+.3f} vs baseline rule")
                    st.caption(f"{m.get('model_kind', 'model')} - trained on {m.get('n_train', '?')} rows, tested on {m.get('n_test', '?')} later rows. "
                               f"Top 10% of scores convert {m.get('top_decile_rate', 0):.0%} against a {m.get('base_rate', 0):.0%} base rate.")
                else:
                    st.caption(f"Skipped: {m.get('skipped', 'no metrics')}")
    top = get("top_propensity")
    title("Most likely to buy next", "Visitors whose 30-day outcome is not known yet, ranked by model score.")
    if top is not None:
        num(top, "score", "days_to_event")
        show(alt.Chart(top).mark_bar(cornerRadiusEnd=4).encode(
            y=alt.Y("journey_key:N", sort="-x", title=None), x=alt.X("score:Q", title="Score", scale=alt.Scale(domain=[0, 1])),
            color=alt.Color("event_category:N", scale=alt.Scale(range=[BLUE, GOLD]), legend=alt.Legend(orient="top", title=None)),
            tooltip=["journey_key:N", alt.Tooltip("score:Q", format=".2f"), "event_category:N", "sport:N", alt.Tooltip("days_to_event:Q", format=".0f")]).properties(height=max(220, 26 * len(top))))
