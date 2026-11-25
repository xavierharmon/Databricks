"""Demo data for previewing the app with no Databricks connection:   TIX_DEMO=1 streamlit run app.py

The numbers are invented and only shaped like the real query results. They exist so you can judge the layout, and so the app can be tested
without a warehouse. Never present them as results."""
import json

import pandas as pd

FRAMES = {
    "earnings_by_sale_type": pd.DataFrame({"sale_type": ["primary", "resale", "company_resale", "buyback"], "transfers": [1450, 497, 157, 215],
                                           "gross_sales": [182000.0, 44863.55, 13270.5, 14383.84], "fee_revenue": [16200.0, 8669.29, 1765.6, 0.0],
                                           "owned_inventory_profit": [0.0, 0.0, 3112.75, 0.0]}),
    "sales_by_week": pd.DataFrame({"week": pd.date_range("2026-08-03", periods=14, freq="W-MON"),
                                   "transfers": [60, 85, 110, 140, 150, 170, 185, 190, 200, 210, 220, 230, 215, 148],
                                   "gross_sales": [5200, 7300, 9800, 12500, 13300, 15100, 16600, 17000, 18200, 19000, 19800, 21000, 19500, 13000],
                                   "fee_revenue": [560, 790, 1050, 1350, 1430, 1630, 1790, 1830, 1960, 2050, 2140, 2260, 2100, 1400]}),
    "sales_by_category": pd.DataFrame({"event_category": ["sport", "concert"], "transfers": [1900, 419], "gross_sales": [205000.0, 49500.0], "fee_revenue": [22000.0, 4846.0]}),
    "closed_sales_by_plan_length": pd.DataFrame({"num_payments": [5, 6, 7, 8, 9, 10], "orders": [60, 52, 48, 50, 45, 45], "closed_now": [31, 22, 18, 15, 11, 10]}),
    "be_backs": pd.DataFrame({"be_backs": [0, 1, 2, 3, 4, 5], "purchases": [628, 290, 130, 40, 20, 8]}),
    "conversion_by_price_drop": pd.DataFrame({"exposure": ["no price drop", "saw a price drop"], "journeys": [900, 600], "conversion_rate": [0.61, 0.83]}),
    "identity_methods": pd.DataFrame({"method": ["direct_t1", "retroactive_same_browser_id", "direct_t2", "fingerprint_after_cookie_reset", "bot", "shared_device_direct_t1",
                                                 "unresolved", "shared_device_direct_t2", "shared_device_ambiguous"],
                                      "sessions": [1344, 885, 616, 153, 138, 39, 32, 18, 16]}),
    "top_propensity": pd.DataFrame({"journey_key": [f"PER{100 + i:05d}|E{10 + i:04d}" for i in range(12)], "score": [0.93, 0.9, 0.88, 0.86, 0.83, 0.81, 0.79, 0.77, 0.74, 0.72, 0.7, 0.68],
                                    "event_category": ["sport"] * 9 + ["concert"] * 3, "sport": ["baseball"] * 5 + ["football"] * 4 + ["none"] * 3, "days_to_event": [12.5, 30.1, 8.2, 44.0, 21.3, 9.9, 15.0, 60.2, 5.5, 33.1, 18.8, 27.4]}),
    "model_runs": pd.DataFrame({"model_name": ["purchase_propensity", "installment_risk"], "trained_at": pd.to_datetime(["2026-11-25 10:00", "2026-11-25 10:00"]),
                                "metrics": [json.dumps({"auc": 0.784, "baseline_auc": 0.693, "top_decile_rate": 0.738, "base_rate": 0.415, "n_train": 1000, "n_test": 429, "model_kind": "boosted trees"}),
                                            json.dumps({"auc": 0.72, "baseline_auc": 0.725, "top_decile_rate": 0.45, "base_rate": 0.3, "n_train": 176, "n_test": 76, "model_kind": "logistic"})]}),
}
