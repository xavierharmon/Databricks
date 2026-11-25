"""Run the redesigned app headless in demo mode and check it builds every chart without an error."""
import os
import sys
from pathlib import Path

import pytest

pytest.importorskip("streamlit")
pytest.importorskip("altair")
from streamlit.testing.v1 import AppTest  # noqa: E402

APP = Path(__file__).resolve().parents[1] / "app"


def run(monkeypatch, demo=True):
    import streamlit as st
    st.cache_data.clear()                            # otherwise demo frames cached by one test leak into the next
    monkeypatch.syspath_prepend(str(APP))
    if demo:
        monkeypatch.setenv("TIX_DEMO", "1")
    else:
        monkeypatch.delenv("TIX_DEMO", raising=False)
    at = AppTest.from_file(str(APP / "app.py"), default_timeout=60)
    at.run()
    return at


def test_demo_mode_renders_every_panel_without_an_exception(monkeypatch):
    at = run(monkeypatch)
    assert not at.exception, [e.value for e in at.exception]
    assert len(at.metric) >= 5 and len(at.tabs) == 4
    charts = at.get("vega_lite_chart") or at.get("arrow_vega_lite_chart")        # the element's name differs between Streamlit versions
    assert len(charts) >= 9, len(charts)


def test_without_data_the_app_warns_instead_of_crashing(monkeypatch):
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    at = run(monkeypatch, demo=False)               # no warehouse: every panel must degrade to a warning
    assert not at.exception, [e.value for e in at.exception]
    assert len(at.warning) >= 1
