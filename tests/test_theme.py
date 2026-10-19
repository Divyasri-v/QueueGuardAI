"""
tests/test_theme.py
Unit tests for QueueGuard AI's theme system.

Tests cover:
  - DARK and LIGHT token dicts have all required keys
  - inject_css() runs without error for both themes and returns valid CSS markup
  - chart_layout() returns correct color values per theme
  - highlight_status() returns readable, distinct colors per theme
  - _T() helper returns the correct token dict based on session state
"""

import sys
import os
import types
import pytest
from unittest.mock import MagicMock

# Make the project root importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ──────────────────────────────────────────────────────────────────────────────
# Minimal session_state that supports both dict-style and attribute-style access
# (Streamlit's real SessionState does both; our mock must too)
# ──────────────────────────────────────────────────────────────────────────────

class _FakeSessionState(dict):
    """dict that also allows attribute-style read/write for session keys."""
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        self[name] = value

    def __delattr__(self, name):
        del self[name]


# ──────────────────────────────────────────────────────────────────────────────
# Helpers: mock streamlit so app.py can be imported without a running server
# ──────────────────────────────────────────────────────────────────────────────

def _make_fake_st():
    fake = types.SimpleNamespace(
        set_page_config=lambda **_: None,
        markdown=lambda *_, **__: None,
        session_state=_FakeSessionState(),
    )
    return fake


@pytest.fixture(autouse=True)
def mock_st(monkeypatch):
    """
    Stub out Streamlit so app.py can be imported in pytest without a server.
    Each test gets a fresh fake_st with its own session_state.
    """
    fake_st = _make_fake_st()
    monkeypatch.setitem(sys.modules, "streamlit", fake_st)

    # Stub heavy dependencies that app.py imports at module level
    for mod in [
        "pandas", "plotly", "plotly.express", "plotly.graph_objects",
        "streamlit_autorefresh",
    ]:
        if mod not in sys.modules:
            monkeypatch.setitem(sys.modules, mod, MagicMock())

    # Stub queueguard modules so importing app.py never hits Redis
    for mod in [
        "config",
        "queueguard",
        "queueguard.redis_connection",
        "queueguard.job_service",
        "queueguard.recovery",
        "queueguard.history",
    ]:
        fake_mod = MagicMock()
        if mod == "config":
            fake_mod.MAX_RETRIES   = 3
            fake_mod.QUEUE_NAME    = "queueguard"
            fake_mod.ALLOWED_TASKS = ["successful_task"]
        monkeypatch.setitem(sys.modules, mod, fake_mod)

    yield fake_st


# ──────────────────────────────────────────────────────────────────────────────
# Import the module under test (after stubs are in place)
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture()
def app(mock_st):
    """Import app module fresh for each test, so module-level code reruns."""
    if "app" in sys.modules:
        del sys.modules["app"]
    import app as _app
    return _app


# ──────────────────────────────────────────────────────────────────────────────
# Token dict completeness
# ──────────────────────────────────────────────────────────────────────────────

REQUIRED_KEYS = [
    "bg", "surface", "surface2", "border", "border2",
    "text", "text_strong", "muted",
    "accent", "accent2", "accent3",
    "sidebar_bg", "tab_bg", "tab_active_bg", "tab_active_fg", "tab_fg",
    "input_bg", "input_text", "shadow",
    "chart_paper", "chart_plot", "chart_font", "chart_grid",
    "metric_value", "metric_label",
    "badge_queued_bg",   "badge_queued_fg",
    "badge_started_bg",  "badge_started_fg",
    "badge_finished_bg", "badge_finished_fg",
    "badge_failed_bg",   "badge_failed_fg",
    "badge_retrying_bg", "badge_retrying_fg",
    "badge_dead_bg",     "badge_dead_fg",
    "sev_critical", "sev_high", "sev_medium", "sev_low",
    "btn_bg", "btn_fg", "divider", "empty_border",
    "hl_queued_bg",   "hl_queued_fg",
    "hl_started_bg",  "hl_started_fg",
    "hl_finished_bg", "hl_finished_fg",
    "hl_failed_bg",   "hl_failed_fg",
    "hl_retrying_bg", "hl_retrying_fg",
    "hl_dead_bg",     "hl_dead_fg",
]


class TestTokenDicts:
    def test_dark_has_all_required_keys(self, app):
        missing = [k for k in REQUIRED_KEYS if k not in app.DARK]
        assert not missing, f"DARK token dict missing keys: {missing}"

    def test_light_has_all_required_keys(self, app):
        missing = [k for k in REQUIRED_KEYS if k not in app.LIGHT]
        assert not missing, f"LIGHT token dict missing keys: {missing}"

    def test_dark_and_light_have_same_key_set(self, app):
        assert set(app.DARK.keys()) == set(app.LIGHT.keys()), (
            "DARK and LIGHT token dicts must have identical key sets"
        )

    def test_all_token_values_are_nonempty_strings(self, app):
        for name, tokens in [("DARK", app.DARK), ("LIGHT", app.LIGHT)]:
            for key, val in tokens.items():
                assert isinstance(val, str) and val.strip(), (
                    f"{name}['{key}'] must be a non-empty string, got {val!r}"
                )


# ──────────────────────────────────────────────────────────────────────────────
# _T() helper
# ──────────────────────────────────────────────────────────────────────────────

class TestTHelper:
    def test_returns_dark_when_theme_is_dark(self, app, mock_st):
        mock_st.session_state["theme"] = "dark"
        assert app._T() is app.DARK

    def test_returns_light_when_theme_is_light(self, app, mock_st):
        mock_st.session_state["theme"] = "light"
        assert app._T() is app.LIGHT

    def test_returns_dark_by_default_when_key_missing(self, app, mock_st):
        mock_st.session_state.clear()
        # _T() uses .get() which defaults to "dark"
        assert app._T() is app.DARK


# ──────────────────────────────────────────────────────────────────────────────
# inject_css()
# ──────────────────────────────────────────────────────────────────────────────

class TestInjectCSS:
    def _capture_css(self, app, mock_st, theme: str) -> str:
        mock_st.session_state["theme"] = theme
        captured: list[str] = []
        mock_st.markdown = lambda html, **kw: captured.append(html)
        app.inject_css()
        assert captured, "inject_css must call st.markdown at least once"
        return captured[0]

    def test_inject_css_calls_st_markdown(self, app, mock_st):
        calls: list = []
        mock_st.session_state["theme"] = "dark"
        mock_st.markdown = lambda *a, **kw: calls.append(a)
        app.inject_css()
        assert len(calls) == 1, "inject_css should call st.markdown exactly once"

    def test_inject_css_dark_contains_dark_bg_token(self, app, mock_st):
        css = self._capture_css(app, mock_st, "dark")
        assert "0f0f1a" in css, "Dark CSS must reference the dark background color"

    def test_inject_css_light_contains_light_bg_token(self, app, mock_st):
        css = self._capture_css(app, mock_st, "light")
        assert "f1f5f9" in css, "Light CSS must reference the light background color"

    def test_inject_css_contains_style_tag(self, app, mock_st):
        for theme in ("dark", "light"):
            css = self._capture_css(app, mock_st, theme)
            assert "<style>" in css, f"inject_css must emit <style> tag for {theme}"

    def test_inject_css_contains_inter_font(self, app, mock_st):
        for theme in ("dark", "light"):
            css = self._capture_css(app, mock_st, theme)
            assert "Inter" in css, f"CSS must include Inter font for {theme}"

    def test_dark_and_light_css_differ(self, app, mock_st):
        dark_css  = self._capture_css(app, mock_st, "dark")
        light_css = self._capture_css(app, mock_st, "light")
        assert dark_css != light_css, "Dark and light CSS must produce different output"


# ──────────────────────────────────────────────────────────────────────────────
# chart_layout()
# ──────────────────────────────────────────────────────────────────────────────

class TestChartLayout:
    def test_returns_dict(self, app, mock_st):
        mock_st.session_state["theme"] = "dark"
        assert isinstance(app.chart_layout(), dict)

    def test_dark_chart_uses_dark_font(self, app, mock_st):
        mock_st.session_state["theme"] = "dark"
        layout = app.chart_layout()
        assert layout["font"]["color"] == app.DARK["chart_font"]

    def test_light_chart_uses_light_font(self, app, mock_st):
        mock_st.session_state["theme"] = "light"
        layout = app.chart_layout()
        assert layout["font"]["color"] == app.LIGHT["chart_font"]

    def test_dark_chart_transparent_paper(self, app, mock_st):
        mock_st.session_state["theme"] = "dark"
        layout = app.chart_layout()
        assert layout["paper_bgcolor"] == app.DARK["chart_paper"]

    def test_light_chart_has_non_transparent_plot_bg(self, app, mock_st):
        mock_st.session_state["theme"] = "light"
        layout = app.chart_layout()
        # Light plot_bgcolor should be something light, not fully transparent
        assert layout["plot_bgcolor"] != "rgba(0,0,0,0)"

    def test_extra_keys_merged(self, app, mock_st):
        mock_st.session_state["theme"] = "dark"
        layout = app.chart_layout({"showlegend": False, "title": "Test"})
        assert layout["showlegend"] is False
        assert layout["title"] == "Test"

    def test_has_xaxis_and_yaxis(self, app, mock_st):
        mock_st.session_state["theme"] = "dark"
        layout = app.chart_layout()
        assert "xaxis" in layout and "yaxis" in layout

    def test_xaxis_gridcolor_matches_theme(self, app, mock_st):
        for theme, tokens in [("dark", app.DARK), ("light", app.LIGHT)]:
            mock_st.session_state["theme"] = theme
            layout = app.chart_layout()
            assert layout["xaxis"]["gridcolor"] == tokens["chart_grid"], (
                f"xaxis gridcolor must match chart_grid token for {theme}"
            )


# ──────────────────────────────────────────────────────────────────────────────
# highlight_status()
# ──────────────────────────────────────────────────────────────────────────────

class TestHighlightStatus:
    STATUSES = ["queued", "started", "finished", "failed", "retrying", "dead"]

    def test_returns_nonempty_string_for_all_statuses_dark(self, app, mock_st):
        mock_st.session_state["theme"] = "dark"
        for status in self.STATUSES:
            result = app.highlight_status(status)
            assert isinstance(result, str) and result, (
                f"highlight_status('{status}') must return non-empty CSS string in dark mode"
            )

    def test_returns_nonempty_string_for_all_statuses_light(self, app, mock_st):
        mock_st.session_state["theme"] = "light"
        for status in self.STATUSES:
            result = app.highlight_status(status)
            assert isinstance(result, str) and result, (
                f"highlight_status('{status}') must return non-empty CSS string in light mode"
            )

    def test_returns_empty_string_for_unknown_status(self, app, mock_st):
        mock_st.session_state["theme"] = "dark"
        assert app.highlight_status("unknown_xyz") == ""

    def test_dark_and_light_produce_different_css_for_finished(self, app, mock_st):
        mock_st.session_state["theme"] = "dark"
        dark_css = app.highlight_status("finished")
        mock_st.session_state["theme"] = "light"
        light_css = app.highlight_status("finished")
        assert dark_css != light_css, (
            "Dark and light highlight_status must produce different CSS strings"
        )

    def test_css_contains_background_color(self, app, mock_st):
        mock_st.session_state["theme"] = "dark"
        for status in self.STATUSES:
            css = app.highlight_status(status)
            assert "background-color" in css, (
                f"highlight_status('{status}') must set background-color"
            )

    def test_css_contains_color(self, app, mock_st):
        mock_st.session_state["theme"] = "dark"
        for status in self.STATUSES:
            css = app.highlight_status(status)
            assert "color:" in css, (
                f"highlight_status('{status}') must set foreground color"
            )
