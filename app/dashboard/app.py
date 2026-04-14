import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Job Market Intelligence",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Design system ──────────────────────────────────────────────────────────────
C = {
    "bg":           "#0A0C10",
    "surface":      "#111318",
    "surface2":     "#181B22",
    "surface3":     "#1E2128",
    "border":       "#2A2D35",
    "border2":      "#363A44",
    "text":         "#F0F2F7",
    "text2":        "#9299A8",
    "text3":        "#555C6B",
    "accent":       "#4EFAC4",        # teal-green
    "accent2":      "#7B8CFF",        # periwinkle
    "accent3":      "#FF6B6B",        # coral
    "remote":       "#4EFAC4",
    "hybrid":       "#7B8CFF",
    "onsite":       "#FF6B6B",
    "gold":         "#F5C842",
}

FONT = "'Syne', 'IBM Plex Sans', sans-serif"
MONO = "'IBM Plex Mono', monospace"

# ── Global CSS ─────────────────────────────────────────────────────────────────
st.markdown(f"""
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=IBM+Plex+Sans:wght@300;400;500&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">

<style>
/* ── Reset & base ── */
*, *::before, *::after {{ box-sizing: border-box; }}

html, body, [data-testid="stAppViewContainer"], [data-testid="stApp"] {{
    background: {C['bg']} !important;
    color: {C['text']} !important;
    font-family: {FONT} !important;
}}

[data-testid="stAppViewContainer"] > .main {{
    background: {C['bg']} !important;
}}

.main .block-container {{
    padding: 2rem 2.5rem 4rem !important;
    max-width: 1400px !important;
}}

/* ── Sidebar ── */
[data-testid="stSidebar"] {{
    background: {C['surface']} !important;
    border-right: 1px solid {C['border']} !important;
}}
[data-testid="stSidebar"] * {{
    color: {C['text']} !important;
    font-family: {FONT} !important;
}}
[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] .stMultiSelect label {{
    color: {C['text2']} !important;
    font-size: 11px !important;
    text-transform: uppercase !important;
    letter-spacing: 0.08em !important;
    font-family: {MONO} !important;
}}

/* Sidebar select boxes */
[data-testid="stSidebar"] .stSelectbox > div > div,
[data-testid="stSidebar"] .stMultiSelect > div > div {{
    background: {C['surface2']} !important;
    border: 1px solid {C['border2']} !important;
    border-radius: 6px !important;
    color: {C['text']} !important;
}}

/* ── Metrics ── */
[data-testid="stMetric"] {{
    background: {C['surface']} !important;
    border: 1px solid {C['border']} !important;
    border-radius: 12px !important;
    padding: 1.1rem 1.3rem !important;
    position: relative !important;
    overflow: hidden !important;
}}
[data-testid="stMetric"]::before {{
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, {C['accent']}, {C['accent2']});
}}
[data-testid="stMetricLabel"] > div {{
    color: {C['text3']} !important;
    font-size: 10px !important;
    font-family: {MONO} !important;
    text-transform: uppercase !important;
    letter-spacing: 0.1em !important;
    font-weight: 500 !important;
}}
[data-testid="stMetricValue"] > div {{
    color: {C['text']} !important;
    font-family: {FONT} !important;
    font-size: 1.7rem !important;
    font-weight: 700 !important;
    letter-spacing: -0.02em !important;
}}
[data-testid="stMetricDelta"] > div {{
    font-size: 11px !important;
    font-family: {MONO} !important;
}}

/* ── Tabs ── */
[data-testid="stTabs"] [role="tablist"] {{
    background: {C['surface']} !important;
    border-radius: 10px !important;
    padding: 4px !important;
    gap: 2px !important;
    border: 1px solid {C['border']} !important;
    width: fit-content !important;
}}
[data-testid="stTabs"] [role="tab"] {{
    background: transparent !important;
    color: {C['text2']} !important;
    font-family: {MONO} !important;
    font-size: 12px !important;
    border-radius: 7px !important;
    padding: 6px 16px !important;
    border: none !important;
    letter-spacing: 0.04em !important;
}}
[data-testid="stTabs"] [role="tab"][aria-selected="true"] {{
    background: {C['accent']} !important;
    color: {C['bg']} !important;
    font-weight: 600 !important;
}}

/* ── Dataframe / table ── */
[data-testid="stDataFrame"] {{
    border: 1px solid {C['border']} !important;
    border-radius: 10px !important;
    overflow: hidden !important;
}}

/* ── Dividers ── */
hr {{
    border: none !important;
    border-top: 1px solid {C['border']} !important;
    margin: 1.5rem 0 !important;
}}

/* ── Section headers ── */
.section-header {{
    font-family: {FONT};
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.12em;
    color: {C['text3']};
    margin: 2rem 0 0.8rem;
    display: flex;
    align-items: center;
    gap: 10px;
}}
.section-header::after {{
    content: '';
    flex: 1;
    height: 1px;
    background: {C['border']};
}}

/* ── KPI accent card ── */
.kpi-accent {{
    background: linear-gradient(135deg, {C['accent']}18, {C['accent2']}10);
    border: 1px solid {C['accent']}40;
    border-radius: 12px;
    padding: 1.1rem 1.3rem;
    position: relative;
    overflow: hidden;
}}
.kpi-accent::before {{
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: {C['accent']};
}}
.kpi-accent .label {{
    font-family: {MONO};
    font-size: 10px;
    color: {C['accent']};
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-bottom: 6px;
}}
.kpi-accent .value {{
    font-family: {FONT};
    font-size: 1.8rem;
    font-weight: 800;
    color: {C['accent']};
    letter-spacing: -0.03em;
    line-height: 1;
}}
.kpi-accent .sub {{
    font-family: {MONO};
    font-size: 11px;
    color: {C['text3']};
    margin-top: 4px;
}}

/* ── Hero header ── */
.hero {{
    padding: 2rem 0 1.5rem;
    border-bottom: 1px solid {C['border']};
    margin-bottom: 1.5rem;
}}
.hero-label {{
    font-family: {MONO};
    font-size: 11px;
    color: {C['accent']};
    text-transform: uppercase;
    letter-spacing: 0.15em;
    margin-bottom: 0.5rem;
}}
.hero-title {{
    font-family: {FONT};
    font-size: 2.6rem;
    font-weight: 800;
    color: {C['text']};
    letter-spacing: -0.04em;
    line-height: 1.1;
    margin-bottom: 0.4rem;
}}
.hero-title span {{
    color: {C['accent']};
}}
.hero-sub {{
    font-family: {FONT};
    font-size: 14px;
    font-weight: 300;
    color: {C['text2']};
    letter-spacing: 0.01em;
}}

/* ── Chart card ── */
.chart-card {{
    background: {C['surface']};
    border: 1px solid {C['border']};
    border-radius: 12px;
    padding: 1.2rem 1.4rem;
    margin-bottom: 1rem;
}}
.chart-title {{
    font-family: {FONT};
    font-size: 13px;
    font-weight: 600;
    color: {C['text']};
    margin-bottom: 2px;
    letter-spacing: -0.01em;
}}
.chart-sub {{
    font-family: {MONO};
    font-size: 10px;
    color: {C['text3']};
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-bottom: 0.8rem;
}}

/* ── Insight pill ── */
.insight {{
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: {C['surface2']};
    border: 1px solid {C['border2']};
    border-radius: 20px;
    padding: 4px 12px;
    font-family: {MONO};
    font-size: 11px;
    color: {C['text2']};
    margin: 3px;
}}
.insight .dot {{
    width: 6px; height: 6px;
    border-radius: 50%;
}}

/* ── Scrollbar ── */
::-webkit-scrollbar {{ width: 4px; height: 4px; }}
::-webkit-scrollbar-track {{ background: transparent; }}
::-webkit-scrollbar-thumb {{ background: {C['border2']}; border-radius: 2px; }}

/* ── Hide streamlit chrome ── */
#MainMenu, footer, [data-testid="stToolbar"] {{ visibility: hidden; }}
[data-testid="stDecoration"] {{ display: none; }}
</style>
""", unsafe_allow_html=True)


# ── Data loading ───────────────────────────────────────────────────────────────
BASE = Path(__file__).parent.parent.parent / "data" / "gold"

@st.cache_data
def load_all():
    def safe(p):
        return pd.read_csv(BASE / p) if (BASE / p).exists() else pd.DataFrame()
    return {
        "overview":          safe("overview_metrics.csv"),
        "by_country":        safe("jobs_by_country.csv"),
        "by_city":           safe("jobs_by_city.csv"),
        "by_remote":         safe("jobs_by_remote_type.csv"),
        "remote_by_country": safe("remote_type_by_country.csv"),
        "salary_country":    safe("salary_by_country.csv"),
        "salary_remote":     safe("salary_by_remote_type.csv"),
        "salary_dist":       safe("salary_distribution.csv"),
    }

data = load_all()


# ── Shared chart config ────────────────────────────────────────────────────────
LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family=FONT, color=C["text2"], size=11),
    margin=dict(l=0, r=16, t=8, b=0),
    hoverlabel=dict(
        bgcolor=C["surface2"],
        bordercolor=C["border2"],
        font=dict(family=FONT, color=C["text"], size=12),
    ),
)

XAXIS = dict(showgrid=False, zeroline=False, showline=False,
             tickfont=dict(size=10, family=FONT, color=C["text2"]))
YAXIS = dict(showgrid=True, gridcolor=C["border"], gridwidth=0.5,
             zeroline=False, showline=False,
             tickfont=dict(size=10, family=FONT, color=C["text2"]))
YAXIS_CLEAN = dict(showgrid=False, zeroline=False, showline=False,
                   tickfont=dict(size=11, family=FONT, color=C["text2"]))


def cfg(fig, height=300, showlegend=False, extra=None):
    kw = {**LAYOUT, "height": height, "showlegend": showlegend}
    if extra:
        kw.update(extra)
    fig.update_layout(**kw)
    return fig


# ── Chart helpers ──────────────────────────────────────────────────────────────
def h_bar(df, val_col, label_col, n=12, color=None, fmt="count"):
    if df.empty or val_col not in df.columns or label_col not in df.columns:
        return go.Figure()
    df = df.nlargest(n, val_col).sort_values(val_col)
    base_color = color or C["accent2"]
    bar_colors = [C["accent"] if i == len(df)-1 else base_color
                  for i in range(len(df))]
    text = df[val_col].apply(
        lambda v: f"${v:,.0f}" if fmt == "currency" else f"{v:,}"
    )
    fig = go.Figure(go.Bar(
        y=df[label_col], x=df[val_col], orientation="h",
        marker=dict(color=bar_colors, line=dict(width=0),
                    opacity=[1.0 if i == len(df)-1 else 0.6 for i in range(len(df))]),
        text=text, textposition="outside",
        textfont=dict(size=10, family=FONT, color=C["text2"]),
        hovertemplate="<b>%{y}</b><br>%{x:,}<extra></extra>",
    ))
    fig.update_xaxes(**XAXIS, showticklabels=False)
    fig.update_yaxes(**YAXIS_CLEAN)
    h = max(240, n * 30)
    return cfg(fig, height=h, showlegend=False)


def donut(df, names_col, val_col):
    if df.empty:
        return go.Figure()
    cmap = {"Remote": C["remote"], "Hybrid": C["hybrid"],
            "On-site": C["onsite"], "Onsite": C["onsite"]}
    colors = [cmap.get(n, C["text3"]) for n in df[names_col]]
    fig = go.Figure(go.Pie(
        labels=df[names_col], values=df[val_col],
        hole=0.65,
        marker=dict(colors=colors, line=dict(color=C["bg"], width=3)),
        textinfo="percent",
        textfont=dict(size=11, family=FONT, color=C["bg"]),
        hovertemplate="<b>%{label}</b><br>%{value:,} jobs — %{percent}<extra></extra>",
        pull=[0.03] * len(df),
    ))
    return cfg(fig, height=260, showlegend=True, extra=dict(
        legend=dict(orientation="h", y=-0.08, x=0.5, xanchor="center",
                    font=dict(size=11, family=FONT, color=C["text2"]),
                    bgcolor="rgba(0,0,0,0)", borderwidth=0),
    ))


def grouped_bar(df, x_col, y_col, color_col):
    if df.empty:
        return go.Figure()
    cmap = {"Remote": C["remote"], "Hybrid": C["hybrid"],
            "On-site": C["onsite"], "Onsite": C["onsite"]}
    fig = px.bar(df, x=x_col, y=y_col, color=color_col,
                 barmode="group", color_discrete_map=cmap, template="none")
    fig.update_traces(marker_line_width=0, opacity=0.85)
    fig.update_xaxes(**XAXIS)
    fig.update_yaxes(**YAXIS, title="")
    return cfg(fig, height=280, showlegend=True, extra=dict(
        bargap=0.25, bargroupgap=0.08,
        legend=dict(orientation="h", y=-0.18, x=0.5, xanchor="center",
                    font=dict(size=11, family=FONT, color=C["text2"]),
                    bgcolor="rgba(0,0,0,0)", borderwidth=0, title=""),
    ))


def salary_gradient_bar(df, label_col, val_col, n=8):
    if df.empty or val_col not in df.columns:
        return go.Figure()
    df = df.nlargest(n, val_col).sort_values(val_col)
    norm = (df[val_col] - df[val_col].min()) / (df[val_col].max() - df[val_col].min() + 1)

    def lerp_color(t):
        r1,g1,b1 = 0x7B,0x8C,0xFF
        r2,g2,b2 = 0x4E,0xFA,0xC4
        return f"rgb({int(r1+(r2-r1)*t)},{int(g1+(g2-g1)*t)},{int(b1+(b2-b1)*t)})"

    colors = [lerp_color(float(t)) for t in norm]
    fig = go.Figure(go.Bar(
        y=df[label_col], x=df[val_col], orientation="h",
        marker=dict(color=colors, line=dict(width=0)),
        text=df[val_col].apply(lambda v: f"${v:,.0f}"),
        textposition="outside",
        textfont=dict(size=10, family=FONT, color=C["text2"]),
        hovertemplate="<b>%{y}</b><br>$%{x:,.0f}<extra></extra>",
    ))
    fig.update_xaxes(**XAXIS, showticklabels=False)
    fig.update_yaxes(**YAXIS_CLEAN)
    return cfg(fig, height=max(240, n*32), showlegend=False)


def histogram(df, col):
    if df.empty or col not in df.columns:
        return go.Figure()
    fig = go.Figure(go.Histogram(
        x=df[col], nbinsx=32,
        marker=dict(color=C["accent2"], opacity=0.75,
                    line=dict(color=C["bg"], width=0.8)),
        hovertemplate="$%{x:,.0f}<br>%{y} roles<extra></extra>",
    ))
    fig.update_xaxes(**XAXIS, tickprefix="$")  # ← remove tickfont here
    fig.update_yaxes(**YAXIS, title="")
    return cfg(fig, height=240, showlegend=False, extra=dict(bargap=0.04))


def line_trend(df, x_col, y_col, color=None):
    if df.empty:
        return go.Figure()
    fig = go.Figure(go.Scatter(
        x=df[x_col], y=df[y_col], mode="lines+markers",
        line=dict(color=color or C["accent"], width=2.5, shape="spline"),
        marker=dict(size=5, color=color or C["accent"],
                    line=dict(width=1.5, color=C["bg"])),
        fill="tozeroy",
        fillcolor=f"{color or C['accent']}18",
        hovertemplate="%{x}<br>%{y:,}<extra></extra>",
    ))
    fig.update_xaxes(**XAXIS)
    fig.update_yaxes(**YAXIS, title="")
    return cfg(fig, height=200, showlegend=False)


def mode_salary_bar(df, label_col, val_col):
    if df.empty:
        return go.Figure()
    cmap = {"Remote": C["remote"], "Hybrid": C["hybrid"],
            "On-site": C["onsite"], "Onsite": C["onsite"]}
    colors = [cmap.get(str(l), C["accent2"]) for l in df[label_col]]
    fig = go.Figure(go.Bar(
        x=df[label_col], y=df[val_col],
        marker=dict(color=colors, line=dict(width=0), opacity=0.85),
        text=df[val_col].apply(lambda v: f"${v:,.0f}"),
        textposition="outside",
        textfont=dict(size=11, family=FONT, color=C["text2"]),
        hovertemplate="<b>%{x}</b><br>$%{y:,.0f}<extra></extra>",
    ))
    fig.update_xaxes(**XAXIS)
    fig.update_yaxes(**YAXIS, showticklabels=False, title="")
    return cfg(fig, height=220, showlegend=False, extra=dict(bargap=0.4))


# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(f"""
    <div style="padding: 1rem 0 1.5rem;">
        <div style="font-family:{MONO}; font-size:10px; color:{C['accent']};
                    text-transform:uppercase; letter-spacing:0.15em; margin-bottom:6px;">
            ◈ Job Intel
        </div>
        <div style="font-family:{FONT}; font-size:18px; font-weight:800;
                    color:{C['text']}; letter-spacing:-0.02em; line-height:1.2;">
            Market<br>Dashboard
        </div>
    </div>
    <hr style="border:none; border-top:1px solid {C['border']}; margin: 0 0 1.5rem;">
    """, unsafe_allow_html=True)

    # Country filter
    country_opts = ["All countries"]
    if not data["by_country"].empty and "country" in data["by_country"].columns:
        country_opts += sorted(data["by_country"]["country"].dropna().unique().tolist())

    selected_country = st.selectbox("Country", country_opts, key="country")

    st.markdown("<br>", unsafe_allow_html=True)

    # Work mode filter
    mode_opts = ["All modes"]
    if not data["by_remote"].empty and "remote_type" in data["by_remote"].columns:
        mode_opts += data["by_remote"]["remote_type"].dropna().unique().tolist()

    selected_mode = st.selectbox("Work mode", mode_opts, key="mode")

    st.markdown(f"""
    <hr style="border:none; border-top:1px solid {C['border']}; margin: 1.5rem 0;">
    <div style="font-family:{MONO}; font-size:10px; color:{C['text3']};
                text-transform:uppercase; letter-spacing:0.1em; line-height:1.8;">
        Data source<br>
        <span style="color:{C['text2']}">data/gold/*.csv</span><br><br>
        Color legend<br>
        <span style="color:{C['remote']}">● Remote</span>&nbsp;&nbsp;
        <span style="color:{C['hybrid']}">● Hybrid</span>&nbsp;&nbsp;
        <span style="color:{C['onsite']}">● On-site</span>
    </div>
    """, unsafe_allow_html=True)


# ── Apply filters ──────────────────────────────────────────────────────────────
def filter_df(df, col="country", val=selected_country, all_label="All countries"):
    if val == all_label or df.empty or col not in df.columns:
        return df
    return df[df[col] == val]

city_df           = filter_df(data["by_city"])
remote_country_df = filter_df(data["remote_by_country"])
salary_country_df = filter_df(data["salary_country"])

# City label column detection
city_label = "city"
if not city_df.empty:
    candidates = [c for c in city_df.columns if c not in ("job_count", "country")]
    if candidates:
        city_label = candidates[0]


# ── Hero header ────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="hero">
    <div class="hero-label">◈ Real-time intelligence</div>
    <div class="hero-title">Job Market <span>Overview</span></div>
    <div class="hero-sub">
        Global hiring trends · Geography · Work mode · Compensation
        {"&nbsp;&nbsp;·&nbsp;&nbsp;<b style='color:{C['accent']}'>" + selected_country + "</b>" if selected_country != "All countries" else ""}
    </div>
</div>
""", unsafe_allow_html=True)


# ── KPI strip ──────────────────────────────────────────────────────────────────
ov = data["overview"]
if not ov.empty:
    row = ov.iloc[0]
    total   = int(row.get("total_jobs", 0))
    remote  = int(row.get("remote_jobs", 0))
    hybrid  = int(row.get("hybrid_jobs", 0))
    onsite  = int(row.get("onsite_jobs", 0))
    avg_sal = float(row.get("average_salary_usd", 0))
    countries_n = int(row.get("total_countries", 0))
    cities_n    = int(row.get("total_cities", 0))

    # Accent salary card
    c0, c1, c2, c3, c4, c5, c6 = st.columns(7)
    with c0:
        st.markdown(f"""
        <div class="kpi-accent">
            <div class="label">Avg Salary</div>
            <div class="value">${avg_sal:,.0f}</div>
            <div class="sub">USD equivalent</div>
        </div>
        """, unsafe_allow_html=True)

    with c1:
        st.metric("Total Jobs", f"{total:,}", help="All postings across all markets")
    with c2:
        st.metric("Countries", f"{countries_n:,}", help="Unique countries represented")
    with c3:
        st.metric("Cities", f"{cities_n:,}", help="Unique hiring locations")
    with c4:
        pct = f"{remote/total*100:.1f}% share" if total else ""
        st.metric("Remote", f"{remote:,}", delta=pct)
    with c5:
        pct = f"{hybrid/total*100:.1f}% share" if total else ""
        st.metric("Hybrid", f"{hybrid:,}", delta=pct)
    with c6:
        pct = f"{onsite/total*100:.1f}% share" if total else ""
        st.metric("On-site", f"{onsite:,}", delta=pct)


# ── Tabs ───────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "◎  Geography",
    "◐  Work Mode",
    "◑  Compensation",
    "◒  Data Table",
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — GEOGRAPHY
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    st.markdown('<div class="section-header">Top Markets</div>', unsafe_allow_html=True)

    col_l, col_r = st.columns([1, 1], gap="medium")

    with col_l:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.markdown('<div class="chart-title">Jobs by Country</div>', unsafe_allow_html=True)
        st.markdown('<div class="chart-sub">Top 12 · by posting volume</div>', unsafe_allow_html=True)
        fig = h_bar(data["by_country"], "job_count", "country", n=12)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False}, key="chart_country")
        st.markdown('</div>', unsafe_allow_html=True)

    with col_r:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.markdown('<div class="chart-title">Top Cities</div>', unsafe_allow_html=True)
        st.markdown('<div class="chart-sub">Hiring hotspots · filtered by selection</div>', unsafe_allow_html=True)
        fig = h_bar(city_df, "job_count", city_label, n=12)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False}, key="chart_city")
        st.markdown('</div>', unsafe_allow_html=True)

    # Quick insight pills
    if not data["by_country"].empty and "country" in data["by_country"].columns:
        top3 = data["by_country"].nlargest(3, "job_count")["country"].tolist()
        st.markdown(
            '<div style="margin-top:0.5rem;">'
            + "".join([
                f'<span class="insight"><span class="dot" style="background:{C["accent"]}"></span>#{i+1} {c}</span>'
                for i, c in enumerate(top3)
            ])
            + '</div>',
            unsafe_allow_html=True
        )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — WORK MODE
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.markdown('<div class="section-header">Work Mode Distribution</div>', unsafe_allow_html=True)

    col_l, col_r = st.columns([1, 1], gap="medium")

    with col_l:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.markdown('<div class="chart-title">Remote · Hybrid · On-site Split</div>', unsafe_allow_html=True)
        st.markdown('<div class="chart-sub">Share of total postings</div>', unsafe_allow_html=True)
        fig = donut(data["by_remote"], "remote_type", "job_count")
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False}, key="chart_donut")
        st.markdown('</div>', unsafe_allow_html=True)

    with col_r:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.markdown('<div class="chart-title">Work Mode by Country</div>', unsafe_allow_html=True)
        st.markdown('<div class="chart-sub">Grouped comparison</div>', unsafe_allow_html=True)
        fig = grouped_bar(remote_country_df, "country", "job_count", "remote_type")
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False}, key="chart_grouped")
        st.markdown('</div>', unsafe_allow_html=True)

    # Mode insight
    if not data["by_remote"].empty:
        top_mode = data["by_remote"].nlargest(1, "job_count").iloc[0]
        mode_name = top_mode.get("remote_type", "")
        mode_count = int(top_mode.get("job_count", 0))
        mode_color = {"Remote": C["remote"], "Hybrid": C["hybrid"]}.get(mode_name, C["onsite"])
        st.markdown(f"""
        <div style="margin-top:1rem; padding:1rem 1.2rem;
                    background:{C['surface']}; border:1px solid {C['border']};
                    border-left:3px solid {mode_color}; border-radius:8px;">
            <span style="font-family:{MONO}; font-size:11px; color:{C['text3']};
                         text-transform:uppercase; letter-spacing:0.08em;">
                Dominant mode
            </span><br>
            <span style="font-family:{FONT}; font-size:16px; font-weight:700;
                         color:{mode_color};">
                {mode_name}
            </span>
            <span style="font-family:{MONO}; font-size:12px; color:{C['text2']};">
                &nbsp;·&nbsp; {mode_count:,} postings
            </span>
        </div>
        """, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — COMPENSATION
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.markdown('<div class="section-header">Salary Intelligence</div>', unsafe_allow_html=True)

    col_l, col_r = st.columns([1.2, 0.8], gap="medium")

    with col_l:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.markdown('<div class="chart-title">Average Salary by Country</div>', unsafe_allow_html=True)
        st.markdown('<div class="chart-sub">USD · top 8 markets</div>', unsafe_allow_html=True)
        fig = salary_gradient_bar(salary_country_df, "country", "avg_salary_usd", n=8)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False}, key="chart_sal_country")
        st.markdown('</div>', unsafe_allow_html=True)

    with col_r:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.markdown('<div class="chart-title">Salary by Work Mode</div>', unsafe_allow_html=True)
        st.markdown('<div class="chart-sub">Compensation gap analysis</div>', unsafe_allow_html=True)
        fig = mode_salary_bar(data["salary_remote"], "remote_type", "avg_salary_usd")
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False}, key="chart_sal_mode")
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="section-header">Distribution</div>', unsafe_allow_html=True)
    st.markdown('<div class="chart-card">', unsafe_allow_html=True)
    st.markdown('<div class="chart-title">Salary Distribution</div>', unsafe_allow_html=True)
    st.markdown('<div class="chart-sub">All roles · USD · 32 bins</div>', unsafe_allow_html=True)
    fig = histogram(data["salary_dist"], "salary_avg_usd")
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False}, key="chart_hist")
    st.markdown('</div>', unsafe_allow_html=True)

    # Salary insights row
    if not data["salary_remote"].empty and "avg_salary_usd" in data["salary_remote"].columns:
        sal_df = data["salary_remote"]
        max_row = sal_df.nlargest(1, "avg_salary_usd").iloc[0]
        min_row = sal_df.nsmallest(1, "avg_salary_usd").iloc[0]
        gap = float(max_row["avg_salary_usd"]) - float(min_row["avg_salary_usd"])
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Highest paying mode",
                      f"${float(max_row['avg_salary_usd']):,.0f}",
                      delta=str(max_row.get("remote_type", "")))
        with c2:
            st.metric("Lowest paying mode",
                      f"${float(min_row['avg_salary_usd']):,.0f}",
                      delta=str(min_row.get("remote_type", "")))
        with c3:
            st.metric("Mode salary gap", f"${gap:,.0f}", delta="USD difference")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — DATA TABLE
# ══════════════════════════════════════════════════════════════════════════════
with tab4:
    st.markdown('<div class="section-header">City Breakdown</div>', unsafe_allow_html=True)

    display_df = city_df.copy()

    if not display_df.empty:
        # Search
        search = st.text_input(
            "Search",
            placeholder="Filter cities...",
            label_visibility="collapsed",
        )
        if search:
            mask = display_df.astype(str).apply(
                lambda col: col.str.contains(search, case=False)
            ).any(axis=1)
            display_df = display_df[mask]

        # Sort
        if "job_count" in display_df.columns:
            display_df = display_df.sort_values("job_count", ascending=False)

        display_df.columns = [c.replace("_", " ").title() for c in display_df.columns]

        st.dataframe(
            display_df.head(50),
            use_container_width=True,
            hide_index=True,
        )

        st.markdown(
            f'<div style="font-family:{MONO}; font-size:10px; color:{C["text3"]}; '
            f'margin-top:0.5rem;">Showing {min(50, len(display_df))} of {len(display_df)} rows</div>',
            unsafe_allow_html=True
        )
    else:
        st.markdown(
            f'<div style="font-family:{MONO}; font-size:13px; color:{C["text3"]}; '
            f'padding:2rem; text-align:center;">No city data available</div>',
            unsafe_allow_html=True
        )