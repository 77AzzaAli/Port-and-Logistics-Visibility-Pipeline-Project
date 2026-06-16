import streamlit as st
import pandas as pd
import plotly.express as px
import random
from datetime import datetime
from streamlit_autorefresh import st_autorefresh

# =====================================================
# CONFIG
# =====================================================

st.set_page_config(
    page_title="Port Operations Control Tower",
    layout="wide"
)

st_autorefresh(interval=15000, key="refresh")

# =====================================================
# COLOR SYSTEM
# =====================================================

COLORS = {
    "primary": "#00D4FF",
    "secondary": "#7C4DFF",
    "success": "#00E676",
    "warning": "#FFB300",
    "danger": "#FF5252",
    "bg": "#0E1117",
    "grid": "#1F2630",
    "text": "#E6E6E6"
}

# =====================================================
# CONSTANTS
# =====================================================

EVENT_TYPES = [
    "VESSEL_ARRIVED",
    "CONTAINER_DISCHARGED",
    "YARD_ENTER",
    "CUSTOMS_SUBMITTED",
    "CUSTOMS_INSPECTED",
    "CUSTOMS_CLEAR",
    "GATE_OUT",
    "DELIVERED"
]

SHIPPING_LINES = ["MAERSK", "MSC", "CMA_CGM", "HAPAG_LLOYD"]
PORT_ZONES = ["BERTH_A", "BERTH_B", "YARD_1", "YARD_2", "CUSTOMS_AREA"]
STATUS = ["NORMAL", "DELAYED", "FAILED", "HOLD"]

# =====================================================
# DATA GENERATION
# =====================================================

def generate_data(n=1200):
    rows = []

    for i in range(n):
        status = random.choices(STATUS, weights=[70, 20, 5, 5])[0]

        planned = random.randint(1, 72)
        actual = planned

        if status == "DELAYED":
            actual += random.randint(4, 48)
        elif status == "FAILED":
            actual += random.randint(1, 12)
        elif status == "HOLD":
            actual += random.randint(6, 24)

        rows.append({
            "container_id": f"CONT-{100000+i}",
            "event_type": random.choice(EVENT_TYPES),
            "event_category": "CUSTOMS" if random.random() > 0.6 else "OPERATIONS",
            "shipping_line": random.choice(SHIPPING_LINES),
            "zone": random.choice(PORT_ZONES),
            "planned_duration": planned,
            "actual_duration": actual,
            "delay_hours": max(0, actual - planned),
            "system_pressure": round(random.uniform(10, 100), 2),
            "status": status,
            "event_time": datetime.utcnow()
        })

    return pd.DataFrame(rows)


def generate_new(n=15):
    return generate_data(n)

# =====================================================
# STATE
# =====================================================

if "df" not in st.session_state:
    st.session_state.df = generate_data()

st.session_state.df = pd.concat(
    [st.session_state.df, generate_new(15)],
    ignore_index=True
)

if len(st.session_state.df) > 3000:
    st.session_state.df = st.session_state.df.tail(3000)

df = st.session_state.df

# =====================================================
# KPI WINDOW (LIVE)
# =====================================================

WINDOW_SIZE = 300
live_df = df.tail(WINDOW_SIZE)

containers = live_df["container_id"].nunique()
delivered = live_df[live_df["event_type"] == "DELIVERED"]["container_id"].nunique()
delayed = live_df[live_df["status"].isin(["DELAYED", "HOLD"])]["container_id"].nunique()
failed = live_df[live_df["status"] == "FAILED"]["container_id"].nunique()

avg_pressure = live_df["system_pressure"].mean()
failure_rate = (failed / containers) * 100 if containers else 0

prev_df = df.tail(WINDOW_SIZE * 2).head(WINDOW_SIZE)

prev_failed = prev_df[prev_df["status"] == "FAILED"]["container_id"].nunique()
prev_pressure = prev_df["system_pressure"].mean()

failed_delta = failed - prev_failed
pressure_delta = avg_pressure - prev_pressure

# =====================================================
# UI
# =====================================================

def kpi_card(title, value, color):
    st.markdown(
        f"""
        <div style="
            background-color:#111827;
            padding:16px;
            border-radius:12px;
            border-left:6px solid {color};
            box-shadow:0px 2px 8px rgba(0,0,0,0.3);
        ">
            <div style="color:#9CA3AF;font-size:13px">{title}</div>
            <div style="color:white;font-size:22px;font-weight:600">{value}</div>
        </div>
        """,
        unsafe_allow_html=True
    )


def style(fig):
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=COLORS["bg"],
        plot_bgcolor=COLORS["bg"],
        font=dict(color=COLORS["text"]),
        margin=dict(l=20, r=20, t=30, b=20),
        xaxis=dict(showgrid=True, gridcolor=COLORS["grid"]),
        yaxis=dict(showgrid=True, gridcolor=COLORS["grid"])
    )
    return fig

# =====================================================
# HEADER
# =====================================================

st.title("🚢 Port of Alexandria - Control Tower")
st.markdown("Real-time container flow, bottlenecks & operational intelligence")
st.markdown("---")

# =====================================================
# KPI SECTION
# =====================================================

c1, c2, c3, c4, c5, c6 = st.columns(6)

with c1:
    kpi_card("Containers (Live)", f"{containers:,}", COLORS["primary"])

with c2:
    kpi_card("Delivered", f"{delivered:,}", COLORS["success"])

with c3:
    kpi_card("Delayed/Hold", f"{delayed:,}", COLORS["warning"])

with c4:
    delta_color = COLORS["danger"] if failed_delta > 0 else COLORS["success"]
    kpi_card("Failures Δ", f"{failed} ({failed_delta:+})", delta_color)

with c5:
    kpi_card("Failure Rate", f"{failure_rate:.2f}%", COLORS["danger"])

with c6:
    pressure_color = COLORS["warning"] if pressure_delta > 0 else COLORS["success"]
    kpi_card("Pressure Δ", f"{avg_pressure:.1f} ({pressure_delta:+.1f})", pressure_color)

st.markdown("---")

# =====================================================
# CHART GRID (FIXED STRUCTURE)
# =====================================================

col1, col2 = st.columns(2)

with col1:
    st.subheader("Container Flow Lifecycle")

    flow = df.groupby("event_type").size().reset_index(name="count")

    fig = px.bar(
        flow,
        x="count",
        y="event_type",
        orientation="h",
        color="count",
        color_continuous_scale=["#1F2630", COLORS["primary"]]
    )

    st.plotly_chart(style(fig), use_container_width=True)

with col2:
    st.subheader("Shipping Line Activity")

    ship = df.groupby("shipping_line").size().reset_index(name="count")

    fig = px.pie(
        ship,
        names="shipping_line",
        values="count",
        hole=0.55,
        color_discrete_sequence=[
            COLORS["primary"],
            COLORS["secondary"],
            COLORS["success"],
            COLORS["warning"]
        ]
    )

    fig.update_traces(textinfo="percent+label")

    st.plotly_chart(style(fig), use_container_width=True)

# =====================================================
# SECOND ROW CHARTS
# =====================================================

col3, col4 = st.columns(2)

with col3:
    st.subheader("Zone Utilization")

    zone = df.groupby("zone").size().reset_index(name="count")

    fig = px.pie(
        zone,
        names="zone",
        values="count",
        hole=0.5,
        color_discrete_sequence=[
            "#334155",
            "#475569",
            "#64748B",
            "#94A3B8"
        ]
    )

    fig.update_traces(textinfo="percent+label")

    st.plotly_chart(style(fig), use_container_width=True)

with col4:
    st.subheader("Operational Status Distribution")

    status_df = df.groupby("status").size().reset_index(name="count")

    fig = px.bar(
        status_df,
        x="status",
        y="count",
        color="count",
        color_continuous_scale=["#1F2630", COLORS["primary"]]
    )

    st.plotly_chart(style(fig), use_container_width=True)

# =====================================================
# LIVE TABLE
# =====================================================

st.subheader("Live Container Movements")

latest = df.sort_values("event_time", ascending=False).head(25)

st.dataframe(
    latest,
    use_container_width=True,
    height=400
)