import streamlit as st
import pandas as pd
import glob
import time
import random
from datetime import datetime, timedelta
import streamlit.components.v1 as components

# 1. PAGE CONFIGURATION (Must be the first Streamlit command)
st.set_page_config(page_title="⚡ FlashPulse Dashboard", page_icon="⚡", layout="wide")

# 2. TARGETED CUSTOM CSS
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;900&display=swap');
    
    html, body, .stApp { 
        background-color: #080B14 !important; 
        font-family: 'Inter', sans-serif !important; 
    }
    
    /* Sidebar Styling */
    section[data-testid="stSidebar"] { 
        background: #0D1120 !important; 
        border-right: 1px solid #1C2340 !important; 
    }
    
    /* Safely target headers and text */
    h1, h2, h3, .stMarkdown p { color: #E2E8F0 !important; }
    h1 { font-size: 2.2rem !important; font-weight: 900 !important; color: #FFFFFF !important; }
    
    /* Metric Cards */
    div[data-testid="metric-container"] { 
        background: #111827 !important; 
        border: 1px solid #1F2D4A !important; 
        border-radius: 12px !important; 
        padding: 20px !important; 
        position: relative; 
        box-shadow: 0 4px 12px rgba(0,0,0,0.2);
        border-top: 3px solid #00E5FF;
    }
    div[data-testid="stMetricValue"] { color: #FFFFFF !important; font-size: 1.8rem !important; font-weight: 800 !important; }
    div[data-testid="stMetricLabel"] { color: #94A3B8 !important; font-size: 0.85rem !important; text-transform: uppercase !important; letter-spacing: 0.05em !important; }
    
    /* Fix for Selectbox / Dropdown menus */
    div[data-baseweb="popover"] > div { background-color: #111827 !important; border: 1px solid #1F2D4A !important; }
    ul[data-baseweb="menu"] { background-color: #111827 !important; }
    ul[data-baseweb="menu"] li { color: #FFFFFF !important; }
    
    /* Custom Elements */
    .status-live { 
        display: inline-flex; align-items: center; gap: 8px; 
        background: rgba(54, 232, 135, 0.1); border: 1px solid rgba(54, 232, 135, 0.3); 
        color: #36E887; padding: 6px 16px; border-radius: 20px; 
        font-size: 0.75rem; font-weight: 800; letter-spacing: 0.1em; text-transform: uppercase; 
    }
    .dot { width: 8px; height: 8px; background: #36E887; border-radius: 50%; animation: blink 1.5s infinite; }
    @keyframes blink { 0%,100%{opacity:1} 50%{opacity:0.3} }
    
    .insight-card { 
        background: #111827; border: 1px solid #1F2D4A; border-left: 4px solid #00E5FF; 
        border-radius: 8px; padding: 16px; margin-bottom: 12px; 
    }
    .insight-title { font-size: 0.85rem; color: #00E5FF; font-weight: 700; margin-bottom: 6px; text-transform: uppercase; letter-spacing: 0.05em; }
    .insight-body  { font-size: 0.95rem; color: #E2E8F0; }
    
    .alert-banner { 
        background: rgba(255, 184, 0, 0.1); border: 1px solid #FFB800; 
        border-radius: 8px; padding: 12px 20px; margin-bottom: 20px; 
        font-size: 0.9rem; color: #FFB800; font-weight: 600; 
    }
    
    .fraud-banner { 
        background: rgba(255, 0, 0, 0.1); border: 1px solid #FF0000; 
        border-radius: 8px; padding: 12px 20px; margin-bottom: 20px; 
        font-size: 0.9rem; color: #FF4444; font-weight: 600; 
    }

    /* ── Goal Progress Ring ── */
    .progress-ring-wrap {
        display: flex; flex-direction: column; align-items: center;
        background: #111827; border: 1px solid #1F2D4A; border-radius: 12px;
        padding: 20px 16px; gap: 8px;
    }
    .ring-label { font-size: 0.75rem; color: #94A3B8; text-transform: uppercase; letter-spacing: 0.07em; font-weight: 700; }
    .ring-pct   { font-size: 1.6rem; font-weight: 900; color: #FFFFFF; }

    /* ── Leaderboard ── */
    .lb-row {
        display: flex; align-items: center; gap: 10px;
        background: #111827; border: 1px solid #1F2D4A;
        border-radius: 10px; padding: 12px 16px; margin-bottom: 8px;
    }
    .lb-rank  { font-size: 1.1rem; font-weight: 900; min-width: 28px; }
    .lb-name  { font-size: 0.9rem; color: #E2E8F0; flex: 1; }
    .lb-rev   { font-size: 0.9rem; font-weight: 700; color: #00E5FF; }
    .lb-bar-bg { height: 6px; background: #1F2D4A; border-radius: 4px; flex: 1; overflow: hidden; }
    .lb-bar-fill { height: 6px; border-radius: 4px; }

    /* ── Category Health ── */
    .health-pill {
        display: inline-block; padding: 3px 12px; border-radius: 20px;
        font-size: 0.72rem; font-weight: 800; letter-spacing: 0.07em; text-transform: uppercase;
    }
    .health-hot  { background: rgba(255,0,100,0.15); color: #FF3366; border: 1px solid rgba(255,0,100,0.3); }
    .health-warm { background: rgba(255,184,0,0.12); color: #FFB800; border: 1px solid rgba(255,184,0,0.3); }
    .health-cold { background: rgba(0,229,255,0.10); color: #00E5FF; border: 1px solid rgba(0,229,255,0.3); }

    /* ── Live Ticker ── */
    .ticker-wrap {
        overflow: hidden; white-space: nowrap;
        background: #0D1120; border-top: 1px solid #1C2340;
        border-bottom: 1px solid #1C2340;
        padding: 8px 0; margin-top: 8px;
    }
    .ticker-inner {
        display: inline-block;
        /* I SLOWED THIS DOWN FROM 30s to 80s */
        animation: scroll-left 80s linear infinite;
        font-size: 0.8rem; color: #94A3B8;
    }
    @keyframes scroll-left { 0%{transform:translateX(100vw)} 100%{transform:translateX(-100%)} }
    .ticker-sep { color: #1F2D4A; margin: 0 24px; }
    .ticker-hi  { color: #36E887; font-weight: 700; }
    .ticker-warn{ color: #FFB800; font-weight: 700; }

    /* ── Heatmap ── */
    .heatmap-cell {
        border-radius: 6px; text-align: center;
        font-size: 0.75rem; font-weight: 700; padding: 14px 4px;
        color: #FFFFFF;
    }

    /* ── Stat Delta Badge ── */
    .delta-up   { color: #36E887; font-size: 0.78rem; font-weight: 700; }
    .delta-down { color: #FF4444; font-size: 0.78rem; font-weight: 700; }

    hr { border-color: #1F2D4A !important; margin: 1.5rem 0 !important; }
</style>
""", unsafe_allow_html=True)


# SESSION STATE
if 'toast_log' not in st.session_state:
    st.session_state.toast_log = []
if 'prev_revenue' not in st.session_state:
    st.session_state.prev_revenue = None
if 'sale_end' not in st.session_state:
    st.session_state.sale_end = datetime.now() + timedelta(hours=2)

# 3. SIDEBAR CONTROLS
with st.sidebar:
    st.markdown("## ⚡ FlashPulse")
    st.caption("Real-time Sales Intelligence v4.0")
    st.markdown("---")

    # ── UPDATED: Live JavaScript Countdown Timer ──
    st.markdown("### ⏳ Flash Sale Ends In")
    
    # Calculate target timestamp for JS
    end_timestamp = int(st.session_state.sale_end.timestamp() * 1000)
    
    # Inject JavaScript and CSS for a smooth 1-second ticking clock
    timer_html = f"""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;900&display=swap');
        body {{ margin: 0; background: #0D1120; font-family: 'Inter', sans-serif; }}
        .countdown-box {{ display: flex; gap: 8px; margin-top: 2px; }}
        .cd-unit {{ display: flex; flex-direction: column; align-items: center; background: #111827; border: 1px solid #1F2D4A; border-radius: 8px; padding: 8px 10px; width: 33%; }}
        .cd-num  {{ font-size: 1.3rem; font-weight: 900; color: #FFFFFF; line-height: 1; }}
        .cd-lbl  {{ font-size: 0.6rem; color: #94A3B8; text-transform: uppercase; letter-spacing: 0.07em; margin-top: 2px; }}
    </style>
    <div class="countdown-box">
        <div class="cd-unit"><div class="cd-num" id="hrs">00</div><div class="cd-lbl">hrs</div></div>
        <div class="cd-unit"><div class="cd-num" id="mins">00</div><div class="cd-lbl">min</div></div>
        <div class="cd-unit"><div class="cd-num" id="secs">00</div><div class="cd-lbl">sec</div></div>
    </div>
    <script>
        const target = {end_timestamp};
        function updateTimer() {{
            const now = new Date().getTime();
            const diff = target - now;
            if (diff > 0) {{
                const h = Math.floor((diff % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));
                const m = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60));
                const s = Math.floor((diff % (1000 * 60)) / 1000);
                document.getElementById("hrs").innerText = h.toString().padStart(2, '0');
                document.getElementById("mins").innerText = m.toString().padStart(2, '0');
                document.getElementById("secs").innerText = s.toString().padStart(2, '0');
            }}
        }}
        setInterval(updateTimer, 1000);
        updateTimer();
    </script>
    """
    # Embed the component with a fixed height so it seamlessly blends into the sidebar
    components.html(timer_html, height=75)
    
    st.markdown("---")
    st.markdown("### ⚙️ Engine Settings")
    refresh_interval = st.selectbox("🔄 Refresh Interval", [5, 10, 15, 30, 60], index=1, format_func=lambda x: f"{x} seconds")
    
    st.markdown("---")
    st.markdown("### 📊 Display Preferences")
    chart_type = st.radio("Chart Style", ["Bar", "Line", "Area"], horizontal=True)
    sort_by  = st.selectbox("Sort Table By", ["Total_Revenue", "Total_Orders"])
    sort_asc = st.checkbox("⬆️ Ascending Order", value=False)
    
    st.markdown("---")
    st.markdown("### 🚨 Thresholds")
    alert_threshold = st.slider("Revenue Target ($)", min_value=1000, max_value=100000, value=25000, step=1000)

    st.markdown("---")
    st.markdown("### 🔎 Filter Products")
    filter_mode = st.radio("Show", ["All Products", "Top 3 Only", "Bottom 3 Only"], index=0)
    
    st.markdown("---")
    st.caption("B.Tech Final Project · Topic 80\nFlash Sale Monitoring System")

# 4. HEADER
hdr_l, hdr_r = st.columns([4, 1])
with hdr_l:
    st.markdown("# ⚡ Flash Sale Monitoring System")
    st.caption("Enterprise Real-time Sales Intelligence Platform")
with hdr_r:
    st.write("")
    st.markdown('<div style="text-align: right;"><span class="status-live"><span class="dot"></span>LIVE DATA</span></div>', unsafe_allow_html=True)

st.markdown("---")

# 5. DATA LOADER
def load_data():
    csv_files = glob.glob('./data_warehouse/final_report/part-*.csv')
    fraud_files = glob.glob('./data_lake/fraud_alerts/*.json')
    num_fraud = len(fraud_files)
    
    if csv_files:
        try:
            return pd.read_csv(csv_files[0]), num_fraud
        except Exception as e:
            st.error(f"Error loading CSV: {e}")
            return None, num_fraud
    else:
        categories = ["Smartphones", "Laptops", "Wireless Earbuds", "Smart Watches", "Gaming Consoles"]
        data = []
        current_min = datetime.now().strftime('%H:%M')
        for cat in categories:
            orders = random.randint(100, 1500)
            price = random.uniform(50.0, 800.0)
            data.append({"Minute": current_min, "product": cat, "Total_Orders": orders, "Total_Revenue": orders * price})
        return pd.DataFrame(data), num_fraud

df, num_fraud_alerts = load_data()

def apply_filter(df, mode):
    if mode == "Top 3 Only":
        return df.nlargest(3, 'Total_Revenue')
    elif mode == "Bottom 3 Only":
        return df.nsmallest(3, 'Total_Revenue')
    return df

# 6. MAIN DASHBOARD LOGIC
if df is None or df.empty:
    st.warning("⏳ Waiting for data... Ensure your data pipeline is running.")
else:
    df_view = apply_filter(df, filter_mode)

    # Calculations
    total_revenue = df_view['Total_Revenue'].sum()
    total_orders  = df_view['Total_Orders'].sum()
    top_product   = df_view.sort_values('Total_Revenue', ascending=False).iloc[0]['product']
    avg_order_val = total_revenue / total_orders if total_orders else 0
    low_product   = df_view.sort_values('Total_Revenue').iloc[0]['product']
    top_rev       = df_view['Total_Revenue'].max()
    goal_pct      = min(total_revenue / alert_threshold * 100, 100)

    # Revenue delta
    delta_rev = 0
    delta_pct = 0.0
    if st.session_state.prev_revenue is not None and st.session_state.prev_revenue > 0:
        delta_rev = total_revenue - st.session_state.prev_revenue
        delta_pct = (delta_rev / st.session_state.prev_revenue) * 100
    st.session_state.prev_revenue = total_revenue

    # Estimated orders/min
    elapsed_mins = max(1, (datetime.now() - (st.session_state.sale_end - timedelta(hours=2))).total_seconds() / 60)
    orders_per_min = total_orders / elapsed_mins

    # Alerts
    if total_revenue > alert_threshold:
        st.markdown(
            f'<div class="alert-banner">🎯 <b>TARGET REACHED:</b> Total revenue <b>${total_revenue:,.2f}</b> has exceeded your threshold of <b>${alert_threshold:,}</b>!</div>',
            unsafe_allow_html=True
        )
        ts = time.strftime('%I:%M:%S %p')
        if not st.session_state.toast_log or st.session_state.toast_log[-1]['msg'] != f"Revenue target ${alert_threshold:,} hit!":
            st.session_state.toast_log.append({"icon": "🎯", "msg": f"Revenue target ${alert_threshold:,} hit!", "time": ts})
        
    if num_fraud_alerts > 0:
        st.markdown(
            f'<div class="fraud-banner">🚨 <b>SECURITY BREACH BLOCKED:</b> PySpark routed <b>{num_fraud_alerts}</b> massive/suspicious transactions to the Fraud Data Lake!</div>',
            unsafe_allow_html=True
        )
        ts = time.strftime('%I:%M:%S %p')
        if not st.session_state.toast_log or st.session_state.toast_log[-1]['msg'] != f"{num_fraud_alerts} fraud alerts intercepted!":
            st.session_state.toast_log.append({"icon": "🚨", "msg": f"{num_fraud_alerts} fraud alerts intercepted!", "time": ts, "danger": True})

    # Top Metrics
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("💰 Total Revenue", f"${total_revenue:,.2f}",
              delta=f"{delta_pct:+.1f}%" if delta_rev != 0 else None)
    c2.metric("📦 Total Orders",  f"{int(total_orders):,}")
    c3.metric("🏆 Top Product",   top_product)
    c4.metric("💵 Avg Order Val", f"${avg_order_val:,.2f}")
    c5.metric("🚨 Fraud Alerts",  num_fraud_alerts)

    st.markdown("<br>", unsafe_allow_html=True)

    ring_col, lb_col, health_col, opm_col = st.columns([1.2, 2, 2, 1.2])

    with ring_col:
        radius   = 52
        stroke   = 10
        circ     = 2 * 3.14159 * radius
        dash_val = circ * goal_pct / 100
        ring_col_hex = "#36E887" if goal_pct >= 100 else ("#FFB800" if goal_pct >= 60 else "#00E5FF")
        st.markdown(f"""
        <div class="progress-ring-wrap">
            <div class="ring-label">Revenue Goal</div>
            <svg width="130" height="130" viewBox="0 0 130 130">
                <circle cx="65" cy="65" r="{radius}" fill="none"
                    stroke="#1F2D4A" stroke-width="{stroke}"/>
                <circle cx="65" cy="65" r="{radius}" fill="none"
                    stroke="{ring_col_hex}" stroke-width="{stroke}"
                    stroke-dasharray="{dash_val:.1f} {circ:.1f}"
                    stroke-linecap="round"
                    transform="rotate(-90 65 65)"/>
                <text x="65" y="60" text-anchor="middle"
                    font-family="Inter,sans-serif" font-size="18" font-weight="900" fill="#FFFFFF">
                    {goal_pct:.0f}%
                </text>
                <text x="65" y="78" text-anchor="middle"
                    font-family="Inter,sans-serif" font-size="9" fill="#94A3B8">
                    of ${alert_threshold:,}
                </text>
            </svg>
        </div>
        """, unsafe_allow_html=True)

    with lb_col:
        st.markdown("<div style='font-size:0.75rem;color:#94A3B8;text-transform:uppercase;letter-spacing:0.07em;font-weight:700;margin-bottom:8px;'>🏅 Revenue Leaderboard</div>", unsafe_allow_html=True)
        top3 = df_view.sort_values('Total_Revenue', ascending=False).head(3).reset_index(drop=True)
        medals = ["🥇", "🥈", "🥉"]
        bar_colors = ["#FFD700", "#C0C0C0", "#CD7F32"]
        max_r = top3['Total_Revenue'].max()
        for i, row in top3.iterrows():
            bar_w = int(row['Total_Revenue'] / max_r * 100)
            st.markdown(f"""
            <div class="lb-row">
                <span class="lb-rank">{medals[i]}</span>
                <span class="lb-name">{row['product']}</span>
                <div class="lb-bar-bg">
                    <div class="lb-bar-fill" style="width:{bar_w}%;background:{bar_colors[i]};"></div>
                </div>
                <span class="lb-rev">${row['Total_Revenue']:,.0f}</span>
            </div>
            """, unsafe_allow_html=True)

    with health_col:
        st.markdown("<div style='font-size:0.75rem;color:#94A3B8;text-transform:uppercase;letter-spacing:0.07em;font-weight:700;margin-bottom:8px;'>🌡️ Category Health</div>", unsafe_allow_html=True)
        rev_series = df_view.set_index('product')['Total_Revenue']
        p75 = rev_series.quantile(0.75)
        p25 = rev_series.quantile(0.25)

        health_rows = ""
        for prod, rev in rev_series.sort_values(ascending=False).items():
            if rev >= p75:
                pill = '<span class="health-pill health-hot">🔥 Hot</span>'
            elif rev >= p25:
                pill = '<span class="health-pill health-warm">⚡ Warm</span>'
            else:
                pill = '<span class="health-pill health-cold">❄️ Cold</span>'
            health_rows += f"""
            <div style="display:flex;align-items:center;justify-content:space-between;
                        padding:9px 14px;background:#111827;border:1px solid #1F2D4A;
                        border-radius:8px;margin-bottom:6px;">
                <span style="font-size:0.85rem;color:#E2E8F0;">{prod}</span>
                {pill}
            </div>"""
        st.markdown(health_rows, unsafe_allow_html=True)

    with opm_col:
        opm_max   = max(orders_per_min * 1.5, 10)
        opm_pct   = min(orders_per_min / opm_max * 100, 100)
        opm_color = "#36E887" if opm_pct > 60 else ("#FFB800" if opm_pct > 30 else "#FF4444")
        st.markdown(f"""
        <div class="progress-ring-wrap">
            <div class="ring-label">Orders / Min</div>
            <svg width="130" height="130" viewBox="0 0 130 130">
                <circle cx="65" cy="65" r="52" fill="none" stroke="#1F2D4A" stroke-width="10"/>
                <circle cx="65" cy="65" r="52" fill="none"
                    stroke="{opm_color}" stroke-width="10"
                    stroke-dasharray="{326.7 * opm_pct / 100:.1f} 326.7"
                    stroke-linecap="round"
                    transform="rotate(-90 65 65)"/>
                <text x="65" y="60" text-anchor="middle"
                    font-family="Inter,sans-serif" font-size="18" font-weight="900" fill="#FFFFFF">
                    {orders_per_min:.0f}
                </text>
                <text x="65" y="78" text-anchor="middle"
                    font-family="Inter,sans-serif" font-size="9" fill="#94A3B8">
                    orders/min
                </text>
            </svg>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    events = []
    for _, row in df_view.iterrows():
        events.append(f'<span class="ticker-hi">📦 {row["product"]}</span>: ${row["Total_Revenue"]:,.0f} revenue · {int(row["Total_Orders"])} orders')
    events.append(f'<span class="ticker-warn">🚨 {num_fraud_alerts} Fraud Alerts Intercepted</span>')
    events.append(f'<span class="ticker-hi">🎯 Goal: {goal_pct:.1f}% complete</span>')
    events.append(f'Avg Order Value: <span class="ticker-hi">${avg_order_val:,.2f}</span>')
    ticker_content = '<span class="ticker-sep">|</span>'.join(events)
    st.markdown(f"""
    <div class="ticker-wrap">
        <span class="ticker-inner">
            &nbsp;&nbsp;&nbsp;&nbsp;{ticker_content}&nbsp;&nbsp;&nbsp;&nbsp;{ticker_content}
        </span>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Revenue Analytics",
        "📋 Live Data Table",
        "🔍 AI Insights",
        "🟥 Revenue Heatmap",
        "🔔 Notifications"
    ])

    with tab1:
        st.subheader("⏱️ Sales Velocity (Revenue per Minute)")
        if 'Minute' in df_view.columns:
            velocity_df = df_view.groupby('Minute')['Total_Revenue'].sum()
            st.line_chart(velocity_df, color="#FF007F", use_container_width=True)
        else:
            st.info("Waiting for PySpark time window data...")
        
        st.markdown("---")
        
        chart_col, orders_col = st.columns([3, 2])
        chart_df = df_view.groupby('product')['Total_Revenue'].sum()
        
        with chart_col:
            st.caption("ALL-TIME REVENUE BY PRODUCT")
            if chart_type == "Bar":
                st.bar_chart(chart_df, color="#00E5FF", use_container_width=True)
            elif chart_type == "Line":
                st.line_chart(chart_df, color="#00E5FF", use_container_width=True)
            else:
                st.area_chart(chart_df, color="#00E5FF", use_container_width=True)
                
        with orders_col:
            st.caption("ORDER VOLUME BY PRODUCT")
            orders_df = df_view.groupby('product')['Total_Orders'].sum()
            st.bar_chart(orders_df, color="#7B5EFF", use_container_width=True)

        st.markdown("---")
        st.subheader("🔵 Revenue vs Orders (Efficiency Scatter)")
        scatter_data = df_view[['product','Total_Revenue','Total_Orders']].set_index('product')
        st.scatter_chart(scatter_data, x='Total_Orders', y='Total_Revenue', use_container_width=True)

    with tab2:
        st.caption("GOLD LAYER - AGGREGATED FEED")
        display_df = df_view.sort_values(sort_by, ascending=sort_asc).copy()
        display_df['Rev_Share_%'] = (display_df['Total_Revenue'] / total_revenue * 100).round(1)
        display_df['Rev_Per_Order'] = (display_df['Total_Revenue'] / display_df['Total_Orders']).round(2)

        st.dataframe(
            display_df, use_container_width=True, hide_index=True,
            column_config={
                "Total_Revenue":  st.column_config.ProgressColumn("Revenue ($)", format="$%.2f", min_value=0, max_value=float(top_rev)),
                "Total_Orders":   st.column_config.NumberColumn("Orders", format="%d"),
                "Rev_Share_%":    st.column_config.NumberColumn("Revenue Share (%)", format="%.1f%%"),
                "Rev_Per_Order":  st.column_config.NumberColumn("Rev / Order ($)", format="$%.2f"),
                "Minute":         st.column_config.TextColumn("Time Window (HH:MM)")
            }
        )

        st.markdown("---")
        ss1, ss2, ss3 = st.columns(3)
        ss1.metric("📈 Highest Rev/Order", f"${display_df['Rev_Per_Order'].max():,.2f}", help="Best margin product")
        ss2.metric("📉 Lowest Rev/Order",  f"${display_df['Rev_Per_Order'].min():,.2f}", help="Lowest margin product")
        ss3.metric("🔢 Products Tracked",  len(display_df))

        dl_csv = display_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="⬇️ Download Snapshot CSV",
            data=dl_csv,
            file_name=f"sales_snapshot_{int(time.time())}.csv",
            mime="text/csv"
        )

    with tab3:
        ins1, ins2 = st.columns(2)
        with ins1:
            st.markdown(f"""
            <div class="insight-card">
                <div class="insight-title">🏆 Best Performer</div>
                <div class="insight-body"><b>{top_product}</b> is dominating with <b>${top_rev:,.2f}</b> in revenue. Ensure inventory levels can support the demand.</div>
            </div>
            <div class="insight-card">
                <div class="insight-title">🛡️ Security Check</div>
                <div class="insight-body">Real-time PySpark routing has intercepted <b>{num_fraud_alerts}</b> massive transactions over $2,500 for manual review.</div>
            </div>
            <div class="insight-card">
                <div class="insight-title">⏱️ Velocity Insight</div>
                <div class="insight-body">The system is processing approximately <b>{orders_per_min:.1f} orders/minute</b>. At this rate, projected hourly volume is <b>{int(orders_per_min*60):,}</b> orders.</div>
            </div>
            """, unsafe_allow_html=True)
        with ins2:
            st.markdown(f"""
            <div class="insight-card">
                <div class="insight-title">📊 Revenue Concentration</div>
                <div class="insight-body">The top product alone accounts for <b>{top_rev/total_revenue*100:.1f}%</b> of total flash sale revenue.</div>
            </div>
            <div class="insight-card">
                <div class="insight-title">🛒 Basket Analytics</div>
                <div class="insight-body">Customers are spending an average of <b>${avg_order_val:,.2f}</b> per transaction across all completed checkouts.</div>
            </div>
            <div class="insight-card">
                <div class="insight-title">❄️ Underperformer Alert</div>
                <div class="insight-body"><b>{low_product}</b> is the lowest revenue product this cycle. Consider applying additional discounts or highlighting it in push notifications.</div>
            </div>
            """, unsafe_allow_html=True)

    with tab4:
        st.subheader("🟥 Revenue Intensity Heatmap")
        st.caption("Color intensity represents revenue share — darker = higher contribution")

        heatmap_df = df_view[['product','Total_Revenue']].copy()
        heatmap_df['share'] = heatmap_df['Total_Revenue'] / heatmap_df['Total_Revenue'].sum()

        def rev_to_color(share):
            r = int(255 * share * 2.5)
            g = int(180 * (1 - share))
            b = 80
            return f"#{min(r,255):02X}{min(g,255):02X}{b:02X}"

        cols_per_row = 3
        products = heatmap_df.sort_values('share', ascending=False).to_dict('records')
        rows = [products[i:i+cols_per_row] for i in range(0, len(products), cols_per_row)]

        for row in rows:
            cols = st.columns(cols_per_row)
            for col_idx, item in enumerate(row):
                bg = rev_to_color(item['share'])
                cols[col_idx].markdown(f"""
                <div class="heatmap-cell" style="background:{bg}; margin-bottom:10px;">
                    <div style="font-size:0.85rem;margin-bottom:4px;">{item['product']}</div>
                    <div style="font-size:1.2rem;font-weight:900;">${item['Total_Revenue']:,.0f}</div>
                    <div style="font-size:0.7rem;opacity:0.85;">{item['share']*100:.1f}% share</div>
                </div>
                """, unsafe_allow_html=True)
            if len(row) < cols_per_row:
                for _ in range(cols_per_row - len(row)):
                    cols[len(row) + _].empty()

        st.markdown("---")
        st.subheader("📐 Revenue Distribution")
        dist_df = heatmap_df.set_index('product')['Total_Revenue']
        st.bar_chart(dist_df, color="#FF007F", use_container_width=True)

    with tab5:
        st.subheader("🔔 Event Notification Center")
        st.caption("Live-updated log of system alerts, threshold crossings, and fraud intercepts")

        pulse_events = []
        now_str = time.strftime('%I:%M:%S %p')

        pulse_events.append({
            "icon": "💹",
            "msg": f"Cycle update: ${total_revenue:,.2f} total revenue · {int(total_orders):,} orders",
            "time": now_str,
            "color": "#36E887"
        })
        if delta_rev > 0:
            pulse_events.append({
                "icon": "📈",
                "msg": f"Revenue up {delta_pct:+.1f}% vs last cycle (+${delta_rev:,.2f})",
                "time": now_str,
                "color": "#36E887"
            })
        elif delta_rev < 0:
            pulse_events.append({
                "icon": "📉",
                "msg": f"Revenue down {delta_pct:.1f}% vs last cycle (${delta_rev:,.2f})",
                "time": now_str,
                "color": "#FF4444"
            })
        if num_fraud_alerts > 0:
            pulse_events.append({
                "icon": "🚨",
                "msg": f"Security: {num_fraud_alerts} suspicious transactions blocked by PySpark",
                "time": now_str,
                "color": "#FF4444"
            })
        if goal_pct >= 100:
            pulse_events.append({
                "icon": "🎯",
                "msg": f"TARGET ACHIEVED! Revenue goal of ${alert_threshold:,} surpassed!",
                "time": now_str,
                "color": "#FFB800"
            })

        if not pulse_events:
            st.info("No notifications yet. Events will appear here as thresholds are crossed.")
        else:
            for ev in reversed(pulse_events):
                st.markdown(f"""
                <div style="background:#111827;border:1px solid {ev['color']};border-left:4px solid {ev['color']};
                            border-radius:10px;padding:12px 18px;margin-bottom:10px;
                            display:flex;align-items:center;gap:12px;">
                    <span style="font-size:1.2rem;">{ev['icon']}</span>
                    <span style="font-size:0.85rem;color:#E2E8F0;flex:1;">{ev['msg']}</span>
                    <span style="font-size:0.72rem;color:#94A3B8;white-space:nowrap;">{ev['time']}</span>
                </div>
                """, unsafe_allow_html=True)

        if st.session_state.toast_log:
            st.markdown("---")
            st.caption("📁 SESSION ALERT HISTORY")
            for ev in reversed(st.session_state.toast_log[-10:]):
                color = "#FF4444" if ev.get("danger") else "#FFB800"
                st.markdown(f"""
                <div style="background:#0D1120;border:1px solid {color};border-radius:8px;
                            padding:10px 16px;margin-bottom:8px;display:flex;gap:12px;align-items:center;">
                    <span style="font-size:1rem;">{ev['icon']}</span>
                    <span style="font-size:0.8rem;color:#94A3B8;flex:1;">{ev['msg']}</span>
                    <span style="font-size:0.7rem;color:#475569;">{ev['time']}</span>
                </div>
                """, unsafe_allow_html=True)

    st.markdown("---")
    st.caption(f"⏱ **Last updated:** {time.strftime('%I:%M:%S %p')} | Auto-refreshing every {refresh_interval} seconds.")

# 7. NATIVE AUTO-REFRESH
time.sleep(refresh_interval)
st.rerun()