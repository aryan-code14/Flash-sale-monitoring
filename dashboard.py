import streamlit as st
import pandas as pd
import glob
import time
import os

# 1. Page Configuration (Must be the very first line!)
st.set_page_config(page_title="Live Sales Dashboard", page_icon="⚡", layout="wide")

# 2.  CUSTOM CSS FOR DARK MODE AND COOL UI
st.markdown("""
<style>
    /* Force the main background to be completely black */
    .stApp {
        background-color: #050505;
        color: #FFFFFF;
    }
    
    /* Make the top headers look cool and Neon Blue */
    h1, h2, h3 {
        color: #00E5FF !important; 
        font-family: 'Arial Black', sans-serif;
    }
    
    /* Style the Metric Boxes (Revenue, Orders, etc.) */
    div[data-testid="metric-container"] {
        background-color: #111111;
        border: 1px solid #00E5FF;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0px 0px 15px rgba(0, 229, 255, 0.2); /* Neon glow effect */
    }
    
    /* Force text inside metric boxes to be white */
    div[data-testid="metric-container"] label {
        color: #CCCCCC !important;
    }
    div[data-testid="metric-container"] div {
        color: #FFFFFF !important;
    }
</style>
""", unsafe_allow_html=True)

# 3. Dashboard Headers
st.title("⚡ Flash Sale Monitoring System")
st.markdown("### Topic 80: End-to-End Data Engineering Architecture")
st.markdown("---")

# 4. Create a placeholder that we will constantly refresh
placeholder = st.empty()

# 5. The Live Loop!
while True:
    # Look for both the normal sales data AND the new fraud alerts
    csv_files = glob.glob('./data_warehouse/final_report/part-*.csv')
    fraud_files = glob.glob('./data_lake/fraud_alerts/*.json')
    
    with placeholder.container():
        if not csv_files:
            st.warning("⏳ Waiting for data pipeline to boot... (Make sure orchestrator.py is running!)")
        else:
            try:
                # Read the latest clean data
                df = pd.read_csv(csv_files[0])
                
                # Calculate Grand Totals
                total_revenue = df['Total_Revenue'].sum()
                total_orders = df['Total_Orders'].sum()
                
                # Safely get the top product
                if not df.empty:
                    top_product = df.groupby('product')['Total_Revenue'].sum().idxmax()
                else:
                    top_product = "N/A"
                
                # Draw 4 Metric Boxes at the top (Now including Fraud!)
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Total Revenue", f"${total_revenue:,.2f}")
                col2.metric("Total Orders", total_orders)
                col3.metric("Top Product", top_product)
                col4.error(f"🚨 Fraud Alerts: {len(fraud_files)} Flagged")
                
                st.markdown("---")
                
                # Draw the interactive Charts
                col_chart1, col_chart2 = st.columns(2)
                
                with col_chart1:
                    st.subheader("Velocity: Revenue Over Time")
                    if 'Minute' in df.columns:
                        time_df = df.groupby('Minute')['Total_Revenue'].sum()
                        st.line_chart(time_df, color="#00E5FF")
                    else:
                        st.info("Waiting for time window data...")
                    
                with col_chart2:
                    st.subheader("All-Time Revenue by Product")
                    prod_df = df.groupby('product')['Total_Revenue'].sum()
                    st.bar_chart(prod_df, color="#FF007F")
                
                st.caption(f"Last updated: {time.strftime('%I:%M:%S %p')} | Auto-refresh active")
                
            except Exception as e:
                st.warning("🔄 Reading new data from warehouse, please wait a moment...")
            
    # Wait 5 seconds before refreshing the page
    time.sleep(5)
    