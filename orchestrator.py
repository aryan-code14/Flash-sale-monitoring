import time
import subprocess
import glob
import os
import pandas as pd
from datetime import datetime

# Run the batch job every 60 seconds
RUN_INTERVAL_SECONDS = 60 

print("========================================")
print("🚀 STARTING DATA PIPELINE ORCHESTRATOR & ALERT ENGINE")
print(f"⏱️  Batch Job scheduled every {RUN_INTERVAL_SECONDS} seconds.")
print("🛑 Press Ctrl+C to stop.")
print("========================================")

try:
    while True:
        current_time = datetime.now().strftime("%H:%M:%S")
        print(f"\n[{current_time}] Waking up. Triggering Batch ETL Job...")

        # This runs your batch_etl.py script automatically
        result = subprocess.run(["python", "batch_etl.py"], capture_output=True, text=True)

        if result.returncode == 0:
            print(f"[{current_time}] ✅ Batch Job Completed Successfully!")
            print("Data Warehouse has been updated with the latest numbers.")
            
            # ---------------------------------------------------------
            # 🌟 NEW FEATURE: AUTOMATED SYSTEM ALERTS
            # ---------------------------------------------------------
            # 1. Check for Revenue Milestones
            csv_files = glob.glob('./data_warehouse/final_report/part-*.csv')
            if csv_files:
                try:
                    df = pd.read_csv(csv_files[0])
                    total_rev = df['Total_Revenue'].sum()
                    
                    # Print a massive alert if we cross half a million dollars
                    if total_rev > 500000:
                        print("🚨 [URGENT ALERT] 🚨 : FLASH SALE CROSSED $500,000 MILESTONE!")
                except Exception as e:
                    pass # Safely ignore if the file is currently locked
                    
            # 2. Check for Fraudulent Transactions in the Data Lake
            fraud_files = glob.glob('./data_lake/fraud_alerts/*.json')
            if len(fraud_files) > 0:
                print(f"⚠️ [SECURITY ALERT] : {len(fraud_files)} Suspicious Transactions Detected in Data Lake!")
                
        else:
            print(f"[{current_time}] ❌ ERROR in Batch Job:")
            print(result.stderr)

        print(f"Sleeping for {RUN_INTERVAL_SECONDS} seconds...")
        time.sleep(RUN_INTERVAL_SECONDS)

except KeyboardInterrupt:
    print("\nOrchestrator stopped by user. Shutting down gracefully.")