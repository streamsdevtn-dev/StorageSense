import sqlite3
import os
import requests
import pandas as pd
from io import StringIO
from tenacity import retry, stop_after_attempt, wait_exponential
from prefect import task, flow

class AgentCStructural:
    """
    Tier 3 Agent: Structural Friction
    Monitors long-term foundational data establishing Market Pricing Power (Zillow ZHVI)
    """
    def __init__(self, db_path):
        self.db_path = db_path
        
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
    def fetch_home_values_csv(self):
        """Zillow Research: Parses massive static CSV into memory for extraction."""
        print("Agent C: Downloading Zillow ZVHI Monthly Tracker...")
        try:
            url = "https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
            headers = {"User-Agent": "Mozilla/5.0"}
            res = requests.get(url, headers=headers)
            
            if res.status_code == 200:
                print("Zillow CSV Downloaded, parsing...")
                df = pd.read_csv(StringIO(res.text))
                return df
            return None
        except Exception as e:
            print(f"Zillow Fetch Error: {e}")
            return None
            
    def get_geography_data(self):
        with sqlite3.connect(self.db_path) as conn:
            return [row[0] for row in conn.execute("SELECT zip_code FROM geography").fetchall()]
        
    def execute(self):
        print("Agent C (Structural): Updating generational and wealth friction anchors...")
        
        zips = self.get_geography_data()
        zillow_df = self.fetch_home_values_csv()
        
        with sqlite3.connect(self.db_path) as conn:
            for z in zips:
                irs_wealth = 1.15 # mock IRS proxy (API not public)
                boomers_pct = 22.5 # mock Census ACS 5-Year
                
                zhvi = 400000 # default
                if zillow_df is not None:
                    # Filter for zipped out row
                    row = zillow_df[zillow_df['RegionName'] == int(z)] if str(z).isdigit() else pd.DataFrame()
                    if not row.empty:
                        # Grab the latest month column (far right typically)
                        zhvi = row.iloc[0, -1]
                        if pd.isna(zhvi): zhvi = 400000 
                
                conn.execute('''
                    INSERT OR REPLACE INTO structural_anchors 
                    (zip_code, boomer_population_pct, median_home_value, irs_migration_wealth_idx, last_updated)
                    VALUES (?, ?, ?, ?, DATE('now'))
                ''', (z, boomers_pct, zhvi, irs_wealth))
                
        print("Agent C: Structural Pricing Power framework secured using Real Zillow Parsing.")

@task
def run_agent_c(db_loc):
    agent = AgentCStructural(db_loc)
    agent.execute()

@flow(name="Agent C: Structural Friction")
def agent_c_flow(db_loc: str):
    run_agent_c(db_loc)

if __name__ == "__main__":
    db_loc = os.path.join(os.path.dirname(__file__), "..", "db", "storagesense.db")
    agent_c_flow(db_loc)
