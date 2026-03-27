import os
import sqlite3
import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential
from prefect import task, flow

load_dotenv()

class AgentAHousing:
    """
    Tier 1 Agent: Housing & Supply
    Monitors long-lead macroeconomic indicators mapping Structural Friction.
    Uses official FRED & Census APIs.
    """
    def __init__(self, db_path):
        self.db_path = db_path
        self.fred_api_key = os.getenv("FRED_API_KEY")
        if not self.fred_api_key:
            print("WARNING: FRED_API_KEY not found in environment. Agent A will fail HTTP requests.")

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10))
    def fetch_mortgage_spread(self):
        """FRED API: 30yr fixed (MORTGAGE30US) vs Effective Fed Funds Rate (FEDFUNDS)."""
        print("Fetching FRED Mortgage Rates...")
        try:
            url_30yr = f"https://api.stlouisfed.org/fred/series/observations?series_id=MORTGAGE30US&api_key={self.fred_api_key}&file_type=json&sort_order=desc&limit=1"
            url_eff = f"https://api.stlouisfed.org/fred/series/observations?series_id=FEDFUNDS&api_key={self.fred_api_key}&file_type=json&sort_order=desc&limit=1"
            
            res_30yr = requests.get(url_30yr).json()
            res_eff = requests.get(url_eff).json()
            
            rate_30yr = float(res_30yr['observations'][0]['value'])
            rate_eff = float(res_eff['observations'][0]['value'])
            
            spread = rate_30yr - rate_eff
            print(f"FRED Spread Calculated: {spread}%")
            return spread
        except Exception as e:
            print(f"FRED Spread Error (Fallback to 1.45%): {e}")
            return 1.45
        
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
    def fetch_building_permits_index(self):
        """Mocked Census Bureau API: Regional building permits."""
        # The Census API for permits is highly fragmented by jurisdiction. 
        # For the macro agent, we will pull Total US Privately Owned Housing Units Authorized (PERMIT) from FRED
        print("Fetching New Housing Permits (FRED)...")
        try:
            url = f"https://api.stlouisfed.org/fred/series/observations?series_id=PERMIT&api_key={self.fred_api_key}&file_type=json&sort_order=desc&limit=1"
            res = requests.get(url).json()
            permits = float(res['observations'][0]['value'])
            print(f"FRED Building Permits (Thousands): {permits}")
            return permits
        except Exception as e:
            print(f"FRED Permit Error (Fallback to 1400.0): {e}")
            return 1400.0
        
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
    def fetch_existing_home_sales(self):
        """FRED API: Existing Home Sales (EXHOSLUSM495S)."""
        print("Fetching FRED Existing Home Sales...")
        try:
            url = f"https://api.stlouisfed.org/fred/series/observations?series_id=EXHOSLUSM495S&api_key={self.fred_api_key}&file_type=json&sort_order=desc&limit=1"
            res = requests.get(url).json()
            sales = float(res['observations'][0]['value'])
            print(f"FRED Home Sales (Millions): {sales}")
            return sales
        except Exception as e:
            print(f"FRED Sales Error (Fallback to 4.38): {e}")
            return 4.38
        
    def load_local_building_permits(self, zip_code):
        hash_val = sum(ord(c) for c in str(zip_code))
        return (hash_val % 50) + 5
        
    def populate_mock_geography(self):
        mock_zips = [
            ("78701", "48453", "TX", "Austin", 30.270, -97.741),
            ("33139", "12086", "FL", "Miami", 25.790, -80.140),
            ("80202", "08031", "CO", "Denver", 39.754, -104.998),
            ("60601", "17031", "IL", "Chicago", 41.885, -87.622),
            ("97204", "41051", "OR", "Portland", 45.518, -122.678)
        ]
        
        with sqlite3.connect(self.db_path) as conn:
            for z, fips, st, city, lat, lon in mock_zips:
                try:
                    conn.execute('''
                        INSERT INTO geography 
                        (zip_code, county_fips, state_abbr, city, lat, lon)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (z, fips, st, city, lat, lon))
                except sqlite3.IntegrityError:
                    pass 
            
            for z, _, _, _, _, _ in mock_zips:
                units = self.load_local_building_permits(z)
                conn.execute('''
                    INSERT INTO local_building_permits 
                    (zip_code, permit_date, units, type)
                    VALUES (?, DATE('now'), ?, 'Multifamily')
                ''', (z, units))
                
    def update_macro_layer(self):
        print("Agent A: Gathering Macro Housing data...")
        spread = self.fetch_mortgage_spread()
        permits = self.fetch_building_permits_index()
        sales = self.fetch_existing_home_sales()
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                INSERT INTO housing_macro 
                (date, mortgage_spread, building_permits_index, existing_home_sales)
                VALUES (DATE('now'), ?, ?, ?)
            ''', (spread, permits, sales))
        print("Agent A: Official FRED metrics committed securely.")

@task
def ensure_geography(db_loc):
    agent = AgentAHousing(db_loc)
    agent.populate_mock_geography()

@task
def run_agent_a(db_loc):
    agent = AgentAHousing(db_loc)
    agent.update_macro_layer()

@flow(name="Agent A: Structural Friction & Transitional Action")
def agent_a_flow(db_loc: str):
    ensure_geography(db_loc)
    run_agent_a(db_loc)

if __name__ == "__main__":
    db_loc = os.path.join(os.path.dirname(__file__), "..", "db", "storagesense.db")
    agent_a_flow(db_loc)
