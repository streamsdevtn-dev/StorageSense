import sqlite3
import os
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from prefect import task, flow

class AgentBBehavioral:
    """
    Tier 2 Agent: Flash Catalysts & Transitional Action
    Monitors 100% free, highly predictive behavioral signals:
    FEMA Declarations and National Weather Service Severe Alerts.
    """
    def __init__(self, db_path):
        self.db_path = db_path
        
    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10))
    def fetch_fema_disasters(self, state_abbr, county_name):
        """OpenFEMA API: Checks if there's a recent disaster declaration."""
        try:
            # Look back over recent declarations (free, open endpoint)
            url = f"https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries?$filter=state eq '{state_abbr}'"
            res = requests.get(url).json()
            # In a full implementation, we filter by county and incidentBeginDate > 90 days ago.
            # Here we simulate finding a match if results returned have this county.
            if "DisasterDeclarationsSummaries" in res:
                for d in res["DisasterDeclarationsSummaries"]:
                    if county_name.lower() in d.get("designatedArea", "").lower():
                        return True
            return False
        except Exception as e:
            print(f"FEMA API Error: {e}")
            return False

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
    def fetch_severe_weather(self, lat, lon):
        """National Weather Service (NWS) API: Active Severe Alerts."""
        # Using free weather.gov API to check for flooding, tornadoes, etc.
        try:
            headers = {"User-Agent": "StorageSenseDataPipeline/1.0"}
            url = f"https://api.weather.gov/alerts/active?point={lat},{lon}"
            res = requests.get(url, headers=headers).json()
            if "features" in res and len(res["features"]) > 0:
                for feature in res["features"]:
                    severity = feature["properties"]["severity"]
                    if severity in ["Extreme", "Severe"]:
                        return True
            return False
        except Exception as e:
            print(f"NWS Alert Parsing Error: {e}")
            return False
            
    def get_geography_data(self):
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute("SELECT zip_code, state_abbr, city, lat, lon FROM geography").fetchall()
        
    def execute(self):
        print("Agent B (Behavioral): Scanning FEMA and NWS for Flash Catalysts...")
        geos = self.get_geography_data()
        
        with sqlite3.connect(self.db_path) as conn:
            # We insert severe_weather into the field where google trends used to be,
            # we will re-configure the database later but for now we map 1=100, 0=0 to keep the PR small.
            for z, st, city, lat, lon in geos:
                fema = self.fetch_fema_disasters(st, city) 
                weather = self.fetch_severe_weather(lat, lon)
                
                # mock net migration
                usps_migration = 15 
                
                weather_mock_index = 100 if weather else 0
                
                conn.execute('''
                    INSERT INTO behavioral_signals 
                    (zip_code, date, usps_net_migration, google_search_index, fema_disaster_active)
                    VALUES (?, DATE('now'), ?, ?, ?)
                ''', (z, usps_migration, weather_mock_index, fema))
                
        print("Agent B: Flash Catalysts recorded securely.")

@task
def run_agent_b(db_loc):
    agent = AgentBBehavioral(db_loc)
    agent.execute()

@flow(name="Agent B: Flash Catalysts")
def agent_b_flow(db_loc: str):
    run_agent_b(db_loc)

if __name__ == "__main__":
    db_loc = os.path.join(os.path.dirname(__file__), "..", "db", "storagesense.db")
    agent_b_flow(db_loc)
