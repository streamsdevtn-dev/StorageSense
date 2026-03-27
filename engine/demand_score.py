import sqlite3
import pandas as pd
import os
from prefect import task, flow

def calculate_all_scores(db_path):
    print("Engine: Computing Pricing Power via Catalysts, Action, and Friction...")
    try:
        with sqlite3.connect(db_path) as conn:
            # We construct a query picking up the latest parameters for 
            # Catalyst (Immediate Spikes)
            # Action (Relocation/Movement)
            # Friction (Supply Constraints & Wealth)
            
            query = """
                SELECT 
                    g.zip_code,
                    h.existing_home_sales,
                    h.mortgage_spread,
                    b.google_search_index,
                    b.fema_disaster_active,
                    b.usps_net_migration,
                    s.irs_migration_wealth_idx,
                    (SELECT SUM(units) FROM local_building_permits WHERE zip_code = g.zip_code) as competition_units
                FROM geography g
                LEFT JOIN (SELECT * FROM housing_macro ORDER BY date DESC LIMIT 1) h ON 1=1
                LEFT JOIN (SELECT * FROM behavioral_signals GROUP BY zip_code HAVING MAX(date)) b ON g.zip_code = b.zip_code
                LEFT JOIN structural_anchors s ON g.zip_code = s.zip_code
            """
            
            df = pd.read_sql_query(query, conn)
            
            if df.empty:
                print("Engine: No data available to score.")
                return
                
            df = df.drop_duplicates(subset=['zip_code'])
            
            # The Revised Algorithm
            for _, row in df.iterrows():
                z = row['zip_code']
                
                # 1. Flash Catalysts (Multipliers: 1.0 = Base, >1.0 = Catalyst Spike)
                catalyst_multiplier = 1.0
                if row.get('fema_disaster_active') == 1:
                    catalyst_multiplier += 0.40  # Massive immediate demand
                search_index = float(row.get('google_search_index', 50) or 50)
                if search_index > 80:
                    catalyst_multiplier += 0.15  # Spiking search intent
                    
                # 2. Transitional Action (Base Demand: 0.0 - 1.0)
                # Driven by USPS Migration (positive net migration) and home sales turnover
                usps = float(row.get('usps_net_migration', 0) or 0)
                sales = float(row.get('existing_home_sales', 4.0) or 4.0)
                
                action_score = 0.5 # start neutral
                action_score += (usps / 1000.0) # add migration delta
                action_score += ((sales - 4.0) * 0.1) # normalized around 4M sales
                action_score = min(max(action_score, 0), 1) # Bounded
                
                # 3. Structural Friction (Pricing Power Constraint: 0.0 - 1.0)
                # High friction means supply is constrained relative to wealth inbound
                spread = float(row.get('mortgage_spread', 1.0) or 1.0)
                wealth = float(row.get('irs_migration_wealth_idx', 1.0) or 1.0)
                competition = float(row.get('competition_units', 10) or 10)
                
                # Higher wealth + higher spread (lock-in) + low competition = High Friction
                friction_score = 0.5 
                friction_score += (wealth - 1.0) * 0.5 
                friction_score += (spread - 1.0) * 0.2
                friction_score -= (competition / 200.0)
                friction_score = min(max(friction_score, 0), 1)
                
                # Engine Final Output
                # Base is Action + Friction (equally weighted), multiplied by Catalyst
                base_demand = (action_score * 0.5) + (friction_score * 0.5)
                final_score = base_demand * catalyst_multiplier
                final_score = min(max(final_score, 0), 1) 
                
                conn.execute('''
                    INSERT OR REPLACE INTO demand_scores 
                    (zip_code, liquidity_score_l, movement_score_m, structural_score_s, competition_score_c, final_score)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (z, action_score, catalyst_multiplier, friction_score, competition, final_score))
                
        print("Engine: Pricing Power updated securely.")
    except Exception as e:
        print(f"Engine Error: {e}")

@task
def run_engine_task(db_loc):
    calculate_all_scores(db_loc)

@flow(name="StorageSense Engine: Scoring & Pricing Power")
def run_engine_flow(db_loc: str):
    run_engine_task(db_loc)

if __name__ == "__main__":
    db_loc = os.path.join(os.path.dirname(__file__), "..", "db", "storagesense.db")
    run_engine_flow(db_loc)
