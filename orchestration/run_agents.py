import sys
import os

# Add parent dir to path so we can import agents
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from agents.agent_a_housing import agent_a_flow
from agents.agent_b_behavioral import agent_b_flow
from agents.agent_c_structural import agent_c_flow
from engine.demand_score import run_engine_flow

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "db", "storagesense.db")

def run_all_pipelines():
    print("====================================")
    print("Initiating StorageSense Data Pipeline")
    print("====================================\n")
    
    # Execute Prefect Flows
    agent_a_flow(DB_PATH)
    agent_b_flow(DB_PATH)
    agent_c_flow(DB_PATH)
    
    # Compute Final Pricing Power
    run_engine_flow(DB_PATH)
    
    print("\n====================================")
    print("Pipeline Complete. Data Ready.")
    print("====================================")

if __name__ == "__main__":
    run_all_pipelines()
