import os
import sys

# Add the project root to the Python path to resolve 'src' module imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.pipeline import data_pipeline
#from src.tools import eda_analysis
from src.config import settings

def print_step(step_name):
    print(f"\n{'='*50}")
    print(f" RUNNING: {step_name}")
    print(f"{'='*50}")

if __name__ == "__main__":
    try:
        # 1. Execute Full Medallion ETL Pipeline
        print_step("Running Full ETL Pipeline (Silver -> Gold)")
        data_pipeline.run_etl()
        
        print("\nAll pipeline phases executed successfully")
    except Exception as e:
        print(f"\n Pipeline failed: {e}")
        sys.exit(1)