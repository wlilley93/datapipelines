#!/usr/bin/env python3
"""
Batch script to run all gen scripts and create markdown outputs.
"""
import subprocess
import sys
from pathlib import Path

def run_all_gen_scripts():
    """Run all gen scripts to create markdown outputs."""
    # Get the directory where this script is located
    script_dir = Path(__file__).parent
    # The batch script doesn't write files directly, so we don't need this mkdir
    # Individual scripts handle creating the directory as needed
    
    # List of all gen scripts to run
    gen_scripts = [
        "gen_fireflies_md.py",
        "gen_hubspot_md.py",
        "gen_intercom_md.py",
        "gen_pandadoc_md.py",
        "gen_trello_md.py",
        "gen_webapp_md.py",
        "gen_xero_md.py",
        "gen_orchestrator_md.py",
        "gen_wassenger_md.py"
    ]
    
    print("Starting generation of all markdown outputs...")
    
    for script in gen_scripts:
        script_path = script_dir / script
        if script_path.exists():
            print(f"Running {script}...")
            try:
                result = subprocess.run([sys.executable, str(script_path)], 
                                      capture_output=True, text=True, check=True)
                print(f"Successfully generated markdown from {script}")
            except subprocess.CalledProcessError as e:
                print(f"Error running {script}: {e}")
                print(f"Error output: {e.stderr}")
        else:
            print(f"Script {script} not found, skipping...")
    
    print("All generation scripts have been executed.")

if __name__ == "__main__":
    run_all_gen_scripts()