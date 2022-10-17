"""
This file contains utility functions that are used by the FileProc
"""
import yaml

# Initialize constants to be parsed from vars.yaml
MISSION_NAME = ""
INSTR_NAMES = []
MISSION_PKG = ""

# Read YAML file and parse variables
try:
    with open("vars.yaml", "r") as f:
        vars = yaml.safe_load(f)
        MISSION_NAME = vars["MISSION_NAME"]
        INSTR_NAMES = vars["INSTR_NAMES"]
        MISSION_PKG = vars["MISSION_PKG"]
        
except FileNotFoundError:
    print("vars.yaml not found. Check to make sure it exists in the root directory.")
    exit(1)


# Initialize other constants after loading YAML file
INSTR_PKG = [f'{MISSION_NAME}_{this_instr}' for this_instr in INSTR_NAMES]
INSTR_TO_BUCKET_NAME = {this_instr:f"{MISSION_NAME}-{this_instr}" for this_instr in INSTR_NAMES}
INSTR_TO_PKG = dict(zip(INSTR_NAMES, INSTR_PKG))