import yaml
from lambda_function import log


def read_config_file(config_file_path="./config.yaml"):
    try:
        with open(config_file_path, "r") as f:
            config = yaml.safe_load(f)
            log.debug("config.yaml loaded successfully")
            return config
    except FileNotFoundError:
        log.error("config.yaml not found")
        raise FileNotFoundError("config.yaml not found")


# Read config file
config = read_config_file()

# Set up global variables
MISSION_NAME = config["MISSION_NAME"]
INSTR_NAMES = config["INSTR_NAMES"]
MISSION_PKG = config["MISSION_PKG"]
TSD_REGION = config["TSD_REGION"]
INSTR_PKG = [f"{MISSION_NAME}_{this_instr}" for this_instr in INSTR_NAMES]
INSTR_TO_BUCKET_NAME = {
    this_instr: f"{MISSION_NAME}-{this_instr}" for this_instr in INSTR_NAMES
}
INSTR_TO_PKG = dict(zip(INSTR_NAMES, INSTR_PKG))

# Import logging and util from mission package
mission_pkg = __import__(MISSION_PKG)
parser = mission_pkg.util.parse_science_filename
