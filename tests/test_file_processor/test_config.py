import yaml
import lambda_function.file_processor.config as config


def test_read_config_file():
    # Read the config.yaml file
    with open("./config.yaml", "r") as f:
        expected_config = yaml.safe_load(f)

    # Check if the config is loaded correctly
    assert config.config == expected_config

    # Check if the global variables are properly set
    assert config.MISSION_NAME == expected_config["MISSION_NAME"]
    assert config.INSTR_NAMES == expected_config["INSTR_NAMES"]
    assert config.MISSION_PKG == expected_config["MISSION_PKG"]
    assert config.TSD_REGION == expected_config["TSD_REGION"]

    # Check INSTR_PKG and INSTR_TO_BUCKET_NAME
    expected_instr_pkg = [
        f"{expected_config['MISSION_NAME']}_{instr}"
        for instr in expected_config["INSTR_NAMES"]
    ]
    expected_instr_to_bucket_name = {
        instr: f"{expected_config['MISSION_NAME']}-{instr}"
        for instr in expected_config["INSTR_NAMES"]
    }

    assert config.INSTR_PKG == expected_instr_pkg
    assert config.INSTR_TO_BUCKET_NAME == expected_instr_to_bucket_name

    # Check INSTR_TO_PKG
    expected_instr_to_pkg = dict(
        zip(expected_config["INSTR_NAMES"], expected_instr_pkg)
    )
    assert config.INSTR_TO_PKG == expected_instr_to_pkg

    # Check if the mission package is imported correctly
    mission_pkg = __import__(expected_config["MISSION_PKG"])
    assert config.mission_pkg == mission_pkg

    # Check if the parser is set correctly
    assert config.parser == mission_pkg.util.parse_science_filename


# Test non-existent file
def test_read_config_file_failure():
    # Set the config file path to a non-existent file
    none_existent = "non-existent-file.yaml"

    # Call the function and expect an exception
    try:
        config.read_config_file(none_existent)

    except Exception as e:
        assert isinstance(e, FileNotFoundError)
