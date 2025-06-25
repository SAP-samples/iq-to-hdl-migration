import subprocess
import sys
import os
import json
import getopt

def run_copy_data_to_hdlfs(config_file_path):
    if not os.path.isfile(config_file_path):
        print(f"Error: Config file '{config_file_path}' does not exist.")
        sys.exit(1)

    # Read JSON config
    with open(config_file_path, 'r') as f:
        config = json.load(f)

    hdlfs_config = config.get("HDLFS_Configuration", {})

    env_vars = os.environ.copy()
    env_vars["ENDPOINT"] = hdlfs_config.get("Files_endpoint", "")
    env_vars["EXTRACT_PATH"] = config.get("Extract_Path", "")
    env_vars["DEST_FOLDER"] = hdlfs_config.get("Directory_Name", "")
    env_vars["CERT_PATH"] = hdlfs_config.get("Cert_path", "")
    env_vars["KEY_PATH"] = hdlfs_config.get("Key_path", "")

    try:
        subprocess.run(['./copy_data_to_hdlfs.sh'], check=True, env=env_vars)
    except subprocess.CalledProcessError as e:
        print(f"Script failed with return code {e.returncode}")
        sys.exit(e.returncode)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    argv = sys.argv[1:]

    # total arguments passed
    n = len(sys.argv)
    if not (n == 3 or n == 2):
        sys.exit("Error: Incorrect/Invalid number of arguments. Run copy_hdlfs.py -h or --help for help")

    try:
        opts, args = getopt.getopt(argv, "hf:", ["help", "config_file="])
    except getopt.GetoptError:
        print("Error: Unsupported option/values. Run copy_hdlfs.py -h or --help for help")
        sys.exit(2)

    config_file = None
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print("Usage:\ncopy_hdlfs.py --config_file <config file path>")
            print("which is the same as:\ncopy_hdlfs.py -f <config file path>")
            print("Switch --config_file or -f denotes utilizing the config file to access parameters from.")
            sys.exit()
        elif opt in ("-f", "--config_file"):
            config_file = arg

    if config_file is None:
        print("Error: Config file not specified. Use -f or --config_file to provide it.")
        sys.exit(1)

    run_copy_data_to_hdlfs(config_file)

