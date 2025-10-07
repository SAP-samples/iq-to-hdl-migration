MAX_RETRIES = 3
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 300
import os
import sys
import json
import time
import requests
import requests.exceptions
from datetime import datetime
from pathlib import Path
import urllib3
import getopt
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import platform

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CHUNK_SIZE = 95 * 1024 * 1024 * 1024  # 95 GB
FAILURE_LOG = "upload_failures.log"
newline = "\r\n" if platform.system() == "Windows" else "\n"

def normalize_path(path_str):
    if platform.system() == "Windows":
        return path_str.replace("/", "\\")
    else:
        return path_str.replace("\\", "/")

def get_remote_size(endpoint, container, rel_path, cert, key):
    url = f"https://{endpoint}/webhdfs/v1/{rel_path}?op=GETFILESTATUS"
    headers = {"x-sap-filecontainer": container}
    try:
        response = requests.get(url, headers=headers, cert=(cert, key), verify=False)
        if response.ok:
            return int(response.json()["FileStatus"]["length"])
    except Exception as e:
        print("Error checking remote size:", e)
    return -1

def already_split(file_path, chunk_size):
    part_index = 1
    base_name = file_path.stem
    ext = file_path.suffix
    output_dir = file_path.parent

    part_files = []
    expected_total = os.path.getsize(file_path)
    actual_total = 0

    while True:
        part_name = output_dir / f"{base_name}_part_{part_index:02d}{ext}"
        if not part_name.exists():
            break
        actual_total += part_name.stat().st_size
        part_files.append(part_name)
        part_index += 1

    return actual_total == expected_total, part_files

def upload_file(file_path, rel_path, config, container):
    file_path = Path(file_path)
    file_size = os.path.getsize(file_path)
    rel_path = rel_path.replace("\\", "/")
    
    url = f"https://{config['ENDPOINT']}/webhdfs/v1/{rel_path}?op=CREATE&data=true&overwrite=true"
    headers = {
        "Content-Type": "application/octet-stream",
        "x-sap-filecontainer": container
    }
    
    print(f"Uploading: {normalize_path(str(file_path))} to {rel_path}")
    
    with open(file_path, 'rb') as f:
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.put(
                    url,
                    headers=headers,
                    data=f,
                    cert=(config["CERT_PATH"], config["KEY_PATH"]),
                    timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                    verify=False
                )
                if resp.ok:
                    break
            except requests.exceptions.Timeout:
                print(f"Timeout while uploading {rel_path}, retrying {attempt+1}/{MAX_RETRIES}")
        else:
            # If all retries failed
            with open(FAILURE_LOG, "a") as log_f:
                log_f.write(rel_path + "\n")
                log_f.flush()
                os.fsync(log_f.fileno())
            return  # Skip the rest if upload failed
    
    # Verify file size after upload
    if resp.ok:
        remote_size = get_remote_size(
            config['ENDPOINT'],
            container,
            rel_path,
            config["CERT_PATH"],
            config["KEY_PATH"]
        )
        
        if file_size == remote_size:
            print(f"Uploaded successfully: {rel_path}")
        else:
            print(f"Size mismatch for {rel_path}")
            with open(FAILURE_LOG, "a") as log_f:
                log_f.write(normalize_path(str(file_path)) + "\n")
                log_f.flush()
                os.fsync(log_f.fileno())
    else:
        print(f"Upload failed: {resp.status_code} - {resp.text}")
        with open(FAILURE_LOG, "a") as log_f:
            log_f.write(normalize_path(str(file_path)) + "\n")
            log_f.flush()
            os.fsync(log_f.fileno())

def split_large_file_python(file_path, max_chunk_size):
    # Skip if filename already looks like a part
    if "_part_" in file_path.stem:
        print(f"Skipping split for {file_path.name}, already a part file.")
        return [file_path]

    already, part_files = already_split(file_path, max_chunk_size)
    if already :
        print(f"Skipping split for {file_path.name}, parts already exist.")
        print("Existing part files:")
        for pf in part_files:
            print(f"  {pf}")
        return part_files

    part_files = []
    part_index = 1
    base_name = file_path.stem
    ext = file_path.suffix
    output_dir = file_path.parent

    print(f"Splitting large file: {file_path.name}")

    buffer_size = 1024 * 1024  # 1MB buffer
    max_bytes_per_part = int(max_chunk_size)

    with open(file_path, 'rb') as src:
        while True:
            part_name = output_dir / f"{base_name}_part_{part_index:02d}{ext}"
            with open(part_name, 'wb') as part_file:
                bytes_written = 0
                while bytes_written < max_bytes_per_part:
                    chunk = src.read(min(buffer_size, max_bytes_per_part - bytes_written))
                    if not chunk:
                        break
                    part_file.write(chunk)
                    bytes_written += len(chunk)

            if bytes_written == 0:
                os.remove(part_name)
                break

            part_files.append(part_name)
            part_index += 1

    print("Created part files:")
    for pf in part_files:
        print(f"  {pf}")

    return part_files

def upload_large_file(file_path, rel_path, config, container):
    file_path = Path(file_path)
    file_size = os.path.getsize(file_path)
    basedir = Path(rel_path).parent
    filename = file_path.name
    parts = split_large_file_python(file_path, CHUNK_SIZE)
    merge_sources = []
    delete_sources = []

    for part in parts:
        part_name = part.name
        remote_part_path = f"{basedir}/{part_name}".replace("\\", "/")
        url = f"https://{config['ENDPOINT']}/webhdfs/v1/{remote_part_path}?op=CREATE&data=true&overwrite=true"
        headers = {
            "Content-Type": "application/octet-stream",
            "x-sap-filecontainer": container
        }
        print(f"Uploading part: {part_name}")

        with open(part, 'rb') as f:
            for attempt in range(MAX_RETRIES):
                try:
                    resp = requests.put(
                        url,
                        headers=headers,
                        data=f,
                        cert=(config["CERT_PATH"], config["KEY_PATH"]),
                        timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                        verify=False
                    )
                    if resp.ok:
                        break
                except requests.exceptions.Timeout:
                    print(f"Timeout while uploading part {part_name}, retrying {attempt+1}/{MAX_RETRIES}")
            else:
                # Failed after retries
                with open(FAILURE_LOG, "a") as log_f:
                    log_f.write(remote_part_path + "\n")
                    log_f.flush()
                    os.fsync(log_f.fileno())
                return  # Stop uploading further parts if one fails

        if not resp.ok:
            print(f"Failed to upload part: {part_name} — {resp.status_code}")
            return

        merge_sources.append({"path": f"/{remote_part_path}"})
        delete_sources.append({"path": f"/{remote_part_path}"})

    # Merge parts
    merge_url = f"https://{config['ENDPOINT']}/webhdfs/v1/{rel_path}?op=MERGE"
    headers = {
        "Content-Type": "application/json",
        "x-sap-filecontainer": container
    }
    print(f"Merging parts into: {rel_path}")
    resp = requests.post(
        merge_url,
        headers=headers,
        data=json.dumps({"sources": merge_sources}),
        cert=(config["CERT_PATH"], config["KEY_PATH"]),
        verify=False
    )
    if not resp.ok:
        print(f"Merge failed: {resp.status_code} - {resp.text}")
        return

    # Delete temporary parts
    delete_url = f"https://{config['ENDPOINT']}/webhdfs/v1/?op=DELETE_BATCH"
    print("Deleting temporary parts from HDLFS")
    resp = requests.post(
        delete_url,
        headers=headers,
        data=json.dumps({"files": delete_sources}),
        cert=(config["CERT_PATH"], config["KEY_PATH"]),
        verify=False
    )
    if not resp.ok:
        print(f"Delete failed: {resp.status_code} - {resp.text}")

    # Verify final size
    remote_size = get_remote_size(
        config['ENDPOINT'], container, rel_path,
        config["CERT_PATH"], config["KEY_PATH"]
    )
    if file_size == remote_size:
        print(f"Large file uploaded and merged successfully: {rel_path}")
    else:
        print(f"Merged file size mismatch for {rel_path}")
        with open(FAILURE_LOG, "a") as log_f:
            log_f.write(normalize_path(str(file_path)) + "\n")
            log_f.flush()
            os.fsync(log_f.fileno())

    # Remove local temporary chunks
    for part in parts:
        os.remove(part)

def copy_files(config):
    extract_path = Path(config["EXTRACT_PATH"])
    source_dir = Path(os.path.normpath(extract_path / "Migration_Data"))
    dest_folder = config["DEST_FOLDER"]
    container = config["ENDPOINT"].split('.')[0]

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f"upload_log_{timestamp}.log"
    log_path = os.path.join(os.getcwd(), log_filename)

    class TeeLogger:
        def __init__(self, filename):
            self.terminal = sys.stdout
            self.log = open(filename, "w", encoding="utf-8")
        def write(self, message):
            self.terminal.write(message)
            self.log.write(message)
        def flush(self):
            self.terminal.flush()
            self.log.flush()

    sys.stdout = TeeLogger(log_path)

    print(f"Starting upload from: {normalize_path(str(source_dir))}")
    print(f"Target container     : {container}")
    print(f"Logging to           : {log_filename}")
    print(f"Failure retry file   : {normalize_path(FAILURE_LOG)}")
    print("=" * 60)

    start_time = time.time()
    fail_count = 0
    success_count = 0
    failure_list = []

    failure_mode = os.path.exists(FAILURE_LOG)
    if failure_mode:
        print(f"Retrying from {normalize_path(FAILURE_LOG)}")
        with open(FAILURE_LOG, "r") as f:
            all_files = [source_dir / line.strip() for line in f if line.strip()]
    else:
        all_files = [f for f in source_dir.rglob("*") if f.is_file()]

    all_files = sorted(all_files, key=lambda x: os.path.getsize(x))

    MAX_THREADS = 8
    thread_lock = threading.Lock()
    active_threads = set()

    def upload_dispatcher_wrapper(file_path):
        thread_name = threading.current_thread().name
        try:
            with thread_lock:
                active_threads.add(thread_name)

            rel_path = file_path.relative_to(source_dir)
            hdlfs_path = Path(dest_folder) / rel_path
            hdlfs_path_str = normalize_path(str(hdlfs_path))

            if os.path.getsize(file_path) < CHUNK_SIZE:
                upload_file(file_path, hdlfs_path_str, config, container)
            else:
                upload_large_file(file_path, hdlfs_path_str, config, container)

            return (file_path, True, None)

        except Exception as e:
            return (file_path, False, e)

        finally:
            with thread_lock:
                active_threads.discard(thread_name)

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {executor.submit(upload_dispatcher_wrapper, f): f for f in all_files}
        for future in as_completed(futures):
            file_path, success, error = future.result()
            if success:
                success_count += 1
            else:
                print(f"Failed : {normalize_path(str(file_path))} | Error: {error}")
                fail_count += 1
                failure_list.append(file_path.relative_to(source_dir))
            print("---")

    if failure_list:
        with open(FAILURE_LOG, "w") as f:
            for fail_item in failure_list:
                f.write(normalize_path(str(fail_item)) + "\n")
    else:
        if os.path.exists(FAILURE_LOG):
            os.remove(FAILURE_LOG)
            print(f"Removed previous failure log: {normalize_path(FAILURE_LOG)}")

    end_time = time.time()
    total_time = end_time - start_time

    print("\n===== SUMMARY =====")
    print(f"Total files attempted  : {len(all_files)}")
    print(f"Successfully uploaded  : {success_count}")
    print(f"Failed uploads         : {fail_count}")
    print(f"Elapsed time           : {int(total_time // 60)} min {int(total_time % 60)} sec")

    sys.stdout.log.close()
    sys.stdout = sys.stdout.terminal


def main(config_path):
    if not os.path.isfile(config_path):
        print(f"Config file not found: {config_path}")
        sys.exit(1)

    with open(config_path, "r") as f:
        json_data = json.load(f)

    hdlfs = json_data.get("HDLFS_Configuration", {})
    config = {
        "ENDPOINT": hdlfs.get("Files_endpoint", ""),
        "EXTRACT_PATH": json_data.get("Extract_Path", ""),
        "DEST_FOLDER": hdlfs.get("Directory_Name", ""),
        "CERT_PATH": hdlfs.get("Cert_path", ""),
        "KEY_PATH": hdlfs.get("Key_path", "")
    }

    for k, v in config.items():
        if not v:
            print(f"Missing config key: {k}")
            sys.exit(1)

    copy_files(config)

if __name__ == "__main__":
    argv = sys.argv[1:]
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

    main(config_file)            

