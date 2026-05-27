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
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import platform
import builtins

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CHUNK_SIZE = 95 * 1024 * 1024 * 1024  # 95 GB
SUCCESS_LOG = "upload_success.log"
newline = "\r\n" if platform.system() == "Windows" else "\n"

def normalize_rel_path(path_str):
    if path_str is None:
        return ""
    return str(path_str).replace("\\", "/").lstrip("/")

def rel_path_key(path_str):
    normalized = normalize_rel_path(path_str)
    return normalized.casefold()

def canonical_uploaded_rel_path(path_str, dest_folder, source_root_name="Migration_Data"):
    normalized = normalize_rel_path(path_str)
    normalized_dest = normalize_rel_path(dest_folder)
    normalized_source_root = normalize_rel_path(source_root_name)

    if normalized_dest and normalized == normalized_dest:
        normalized = ""
    elif normalized_dest and normalized.startswith(normalized_dest + "/"):
        normalized = normalized[len(normalized_dest) + 1:]

    if normalized_source_root and normalized.startswith(normalized_source_root + "/"):
        normalized = normalized[len(normalized_source_root) + 1:]

    return normalize_rel_path(normalized)

def _write_entry(path):
    with open(SUCCESS_LOG, "a") as log_f:
        log_f.write(path + "\n")
        log_f.flush()
        os.fsync(log_f.fileno())

def write_success_log_entry(rel_path, config, success_log_lock=None):
    clean_path = canonical_uploaded_rel_path(rel_path, config["DEST_FOLDER"])
    if not clean_path:
        return

    if success_log_lock is None:
        _write_entry(clean_path)
    else:
        with success_log_lock:
            _write_entry(clean_path)

def normalize_path(path_str):
    if platform.system() == "Windows":
        return path_str.replace("/", "\\")
    else:
        return path_str.replace("\\", "/")

def is_split_part_file(file_path):
    return bool(re.search(r"_part_\d+$", file_path.stem, re.IGNORECASE))

def get_remote_size(endpoint, container, rel_path, cert, key):
    url = f"https://{endpoint}/webhdfs/v1/{rel_path}?op=GETFILESTATUS"
    headers = {"x-sap-filecontainer": container}
    try:
        response = requests.get(url, headers=headers, cert=(cert, key), verify=False)
        if response.ok:
            return int(response.json()["FileStatus"]["length"])
    except Exception as e:
        print("[%s] :" % datetime.now(), "Error checking remote size:", e)
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

def upload_file(file_path, rel_path, config, container, success_log_lock=None):
    file_path = Path(file_path)
    file_size = os.path.getsize(file_path)
    rel_path = rel_path.replace("\\", "/")

    url = f"https://{config['ENDPOINT']}/webhdfs/v1/{rel_path}?op=CREATE&data=true&overwrite=true"
    headers = {"Content-Type": "application/octet-stream", "x-sap-filecontainer": container}

    print("[%s] :" % datetime.now(), f"Uploading: {normalize_path(str(file_path))} to {rel_path}")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with open(file_path, 'rb') as f:
                resp = requests.put(
                    url,
                    headers=headers,
                    data=f,
                    cert=(config["CERT_PATH"], config["KEY_PATH"]),
                    timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                    verify=False
                )

            if resp.ok:
                remote_size = get_remote_size(
                    config['ENDPOINT'], container, rel_path, config["CERT_PATH"], config["KEY_PATH"]
                )
                if file_size == remote_size:
                    print("[%s] :" % datetime.now(), f"Uploaded successfully: {rel_path} ({file_size} bytes)")
                    write_success_log_entry(rel_path, config, success_log_lock)
                    return True  #  success
                else:
                    print("[%s] :" % datetime.now(), f"Size mismatch for {rel_path}: local={file_size}, remote={remote_size}")
            else:
                print("[%s] :" % datetime.now(), f"Attempt {attempt}/{MAX_RETRIES} failed for {rel_path} "
                      f"({resp.status_code}) - {resp.text}")

        except requests.exceptions.Timeout:
            print("[%s] :" % datetime.now(), f"Timeout while uploading {rel_path}, retrying {attempt}/{MAX_RETRIES}")
        except Exception as e:
            print("[%s] :" % datetime.now(), f"Error while uploading {rel_path}: {e}")

    print("[%s] :" % datetime.now(), f"Upload failed after {MAX_RETRIES} retries: {rel_path}")
    return False  #  fail

                    
def split_large_file_python(file_path, max_chunk_size):
    # Skip if filename already looks like a part
    if "_part_" in file_path.stem:
        print("[%s] :" % datetime.now(), f"Skipping split for {file_path.name}, already a part file.")
        return [file_path]

    already, part_files = already_split(file_path, max_chunk_size)
    if already :
        print("[%s] :" % datetime.now(), f"Skipping split for {file_path.name}, parts already exist.")
        print("[%s] :" % datetime.now(), "Existing part files:")
        for pf in part_files:
            print("[%s] :" % datetime.now(), f"  {pf}")
        return part_files

    part_files = []
    part_index = 1
    base_name = file_path.stem
    ext = file_path.suffix
    output_dir = file_path.parent

    print("[%s] :" % datetime.now(), f"Splitting large file: {file_path.name}")

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

    print("[%s] :" % datetime.now(), "Created part files:")
    for pf in part_files:
        print("[%s] :" % datetime.now(), f"  {pf}")

    return part_files

def upload_large_file(file_path, rel_path, config, container, success_log_lock=None):
    file_path = Path(file_path)
    file_size = os.path.getsize(file_path)
    basedir = Path(rel_path).parent
    filename = file_path.name
    parts = split_large_file_python(file_path, CHUNK_SIZE)
    merge_sources = []
    delete_sources = []

    # --- Upload each part with retry ---
    for part in parts:
        part_name = part.name
        remote_part_path = f"{basedir}/{part_name}".replace("\\", "/")
        url = f"https://{config['ENDPOINT']}/webhdfs/v1/{remote_part_path}?op=CREATE&data=true&overwrite=true"
        headers = {
            "Content-Type": "application/octet-stream",
            "x-sap-filecontainer": container
        }

        print("[%s] :" % datetime.now(), f"Uploading part: {part_name}")
        part_success = False

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                with open(part, 'rb') as f:
                    resp = requests.put(
                        url,
                        headers=headers,
                        data=f,
                        cert=(config["CERT_PATH"], config["KEY_PATH"]),
                        timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                        verify=False
                    )

                if resp.ok:
                    part_success = True
                    break
                else:
                    print("[%s] :" % datetime.now(), f"Attempt {attempt}/{MAX_RETRIES} failed for {part_name}: {resp.status_code} - {resp.text}")

            except requests.exceptions.Timeout:
                print("[%s] :" % datetime.now(), f"Timeout while uploading part {part_name}, retrying {attempt}/{MAX_RETRIES}")
            except Exception as e:
                print("[%s] :" % datetime.now(), f"Error while uploading part {part_name}: {e}")

        if not part_success:
            print("[%s] :" % datetime.now(), f"Failed to upload part after {MAX_RETRIES} retries: {part_name}")
            # cleanup temp chunks before returning
            for p in parts:
                try:
                    os.remove(p)
                except:
                    pass
            return False  # fail

        merge_sources.append({"path": f"/{remote_part_path}"})
        delete_sources.append({"path": f"/{remote_part_path}"})

    # --- Merge parts ---
    merge_url = f"https://{config['ENDPOINT']}/webhdfs/v1/{rel_path}?op=MERGE"
    headers = {
        "Content-Type": "application/json",
        "x-sap-filecontainer": container
    }
    print("[%s] :" % datetime.now(), f"Merging {len(parts)} parts into: {rel_path}")
    resp = requests.post(
        merge_url,
        headers=headers,
        data=json.dumps({"sources": merge_sources}),
        cert=(config["CERT_PATH"], config["KEY_PATH"]),
        verify=False
    )

    if not resp.ok:
        print("[%s] :" % datetime.now(), f"Merge failed: {resp.status_code} - {resp.text}")
        return False

    # --- Delete temporary parts ---
    delete_url = f"https://{config['ENDPOINT']}/webhdfs/v1/?op=DELETE_BATCH"
    print("[%s] :" % datetime.now(), "Deleting temporary parts from HDLFS")
    resp = requests.post(
        delete_url,
        headers=headers,
        data=json.dumps({"files": delete_sources}),
        cert=(config["CERT_PATH"], config["KEY_PATH"]),
        verify=False
    )
    if not resp.ok:
        print("[%s] :" % datetime.now(), f"Delete failed: {resp.status_code} - {resp.text}")

    # --- Verify final merged file ---
    remote_size = get_remote_size(
        config['ENDPOINT'],
        container,
        rel_path,
        config["CERT_PATH"],
        config["KEY_PATH"]
    )
    if file_size == remote_size:
        print("[%s] :" % datetime.now(), f"Large file uploaded and merged successfully: {rel_path} ({file_size} bytes)")
        # Log success only now
        write_success_log_entry(rel_path, config, success_log_lock)

        # Remove local temporary chunks only after success
        for part in parts:
            os.remove(part)

        return True  #  success
    else:
        print("[%s] :" % datetime.now(), f"Merged file size mismatch for {rel_path}: local={file_size}, remote={remote_size}")
        return False  #  fail


def copy_files(config):
    extract_path = Path(config["EXTRACT_PATH"])
    source_dir = Path(os.path.normpath(extract_path / "Migration_Data"))
    dest_folder = config["DEST_FOLDER"]
    container = config["ENDPOINT"].split('.')[0]
    global SUCCESS_LOG

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f"upload_log_{timestamp}.log"
    log_path = os.path.join(os.getcwd(), log_filename)

    class TeeLogger:
        def __init__(self, filename):
            self.terminal = sys.stdout
            self.log = open(filename, "w", encoding="utf-8")
            self.lock = threading.Lock() # Add a lock for thread safety
        def write(self, message):
            with self.lock: # Ensure only one thread writes at a time
                self.terminal.write(message)
                self.log.write(message)
                self.log.flush() # Force write to disk immediately
                os.fsync(self.log.fileno()) # Force OS-level buffer synchronization
        def flush(self):
            with self.lock:
                self.terminal.flush()
                self.log.flush()
    original_stdout = sys.stdout
    original_print = builtins.print
    print_lock = threading.Lock()

    try:
        sys.stdout = TeeLogger(log_path)

        def thread_safe_print(*args, **kwargs):
            kwargs.setdefault("flush", True)
            with print_lock:
                original_print(*args, **kwargs)

        builtins.print = thread_safe_print

        start_time = time.time()
        fail_count = 0
        success_count = 0

        # Load list of successfully uploaded files
        if os.path.exists(SUCCESS_LOG):
            with open(SUCCESS_LOG, "r") as s:
                uploaded_files = set(
                    rel_path_key(canonical_uploaded_rel_path(line.strip(), dest_folder, source_dir.name))
                    for line in s
                    if line.strip()
                )
        else:
            uploaded_files = set()

        # Get all source files
        source_files = [f for f in source_dir.rglob("*") if f.is_file() and not is_split_part_file(f)]
        source_file_keys = {rel_path_key(f.relative_to(source_dir)) for f in source_files}
        previous_uploaded_count = len(uploaded_files & source_file_keys)

        # Exclude already successfully uploaded files
        all_files = [f for f in source_files if rel_path_key(f.relative_to(source_dir)) not in uploaded_files]
        all_files = sorted(all_files, key=lambda x: os.path.getsize(x))

        print("[%s] :" % datetime.now(), f"Starting upload from: {normalize_path(str(source_dir))}")
        print("[%s] :" % datetime.now(), f"Target container     : {container}")
        print("[%s] :" % datetime.now(), f"Logging to           : {log_filename}")
        print("[%s] :" % datetime.now(), f"Success log file     : {normalize_path(SUCCESS_LOG)}")
        print("[%s] :" % datetime.now(), f"Total source files   : {len(source_files)}")
        print("[%s] :" % datetime.now(), f"Previously uploaded  : {previous_uploaded_count}")
        print("[%s] :" % datetime.now(), "=" * 60)

        MAX_THREADS = 8
        thread_lock = threading.Lock()
        success_log_lock = threading.Lock()
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
                    success = upload_file(file_path, hdlfs_path_str, config, container, success_log_lock)
                else:
                    success = upload_large_file(file_path, hdlfs_path_str, config, container, success_log_lock)

                return (file_path, success, None if success else "Upload failed")

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
                    print("[%s] :" % datetime.now(), f"Failed : {normalize_path(str(file_path))} | Error: {error}")
                    fail_count += 1
                print("[%s] :" % datetime.now(), "---")

        end_time = time.time()
        total_time = end_time - start_time

        print("[%s] :" % datetime.now(), "===== SUMMARY =====")
        print("[%s] :" % datetime.now(), f"Total files attempted  : {len(all_files)}")
        print("[%s] :" % datetime.now(), f"Successfully uploaded  : {success_count}")
        print("[%s] :" % datetime.now(), f"Failed uploads         : {fail_count}")
        print("[%s] :" % datetime.now(), f"Elapsed time           : {int(total_time // 60)} min {int(total_time % 60)} sec")

    finally:
        builtins.print = original_print
        if isinstance(sys.stdout, TeeLogger):
            sys.stdout.log.close()
        sys.stdout = original_stdout


def main(config_path):
    if not os.path.isfile(config_path):
        print("[%s] :" % datetime.now(), f"Config file not found: {config_path}")
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
            print("[%s] :" % datetime.now(), f"Missing config key: {k}")
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
        print("[%s] :" % datetime.now(), "Error: Unsupported option/values. Run copy_hdlfs.py -h or --help for help")
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


