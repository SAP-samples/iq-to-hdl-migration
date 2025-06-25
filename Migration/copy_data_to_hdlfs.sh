#!/bin/bash

# This version uses environment variables passed from Python

: "${ENDPOINT:?Need ENDPOINT}"
: "${EXTRACT_PATH:?Need EXTRACT_PATH}"
: "${DEST_FOLDER:?Need DEST_FOLDER}"
: "${CERT_PATH:?Need CERT_PATH}"
: "${KEY_PATH:?Need KEY_PATH}"

SOURCE_DIR="${EXTRACT_PATH}/Migration_Data"  # always fixed
CERT_DIR=$(dirname "$CERT_PATH")
CLIENT=$(basename "$CERT_PATH" .crt)
CONTAINER=$(echo "$ENDPOINT" | cut -f 1 -d '.')

CHUNK_SIZE=$((95 * 1024 * 1024 * 1024)) # 95GB

start_time=$(date +%s)
success_count=0
fail_count=0
all_files=()
success_files=()
log_file="upload_log_$(date +%Y%m%d_%H%M%S).log"
success_record_file="successful_uploads.log"
touch "$success_record_file"

exec > >(tee -a "$log_file") 2>&1

echo "Logging to: $log_file"
echo "Starting recursive upload from $SOURCE_DIR"
echo "========================"
echo "Start Time: $(date)"
echo "========================"
echo "Using certificate: $CERT_PATH and $KEY_PATH"
echo "Uploading to HDLFS container: $CONTAINER"
echo "Target destination: $DEST_FOLDER"

get_relative_path() {
  echo "$1" | sed "s|^$SOURCE_DIR/||"
}

upload_small_file() {
  local file=$1
  local rel_path=$(get_relative_path "$file")

  if grep -Fxq "$rel_path" "$success_record_file"; then
    echo "Skipping already uploaded file: $rel_path"
    return
  fi

  echo "Uploading $file as $rel_path..."
  curl -s -w "\nTime Total: %{time_total}s\n" \
    -H 'Content-Type: application/octet-stream' \
    -H "x-sap-filecontainer: $CONTAINER" \
    --cert "$CERT_PATH" --key "$KEY_PATH" \
    --upload-file "$file" \
    -X PUT \
    "https://${ENDPOINT}/webhdfs/v1/${DEST_FOLDER}/${rel_path}?op=CREATE&data=true&overwrite=true"

  local_size=$(stat -c%s "$file")
  remote_size=$(curl -s \
    -H "x-sap-filecontainer: $CONTAINER" \
    --cert "$CERT_PATH" --key "$KEY_PATH" \
    "https://${ENDPOINT}/webhdfs/v1/${DEST_FOLDER}/${rel_path}?op=GETFILESTATUS" \
    | grep -o '"length":[0-9]*' | sed 's/[^0-9]*//g')

  if [[ "$local_size" == "$remote_size" ]]; then
    echo "File size matches for $file"
    success_files+=("$rel_path")
    echo "$rel_path" >> "$success_record_file"
    ((success_count++))
  else
    echo "File size mismatch for $file"
    ((fail_count++))
  fi
}

upload_large_file() {
  local file=$1
  local rel_path=$(get_relative_path "$file")

  if grep -Fxq "$rel_path" "$success_record_file"; then
    echo "Skipping already uploaded file: $rel_path"
    return
  fi

  local basedir=$(dirname "$rel_path")
  local filename=$(basename "$file")
  local name_no_ext="${filename%.*}"
  local ext=".${filename##*.}"
  local filedir=$(dirname "$file")
  local prefix="${filedir}/${name_no_ext}_part_"

  echo "Splitting $file into 95GB parts..."
  split --bytes=95G --numeric-suffixes=1 --additional-suffix="$ext" "$file" "$prefix"

  echo "Uploading parts of $filename..."
  for part in "${prefix}"*"$ext"; do
    part_name=$(basename "$part")
    echo "Uploading $part_name to ${DEST_FOLDER}/${basedir}/"
    curl -s -w "\nTime Total: %{time_total}s\n" \
      -H 'Content-Type: application/octet-stream' \
      -H "x-sap-filecontainer: $CONTAINER" \
      --cert "$CERT_PATH" --key "$KEY_PATH" \
      --upload-file "$part" \
      -X PUT \
      "https://${ENDPOINT}/webhdfs/v1/${DEST_FOLDER}/${basedir}/${part_name}?op=CREATE&data=true&overwrite=true"
  done

  echo "Merging parts into $rel_path on remote..."
  merge_json='{"sources":['
  delete_json='{"files":['
  i=0
  for part in "${prefix}"*"$ext"; do
    part_name=$(basename "$part")
    [[ $i -gt 0 ]] && merge_json+="," && delete_json+="," 
    merge_json+="{\"path\":\"/${DEST_FOLDER}/${basedir}/${part_name}\"}"
    delete_json+="{\"path\":\"/${DEST_FOLDER}/${basedir}/${part_name}\"}"
    ((i++))
  done
  merge_json+="]}"
  delete_json+="]}"

  curl -s -w "\nTime Total: %{time_total}s\n" \
    -H 'Content-Type: application/json' \
    -H "x-sap-filecontainer: $CONTAINER" \
    --cert "$CERT_PATH" --key "$KEY_PATH" \
    -X POST \
    "https://${ENDPOINT}/webhdfs/v1/${DEST_FOLDER}/${rel_path}?op=MERGE" \
    --data-raw "$merge_json"

  echo "Deleting parts from the HDLFS filecontainer"
  curl -s -w "\nTime Total: %{time_total}s\n" \
    -H 'Content-Type: application/json' \
    -H "x-sap-filecontainer: $CONTAINER" \
    --cert "$CERT_PATH" --key "$KEY_PATH" \
    -X POST \
    "https://${ENDPOINT}/webhdfs/v1/?op=DELETE_BATCH" \
    --data-raw "$delete_json"

  local_size=$(stat -c%s "$file")
  remote_size=$(curl -s \
    -H "x-sap-filecontainer: $CONTAINER" \
    --cert "$CERT_PATH" --key "$KEY_PATH" \
    "https://${ENDPOINT}/webhdfs/v1/${DEST_FOLDER}/${rel_path}?op=GETFILESTATUS" \
    | grep -o '"length":[0-9]*' | sed 's/[^0-9]*//g')

  if [[ "$local_size" == "$remote_size" ]]; then
    echo "File $file copied successfully"
    success_files+=("$rel_path")
    echo "$rel_path" >> "$success_record_file"
    ((success_count++))
  else
    echo "On-prem file size is not matching with file copied into HDLFS for $file"
    ((fail_count++))
  fi

  rm -f "${prefix}"*"$ext"
}

export -f upload_small_file
export -f upload_large_file
export -f get_relative_path

while IFS= read -r -d '' file; do
  all_files+=("$file")
  filesize=$(stat -c%s "$file")
  if (( filesize < CHUNK_SIZE )); then
    upload_small_file "$file"
  else
    upload_large_file "$file"
  fi
  sleep 1
  echo "---"
done < <(find "$SOURCE_DIR" -type f -print0)

total_files=${#all_files[@]}
echo -e "\n===== SUMMARY ====="
echo "Total files found         : $total_files"
echo "Successfully uploaded     : $success_count"
echo "Failed uploads            : $fail_count"
echo "==========================="

end_time=$(date +%s)
total_time=$((end_time - start_time))
minutes=$((total_time / 60))
seconds=$((total_time % 60))
echo "Total time taken: ${minutes} minutes and ${seconds} seconds"
echo "All steps completed."
echo "Log file saved as: $log_file"

