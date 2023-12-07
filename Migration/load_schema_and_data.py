# ----------------------------------------------------------------------
# @(#)Migration                      2021              SAP
# ----------------------------------------------------------------------
# Migration utilities to migrate SAP IQ on SAP datalake IQ.
# ----------------------------------------------------------------------
#
# ***************************************************************************
# Copyright (c) 2021 SAP SE or an SAP affiliate company. All rights reserved.
# ***************************************************************************
import sys, getopt
import socket
import getpass
import stat
import os
import subprocess
import datetime
import time,shutil
import multiprocessing
import os, uuid
import ctypes
import re
from time import sleep
import platform
import csv
import hashlib
from fnmatch import fnmatch
import json
import logging
import codecs
import sqlanydb
from logging.handlers import QueueHandler, QueueListener

try:
   from azure.storage.blob import BlobServiceClient
except:
   BlobServiceClient = None

try:
   import boto3
except:
    boto3 = None

argv = sys.argv[1:]
# total arguments passed
n = len(sys.argv)

if not(n == 3 or n==2):
    sys.exit("Error: Incorrect/Invalid number of arguments. Run load_schema_and_data.py with -h or --help for help")

try:
    opts, args = getopt.getopt(argv,"ht:f:",["help","config_file="])
except getopt.GetoptError:
    print ('Error : Unsupported option/values. Run load_schema_and_data.py -h or --help for help')
    sys.exit(2)
for opt, arg in opts:
    if opt in ("-h", "--help"):
        print ('usage:\nload_schema_and_data.py --config_file <config file path>')
        print ('which is the same as:\nload_schema_and_data.py -f <config file path>')
        print ('Switch --config_file or -f denote utilizing the config file to access parameters from.')
        sys.exit()
    elif opt in ("-f", "--config_file"):
        config_file = arg

# detect the current working directory and print it
path = os.getcwd()
global newline
global is_windows
global path_sep
is_windows = False
if(platform.system() == "Windows"):
    path_sep = "\\"
    newline = "\r\n"
    is_windows = True
else:
    path_sep = "/"
    newline = "\n"

sys.path.insert(0, '%s%s..%sCommon%s'%(path,path_sep,path_sep,path_sep))
import common
common.load_inputs(config_file,'load_schema_and_data')
global migrationpath
migrationpath = "%s%sMigration_Data"%(common.extract_path,path_sep)
global extractedTables_out
extractedTables_out = "%s%sExtractedTables.out"%(migrationpath,path_sep)
global reload_file_location
reload_file_location = "%s%sMigration_Data"%(common.extract_path,path_sep)

global loadFailure_err
loadFailure_err = "%s%sHDL_LoadFailure.err"%(reload_file_location,path_sep)
global loadFailure_err_bkp
loadFailure_err_bkp = "%s%sHDL_LoadFailure_bkp.err"%(reload_file_location,path_sep)
global HDLLoad_out
HDLLoad_out = "%s%sHDL_LoadedTables.out"%(reload_file_location,path_sep)
load_schema_and_data_log = "%s%sload_schema_and_data.log"%(path,path_sep)
lock = multiprocessing.Lock()
tables_count = multiprocessing.Value(ctypes.c_int, 0)
fail_count = multiprocessing.Value(ctypes.c_int, 0)
total_table = multiprocessing.Value(ctypes.c_int, 0)

# Read the json config file and get all values
def get_inputs(config_file):

    print ('Reading Config File: %s' %(config_file))

    #set paths
    global data_path
    data_path =  "%s%sExtracted_Data"%(reload_file_location,path_sep)

    global iqtables_list
    iqtables_list = "%s%siq_tables.list"%(reload_file_location,path_sep)

    global AutoUpdated_Reload_file
    AutoUpdated_Reload_file = "%s%sAutoUpdated_Reload.sql"%(reload_file_location,path_sep)

    global Foreign_Key_Constraint_file
    Foreign_Key_Constraint_file = "%s%sForeign_Key_Constraint.sql"%(reload_file_location,path_sep)

    global resume_mode
    resume_mode = False

    if os.path.isfile(HDLLoad_out):
        resume_mode = True


    if is_windows:
        global listener_q,log_q,logger
        listener_q,log_q,logger = logger_init()
    else:
        if (resume_mode):
            logging.basicConfig(filename=load_schema_and_data_log, filemode='a', format='%(message)s', level=logging.INFO)
        else:
            logging.basicConfig(filename=load_schema_and_data_log, filemode='w', format='%(message)s', level=logging.INFO)


    logging.info("%s*************************************************************"%newline)
    logging.info("[%s] : Schema and Data Load Started."%(datetime.datetime.now()))
    logging.info("*************************************************************")
    logging.info(common.config_valid_str)

    # Validation of right hyperscaler names supported
    if common.object_store.lower() == 'azure' or common.object_store.lower() == 'aws':
        logging.info("%s"%(common.dividerline))
        logging.info("Selected Object_store = %s"%common.object_store)

    if common.w == common.t:
        logging.info( "%sConfiguration file is correct and Reading credentials "%newline)
        logging.info("%s"%(common.dividerline))

    if common.object_store.lower() == 'azure':
        if (common.Object_Store_Copy_Validation.lower() == 'yes' and BlobServiceClient == None):
            sys.exit("Error: Module azure.storage.blob not found. %sPlease enter Object_Store_Copy_Validation as 'No' in %s file to proceed load without validating data copied to object store"%(newline,config_file))

    else:
        if (common.Object_Store_Copy_Validation.lower() == 'yes' and boto3 == None):
            sys.exit("Error: Module boto3 not found. %sPlease enter Object_Store_Copy_Validation as 'No' in %s file to proceed load without validating data copied to object store"%(newline,config_file))

    logging.info("%s"%(common.dividerline))
    logging.info("Data lake IQ common.charset: %s"%common.charset)

# Initialize logger with handler and queue the records and send them to handler
# This function is applicable only for Windows OS as
# On Windows child processes will only inherit the level of the parent process’s logger –
# any other customization of the logger will not be inherited." Subprocesses won't inherit the handler,
# and  can't pass it explicitly because it's not pickleable
def logger_init():
    global resume_mode
    q = multiprocessing.Queue()
    # this is the handler for all log records
    file_handler = logging.StreamHandler()
    if (resume_mode):
        file_handler = logging.FileHandler(load_schema_and_data_log,mode='a')
    else:
        file_handler = logging.FileHandler(load_schema_and_data_log,mode='w')

    # ql gets records from the queue and sends them to the handler
    ql = QueueListener(q, file_handler)
    ql.start()

    logger = logging.getLogger()
    formatter    = logging.Formatter('%(message)s')
    file_handler.setFormatter(formatter)
    # add file handler to logger
    logger.addHandler(file_handler)
    logger.setLevel(logging.INFO)
    # add the handler to the logger so records from this process are handled
    logger.addHandler(file_handler)

    return ql, q ,logger

# validate required directories and files exists to proceed with load
def validate_dir_and_files():
    if not os.path.isdir(common.extract_path):
        sys.exit("Error: Extract directory %s does not exist"%(common.extract_path))

    if not os.path.isdir(reload_file_location):
        sys.exit("Error: Migration_Data directory %s does not exist"%(reload_file_location))

    if not os.path.isdir(data_path):
        sys.exit("Error: Extracted_Data directory  %s does not exist"%(data_path))


    if not os.path.isfile(AutoUpdated_Reload_file):
        sys.exit("Error: %s file does not exist"%(AutoUpdated_Reload_file))
    else:
        with codecs.open(AutoUpdated_Reload_file, "r", common.charset) as f:
            if ('Creation of AutoUpdated_Reload.sql completed.' not in f.read()):
                sys.exit("Error: %s file is not complete. SAP recommends to re-run the migration utility."%(AutoUpdated_Reload_file))
            f.close()

    Foreign_Key_Constraint_Present = False
    if os.path.isfile(extractedTables_out):
        with codecs.open(extractedTables_out, "r", common.charset) as f:
            if ('FOREIGN' in f.read()):
                Foreign_Key_Constraint_Present = True
        f.close()
    else:
        sys.exit("Error: %s file does not exist"%(extractedTables_out))

    if os.path.isfile(Foreign_Key_Constraint_file):
        with codecs.open(Foreign_Key_Constraint_file, "r", common.charset) as f:
            if ('Creation of Foreign_Key_Constraint.sql completed.' not in f.read()):
                sys.exit("Error: %s file is not complete. SAP recommends to re-run the migration utility."%(Foreign_Key_Constraint_file))
            f.close()
    elif Foreign_Key_Constraint_Present:
        sys.exit("Error: %s file does not exist"%(Foreign_Key_Constraint_file))

    if not os.path.isdir("%s%sHDL_Conn_Logs"%(reload_file_location,path_sep)):
        os.mkdir("%s%sHDL_Conn_Logs"%(reload_file_location,path_sep))

    if os.path.isfile(loadFailure_err):
        shutil.move(loadFailure_err,loadFailure_err_bkp)


#Function to load schema on HDL
def load_schema_or_foreignkey(schema_flag, foreignkey_flag):
    #set permission to run shell scripts
    os.chmod("./load_schema.sh", stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    os.chmod("./load_table.sh", stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)

    if schema_flag == 1:
        str1 = "Schema load on Hana Datalake IQ started."
        str2 = "Schema load on Hana Datalake IQ complete."
        errmsg = "Error: Schema load failed."
        file1 = "Schema load"
        file2 = "%sAutoUpdated_Reload.sql"%(path_sep)
    elif foreignkey_flag == 1:
        str1 = "Foreign key constraints load on data lake IQ started."
        str2 = "Foreign key constraints load on data lake IQ complete."
        errmsg = "Error: Foreign key constraints load failed."
        file1 = "Foreign Key Constraint load"
        file2 = "%sForeign_Key_Constraint.sql"%(path_sep)

    common.print_and_log(str1)
    if is_windows:
        conn_log_file="%s%sHDL_LoadSchema_conn.log"%(reload_file_location,path_sep)
        log_file="%s%sHDL_LoadSchema.log"%(reload_file_location,path_sep)
        with codecs.open(log_file, "a", 'utf-16') as f:
            f.write("%s%s"%(newline,common.dividerline))
            f.write("%s%s log"%(newline,file1))
            f.write("%s%s%s"%(newline,common.dividerline,newline))
        command="""dbisql  -nogui  -c 'uid=%s;pwd=%s;host=%s;ENC=tls(fips=NO;tls_type=rsa;skip_certificate_name_check=1; direct=yes;);log=%s' READ ENCODING "'%s'" %s%s%s -onerror continue >> %s"""%(common.user,common.password, common.coord_host,conn_log_file,common.charset,reload_file_location,path_sep,file2,log_file)
        output=subprocess.call(['powershell','-command',command ])
    else:
        #Run "./load.sh" to reload the table schema
        output=subprocess.call(['bash', 'load_schema.sh', common.user, common.password, common.coord_host, reload_file_location, common.Datalake_Client_Install_Path, file1, file2, common.charset ])


    if output == 0:
        common.print_and_log(str2)
    else:
        sys.exit("%s"%(errmsg))

    str1 = "For more details, Please check file: %s%s%sHDL_LoadSchema.log"%(newline,reload_file_location,path_sep)
    logging.info("%s"%(str1))
    print("%s"%(common.dividerline))
    print("%s"%(str1))
    logging.info("%s"%(common.dividerline))

#Function to check if schema reload needed in resume mode
def check_schema_load_required():

    schema_load_needed = False
    error_found = False
    if is_windows:
        enc='utf-16'
    else:
        enc=common.charset
    load_schema_output_file="%s%sHDL_LoadSchema.log"%(reload_file_location,path_sep)
    if (os.path.isfile(load_schema_output_file)):

        f=codecs.open(load_schema_output_file,'r', enc)
        lines = f.readlines()

        for line in reversed(lines):
            if("Schema load log" in line.strip()):
                break
            if("Could not" in line.strip()):
                error_found=True

        if (error_found == True) or (os.stat(load_schema_output_file).st_size == 0):
            schema_load_needed = True
    else :
        schema_load_needed = True
    return schema_load_needed

#Function to validate data uplaoded on Azure object store
def validate_upload_azure(table):
    # Instantiate a BlobServiceClient using a connection string
    blob_service_client = BlobServiceClient.from_connection_string(common.connection_string)

    # Instantiate a ContainerClient
    global container_client
    container_client = blob_service_client.get_container_client(common.az_container_name)

    # List the blobs in the container
    blob_list = container_client.list_blobs()
    upload_success = False
    tablename = table
    pattern = "%s*.gz"%(tablename)
    pattern1 = "%s*.inp"%(tablename)
    pattern2 = "%s*.txt"%(tablename)
    pattern3 = "%srow*"%(tablename)
    file_count = 0
    blob_count = 0
    table_path = data_path + "/" +table

    for path, subdirs, files in os.walk(table_path):
        for name in files:
            if fnmatch(name, pattern) or fnmatch(name, pattern1) or fnmatch(name, pattern2) or fnmatch(name, pattern3):
                file_count += 1

    blob_list = container_client.list_blobs(name_starts_with='Extracted_Data')
    for blob in blob_list:
        if (table in blob.name) and (blob.name.endswith(".gz") or blob.name.endswith(".inp") or blob.name.endswith(".txt") or ("%srow"%(tablename) in blob.name)):
            file_path = "%s%s%s"%(reload_file_location,path_sep,blob.name)
            blob_size = blob.size

            if os.path.exists(file_path):
                file_size = os.stat(file_path).st_size
            else:
                logging.info("Table %s data copy failed on Azure blob store."%(table))
                upload_success = False

            if blob_size == file_size:
                blob_count += 1

    logging.info("%s"%newline)

    if (file_count == blob_count) and (file_count or blob_count):
       logging.info("Table %s copied successfully on Azure blob store"%(table))
       upload_success = True
    else:
       files_not_uploaded = file_count - blob_count
       logging.info("Table %s data copy failed on Azure blob store"%(table))
       logging.info("File count mismatch. %s%s files not yet copied from %s to Azure blob store"%(newline,files_not_uploaded,table_path))
       upload_success = False

    return upload_success


#Function to validate data uplaoded on AWS object store
def validate_upload_aws(table):

    resource = boto3.resource(
    's3',
    aws_access_key_id = common.aws_access_key,
    aws_secret_access_key = common.aws_secret_key,
    region_name = common.aws_region
    )

    bucket = resource.Bucket(common.aws_bucket)

    tablename = table
    pattern = "%s*.gz"%(tablename)
    pattern1 = "%s*.inp"%(tablename)
    pattern2 = "%s*.txt"%(tablename)
    pattern3 = "%srow*"%(tablename)

    upload_success = False
    file_count = 0
    object_count = 0
    table_path = data_path + "/" + table

    for path, subdirs, files in os.walk(table_path):
        for name in files:
            if fnmatch(name, pattern) or fnmatch(name, pattern1) or fnmatch(name, pattern2) or fnmatch(name, pattern3):
                file_count += 1

    for s3_file in bucket.objects.filter(Prefix='Extracted_Data/'):
        if (table in s3_file.key) and (s3_file.key.endswith(".gz") or s3_file.key.endswith(".inp") or s3_file.key.endswith(".txt") or ("%srow"%(tablename) in s3_file.key)):
            file_path = "%s%s%s"%(reload_file_location,path_sep,s3_file.key)
            object_size = s3_file.size
            if os.path.exists(file_path):
                file_size = os.stat(file_path).st_size
            else:
                logging.info("Table %s data copy failed on AWS S3 bucket"%(table))
                upload_success = False
            if object_size == file_size:
                object_count += 1

    logging.info("%s"%newline)
    if (file_count == object_count) and (file_count or object_count):
       logging.info("Table %s copied successfully on AWS S3 bucket"%(table))
       upload_success = True
    else:
       files_not_uploaded = file_count - object_count
       logging.info("Table %s data copy failed on AWS S3 bucket"%(table))
       logging.info("File count mismatch. %s%s files not yet copied from %s to AWS S3 bucket"%(newline,files_not_uploaded,table_path))
       upload_success = False

    return upload_success

def get_rowcount(owner,tableName):
    ENC="tls(tls_type=rsa;direct=yes)"
    conn = sqlanydb.connect( uid=common.user, pwd=common.password, host=common.coord_host, enc=ENC )
    cursor = conn.cursor()
    cursor.execute("""select count(*) from "%s"."%s";"""%(owner,tableName))
    row_count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return row_count

def load_table(host_name, table, tableid, already_processed, expected_rowcount):
    tbl=table.split(".")
    owner=tbl[0]
    tableName=tbl[1]
    row_count=0
    row_missmatch=0
    is_loaded_successfully=False
    status="FAIL"

    log_file="%s%sHDL_Conn_Logs%s%s_load.log"%(reload_file_location,path_sep,path_sep,tableid)
    conn_log_file="%s%sHDL_Conn_Logs%s%s_load_conn.log"%(reload_file_location,path_sep,path_sep,tableid)
    out_file="%s_out"%(tableid)
    sql_file="%s%sExtracted_Data%s%s%s%s.sql"%(reload_file_location,path_sep,path_sep,tableid,path_sep,tableid)

    if owner.lower() == "dba":
        owner="HDLADMIN"

    tbl=owner+"."+tableName

    if already_processed == True:
        row_count=get_rowcount(owner,tableName)

    if (row_count == int(expected_rowcount)):
        is_loaded_successfully=True

    if is_loaded_successfully:
        status="OK"
    else:
        command="""dbisql  -nogui  -c 'uid=%s;pwd=%s;host=%s;ENC=tls(fips=NO;tls_type=rsa;skip_certificate_name_check=1; direct=yes;);log=%s' READ ENCODING "'%s'" %s -onerror exit >%s 2>>%s"""%(common.user, common.password, host_name,conn_log_file,common.charset,sql_file,out_file,log_file)
        output=subprocess.call(['powershell','-command',command ])

        f=codecs.open(out_file,'r', 'utf-16')
        lines = f.readlines()
        f.close()
        for line in lines:
            if "Error: For table" in line:
                with codecs.open(log_file, "a+", common.charset) as f:
                    f.write(line + newline)
                f.close()
                row_missmatch=1
                break

        if output==0 and row_missmatch != 1:
            status="OK"
        else:
            status="FAIL"

        row_count=get_rowcount(owner,tableName)

    if os.path.exists(out_file):
        os.remove(out_file)

    return status,row_count

#Function to track status of tables loaded successfuly
def updateLoadStatus(qSuccess,total_table,tables_count,fail_count):
    with codecs.open(HDLLoad_out, "a",common.charset) as f:
        while True:
            try:
                tableName,tableid,load_rowcount= qSuccess.get_nowait()
                f.write("%s,%s,%s%s"%(tableName,tableid,load_rowcount,newline))
                logging.info( "Adding entry in %s file %sfor table : %s [tableID:%s]"%(HDLLoad_out,newline,tableName,tableid))
                tables_count.value = tables_count.value + 1
                progressBar(tables_count,fail_count,total_table)
                if total_table.value == tables_count.value + fail_count.value:
                    break
            except Exception as exp:
                break
    f.close()

#Function to track status of tables failed to load
def updateFailureStatus(qFail,total_table,tables_count,fail_count):
    with codecs.open(loadFailure_err, "a+", common.charset) as f:
        while True:
            try:
                tableName,tableid,exp= qFail.get_nowait()
                if not tableName in f.read():
                    if exp:
                        f.write( "%s,%s:%s%s"%(tableName,tableid,str(exp),newline))
                    else:
                        f.write( "%s,%s%s"%(tableName,tableid,newline))
                logging.info("Adding entry in %s file %sfor table : %s [tableID:%s] "%(loadFailure_err,newline,tableName,tableid))
                fail_count.value = fail_count.value + 1
                progressBar(tables_count,fail_count,total_table)
                if total_table.value == tables_count.value + fail_count.value:
                    break

            except Exception as exp:
                break
    f.close()

# Function to load data for tables in multiprocessing queue by host from hostname list
# I/P parameters:
# q1 = multiprocessing queue of tables to be loaded
# hostname = Host by which tables will be loaded
# already_processed = flag indicating already processed tables for load
def load_single( q1,hostname,already_processed,total_table,log_q,qSuccess,qFail,tables_count,fail_count):
    if log_q:
        qh = QueueHandler(log_q)
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        logger.addHandler(qh)
    upload_success = False
    while True:
        try:
            table_with_tid = q1.get_nowait()
            tableName = table_with_tid[0]
            tableid = table_with_tid[1]
            host_name = hostname[0]
            expected_rowcount = table_with_tid[2]
            if common.object_store.lower() == 'azure':
                if common.Object_Store_Copy_Validation.lower() == 'yes':
                    upload_success = validate_upload_azure(tableid)
                else:
                    upload_success = True
            else:
                if common.Object_Store_Copy_Validation.lower() == 'yes':
                    upload_success = validate_upload_aws(tableid)
                else:
                    upload_success = True

            if(upload_success):
                strt = datetime.datetime.now()
                logging.info( "Starting loading of table: %s [tableID:%s] by : %s"%(tableName,tableid,hostname[1]))
                logging.info("%s"%(common.dividerline))
                loadstatus = 0
                if is_windows:
                    status,load_rowcount=load_table(host_name,tableName,tableid,already_processed, expected_rowcount)
                    if status == 'OK' and load_rowcount != 0:
                        loadstatus = 1
                else:
                    try:
                        output=subprocess.check_output(['bash', 'load_table.sh', common.user, common.password, host_name,  reload_file_location, tableName, common.Datalake_Client_Install_Path, tableid, common.charset, str(already_processed), expected_rowcount] )
                    except subprocess.CalledProcessError as loadTable:
                        logging.info( "Script load_table.sh failed with %s error code "%( loadTable.returncode))

                    if sys.version_info < (3, 0):
                        out_put = str(output)
                    else:
                        out_put = str(output,'utf-8')

                    out = out_put.splitlines()
                    status = out[0].strip()
                    res = (out[1].strip()).split()

                    if len(res) == 5:
                        load_rowcount = int(res[2].strip())
                        loadstatus = 1
                    else:
                        # Unable to fetch result in case if table load failed
                        load_rowcount = 0

                if already_processed:
                    log_str="Table :%s [tableID:%s] data with row_count :%s already loaded successfully "%(tableName,tableid,load_rowcount)
                else:
                    log_str="Table :%s [tableID:%s] data with row_count :%s loaded successfully "%(tableName,tableid,load_rowcount)

                if (status == 'OK') and (loadstatus == 1):
                    logging.info(log_str)

                    qSuccess.put((tableName,tableid,load_rowcount))
                    updateLoadStatus(qSuccess,total_table,tables_count,fail_count)

                    elap_sec = common.elap_time(strt)
                    days, hours, minutes, seconds = common.calculate_time(elap_sec)

                    logging.info("%s"%(common.dividerline))
                    logging.info("Time taken to load table = %s [tableID:%s] is : %d days, %d hours, %d minutes and %d seconds" % (tableName,tableid, days[0], hours[0], minutes[0], seconds[0]))
                    logging.info("%s"%(common.dividerline))
                else:
                    logging.info("%s"%(common.dividerline))
                    logging.info("Loading of table :%s [tableID:%s] failed"%(tableName,tableid))
                    logging.info("%s"%(common.dividerline))

                    qFail.put((tableName,tableid,None))
                    updateFailureStatus(qFail,total_table,tables_count,fail_count)

                    logging.info("%s"%newline)

        except Exception as exp:
            if str(exp) != "":
                logging.error("Unexpected error reported while loading data: %s"%(str(exp)))
                qFail.put((tableName,tableid,exp))
                updateFailureStatus(qFail,total_table,tables_count,fail_count)
                logging.info("%s"%newline)
            else:
                return


# Function to form host names for each connection based on
# value of coord_conn_num and writer_conn_num provided in json config file
def hosts_list(conn_num,hostname,servertype):
    for i in range(conn_num):
        host_list.append((hostname,servertype))

# Function to form table list to be loaded when the loading is in resume mode
def recover_table_list():
    q1 = multiprocessing.Queue() #Queue of failed tables in previous run
    q2 = multiprocessing.Queue() #Queue of tables not yet processed for load
    loaded_tables = []
    copied_tables = []
    failed_tables = []
    logging.info("*************************************************")
    logging.info("%s Data Load started in resume mode"%(datetime.datetime.now()))
    logging.info("*************************************************")

    load_file = codecs.open(HDLLoad_out,'r', common.charset)
    lines = load_file.readlines()
    for line in lines:
        stripped_line = line.strip()
        tbl = stripped_line.split(",")
        #tbl is list of [<owner>.<tablename>,tableid,rowcount]
        #validate length of tbl list, should always be 3
        len_load_tbl = len(tbl)
        if len_load_tbl == 3 :
            loaded_tables.append(tbl[0].strip())
    load_file.close()

    if os.path.exists(loadFailure_err_bkp):
        failure_file = codecs.open(loadFailure_err_bkp,'r', common.charset)
        lines = failure_file.readlines()
        for line in lines:
            stripped_line = line.strip()
            tbl = stripped_line.split(',')
            #tbl is list of [<owner>.<tablename>,tableid] or [<owner>.<tablename>,tableid:exp] - exp can 0 or multiple commas
            #validate length of tbl list, should always be greater than 2
            len_fail_tbl = len(tbl)
            if len_fail_tbl >= 2:
                failed_tables.append(tbl[0].strip())
        failure_file.close()

    if os.path.exists(extractedTables_out):
        extract_file = codecs.open(extractedTables_out,'r', common.charset)
        lines = extract_file.readlines()
        for line in lines:
            stripped_line = line.strip()
            tbl = stripped_line.split(',')
            #tbl is list of [<owner>.<tablename>,rowcount,tableid,table_type]
            #validate length of tbl list, should always be 4
            len_ext_tbl = len(tbl)
            if len_ext_tbl == 4:
                copied_tables.append(tbl[0].strip())
        extract_file.close()

        delta = [item for item in copied_tables  if item not in loaded_tables]
        delta_in_success = [item for item in delta  if item not in failed_tables]
        tables_already_loaded = len(loaded_tables)
        total_extracted_table = len(copied_tables)

        print("%s"%(common.dividerline))
        print("%s tables out of %s tables already successfully loaded by previous run of load utility."%(tables_already_loaded,total_extracted_table))

        for i in failed_tables:
            total_table.value = total_table.value + 1
            for line in lines:
                stripped_line = line.strip()
                tbl = stripped_line.split(',')
                #tbl is list of [<owner>.<tablename>,rowcount,tableid,table_type]
                #validate length of tbl list, should always be 4
                len_ext_tbl = len(tbl)
                if len_ext_tbl == 4:
                    if i == tbl[0].strip():
                        q1.put((i,tbl[2],tbl[1]))

        if (not delta_in_success) and (not failed_tables):
            logging.info("All extracted tables are already processed for data load")
        else:
            for i in delta_in_success:
                total_table.value = total_table.value + 1
                for line in lines:
                    stripped_line = line.strip()
                    tbl = stripped_line.split(',')
                    #tbl is list of [<owner>.<tablename>,rowcount,tableid,table_type]
                    #validate length of tbl list, should always be 4
                    len_ext_tbl = len(tbl)
                    if len_ext_tbl == 4:
                        if i == tbl[0].strip() and tbl[1] == 0:
                            tables_count.value = tables_count.value + 1
                            with codecs.open(HDLLoad_out, "a", common.charset) as f:
                                f.write(tbl[0].strip()  + "," + tbl[2].strip() + ",0"  + newline)
                                logging.info( "Adding entry in %s file %sfor table : %s [tableID:%s]"%(HDLLoad_out,newline,tbl[0].strip(),tbl[2].strip()))
                            f.close()
                        elif i == tbl[0].strip():
                            q2.put((i,tbl[2],tbl[1]))
    else:
        logging.info("%s file does not exist."%extractedTables_out)

    return q1,q2

# Function to form table list to be loaded in normal mode
def load_table_list():
    q = multiprocessing.Queue()
    if os.path.exists(extractedTables_out):
        f = codecs.open(extractedTables_out,"r", common.charset)
        lines = f.readlines()
        for line in lines:
            stripped_line = line.strip()
            tbl = stripped_line.split(',')
            #tbl is list of [<owner>.<tablename>,rowcount,tableid,table_type]
            #validate length of tbl list, should always be 4
            len_ext_tbl = len(tbl)
            if len_ext_tbl == 4:
                total_table.value = total_table.value + 1
                if tbl[1] == "0":
                    tables_count.value = tables_count.value + 1
                    with codecs.open(HDLLoad_out, "a", common.charset) as f:
                        f.write(tbl[0].strip() + "," + tbl[2].strip() +",0" + newline)
                        logging.info( "Adding entry in %s file %sfor table : %s [tableID:%s]"%(HDLLoad_out,newline,tbl[0].strip(),tbl[2].strip()))
                        logging.info("%s"%(common.dividerline))
                    f.close()
                else:
                    q.put((tbl[0].strip(),tbl[2],tbl[1]))
        f.close()
    else:
        logging.info("%s file does not exist."%extractedTables_out)

    return q

# Get count of tables loaded successfully to HDL
# input file is HDL_LoadedTables.out
def loaded_tables_count(f):
    l1 = list()
    global table_cnt
    if not os.path.isfile(HDLLoad_out):
        table_cnt = 0
    else:
        with codecs.open(f, "r", common.charset) as f:
            for line in f.readlines():
                stripped_line = line.strip()
                tbl = stripped_line.split(",")
                #tbl is list of [<owner>.<tablename>,tableid,rowcount]
                #validate length of tbl list, should always be 3
                len_load_tbl = len(tbl)
                if len_load_tbl == 3:
                    l1.append(tbl[0].strip())

        table_cnt = len(l1)

# Function to display loading progress
def progressBar(current,fail_count,total_table):
    if (((current.value + fail_count.value )% 20) == 0) or ((current.value + fail_count.value )== total_table.value):
        print("%s tables successfully loaded and %s tables failed out of total %s tables."%(current.value,fail_count.value,total_table.value))
        print("%s%s"%(newline,common.dividerline))

# Function to check the status of migration
# It will compare the IQ table list with the list of loaded tables
# If both list are equal then extraction of all tables done successfully
def check_migration_status(f1,f2):
    extracted_table_list = list()
    foreign_table_list = list()
    iq_table_list = list()
    with codecs.open(f1, "r", common.charset) as f:
        for line in f.readlines():
            line = line.rstrip('\n')
            splits = line.split(',')
            #tbl is list of [<owner>.<tablename>,rowcount,tableid,table_type]
            #validate length of tbl list, should always be 4
            len_ext_tbl = len(splits)
            if len_ext_tbl == 4:
                splitdot = splits[0].split('.')
                extracted_table_list.append((splits[0]))
                iq_table_list.append(splits[0])
                if splits[3] == 'FOREIGN':
                    foreign_table_list.append(splits[0])

    if not os.path.isfile(f2):
        logging.info("%s"%(common.double_divider_line))
        logging.info("%s file does not exist. %sLoading of all tables failed."%(f2,newline))
        logging.info("%s"%(common.double_divider_line))

    else:
        loaded_table_list = list()

        with codecs.open(f2, "r" , common.charset) as f:
            for line in f.readlines():
                line = line.rstrip('\n')
                splits = line.split(',')
                #tbl is list of [<owner>.<tablename>,tableid,rowcount]
                #validate length of tbl list, should always be 3
                len_load_tbl = len(splits)
                if len_load_tbl == 3:
                    splitdot = splits[0].split('.')
                    loaded_table_list.append((splits[0]))

        failed_list = [item for item in extracted_table_list  if item not in loaded_table_list]

        if len(failed_list) == 0:
            logging.info("%s"%(common.double_divider_line))
            logging.info("Loading of all tables is successful.")
            logging.info("%s"%(common.double_divider_line))
        else:
            fail = len(failed_list)
            logging.info("%s"%(common.double_divider_line))
            logging.info("Loading of %s tables failed."%fail)
            logging.info("Please check %s file for loading failures. %sRerun Load Utility to load remaining tables."%(loadFailure_err,newline))
            logging.info("%s"%(common.double_divider_line))

    if len(foreign_table_list) != 0:
        flag = 0
        # condition to check that all foreign key constraint tables have been loaded

        if((set(foreign_table_list) & set(iq_table_list))== set(foreign_table_list)) :
            flag = 1

        if flag == 1:
            foreign_strt = datetime.datetime.now()
            load_schema_or_foreignkey(0,1)
            foreign_elaptime = common.elap_time(foreign_strt)
            days, hours, minutes, seconds = common.calculate_time(foreign_elaptime)
            str1 = "Time taken in Foreign key constraints load on data lake IQ : %s%d days, %d hours, %d minutes and %d seconds" % ( newline, days[0], hours[0], minutes[0], seconds[0])
            common.print_and_log(str1)
        else:
            str1 = "As there are table load errors, foreign key constraints have not been loaded in data lake IQ. %sAnalyze and fix the errors and re-run the load utility."%newline
            common.print_and_log(str1)

# Function which will do parallel load and load table data
def load_main():
    global q_listener,log_q,logger

    if not is_windows:
        log_q = None

    start = datetime.datetime.now()
    hosts_list(common.coord_conn_num,common.coord_host,'Coordinator')
    hosts_list(common.writer_conn_num,common.writer_host,'Writer')

    str1 = "Data load on Hana Datalake IQ started."
    common.print_and_log(str1)
    print("%s"%(common.dividerline))
    print("Data Loading on Hana Datalake IQ is in progress.%sFor details of tables loaded on Hana Datalake IQ successfully, Please check file: %s%s"%(newline,newline,HDLLoad_out))

    print("%s%s"%(newline,common.dividerline))
    print("For Load progress, Please check file: %s%s"%(newline,load_schema_and_data_log))
    print("%s"%(common.dividerline))

    load_table_q = multiprocessing.Queue()
    failed_table_q = multiprocessing.Queue()
    qSuccess = multiprocessing.Queue()
    qFail = multiprocessing.Queue()


    if(resume_mode):
        failed_table_q,load_table_q = recover_table_list()
    else:
        load_table_q = load_table_list()

    process = []

    #Start failed table processing
    for i in range(len(host_list)):
        p = multiprocessing.Process(target=load_single, args=(failed_table_q,host_list[i],True,total_table,log_q,qSuccess,qFail,tables_count,fail_count))
        process.append(p)
        p.start()
    for p in process:
        p.join()

    #Remove backup file after failed table processing
    if os.path.isfile(loadFailure_err_bkp):
        os.remove(loadFailure_err_bkp)

    #Start processing of tables which are yet to process
    for i in range(len(host_list)):
        p = multiprocessing.Process(target=load_single, args=(load_table_q,host_list[i],False,total_table,log_q,qSuccess,qFail,tables_count,fail_count))
        process.append(p)
        p.start()
    for p in process:
        p.join()

    total_elap_sec = common.elap_time(start)
    days, hours, minutes, seconds = common.calculate_time(total_elap_sec)

    logging.info("Data load on Hana Datalake IQ completed. ")
    loaded_tables_count(HDLLoad_out)
    logging.info("Total number of loaded tables = %s"%table_cnt)
    logging.info("Total number of tables which are failed to load = %s"%fail_count.value)
    logging.info("%s"%(common.dividerline))
    logging.info("Total Data Load Time : %s%d days, %d hours, %d minutes and %d seconds" % ( newline, days[0], hours[0], minutes[0], seconds[0]))
    logging.info("%s"%(common.dividerline))
    check_migration_status(extractedTables_out, HDLLoad_out)

if __name__ == '__main__':
    host_list = []
    total_strt = datetime.datetime.now()
    global resume_mode
    resume_mode = False
    get_inputs(config_file)
    validate_dir_and_files()
    if check_schema_load_required():
        strt = datetime.datetime.now()
        str1 = "%sDo you want to %sR - Restart schema load%sS - Skip schema load in resume mode? (R/S): "%(newline,newline,newline)
        if resume_mode:
            if(sys.version[0:2] == '2.'):
                val = str(raw_input(str1))
            else:
                val = str(input(str1))
            if val.lower() == 'r':
                load_schema_or_foreignkey(1,0)
            elif val.lower() != 's':
                sys.exit("Enter correct input value. Supported values are R(Restart) and S(Skip)")
        else:
            load_schema_or_foreignkey(1,0)
        elaptime = common.elap_time(strt)
        days, hours, minutes, seconds = common.calculate_time(elaptime)
        str1 = "Time taken in Schema Load : %s%d days, %d hours, %d minutes and %d seconds" % ( newline, days[0], hours[0], minutes[0], seconds[0])
        common.print_and_log(str1)
    if len(extractedTables_out) == 0 or not os.path.isfile(extractedTables_out):
        logging.info("%s"%(common.double_divider_line))
        logging.info("The Database has no IQ tables. No need of Loading.")
        logging.info("%s"%(common.double_divider_line))
    else:
        load_main()

    print("Loading of Schema and Data on Hana Datalake IQ completed.%sPlease check file for details :%s%s"%(newline,newline,load_schema_and_data_log))
    total_elaptime = common.elap_time(total_strt)
    days, hours, minutes, seconds = common.calculate_time(total_elaptime)
    string = "Total Time taken in load utility : %s%d days, %d hours, %d minutes and %d seconds" % ( newline, days[0], hours[0], minutes[0], seconds[0])
    common.print_and_log(string)
    print("%s"%(common.dividerline))
    logging.info("%s"%(common.dividerline))
