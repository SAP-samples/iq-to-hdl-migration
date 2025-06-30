# ----------------------------------------------------------------------
# @(#)Migration                      2021              SAP
# ----------------------------------------------------------------------
# Migration utilities to migrate SAP IQ on SAP datalake IQ.
# ----------------------------------------------------------------------
#
# ***************************************************************************
# Copyright (c) 2021 SAP SE or an SAP affiliate company. All rights reserved.
# ***************************************************************************
import pyodbc
import multiprocessing,ctypes
import time
import datetime
import os
import subprocess
import csv
import sys, getopt
import socket
import getpass
import os.path
import platform
import re
import logging
from multiprocessing import Process, Manager
from multiprocessing.managers import BaseManager, SyncManager
import json
import shutil
import codecs
from logging.handlers import QueueHandler, QueueListener
import fnmatch
import heapq
import math
argv = sys.argv[1:]
n = len(sys.argv)

# Defaults
config_file = ''
dbmode = 'r'
onlyschema = 'n'
onlydata = 'n'
fullextraction = 'n'

# Handle help first
if '-h' in argv or '--help' in argv:
    print('''
Usage:
    migration.py --config_file <config file path> [--mode w] [--onlyschema y] [--onlydata y] [--fullextraction y]
Same as:
    migration.py -f <config file path> [-m w] [-s y] [-d y] [-e y]

Switch Details:
    --config_file or -f     : Mandatory. Denotes utilizing the config file to access parameters from.
    --mode or -m            : Optional. To run the migration utility when database is in read-write mode. Use 'w' for read-write.
    --onlyschema or -s      : Optional. To run the migration utility only for schema unload. Use 'y' to unload only schema.
    --onlydata or -d        : Optional. To run the migration utility only for data unload. Use 'y' to unload only data.
    --fullextraction or -e  : Optional. To run the migration utility for both schema and data unload. Use 'y' to unload both schema and data.

Note:
    Only one of --onlyschema, --onlydata, or --fullextraction can be 'y'. They are mutually exclusive.
    One of the three options must be provided and set to 'y'.
    SAP recommends to run coordinator node with -iqro flag while migration process.
        ''')
    sys.exit()

# Validate for incorrect short forms like -mode, -onlyschema etc.
for arg in argv:
    if arg.startswith('-') and not arg.startswith('--'):
        if arg not in ['-h', '-f', '-m', '-s', '-d', '-e']:
            print(f"Error: Unsupported or incorrectly formatted option '{arg}'. Use proper short or long options.")
            sys.exit(2)

try:
    opts, args = getopt.getopt(argv, "hf:m:s:d:e:", ["help", "config_file=", "mode=", "onlyschema=", "onlydata=", "fullextraction="])
except getopt.GetoptError:
    print("Error : Unsupported option/values. Run migration.py -h or --help for help")
    sys.exit(2)

for opt, arg in opts:
    if opt in ("-f", "--config_file"):
        config_file = arg
    elif opt in ("-m", "--mode"):
        if arg != 'w':
            sys.exit("Error: --mode or -m only supports 'w' as a valid value.")
        dbmode = arg
    elif opt in ("-s", "--onlyschema"):
        if arg.lower() != 'y':
            sys.exit("Error: --onlyschema or -s only supports 'y'. Use this option only if you want to unload only schema.")
        onlyschema = arg.lower()
    elif opt in ("-d", "--onlydata"):
        if arg.lower() != 'y':
            sys.exit("Error: --onlydata or -d only supports 'y'. Use this option only if you want to unload only data.")
        onlydata = arg.lower()
    elif opt in ("-e", "--fullextraction"):
        if arg.lower() != 'y':
            sys.exit("Error: --fullextraction or -e only supports 'y'. Use this option only if you want unload both schema and data.")
        fullextraction = arg.lower()    

# Check if config_file is provided
if config_file.strip() == '':
    sys.exit("Error: --config_file or -f is a mandatory option. Please specify a valid config file path.")

# Validation for mutual exclusivity
flags = [onlyschema == 'y', onlydata == 'y', fullextraction == 'y']
if flags.count(True) > 1:
    sys.exit("Error: --onlyschema, --onlydata, and --fullextraction are mutually exclusive. Only one can be 'y'. Run migration.py -h or --help for help.")
elif flags.count(True) == 0:
    sys.exit("Error: One of --onlyschema, --onlydata, or --fullextraction must be 'y'. Run migration.py -h or --help for help.")

# detect the current working directory and print it
path = os.getcwd()
global path_sep
global newline
global is_windows
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
common.get_inputs(config_file,'Migration')
common.host_validation(config_file,'Migration')
common.mig_inputs(config_file,'Migration')

global migrationpath
migrationpath = "%s%sMigration_Data"%(common.shared_path,path_sep)

global datapath
datapath = "%s%sExtracted_Data"%(migrationpath,path_sep)

global extractedTables_out
extractedTables_out = "%s%sExtractedTables.out"%(migrationpath,path_sep)

global extractedFailures_err
extractedFailures_err = "%s%sextractFailure.err"%(migrationpath,path_sep)

global migration_log
migration_log = "%s%smigration.log"%(path,path_sep)

global no_extraction_file
no_extraction_file="%s%sno_extraction.list"%(migrationpath,path_sep)

global compressed_data
compressed_data=1

global byteorder
byteorder=sys.byteorder

global nodes_count
nodes_count = 1

global extract_list
extract_list = []

global connection_list
connection_list = []

lock = multiprocessing.Lock()
tables_count = multiprocessing.Value(ctypes.c_int, 0)
fail_count = multiprocessing.Value(ctypes.c_int, 0)
total_table = multiprocessing.Value(ctypes.c_int, 0)
empty_table_count = multiprocessing.Value(ctypes.c_int, 0)


# Read the json config file and get all values
def get_inputs(config_file):
    strt = datetime.datetime.now()

    print ('Reading Config File: %s' %(config_file))
    print("%s"%(common.dividerline))

    if (len(common.shared_path) > 90):
        sys.exit("Please restrict Extract_Path length to below 90 bytes as there is limitation of 128 Bytes for Extract_Path and Migration Utility further creates folders in Extract_Path. Current length: %s bytes"%len(common.shared_path))

    global iqtables_list
    iqtables_list = "%s%siq_tables.list"%(migrationpath,path_sep)

    global reload_path
    reload_path = "%s%sreload.sql"%(migrationpath,path_sep)

    global AutoUpdatedReload_path
    AutoUpdatedReload_path = "%s%sAutoUpdated_Reload.sql"%(migrationpath,path_sep)

    global resume
    resume = False
    create_shareddir()
    global val
    global listener_q,log_q,logger

    str1 = "Migration path %s already exists. %sDo you want to %sR - Restart(Existing %s folder and its content would be deleted)%sE - Resume%sC - Cancel%sthe Migration process? (R/E/C): "%(migrationpath,newline,newline,migrationpath,newline,newline,newline)
    if os.path.isdir(migrationpath):
        if(sys.version[0:2] == '2.'):
            val = str(raw_input(str1))
        else:
            val = str(input(str1))

        if val.lower() == "r":
            if onlydata == 'y':
                sys.exit("Restart is not allowed in data-only mode. Use Resume or Cancel.")
            try:
                shutil.rmtree(migrationpath)
                if not is_windows:
                    logging.basicConfig(filename=migration_log, filemode='w', format='%(message)s', level=logging.INFO)
                else:
                    listener_q,log_q,logger = logger_init('w')
                logging.info("%s*************************************************************"%newline)
                logging.info("[%s] : Migration Utility restarted. %sDeleted existing %s folder."%(datetime.datetime.now(),newline,migrationpath))
                logging.info("*************************************************************")
            except OSError as e:
                print("Error: %s : %s" % (migrationpath, e.strerror))

            create_migrationdir()
        elif val.lower() == "e":
            resume = True
            if not is_windows:
                logging.basicConfig(filename=migration_log, filemode='a', format='%(message)s', level=logging.INFO)
            else:
                listener_q,log_q,logger = logger_init('a')
            logging.info("%s%s%s%s"%(newline,newline,newline,common.double_divider_line))
            logging.info("[%s] : Migration Utility started in Resume mode."%(datetime.datetime.now()))
            logging.info("%s"%(common.double_divider_line))
            logging.info("It will use pre-existing folder to store Migration Utility output data :%s%s "%(newline,migrationpath))
            logging.info("%s"%(common.dividerline))
        elif val.lower() == "c":
            sys.exit("Migration Utility exit.")
        else:
            sys.exit("Enter correct input value. Supported values are R(Restart), E(Resume) and C(Cancel).")
    else:
        if not is_windows:
            logging.basicConfig(filename=migration_log, filemode='w', format='%(message)s', level=logging.INFO)
        else:
            listener_q,log_q,logger = logger_init('w')
        logging.info("%s*************************************************************"%newline)
        if onlyschema == 'y':
            logging.info("[%s] : Schema Unload Started."%(datetime.datetime.now()))
        elif onlydata == 'y':
            logging.info("[%s] : Data Unload Started."%(datetime.datetime.now()))
        elif fullextraction == 'y':
            logging.info("[%s] : Schema and Data Unload Started."%(datetime.datetime.now()))
        logging.info("*************************************************************")
        create_migrationdir()

    logging.info("%s"%(common.dividerline))
    logging.info("Reading and Verification of config file : %s%s started"%(newline,config_file))

    logging.info("%s"%(common.dividerline))
    logging.info(common.config_valid_str)

    logging.info("%s"%(common.dividerline))
    logging.info("The current working directory is:\n%s" % path)

    logging.info("%s"%(common.dividerline))
    logging.info("IQ Server Hostname : %s"%(common.hostname))

    logging.info("%s"%(common.dividerline))
    logging.info("Python version: %s"%sys.version)

    if common.w == common.t:
        logging.info("Verifying HDLFS_Configuration")
        logging.info("%s"%(common.dividerline))

    logging.info("Client Hostname: %s , ip-address : %s , full-hostname : %s"%(common.host,common.ipaddress,common.fullhostname))

    global connectstr
    connectstr = common.conn_str

    elap_sec = common.elap_time(strt)
    logging.info("%s"%(common.dividerline))
    logging.info("Reading and Verification of config file : %s%s completed in %s seconds"%(newline,config_file,elap_sec))

# Initialize logger with handler and queue the records and send them to handler
# This function is applicable only for Windows OS as
# On Windows child processes will only inherit the level of the parent process’s logger –
# any other customization of the logger will not be inherited." Subprocesses won't inherit the handler,
# and  can't pass it explicitly because it's not pickleable
def logger_init(file_mode):
    global val
    q = multiprocessing.Queue()
    # this is the handler for all log records
    file_handler = logging.StreamHandler()
    if os.path.isdir(migrationpath):
        # val 'r' indicates restart mode and 'e' indicates resume mode
        if val.lower() == "r":
            try:
                file_handler = logging.FileHandler(migration_log,mode=file_mode)

            except OSError as e:
                print("Error: %s : %s" % (migrationpath, e.strerror))

        elif val.lower() == "e":
            file_handler = logging.FileHandler(migration_log,mode=file_mode)
    else:
        file_handler = logging.FileHandler(migration_log,mode=file_mode)


    # ql gets records from the queue and sends them to the handler
    record_q = QueueListener(q, file_handler)
    record_q.start()

    logger = logging.getLogger()
    formatter    = logging.Formatter('%(message)s')
    file_handler.setFormatter(formatter)
    # add file handler to logger
    logger.addHandler(file_handler)
    logger.setLevel(logging.INFO)
    # add the handler to the logger so records from this process are handled
    logger.addHandler(file_handler)

    return record_q, q ,logger


# Function to check if we are connected to MPX coordinator or not
def mpx_verify(connectstr):
    try:
        conn = pyodbc.connect(connectstr, timeout=0)
    except Exception as exp:
        sys.exit("Exception: %s"%str(exp))
    cursor = conn.cursor()
    cursor.execute("select count(*) from sp_iqmpxinfo()")
    mpx_count = cursor.fetchone()[0]
    if mpx_count > 0:
        cursor.execute("select server_name from SYS.SYSIQMPXSERVER where role=0")
        coord_name = cursor.fetchone()[0]
        cursor.execute("select @@servername")
        server_name = cursor.fetchone()[0]
        if coord_name != server_name:
            sys.exit("Migration Utility can be run only on MPX coordinator node")
        cursor.execute("select count(*) from sp_iqmpxinfo() where ((status='included' and inc_state='active') and (role='writer' or role='reader'));")
        sec_nodes = cursor.fetchone()[0]
        global nodes_count
        nodes_count = nodes_count + sec_nodes

    cursor.close()
    conn.close()

# Function to verify hostname with the host from which Migration Utility is running
def verify_hostname():
    if common.same_host == True:
        logging.info("%s"%(common.dividerline))
        logging.info("Migration Utility is run from same host as of Coordinator / IQ server")
        return 1
    else:
        logging.info("%s"%(common.dividerline))
        logging.info("Migration Script is run from different host as of Coordinator / IQ server")
        return 0

# Function which whether we enable parallel extract or not based on IQ version
def version_verify(connectstr):
    try:
        conn = pyodbc.connect(connectstr, timeout=0)
    except Exception as exp:
        sys.exit("Exception: %s"%str(exp))
    cursor = conn.cursor()
    cursor.execute("select @@version;")
    global version
    version = cursor.fetchone()[0]

    logging.info("%s"%(common.dividerline))
    logging.info("IQ Database version:%s%s"%(newline,version))
    global compressed_data
    if 'SAP IQ/16.0.' in version:
        logging.info("%s"%(common.dividerline))
        logging.info("16.0. version is not supported with data lake Files due to time constraint, as data extraction will be in binary or ascii format without parallelization.")
        sys.exit("16.0. version is not supported with data lake Files due to time constraint, as data extraction will be in binary or ascii format without parallelization.")
    else:
        if 'SAP IQ/16.1.01' in version:
            compressed_data = 0
            logging.info("Extraction of data will be in text/binary format with parallelization.")
        else:
            compressed_data = 1
            logging.info("Extraction of data will be in compressed format with parallelization.")
    cursor.close()
    conn.close()

# Function to verify IQ charset based on which AutoUpdated_Reload.sql will be read and written.
def charset_verify():
    logging.info("%s"%(common.dividerline))
    logging.info("IQ Database Charset: %s"%common.charset)

# Function to check if db is readonly or not
# If it is readonly then proceed else the utility should exit
def db_readonly_verify(connectstr):
    try:
        conn = pyodbc.connect(connectstr, timeout=0)
    except Exception as exp:
        sys.exit("Exception: %s"%str(exp))
    cursor = conn.cursor()
    cursor.execute("select db_property('readonly');")
    db_readonly = cursor.fetchone()[0]
    logging.info("%s"%(common.dividerline))
    if db_readonly == 'Off':
        try:
            s1 = 'Migration_Data'
            s2 = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
            s = s1 + "_" + s2
            logging.info("Creating a test table to verify if database is in readonly mode.")
            cursor.execute("create table %s(c1 int)"%(s))
        except Exception as exp:
            if "no dbspaces open in readwrite mode for this RW operation" in str(exp):
                logging.error("SAP IQ Coordinator started with -iqro start-up flag and no dbspaces is open in read-write mode.")
                logging.error("Migration will continue, as database is in readonly mode")
            else:
                logging.error("Unexpected error reported while creating a test table: %s"%str(exp))
                sys.exit("Check the error and rerun the Migration Utility")
        else:
            cursor.execute("drop table %s"%(s))
            logging.info("%s"%(common.dividerline))
            logging.info("Warning: It is recommended to start SAP IQ DB in readonly mode to execute Migration Utility.%sStart coordinator with -iqro start-up flag else on-prem and HDL tables may have delta which will not be handled by Migration Utility."%newline)
            sys.exit("If you still to want to proceed with Migration without making database in readonly mode, run the Migration Utility with -m w or --mode w%s "%newline)
    cursor.close()
    conn.close()

# Create shared directory if not exists where the actual extracted files stored
def create_shareddir():
    try:
        if not(os.path.exists(common.shared_path)):
            str1 = "Shared Directory %s does not exists.%sDo you want to create Shared Directory? (Y/N): "%(common.shared_path,newline)
            if(sys.version[0:2] == '2.'):
                val = str(raw_input(str1))
            else:
                val = str(input(str1))
            if val.lower() == "y":
                try:
                    os.mkdir(common.shared_path)
                    print("Shared directory %s created to store Extracted files." % common.shared_path)
                    print("%s"%(common.dividerline))
                except OSError as e:
                    sys.exit("%sCreation of the shared directory %s failed." % (newline,common.shared_path))
            elif val.lower() == "n":
                sys.exit("Migration Utility terminated as it could not create common.shared_path: %s"%common.shared_path)
            else:
                sys.exit("Enter correct input. It only accepts Y(Yes) or N(No) as an input. ")
        else:
            print("Shared directory %s already exists.%sExtracted data will be stored in this location." % (common.shared_path,newline))
    except OSError:
        sys.exit("%sMigration Utility terminated as it could not create common.shared_path:%s%s" %(newline,newline, common.shared_path))

# Given the location of shared directory where either fuse driver is mounted
# or a shared directory where we will extract data from all nodes
# create a Migration_Data folder where all the schema and extracted files are placed
def create_migrationdir():
    try:
        os.mkdir(migrationpath)
    except OSError:
        sys.exit("%sMigration Utility terminated as it could not create directory :%s%s" % (newline,newline,migrationpath))
    else:
        logging.info("%s"%(common.dividerline))
        logging.info("To store all schema and table extracted data, migration directory created :%s%s" % (newline,migrationpath))

# create a Extracted_Data folder where all the schema and extracted files are placed
def create_extractdatadir():
    try:
        os.mkdir(datapath)
    except OSError:
        sys.exit("%sCreation of the directory %s failed." % (newline,datapath))
    else:
        logging.info("%s"%(common.dividerline))
        logging.info("To store table extracted data, extract directory created :%s%s" % (newline,datapath))

# Function which check for Extracted_Data directory, if not exist then create
def check_create_dataextractdir():
    if os.path.isdir(datapath) and resume == True:
        logging.info("%s"%(common.dividerline))
        logging.info("Extracted_Data directory already exist as migration utility started in resume mode :%s%s" % (newline,datapath))
    else:
        create_extractdatadir()


# Step 1 : Unload the schema using iqunload

# Function for schema unload with iqunload
# If host is same directly run iqunload
def iqunload_samehost():
    str1 = "Starting schema unload"
    common.print_and_log(str1)
    print("%s"%(common.dividerline))
    cmd1 = "iqunload -n -c\"UID=%s;PWD=%s;ENG=%s;dbn=%s;links=tcpip(host=%s;port=%s);ENC=%s\" "%(common.userid,common.password,common.server_name,common.dbname,common.hostname,common.port,common.enc_string)
    cmd2 = "-up"
    if 'SAP IQ/16.0.' in version:
        command = cmd1
    else:
        command = cmd1 + cmd2
    try:
        if os.system(command) != 0:
            raise Exception('IQUNLOAD command failed')
    except:
        sys.exit("Exception Occurred")
    reloadsql_path = '%s%sreload.sql'%(path,path_sep)
    if os.path.isfile(reloadsql_path) and os.access(reloadsql_path, os.R_OK):
        str1 = "Schema unload completed successfully and output logged in :%s%s"%(newline,reloadsql_path)
        common.print_and_log(str1)
    else:
        print("%s"%(common.dividerline))
        sys.exit("Schema unload fails.%sEither the %s file is missing or not readable"%(newline,reloadsql_path))
    shutil.move(reloadsql_path,migrationpath)
    str1 = "%s %sis moved to %s%s"%(reloadsql_path,newline,newline,reload_path)
    common.print_and_log(str1)

# Function to make a script for inqunload to run on different host
def make_iqunloadscript(path ):
    with codecs.open("SchemaUnload.sh", "w", common.charset) as f:
        f.write( "cd " + path + "%s"%(newline) )
        f.write( "SYBASE=" + path + ";export SYBASE"+ "%s"%(newline) )
        f.write( ". $SYBASE/IQ.sh " + "%s"%(newline) )
        cmd1 = "iqunload -n -c\"UID=%s;PWD=%s;ENG=%s;dbn=%s;links=tcpip(host=%s;port=%s);ENC=%s\" "%(common.userid,common.password,common.server_name,common.dbname,common.hostname,common.port,common.enc_string)
        cmd2 = "-up"
        if 'SAP IQ/16.0.' in version:
            command = cmd1
        else:
            command = cmd1 + cmd2

        f.write(command + "%s"%(newline) )
        f.close()

# Function to do iqunload using ssh on different host
def iqunload_differenthost():
    import paramiko # type: ignore

    str1 = "Starting schema unload"
    common.print_and_log(str1)
    print("%s"%(common.dividerline))
    make_iqunloadscript(common.sybase_path)

    client = None
    sftp = None

    try:
        # Connect to remote host
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect( common.hostname, username=common.client_id, password=common.client_pwd)

        # Setup sftp connection and transmit this utility
        localfile = path + '%sSchemaUnload.sh'%path_sep
        remote_file = '%s%sSchemaUnload.sh' % (common.sybase_path, path_sep)
        sftp = client.open_sftp()
        sftp.put(localfile, remote_file)
        logging.info(f"Uploaded {localfile} to {remote_file}")
        sftp.close()
        sftp = None

        # Execute the unload script
        commands = [
            f'cd {common.sybase_path}; rm -f reload.sql; chmod 755 SchemaUnload.sh; sh SchemaUnload.sh >& SchemaUnload.out',
            f'cd {common.sybase_path}; ls reload.sql'
        ]

        for command in commands:
            stdin, stdout, stderr = client.exec_command(command, get_pty=True)
            exit_status = stdout.channel.recv_exit_status()
            stderr_content = stderr.read().decode().strip()
            if exit_status != 0:
                raise Exception(f"Command failed: {command}\nError: {stderr_content}")
            logging.info(f"Command succeeded: {command}")

        # Check if reload.sql was successfully created
        success_iqunload = stdout.readlines()
        if any('reload.sql' in line for line in success_iqunload):
            str1 = f"Schema unload completed successfully and output logged in: {newline}{common.sybase_path}/reload.sql"
            common.print_and_log(str1)
        else:
            raise Exception(f"Schema unload failed on {common.hostname}. Please check: {common.sybase_path}/SchemaUnload.out for failure(s).")

        # Download the reload.sql file
        newfilepath = path + '%sreload.sql' % path_sep
        oldfilepath = common.sybase_path + '%sreload.sql' % path_sep
        sftp = client.open_sftp()
        sftp.get(oldfilepath, newfilepath)
        sftp.close()
        sftp = None

        # Verify the downloaded file
        reloadsql_path = '%s%sreload.sql' % (path, path_sep)
        if os.path.isfile(reloadsql_path) and os.access(reloadsql_path, os.R_OK):
            logging.info(f"Schema unload completed successfully and moved reload.sql file to: {newline}{reloadsql_path}")
        else:
            raise Exception(f"Schema unload failed. Either the {reloadsql_path} file is missing or not readable.")

        # Move the file to the migration data path
        data_path = "%s%sMigration_Data" % (common.shared_path, path_sep)
        shutil.move(reloadsql_path, data_path)
        str1 = f"{reloadsql_path} {newline}is moved to {newline}{reload_path}"
        common.print_and_log(str1)
    except Exception as e:
        logging.error(f"Error during schema unload: {str(e)}")
        raise

    finally:
        # Ensure resources are cleaned up
        if sftp:
            sftp.close()
        if client:
            client.close()

# Function which call iqunload on the basis of verification of host
def iqunload_call():
    val = verify_hostname()
    if val ==1:
        iqunload_samehost()
    else:
        iqunload_differenthost()

# Step 2 : To modify the reload.sql file generated using iqunload

# Function to get original schema reload.sql into a list
def load_reloadfile_to_list(filename):
    file_list = list()
    file_list.append("-- Creation of AutoUpdated_Reload.sql started. ")
    with codecs.open("%s%s%s"%(migrationpath,path_sep,filename), "r", common.charset) as f:
        for line in f.readlines():
            file_list.append(line)

    file_list.append("-- Creation of AutoUpdated_Reload.sql completed. ")
    return file_list

# Function to get DB_artifacts file into a list
def load_artifactsfile_to_list(filename):
    artifact_list = list()
    pwd = os.getcwd()

    with codecs.open("%s%s..%sCommon%s%s"%(pwd,path_sep,path_sep,path_sep,filename), "r", common.charset) as f:
        for line in f.readlines():
            line = line.rstrip(newline)
            splits = line.split(',')
            length = len(splits)
            if length == 2:
                artifact_list.append((splits[0], splits[1], "NULL","NULL"))
            elif length == 3:
                artifact_list.append((splits[0], splits[1], splits[2],"NULL"))
            else:
                artifact_list.append((splits[0], splits[1], splits[2], splits[3]))
    return artifact_list

# Function to write the list into a new file
# This function is used to write modified schema into AutoUpdated_Reload.sql
def write_lines_intofile(filename, lines):
    with codecs.open("%s"%(filename), "w", common.charset) as f:
        for line in lines:
            f.write(line + "%s"%(newline))
    return

# Function to add dbo user dependent objects in DB_Artifacts.list
def dbo_user_dependent_objects(conn, dbo_artifact_list):
    cursor = conn.cursor()

    table_id_list = []
    # Get the tableid and tablename of dbo user tables
    cursor.execute("select table_id, table_name from SYS.SYSTABLE JOIN SYS.SYSUSER ON user_id = creator WHERE lower(user_name) in ('dbo')  AND table_type = 'BASE' and server_type='IQ';")
    records = cursor.fetchall()
    for i in records:
        dbo_artifact_list.append((i[1],"COMMENT","CREATE TABLE","dbo"))
        dbo_artifact_list.append((i[1],"COMMENT","CREATE procedure","dbo"))

        table_id_list.append(i[0])

    view_id_list = list()
    view_name_list = list()
    for row in table_id_list:
        # Add index of dbo user tables
        cursor.execute("select index_name from SYS.SYSINDEX i join SYS.SYSTABLE t on t.table_id = i.table_id where t.table_id = %s"%row)
        records = cursor.fetchall()
        for i in records:
            dbo_artifact_list.append((i[0],"COMMENT","CREATE","INDEX"))

        # Add triggers on dbo user tables
        cursor.execute("select trigger_name from SYS.SYSTRIGGER where table_id=%s"%row)
        trigger_record = cursor.fetchall()
        for i in trigger_record:
            dbo_artifact_list.append((i[0],"COMMENT","CREATE TRIGGER","NULL"))
            dbo_artifact_list.append((i[0],"COMMENT", "COMMENT ON", "TRIGGER"))
            dbo_artifact_list.append((i[0],"COMMENT", "COMMENT TO PRESERVE FORMAT ON", "TRIGGER"))

        # Add view on dbo user tables in DB_Artifacts.list
        cursor.execute("""select v.table_name,d.dep_object_id from SYSDEPENDENCY d join systab t on t.object_id = d.ref_object_id
                     join systab v on v.object_id = d.dep_object_id where t.table_id = %s"""%row)
        records = cursor.fetchall()
        for i in records:
            view_id_list.append(i[1])
            view_name_list.append(i[0])

    # Checking for multi level view dependency
    if (len(view_id_list) != 0):
        while (len(view_id_list) != 0):
            ref_object = view_id_list.pop()
            cursor.execute("""select v.table_name,d.dep_object_id from SYSDEPENDENCY d join systab t on t.object_id = d.ref_object_id join systab v on v.object_id = d.dep_object_id where d.ref_object_id = %s"""%(ref_object))
            records = cursor.fetchall()
            for i in records:
                view_id_list.append(i[1])
                view_name_list.append(i[0])

    # append all the view names to artifacts list
    for i in view_name_list:
        dbo_artifact_list.append((i,"COMMENT","CREATE VIEW","NULL"))
        dbo_artifact_list.append((i,"COMMENT", "COMMENT ON", "VIEW"))
        dbo_artifact_list.append((i,"COMMENT", "COMMENT TO PRESERVE FORMAT ON", "VIEW"))

    # procedure and functions whose creator is dbo user
    cursor.execute("""select proc_name from SYS.SYSPROCEDURE p join sysuser s ON (p.creator = s.user_id) where
                        lower(s.user_name)='dbo' and proc_name not like 'sp_%' and proc_name not like 'sa_%'""")
    records = cursor.fetchall()
    for i in records:
        dbo_artifact_list.append((i[0],"COMMENT","CREATE procedure","NULL"))
        dbo_artifact_list.append((i[0],"COMMENT", "COMMENT ON", "Procedure"))
        dbo_artifact_list.append((i[0],"COMMENT", "COMMENT TO PRESERVE FORMAT ON", "Procedure"))
        dbo_artifact_list.append((i[0],"COMMENT","CREATE FUNCTION","NULL"))

    # Add dbo user triggers in DB_Artifacts.list
    cursor.execute("""select  s.name from sysobjects s JOIN SYS.SYSUSER u ON s.uid = u.user_id WHERE lower(u.user_name) ='dbo' and s.type = 'TR'""")
    records = cursor.fetchall()
    for i in records:
        dbo_artifact_list.append((i[0],"COMMENT","CREATE TRIGGER","NULL"))
        dbo_artifact_list.append((i[0],"COMMENT", "COMMENT ON", "TRIGGER"))
        dbo_artifact_list.append((i[0],"COMMENT", "COMMENT TO PRESERVE FORMAT ON", "TRIGGER"))

    # In reload.sql we have call dbo.sa_recompile_views and more. To comment all this
    # a list made from iqunload code to add all this calls in artifacts list
    dbo_artifact_list.append(("call","COMMENT","sa_reset_identity","dbo"))
    dbo_artifact_list.append(("call","COMMENT","sa_make_object","dbo"))
    dbo_artifact_list.append(("call","COMMENT","sa_sync","dbo"))
    dbo_artifact_list.append(("call","COMMENT","sa_sync_sub","dbo"))
    dbo_artifact_list.append(("call","COMMENT","sa_internal_load_cost_model","dbo"))
    dbo_artifact_list.append(("call","COMMENT","sa_internal_system_task","dbo"))
    dbo_artifact_list.append(("call","COMMENT","sa_refresh_text_indexes","dbo"))
    dbo_artifact_list.append(("call","COMMENT","sa_refresh_materialized_views","dbo"))

    cursor.close()

def sap_user_dependent_objects(conn, sap_artifact_list):

    cursor = conn.cursor()

    # Add _sap_* users as CREATE USER entries
    cursor.execute(r"SELECT user_name FROM SYS.SYSUSER WHERE lower(user_name) LIKE '_sap\_%' ESCAPE '\'")
    rows = cursor.fetchall()
    for i in rows:
        if i[0]:
            sap_artifact_list.append((i[0], "COMMENT", "GRANT CONNECT", "NULL"))

    table_id_list = []
    cursor.execute(r"""SELECT t.table_id, t.table_name, u.user_name 
                       FROM SYS.SYSTABLE t 
                       JOIN SYS.SYSUSER u ON t.creator = u.user_id 
                       WHERE lower(u.user_name) LIKE '_sap\_%' ESCAPE '\' 
                       AND t.table_type = 'BASE' AND t.server_type = 'IQ'""")
    records = cursor.fetchall()
    for table_id, table_name, user_name in records:
        sap_artifact_list.append((table_name, "COMMENT", "CREATE TABLE", user_name))
        sap_artifact_list.append((table_name, "COMMENT", "CREATE procedure", user_name))
        table_id_list.append(table_id)

    view_id_list = []
    view_name_list = []

    for table_id in table_id_list:
        cursor.execute(f"SELECT index_name FROM SYS.SYSINDEX i JOIN SYS.SYSTABLE t ON t.table_id = i.table_id WHERE t.table_id = {table_id}")
        for i in cursor.fetchall():
            sap_artifact_list.append((i[0], "COMMENT", "CREATE", "INDEX"))

        cursor.execute(f"SELECT trigger_name FROM SYS.SYSTRIGGER WHERE table_id = {table_id}")
        for i in cursor.fetchall():
            sap_artifact_list.append((i[0], "COMMENT", "CREATE TRIGGER", "NULL"))
            sap_artifact_list.append((i[0], "COMMENT", "COMMENT ON", "TRIGGER"))
            sap_artifact_list.append((i[0], "COMMENT", "COMMENT TO PRESERVE FORMAT ON", "TRIGGER"))

        cursor.execute(f"""SELECT v.table_name, d.dep_object_id 
                           FROM SYSDEPENDENCY d 
                           JOIN systab t ON t.object_id = d.ref_object_id 
                           JOIN systab v ON v.object_id = d.dep_object_id 
                           WHERE t.table_id = {table_id}""")
        for view_name, view_id in cursor.fetchall():
            view_id_list.append(view_id)
            view_name_list.append(view_name)

    while view_id_list:
        ref_object = view_id_list.pop()
        cursor.execute(f"""SELECT v.table_name, d.dep_object_id 
                           FROM SYSDEPENDENCY d 
                           JOIN systab t ON t.object_id = d.ref_object_id 
                           JOIN systab v ON v.object_id = d.dep_object_id 
                           WHERE d.ref_object_id = {ref_object}""")
        for view_name, dep_id in cursor.fetchall():
            view_id_list.append(dep_id)
            view_name_list.append(view_name)

    for view in view_name_list:
        sap_artifact_list.append((view, "COMMENT", "CREATE VIEW", "NULL"))
        sap_artifact_list.append((view, "COMMENT", "COMMENT ON", "VIEW"))
        sap_artifact_list.append((view, "COMMENT", "COMMENT TO PRESERVE FORMAT ON", "VIEW"))

    cursor.execute(r"""SELECT p.proc_name 
                       FROM SYS.SYSPROCEDURE p 
                       JOIN SYS.SYSUSER u ON p.creator = u.user_id 
                       WHERE lower(u.user_name) LIKE '_sap\_%' ESCAPE '\' 
                       AND proc_name NOT LIKE 'sp_%' AND proc_name NOT LIKE 'sa_%'""")
    for i in cursor.fetchall():
        sap_artifact_list.append((i[0], "COMMENT", "CREATE procedure", "NULL"))
        sap_artifact_list.append((i[0], "COMMENT", "COMMENT ON", "Procedure"))
        sap_artifact_list.append((i[0], "COMMENT", "COMMENT TO PRESERVE FORMAT ON", "Procedure"))
        sap_artifact_list.append((i[0], "COMMENT", "CREATE FUNCTION", "NULL"))

    cursor.execute(r"""SELECT s.name 
                       FROM sysobjects s 
                       JOIN SYS.SYSUSER u ON s.uid = u.user_id 
                       WHERE lower(u.user_name) LIKE '_sap\_%' ESCAPE '\' AND s.type = 'TR'""")
    for i in cursor.fetchall():
        sap_artifact_list.append((i[0], "COMMENT", "CREATE TRIGGER", "NULL"))
        sap_artifact_list.append((i[0], "COMMENT", "COMMENT ON", "TRIGGER"))
        sap_artifact_list.append((i[0], "COMMENT", "COMMENT TO PRESERVE FORMAT ON", "TRIGGER"))

    cursor.close()


# Function to add remote server dependent objects in DB_Artifacts.list
def remote_dependent_objects(conn, l):
    cursor = conn.cursor()

    # Add procedures on remote servers in DB_Artifacts.list
    cursor.execute("select proc_name from SYSPROCEDURE p join sysserver s  on p.srvid = s.srvid;")
    records = cursor.fetchall()
    for i in records:
        l.append((i[0],"COMMENT","CREATE procedure","NULL"))
        l.append((i[0],"COMMENT", "COMMENT ON", "Procedure"))
        l.append((i[0],"COMMENT", "COMMENT TO PRESERVE FORMAT ON", "Procedure"))

    # Add indexes on remote tables in DB_Artifacts.list
    cursor.execute("""select index_name from SYS.SYSINDEX i join ( SYSTAB t JOIN SYSPROXYTAB p ON t.object_id = p.table_object_id) on t.table_id = i.table_id""")
    records = cursor.fetchall()
    for i in records:
        l.append((i[0],"COMMENT","CREATE","INDEX"))

    # Add triggers on remote tables in DB_Artifacts.list
    cursor.execute("""select  T.trigger_name from SYS.SYSTRIGGER T JOIN SYS.SYSTABLE TAB JOIN SYSPROXYTAB p
                       ON TAB.object_id = p.table_object_id and ( T.table_id = TAB.table_id );""")
    records = cursor.fetchall()
    for i in records:
        l.append((i[0],"COMMENT","CREATE TRIGGER","NULL"))
        l.append((i[0],"COMMENT", "COMMENT ON", "TRIGGER"))
        l.append((i[0],"COMMENT", "COMMENT TO PRESERVE FORMAT ON", "TRIGGER"))

    # Add view on remote servers in DB_Artifacts.list
    cursor.execute("""select v.table_name,d.dep_object_id from SYSDEPENDENCY d join systab t on t.object_id = d.ref_object_id
                     join systab v on v.object_id = d.dep_object_id JOIN SYSPROXYTAB p ON t.object_id = p.table_object_id;""")
    records = cursor.fetchall()
    view_id_list = list()
    view_name_list = list()
    for i in records:
        view_id_list.append(i[1])
        view_name_list.append(i[0])

    # Checking for multi-level view dependency on remote server
    if (len(view_id_list) != 0):
        while (len(view_id_list) != 0):
            ref_object = view_id_list.pop()
            cursor.execute("""select v.table_name,d.dep_object_id from SYSDEPENDENCY d join systab t on t.object_id = d.ref_object_id join systab v on v.object_id = d.dep_object_id where d.ref_object_id = %s"""%(ref_object))
            records = cursor.fetchall()
            for i in records:
                view_id_list.append(i[1])
                view_name_list.append(i[0])

    # append all the view names to artifacts list
    for i in view_name_list:
        l.append((i,"COMMENT","CREATE VIEW","NULL"))
        l.append((i,"COMMENT", "COMMENT ON", "VIEW"))
        l.append((i,"COMMENT", "COMMENT TO PRESERVE FORMAT ON", "VIEW"))

    # Add srvname with create externlogin in DB_Artifacts.list
    cursor.execute("select srvname from sysserver;")
    records = cursor.fetchall()
    for i in records:
        l.append((i[0],"COMMENT","CREATE EXTERNLOGIN","NULL"))

    # Add tablename with create existing table in DB_Artifacts.list
    cursor.execute("SELECT t.table_name FROM SYSTAB t JOIN SYSPROXYTAB p ON t.object_id = p.table_object_id KEY JOIN SYSSERVER s;")
    records = cursor.fetchall()
    for i in records:
        l.append((i[0],"COMMENT","CREATE EXISTING TABLE","NULL"))

    cursor.close()
    return l

# Function to get dbspaces names and put that in artifacts file
# Get SA tables names and add to comment that in artifacts file
# so that they can be replaced in schema file
def modify_artifacts_file(filename, connectstr):
    artifact_list = list()

    pwd = os.getcwd()
    DBArtifacts_list = '%s%s..%sCommon%sDB_Artifacts.list'%(pwd,path_sep,path_sep,path_sep)
    if not(os.path.isfile(DBArtifacts_list) and os.access(DBArtifacts_list, os.R_OK)):
        sys.exit("%s file does not exist."%DBArtifacts_list)

    dbopts_list = '%s%s..%sCommon%sdbopts_noncustomer.csv'%(pwd,path_sep,path_sep,path_sep)
    if not(os.path.isfile(dbopts_list) and os.access(dbopts_list, os.R_OK)):
        sys.exit("%s file does not exist."%dbopts_list)

    hosparams_list = '%s%s..%sCommon%shosparams_noncustomer.csv'%(pwd,path_sep,path_sep,path_sep)
    if not(os.path.isfile(hosparams_list) and os.access(hosparams_list, os.R_OK)):
        sys.exit("%s file does not exist."%hosparams_list)

    artifact_list = load_artifactsfile_to_list(filename)

    # Add dbopts_noncustomer.csv for comment in list
    with codecs.open(dbopts_list, "r", common.charset) as f:
        for line in f.readlines():
            line = line.rstrip(newline)
            artifact_list.append((line,"COMMENT","SET OPTION","NULL"))
            artifact_list.append((line,"COMMENT","SET TEMPORARY OPTION","NULL"))

    # Add hosparams_noncustomer.csv for comment in list
    with codecs.open(hosparams_list, "r", common.charset) as f:
        for line in f.readlines():
            line = line.rstrip(newline)
            artifact_list.append((line,"COMMENT","SET OPTION","NULL"))
            artifact_list.append((line,"COMMENT","SET TEMPORARY OPTION","NULL"))

    # Add dbspaces to be replaced by user_main in list
    try:
        conn = pyodbc.connect(connectstr, timeout=0)
    except Exception as exp:
        sys.exit("Exception: %s"%str(exp))
    cursor = conn.cursor()
    cursor.execute("select DBSpaceName from sp_iqdbspace() where UPPER(DBSpaceName)!='IQ_SYSTEM_TEMP';")
    records = cursor.fetchall()
    for i in records:
        artifact_list.append((i[0],"user_object_store"))

    # Add sa tables in DB_Artifacts.list
    cursor.execute("select table_name from SYS.SYSTABLE JOIN SYS.SYSUSER ON user_id = creator WHERE user_name not in ('SYS','rs_systabgroup','SA_DEBUG','dbo') AND table_type = 'BASE' and server_type='SA';")
    records = cursor.fetchall()
    for i in records:
        artifact_list.append((i[0],"COMMENT", "system","Create table" ))
        artifact_list.append((i[0],"COMMENT", "CREATE TEXT","NULL" ))
        artifact_list.append((i[0],"COMMENT", "ALTER TEXT","NULL" ))
        artifact_list.append((i[0],"COMMENT", "ALTER VIEW","NULL" ))
        artifact_list.append((i[0],"COMMENT", "CREATE VIEW","NULL" ))
        artifact_list.append((i[0],"COMMENT", "MATERIALIZED VIEW","NULL" ))
        artifact_list.append((i[0],"COMMENT", "INDEX","CREATE" ))

    # Add srvclass with create server and remote server dependent objects in DB_Artifacts.list
    cursor.execute("select srvclass from sysserver;")
    records = cursor.fetchall()
    if len(records) != 0:
        for i in records:
            artifact_list.append((i[0],"COMMENT","CREATE SERVER","NULL"))
        remote_dependent_objects(conn,artifact_list)

    # Add srvname,capname with sa_unload_define_capability in DB_Artifacts.list
    cursor.execute("SELECT srvname,capname FROM SYS.SYSSERVER s JOIN SYS.SYSCAPABILITY c ON (c.srvid = s.srvid) JOIN SYS.SYSCAPABILITYNAME cn ON (cn.capid = c.capid)")
    records = cursor.fetchall()
    for i in records:
        artifact_list.append((i[0],"COMMENT", "sa_unload_define_capability",i[1]))

    # Add srvname with empty capname with sa_unload_define_capability in DB_Artifacts.list
    cursor.execute("SELECT srvname FROM SYS.SYSSERVER s JOIN SYS.SYSCAPABILITY c ON (c.srvid = s.srvid) JOIN SYS.SYSCAPABILITYNAME cn ON (cn.capid = c.capid) group by srvname")
    records = cursor.fetchall()
    for i in records:
        artifact_list.append((i[0],"COMMENT", "sa_unload_define_capability",""))

    cursor.execute("select user_name from SYS.SYSUSERPERM where user_name LIKE 'SYS_%_ROLE';")
    records = cursor.fetchall()
    for i in records:
        artifact_list.append((i[0],"COMMENT","NULL","NULL"))

    # Add for RLV enabled tables
    cursor.execute("SELECT table_name FROM SYS.SYSTABLE t JOIN SYS.SYSUSER u ON u.user_id = t.creator JOIN SYS.SYSIQTAB it ON (t.table_id = it.table_id) WHERE  t.server_type = 'IQ' AND it.is_rlv = 'T'")
    records = cursor.fetchall()
    for i in records:
        artifact_list.append((i[0],"COMMENT","CREATE TABLE","ENABLE RLV STORE"))

    # Add default_logical_server
    cursor.execute("select login_option_name name, login_option_value val from SYS.SYSLOGINPOLICY lp join SYS.SYSLOGINPOLICYOPTION lpo ON (lp.login_policy_id = lpo.login_policy_id)where login_policy_name not in ('root') and login_option_name = 'default_logical_server';")
    records = cursor.fetchall()
    for i in records:
        artifact_list.append((i[0],"COMMENT",i[1],"NULL"))

    # Add revoke with create dbspace name which get from SYSDBSPACEPERM query in DB_Artifacts.list
    artifact_list.append(("revoke","COMMENT","CREATE","dbspace_name"))
    artifact_list.append(("revoke","COMMENT","CREATE","user_object_store"))

    dbo_user_dependent_objects(conn,artifact_list)
    sap_user_dependent_objects(conn,artifact_list)

    cursor.close()
    conn.close()

    return artifact_list

# Function to read login options file
def load_login_list(f):
    login_list = list()
    with codecs.open("%s"%(f),"r", common.charset) as f:
        for line in f.readlines():
            line = line.rstrip(newline)
            splits = line.split('=')
            login_list.append((splits[0].strip(),splits[1].strip()))
    return login_list

# Function which check whether login policy modification is need or not
def login_policy_value():
    login_options = '%s%s..%sCommon%slogin_policy.csv'%(path,path_sep,path_sep,path_sep)
    com = load_login_list(login_options)
    sql_lines = load_reloadfile_to_list('reload.sql')

    lg = list()
    pattern = "LOGIN POLICY"
    i = 0
    while i < len(sql_lines):
        line = sql_lines[i]
        pat1 = "root"
        if (re.search(r"\b{}\b".format(pattern.lower()),line.lower())) and (re.search(r"\b{}\b".format(pat1.lower()),line.lower())) and not "--" in line:
            j = i
            while j < len(sql_lines):
                if "go".strip()  == sql_lines[j].strip() :
                    break
                j +=1
                splits = sql_lines[j].split('=')
                length = len(splits)
                if length == 2:
                    lg.append((splits[0].strip(),splits[1].strip()))

            j = i-1
            while j > 0:
                if "go".strip()  == sql_lines[j].strip()or "--" in sql_lines[j].strip() or sql_lines[j] == newline :
                    break
                j -=1
        i += 1

    t = []
    # get all the common elements of both default list and list created by reading reload.sql
    t =list(set(lg) & set(com))

    res = 0
    # if both the length of common list and list of options reading by reload.sql matches
    # then root login policy uses default values and hence modification is required
    if len(lg) == len(t):
        res = 0
    else:
        res = 1

    return res

# Function which does modification of login policy and modify users also based on login policy
def login_policy():
    sql_lines = load_reloadfile_to_list('reload.sql')

    pattern = "LOGIN POLICY"
    i = 0
    while i < len(sql_lines):
        line = sql_lines[i]
        pat1 = "root"
        if (re.search(r"\b{}\b".format(pattern.lower()),line.lower())) and (re.search(r"\b{}\b".format(pat1.lower()),line.lower())) and not "--" in line:
            j = i
            dest = "c_root"
            tmp = re.sub(r"\b%s\b"%(pat1), dest,line, flags=re.I)
            sql_lines[i] = "CREATE LOGIN POLICY \"c_root\"" + "%sgo%s"%(newline,newline) + tmp
            while j < len(sql_lines):
                if "go".strip()  == sql_lines[j].strip() :
                    tmp = sql_lines[j] + "%s--**LOGIN POLICY Modified by Migration Utility: end%s"%(newline,newline)
                    sql_lines[j] = tmp
                    break
                j +=1
            j = i-1
            while j > 0:
                if "go".strip()  == sql_lines[j].strip()or "--" in sql_lines[j].strip() or sql_lines[j] == newline :
                    tmp = sql_lines[j] + "%s--**LOGIN POLICY Modified by Migration Utility: Begin%s"%(newline,newline)
                    sql_lines[j] = tmp
                    break
                j -=1

        pat2 = "GRANT CONNECT TO"
        pat3 = "DBA"
        if (re.search(r"\b{}\b".format(pat2.lower()),line.lower())) and not (re.search(r"\b{}\b".format(pat3.lower()),line.lower())) and not "--" in line:
            temp = line.strip()
            dest = "ALTER USER"
            temp = re.sub(r"\b%s\b"%(pat2), dest,temp, flags=re.I)
            temp = temp.strip() + " login policy \"c_root\"" + newline
            tmp = line.strip() +  "%s%s"%(newline,newline)
            sql_lines[i] = tmp + temp

        i += 1

    return sql_lines


# Function to modify the generated schema
# First it will modify the commands file
# Read the generated file reload.sql into list
# Read commands file also into list
# Perform operations which can be comment or either string replace
# write the modified list into new reload.sql file
def modify_reloadsql():
    str1 = "Modifying generated schema file: %s%s" %(newline,reload_path)
    common.print_and_log(str1)

    commands = modify_artifacts_file('DB_Artifacts.list', connectstr)
    res = login_policy_value()
    if res == 1:
        sql_lines = login_policy()
    else:
        sql_lines = load_reloadfile_to_list('reload.sql')
        commands.append(("ALTER LOGIN POLICY","COMMENT","root","NULL"))

    commands.append(("create procedure","COMMENT","NULL","NULL"))
    commands.append(("replace procedure","COMMENT","NULL","NULL"))

    foreign_list = list()
    for cmd in commands:
        # Condition that handles to comment when "not create domain" and "not text configuration"
        if "COMMENT" == cmd[1] and cmd[0] != "replace procedure" and cmd[0] != "create procedure" and cmd[0] != "password values" and cmd[0] != "CREATE DOMAIN" and cmd[0] != "TEXT CONFIGURATION" and cmd[0] != "default_logical_server":
            i = 0
            while i < len(sql_lines):
                line = sql_lines[i]
                pattern = cmd[0]
                pat1 = cmd[2] if cmd[2]!='NULL' else ' '
                pat2 = cmd[3] if cmd[3]!='NULL' else ' '
                flag = 0
                flag1 = 0
                flag2 = 0
                if (cmd[0] == "CORE_Options" or cmd[0] == "MPX_Options") and ("COMMENT" == cmd[1]):
                   condition = (pattern.lower() in line.lower())
                else:
                   condition = (re.search(r"\b{}\b".format(pattern.lower()),line.lower()))
                if condition and not "--" in line:
                    j = i
                    while j < len(sql_lines):
                        if "go".strip()  == sql_lines[j].strip() :
                            bottom = j
                            break
                        j +=1
                    j = i-1
                    while j > 0:
                        if "go".strip()  == sql_lines[j].strip()or "--" in sql_lines[j].strip() or sql_lines[j] == newline :
                            top = j
                            break
                        j -=1
                    j = top
                    while j < bottom :
                        if cmd[2] != "NULL" and (re.search(r"\b{}\b".format(cmd[2].lower()),sql_lines[j].lower())) and not "--" in line:
                            flag1 = 1
                        if cmd[3] != "NULL" and (re.search(r"\b{}\b".format(cmd[3].lower()),sql_lines[j].lower())) and not "--" in line:
                            flag2 = 1
                        j +=1

                    if ((cmd[2] != "NULL" and flag1 == 1) or (cmd[2] == "NULL" and flag1 == 0)) and ((cmd[3] != "NULL" and flag2 == 1) or (cmd[3] == "NULL" and flag2 == 0)):
                       flag = 1
                    if flag == 1:
                        j = i
                        while j < len(sql_lines):
                            if "go".strip()  == sql_lines[j].strip() :
                                if pattern.lower() == "alter table" and pat1.lower() == "foreign key" and pat2.lower() == "references":
                                    foreign_list.append(sql_lines[j])
                                tmp = "-- " + sql_lines[j] + "%s--** Commented by Migration Utility: end%s"%(newline,newline)
                                sql_lines[j] = tmp
                                break
                            if pattern.lower() == "alter table" and pat1.lower() == "foreign key" and pat2.lower() == "references":
                                foreign_list.append(sql_lines[j])
                            tmp = "-- " + sql_lines[j]
                            sql_lines[j] = tmp
                            j +=1
                        j = i-1
                        while j > 0:
                            if "go".strip()  == sql_lines[j].strip()or "--" in sql_lines[j].strip() or sql_lines[j] == newline :
                                if pattern.lower() == "alter table" and pat1.lower() == "foreign key" and pat2.lower() == "references":
                                    foreign_list.append(sql_lines[j])
                                    tmp =  sql_lines[j] + newline + "--** Commented by Migration Utility: Begin" + newline + newline + "--** " + pat1 + " " + pattern + " " + pat2 + " will be created after tables load on HDL. It will be moved to Foreign_Key_Constraint.sql" + newline
                                else:
                                    tmp =  sql_lines[j] + newline + "--** Commented by Migration Utility: Begin" + newline + newline + "--** " + pat1 + " " + pattern + " " + pat2 + " Not supported on HDL" + newline
                                sql_lines[j] = tmp
                                break
                            if pattern.lower() == "alter table" and pat1.lower() == "foreign key" and pat2.lower() == "references":
                                foreign_list.append(sql_lines[j])
                            tmp = "-- " + sql_lines[j]
                            sql_lines[j] = tmp
                            j -=1
                i += 1
        # Condition which handles create/alter text configuration owned by SYS
        elif "COMMENT" == cmd[1] and cmd[0] == "TEXT CONFIGURATION":
            i = 0
            while i < len(sql_lines):
                line = sql_lines[i]
                pattern = "CREATE " + cmd[0] + " \"SYS\""
                pattern1 = "CREATE " + cmd[0] + " SYS"
                pattern2 = "ALTER " + cmd[0] + " \"SYS\""
                pattern3 = "ALTER " + cmd[0] + " SYS"
                cond1 = (pattern.lower() in line.lower()) or (pattern1.lower() in line.lower())
                cond2 = (pattern2.lower() in line.lower()) or (pattern3.lower() in line.lower())
                if (cond1 or cond2) and not "--" in line:
                    j = i
                    while j < len(sql_lines):
                        if "go".strip()  == sql_lines[j].strip() :
                            tmp = "-- " + sql_lines[j] + "%s--** Commented by migration script: end%s"%(newline,newline)
                            sql_lines[j] = tmp
                            break
                        tmp = "-- " + sql_lines[j]
                        sql_lines[j] = tmp
                        j +=1
                    j = i-1
                    while j > 0:
                        if "go".strip()  == sql_lines[j].strip()or "--" in sql_lines[j].strip() or sql_lines[j] == newline :
                            tmp =  sql_lines[j] + "%s--** Commented by migration script: Begin%s"%(newline,newline) + "%s--** "%(newline) + pattern + " or " + pattern2 + " Not supported on HDL" + newline
                            sql_lines[j] = tmp
                            break
                        tmp = "-- " + sql_lines[j]
                        sql_lines[j] = tmp
                        j -=1
                i += 1
        # Condition which handles create domain statement to drop AS USER clause
        elif cmd[0] == "CREATE DOMAIN" and "COMMENT" == cmd[1]:
            i = 0
            while i < len(sql_lines):
                line = sql_lines[i]
                pattern = cmd[0]
                if re.search(r"\b{}\b".format(pattern.lower()),line.lower()) and not "--" in line:
                    result = re.split(r"AS USER +", line)
                    sql_lines[i] = result[0] + newline
                i += 1

        elif cmd[0] == "password values" and "COMMENT" == cmd[1]:
            i = 0
            while i < len(sql_lines):
                line = sql_lines[i]
                pattern = cmd[0]
                if pattern in line and not "--" in line:
                    j = i
                    k = i
                    while j < i+4:
                        tmp = "-- " + sql_lines[j]
                        sql_lines[j] = tmp
                        j +=1
                    while k > i-4:
                        tmp = "-- " + sql_lines[k]
                        sql_lines[k] = tmp
                        k -=1
                i += 1
        elif cmd[0] == "default_logical_server" and "COMMENT" == cmd[1]:
            i = 0
            while i < len(sql_lines):
                line = sql_lines[i]
                pattern = cmd[0]
                pat1 = cmd[2]
                if (pattern.lower() in line.lower()) and (pat1.lower() in line.lower()) and not "--" in line:
                    sql_lines[i] = "--" + sql_lines[i] + newline
                i += 1
        # Condition to comment create procedure or replace procedure statement if it has any unsupported artifacts within
        # Logic would be that first all unsupported artifacts would already be commented in stored procedure and then
        # if you find stored procedure with commented unsupported artifacts, you are commenting entire stored procedure
        elif (cmd[0] == "create procedure" or cmd[0] == "replace procedure")  and "COMMENT" == cmd[1]:
            i = 0
            while i < len(sql_lines):
                line = sql_lines[i]
                pattern = cmd[0]
                cmt_flg = 0
                if (pattern.lower() in line.lower()) and  not "--" in line:
                    j = i
                    while j < len(sql_lines):
                        # If the go statement of that procedure is commented out due to some unsupported artifacts, then comment proc
                        if "Commented by Migration Utility" in sql_lines[j] and "-- go".strip()  == sql_lines[j].split('\n')[0].strip() :
                            bottom = j
                            cmt_flg = 1
                            break
                        elif "go".strip()  == sql_lines[j].strip() :
                            cmt_flg = 0
                            break

                        j +=1
                    j = i
                    if "COMMENT TO PRESERVE FORMAT ON" in sql_lines[j-1] :
                        top = j - 1
                    else:
                        top = j
                    if cmt_flg == 1:
                        tmp =  "\n--** Commented by Migration Utility: Begin\n" + "\n--** " + pattern + " with unsupported artifacts not supported on HDL\n" + "\n-- " + sql_lines[top]
                        sql_lines[top] = tmp
                        top = top + 1
                        while top < bottom:
                            tmp = "-- " + sql_lines[top]
                            sql_lines[top] = tmp
                            top +=1

                i += 1

        # Condition which does pattern replace
        else:
            i = 0
            while i < len(sql_lines):
                line = sql_lines[i]
                src = cmd[0]
                dest = cmd[1]
                sql_lines[i] = re.sub(r"\b%s\b"%(src), dest,line, flags=re.I)
                i += 1

    write_lines_intofile(AutoUpdatedReload_path,sql_lines)

    return

# Function to verify the Foreign_Key_Constraint.sql
# with the last line pattern in that file
def verify_foreignkey_constraint():

    ForeignKey_complete = 0
    if os.path.isfile(ForeignKey_path):
        with codecs.open("%s"%(ForeignKey_path), "r", common.charset) as f:
            last_line = f.readlines()[-1]
            if "Creation of Foreign_Key_Constraint.sql completed." in last_line:
                ForeignKey_complete = 1

    return ForeignKey_complete

# Check if the database has foreign key constraints
def check_foreign_tables(connectstr):
    try:
        conn = pyodbc.connect(connectstr, timeout=0)
    except Exception as exp:
        sys.exit("Exception: %s"%str(exp))
    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM SYS.SYSTABLE JOIN SYS.SYSFOREIGNKEY ON table_id = foreign_table_id WHERE server_type='IQ';")
    foreign_tables_count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return foreign_tables_count

# Function to verify the AutoUpdatedReload.sql
# with the last line pattern in that file
def verify_autoupdated_reload():

    AutoUpdatedReload_complete = 0
    if os.path.isfile(AutoUpdatedReload_path):
        with codecs.open("%s"%(AutoUpdatedReload_path), "r", common.charset) as f:
            last_line = f.readlines()[-1]
            if "Creation of AutoUpdated_Reload.sql completed." in last_line:
                AutoUpdatedReload_complete = 1

    return AutoUpdatedReload_complete

# Function to create Foreign_Key_Constraint.sql file in resume mode if not completely done
def create_foreignkey_resume():
    sql_lines = list()
    sql_lines = load_reloadfile_to_list('AutoUpdated_Reload.sql')

    foreign_list_resume = list()
    i = 0
    while i < len(sql_lines):
        line = sql_lines[i]
        pattern = "ALTER TABLE"
        pat1 = "FOREIGN KEY"
        pat2 = "REFERENCES"
        flag = 0
        flag1 = 0
        flag2 = 0
        condition = (re.search(r"\b{}\b".format(pattern.lower()),line.lower()))
        if condition and "--" in line and "--*" not in line :
            j = i
            while j < len(sql_lines):
                if "-- go".strip()  == sql_lines[j].strip() :
                    bottom = j
                    break
                j +=1
            j = i-1
            while j < bottom :
                if (re.search(r"\b{}\b".format(pat1.lower()),sql_lines[j].lower())) and "--" in line:
                    flag1 = 1
                if (re.search(r"\b{}\b".format(pat2.lower()),sql_lines[j].lower())) and "--" in line:
                    flag2 = 1
                j +=1
            if flag1 == 1 and flag2 == 1:
                flag = 1
            if flag == 1:
                j = i
                while j < bottom + 1:
                    line = sql_lines[j].strip()
                    if "-- " in line or "--" in line:
                        line = line[2:]
                    if "-- go".strip()  == sql_lines[j].strip() :
                        foreign_list_resume.append(line)
                        break
                    foreign_list_resume.append(line)
                    j +=1
        i += 1


    if len(foreign_list_resume) != 0:
        foreign_list_resume.append("-- Creation of Foreign_Key_Constraint.sql completed. ")
        write_lines_intofile(ForeignKey_path,foreign_list_resume)
    return


# Function to schema original file exist or not and AutoUpdated_Reload.sql exist or not
def schema_unload_and_modify(connectstr):
    iqunload_total_strt = datetime.datetime.now()

    if os.path.isfile(reload_path) and resume == True:
        logging.info("%s"%(common.dividerline))
        logging.info("%s file already exists.%sSkipping schema unload as migration utility started in resume mode." % (reload_path,newline) )
    else:
        iqunload_call()

    iqunload_total_elaptime = common.elap_time(iqunload_total_strt)
    days, hours, minutes, seconds = common.calculate_time(iqunload_total_elaptime)
    logging.info("%s"%(common.dividerline))
    logging.info("Time taken to unload Schema: %s%d days, %d hours, %d minutes and %d seconds" % ( newline,days[0], hours[0], minutes[0], seconds[0]))
    logging.info("%s"%(common.dividerline))

    modify_total_strt = datetime.datetime.now()

    # There can be three senarios:
    # 1. File exists and it is resume mode, verification is true then do nothing
    # 2. File exists and it is resume mode, verification is false, call function again and after that again do verification
    # 3. File does not exist, call function and after that again do verification
    resume_autoreload_complete = 0
    if os.path.isfile(AutoUpdatedReload_path) and resume == True:
        AutoUpdatedReload_complete = verify_autoupdated_reload()
        if AutoUpdatedReload_complete == 1:
            # The AutoUpdatedReload file is successfull and no need to call verify_autoupdated_reload again
            # hence setting resume_autoreload_complete = 1 here
            logging.info("%s"%(common.dividerline))
            logging.info("%s file already exists.%sSkipping unloaded schema modification as migration utility started in resume mode." %(AutoUpdatedReload_path,newline))
            resume_autoreload_complete = 1
        else:
            modify_reloadsql()
    else:
        modify_reloadsql()

    # After generation of AutoUpdatedReload file, verify the file
    if resume_autoreload_complete == 0:
        AutoUpdatedReload_complete = verify_autoupdated_reload()
        if AutoUpdatedReload_complete == 0:
            sys.exit("Creation of %s file failed. Re-run the migration utility in resume mode."%(AutoUpdatedReload_path))
        else:
            str1 = "Modification of schema is complete and output is saved at: %s%s" % (newline,AutoUpdatedReload_path)
            common.print_and_log(str1)

    modify_total_elaptime = common.elap_time(modify_total_strt)
    days, hours, minutes, seconds = common.calculate_time(modify_total_elaptime)
    logging.info("%s"%(common.dividerline))
    logging.info("Time taken to modify the Schema: %s%d days, %d hours, %d minutes and %d seconds" % ( newline, days[0], hours[0], minutes[0], seconds[0]))
    logging.info("%s"%(common.dividerline))


# Step 3 : Data unload into shared directory location given

# Function to read esinfo file or extractinfo and return a list of extracted file generated
def getfilelist_fromesinfo(tableid,path_to_copy):
    filelst = list()
    PATH = '%s%s%s%s%sextractinfo'%(datapath,path_sep,tableid,path_sep,tableid)

    if os.path.isfile(PATH) and os.access(PATH, os.R_OK):
        with codecs.open("%s%s%sextractinfo"%(path_to_copy,path_sep,tableid), "r", common.charset) as f:
            for lines in f:
                for word in lines.split("'"):
                    for words in word.split(','):
                        # For getting file list we need to search tableid and file extension in its name
                        # Only searching tableid will fail if record count has tableid in it
                        if '%s'%str(tableid) in words and (('gz' in words ) or ('txt' in words) or ('inp' in words)):
                            filelst.append(words)
    return filelst

# Function which verify row count in HDL which is added in load statement table
def rowCountVerifyInHDL(tablename,path_to_copy,cntstmt,f):
    rowcount = tablename[1]
    splits = tablename[0].split('.')
    table = splits[1]
    owner = splits[0]
    if owner.lower() == 'dba':
        owner = "HDLADMIN"

    tab = "     "
    f.write(newline + newline + "BEGIN")
    f.write(newline + tab + "DECLARE cnt UNSIGNED BIGINT;")
    f.write(newline + newline + tab + cntstmt + ";")
    f.write(newline + tab + "IF cnt != " + rowcount + " THEN")
    f.write(newline + tab + "  MESSAGE 'Error: For table " + owner+ "."+ table + " extracted table row-count: " + rowcount + " not matching with loaded table row-count in HDL: ',cnt type warning to client;")
    f.write(newline + tab + "  ROLLBACK;")
    f.write(newline + tab + "ELSE")
    f.write(newline + tab + "  COMMIT;")
    f.write(newline + tab + "END IF;")
    f.write(newline + "END")
    f.close()

# Function which forms load table statements by reading esinfo/extractinfo
# Form the load table statement and write it into a file
# This function is not for 16.1 SP01 and 16.0 SP11 versions
def form_load_table_stmt(tablename, conn, path_to_copy, binary):
    global byteorder
    splits = tablename[0].split('.')
    tableid = tablename[3]
    table = splits[1]
    owner = splits[0]
    with codecs.open("%s%s%s.sql"%(path_to_copy,path_sep,tableid), "w", common.charset) as f1:
        f1.write("COMMIT;" + newline)
        f1.write("set temporary option \"auto_commit\" = 'OFF';"+ newline)
        f1.write("set temporary option disable_ri_check= 'on';"+ newline)
        f1.write("BEGIN TRANSACTION;" + newline)
        cursor3 = conn.cursor()
        cursor3.execute("select setting from SYSOPTIONS where \"option\"='string_rtruncation';")
        string_truncation = cursor3.fetchone()[0]
        if string_truncation.lower() == "on":
            cursor3.execute("set temporary option STRING_RTRUNCATION = 'off'")
        cursor3.execute("select \"default\" from SYS.SYSCOLUMN c JOIN SYS.SYSTABLE t ON (c.table_id = t.table_id ) JOIN SYS.SYSUSER u ON (u.user_id = t.creator) where  t.table_name='%s' and u.user_name='%s'"%(table, owner ))
        idtcol = []
        idtflag = 0
        for idt in cursor3:
            idtcol.append(idt[0])
        if 'Identity/Autoincrement' in idtcol or 'autoincrement' in idtcol:
            idtflag = 1
        if owner.lower() == 'dba':
            command1 = "LOAD TABLE \"HDLADMIN\".\"%s\""%(table)
            f1.write("sp_iqlogtoiqmsg('HDL-Migration: " + command1 + "');" + newline + newline)
            if idtflag == 1:
                f1.write("set temporary option \"identity_insert\" = \"HDLADMIN.%s\";"%(table)+ newline + newline)
            cntstmt = "SELECT count(*) into cnt FROM  \"HDLADMIN\".\"%s\""%(table)
        else:
            command1 = "LOAD TABLE \"%s\".\"%s\""%(owner,table)
            f1.write("sp_iqlogtoiqmsg('HDL-Migration: " + command1 + "');" + newline + newline)
            if idtflag == 1:
                f1.write("set temporary option \"identity_insert\" = \"%s.%s\";"%(owner,table)+ newline + newline)
            cntstmt = "SELECT count(*) into cnt FROM  \"%s\".\"%s\""%(owner,table)
        cursor3.execute("select column_name from SYS.SYSCOLUMN c JOIN SYS.SYSTABLE t ON (c.table_id = t.table_id ) JOIN SYS.SYSUSER u ON (u.user_id = t.creator) where  t.table_name='%s' and u.user_name='%s'"%(table, owner ))
        command2=""
        collst = []
        for col in cursor3:
            collst.append(col[0])
        n = len(collst)
        if binary == 0:
            for i in range(n-1):
                command2 = command2 + '\"%s\"'%(collst[i]) + ", "
            command2 = command2 + '\"%s\"'%(collst[n-1])
        else:
            for i in range(n-1):
                command2 = command2 + '\"%s\"'%(collst[i]) + " BINARY WITH NULL BYTE, "
            command2 = command2 + '\"%s\"'%(collst[n-1]) + " BINARY WITH NULL BYTE"
        command = " " + command1 + "( " + command2 + " )"
        f1.write(command)
        cursor3.close()

        tab = "     "
        f1.write(newline + tab + "FROM" + " ")
        filelst = list()
        l = list()

        l = getfilelist_fromesinfo(tableid,path_to_copy)

        obj_path = "hdlfs:///%s/Extracted_Data/%s/"%(common.hdlfs_directory,tableid)
        for lst in l:
            filelst.append("'"+obj_path+str(lst)+"'")

        my_string = ','.join(map(str, filelst))
        f1.write(my_string )

        if binary == 0:
            f1.write(newline + tab + "quotes off")
            f1.write(newline + tab + "escapes off")
            f1.write(newline + tab + "row delimited by \'\\" + "x0a" "'"+";")
        else:
            f1.write(newline + tab + "escapes off")
            f1.write(newline + tab + "defaults off")
            f1.write(newline + tab + "FORMAT BINARY")
            f1.write(newline + tab + "CHECK CONSTRAINTS OFF")
            if byteorder == "little":
                f1.write(newline + tab + "BYTE ORDER LOW"+ ";")
            else:
                f1.write(newline + tab + "BYTE ORDER HIGH"+ ";")

        rowCountVerifyInHDL(tablename,path_to_copy,cntstmt,f1)

# Function to load iq_tables.list into a list
def form_load_table_sequential(tablename, conn, path_to_copy, binary):
    global byteorder
    splits = tablename[0].split('.')
    table = splits[1]
    tableid = tablename[3]
    owner = splits[0]
    with codecs.open("%s%s%s.sql"%(path_to_copy,path_sep,tableid), "w", common.charset) as f1:
        f1.write("set temporary option \"auto_commit\" = 'OFF';"+ newline + newline)
        f1.write("set temporary option disable_ri_check= 'on';"+ newline)
        f1.write("COMMIT;" + newline)
        f1.write("BEGIN TRANSACTION;" + newline)
        cursor3 = conn.cursor()
        cursor3.execute("select \"default\" from SYS.SYSCOLUMN c JOIN SYS.SYSTABLE t ON (c.table_id = t.table_id ) JOIN SYS.SYSUSER u ON (u.user_id = t.creator) where  t.table_name='%s' and u.user_name='%s'"%(table, owner ))
        idtcol = []
        idtflag = 0
        for idt in cursor3:
            idtcol.append(idt[0])
        if 'Identity/Autoincrement' in idtcol or 'autoincrement' in idtcol:
            idtflag = 1
        if owner.lower() == 'dba':
            command1 = "LOAD TABLE \"HDLADMIN\".\"%s\""%(table)
            f1.write("sp_iqlogtoiqmsg('HDL-Migration: " + command1 + "');" + newline + newline)
            if idtflag == 1:
                f1.write("set temporary option \"identity_insert\" = \"HDLADMIN.%s\";"%(table)+ newline + newline)
            cntstmt = "SELECT count(*) into cnt FROM  \"HDLADMIN\".\"%s\""%(table)
        else:
            command1 = "LOAD TABLE \"%s\".\"%s\""%(owner,table)
            f1.write("sp_iqlogtoiqmsg('HDL-Migration: " + command1 + "');" + newline + newline)
            if idtflag == 1:
                f1.write("set temporary option \"identity_insert\" = \"%s.%s\";"%(owner,table)+ newline + newline)
            cntstmt = "SELECT count(*) into cnt FROM  \"%s\".\"%s\""%(owner,table)
        cursor3.execute("select column_name from SYS.SYSCOLUMN c JOIN SYS.SYSTABLE t ON (c.table_id = t.table_id ) JOIN SYS.SYSUSER u ON (u.user_id = t.creator) where  t.table_name='%s' and u.user_name='%s'"%(table, owner ))
        command2=""
        collst = []
        for col in cursor3:
            collst.append(col[0])
        n = len(collst)
        if binary == 0:
            for i in range(n-1):
                command2 = command2 + '\"%s\"'%(collst[i]) + ", "
            command2 = command2 + '\"%s\"'%(collst[n-1])
        else:
            for i in range(n-1):
                command2 = command2 + '\"%s\"'%(collst[i]) + " BINARY WITH NULL BYTE, "
            command2 = command2 + '\"%s\"'%(collst[n-1]) + " BINARY WITH NULL BYTE"
        command = " " + command1 + "( " + command2 + " )"
        f1.write(command)
        cursor3.close()
        tab = "     "
        f1.write(newline + tab + "FROM" + " ")
        fileext = ""
        l = list()
        filelst =  list()
        if binary == 0:
            fileext = 'txt'
        else:
            i = 1
            while i < 9:
                var = '%s_%s.inp'%(tableid,i)
                l.append(var)
                i = i + 1

        obj_path = "hdlfs:///%s/Extracted_Data/%s/"%(common.hdlfs_directory,tableid)
        for lst in l:
            filelst.append("'"+obj_path+str(lst)+"'")

        my_string = ','.join(map(str, filelst))
        f1.write(my_string )

        if binary == 0:
            f1.write(newline + tab + "defaults off")
            f1.write(newline + tab + "escapes off")
            f1.write(newline + tab + "row delimited by \'\\" + "n" "'"+";")
        else:
            f1.write(newline + tab + "escapes off")
            f1.write(newline + tab + "defaults off")
            f1.write(newline + tab + "FORMAT BINARY")
            f1.write(newline + tab + "CHECK CONSTRAINTS OFF")
            if byteorder == "little":
                f1.write(newline + tab + "BYTE ORDER LOW"+ ";")
            else:
                f1.write(newline + tab + "BYTE ORDER HIGH"+ ";")

        rowCountVerifyInHDL(tablename,path_to_copy,cntstmt,f1)

# This function returns a key to be used in sorting list
# In this case we are returning 3rd element which is size of that table
def sorting_list_key(e):
    return int(e[2])

# Function which divides a list of tables into groups based on active nodes
# of nearly equal size in order to balance load between nodes
def divide_almost_equally(tablelist, num_chunks):
    tablelist.sort(reverse=True,key=sorting_list_key)
    TablesSizeHeap = [(0, idx) for idx in range(num_chunks)]
    heapq.heapify(TablesSizeHeap)
    table_sets = {}
    for i in range(num_chunks):
        table_sets[i] = []
    l_idx = 0
    while l_idx < len(tablelist):
        set_sum, set_idx = heapq.heappop(TablesSizeHeap)
        table_sets[set_idx].append(tablelist[l_idx])
        set_sum += int(tablelist[l_idx][2])
        heapq.heappush(TablesSizeHeap, (set_sum, set_idx))
        l_idx += 1

    for i in range(num_chunks):
        extract_list.append(table_sets[i])

# Function to store table name and its size in Kbytes into a txt file
# Also make different folders in Migration_Data for each table name
# where all the data files and esinfo and its load table statements will be there
def generate_iqtablesize_withrowscount(connectstr):
    # write table_name and its size in a file
    try:
        conn = pyodbc.connect(connectstr, timeout=0)
    except Exception as exp:
        sys.exit("Exception: %s"%str(exp))

    with codecs.open(iqtables_list, "w", common.charset) as f:
        cursor = conn.cursor()
        cursor.execute("SELECT table_name,user_name,t.table_id FROM SYS.SYSTABLE t JOIN SYS.SYSUSER u ON u.user_id = t.creator JOIN SYS.SYSIQTAB it ON (t.table_id = it.table_id) WHERE t.table_type not like '%GBL TEMP%' and t.server_type = 'IQ' AND it.is_rlv = 'F' and lower(user_name) != 'dbo' and lower(user_name) != 'hdladmin' AND lower(user_name) NOT LIKE '_sap\\_%' ESCAPE '\\'")
        records = cursor.fetchall()
        cursor.close()
        i=0
        for row in records:
            i = i +1
            count = 0
            cursor1 = conn.cursor()
            cursor1.execute("""select count(*) from "%s"."%s";"""%(row[1],row[0]) )
            count = cursor1.fetchone()[0]
            cursor1.close()
            tablesize = 0
            cursor1 = conn.cursor()
            cursor1.execute("""select sum(width) from SYS.SYSCOLUMN  where table_id=%s;"""%(row[2]) )
            tablewidth = cursor1.fetchone()[0]
            tablesize = tablewidth * count
            cursor1.close()
            cursor1 = conn.cursor()
            cursor1.execute("""select count(*)  from SYS.SYSFOREIGNKEY where foreign_table_id =%s;"""%(row[2]) )
            foreignkey_flag = cursor1.fetchone()[0]
            # Append newline to every record except the last record
            if i  == len(records):
                if foreignkey_flag == 0:
                    data = row[1] + "." + row[0] + "," + str(count) +  "," + str(tablesize) + "," + str(row[2]) + ","
                else:
                    data = row[1] + "." + row[0] + "," + str(count) +  "," + str(tablesize) + "," + str(row[2]) + "," + "FOREIGN"
            else:
                if foreignkey_flag == 0:
                    data = row[1] + "." + row[0] + "," + str(count) +  "," + str(tablesize) + "," + str(row[2]) + "," + newline
                else:
                    data = row[1] + "." + row[0] + "," + str(count) +  "," + str(tablesize) + "," + str(row[2]) + "," + "FOREIGN" + newline
            f.write(data)
            cursor1.close()

    conn.close()

    # If doing batch wise extraction, then partition the iqtables list based on batch size provided
    original_iqtables_generate_batches()

# Function which generate batches form iqtables list based on batch_size in config file
def original_iqtables_generate_batches():
    if batch != 0:
        tbl_list=[]
        with codecs.open(iqtables_list, "r", common.charset) as f:
            for line in f.readlines():
                stripped_line = line.rstrip(newline)
                tbl = stripped_line.split(',')
                tbl_list.append((tbl[0],tbl[1],int(tbl[2]),tbl[3],tbl[4]))

        generate_batches(tbl_list,'iqtable')

# Function which create batches for iqtables list or for failed and no_extraction list
def generate_batches(tbl_list,str1):
    global cnt
    # Since this is the first run and batches will be make on iqtables.list so count starts from 1
    if str1 == 'iqtable':
        cnt = 1
    # This is not first execution and batches will make on failed and no extraction tables so
    # make files should start from next one count of already existing
    elif str1 == 'failed':
        cnt = count_batches_generated_failed() + 1

    # sort the tbl_list in ascending order
    tbl_list.sort(key=sorting_list_key)
    partition_batches_on_size(tbl_list,common.batch_size,cnt)

# This function will generate batches based on batch_size sum provided
def partition_batches_on_size(table_list,batch_size,batch_cnt):
    batch_table_list=[]
    batch_file="%s%siq_tables_Batch_%s.list"%(migrationpath,path_sep,batch_cnt)

    if len(table_list) == 0 or int(table_list[0][2]) > int(batch_size):
        if len(table_list) != 0 :
            with codecs.open(no_extraction_file,"w", common.charset) as f:
                for row in table_list:
                    data = row[0] + "," + str(row[1]) + "," + str(row[2]) +  "," + str(row[3]) + "," + row[4] +  newline
                    f.write(data)

        return
    for i in table_list:
        batch_size = batch_size - int(i[2])
        if batch_size > 0:
            batch_table_list.append(i)
        else:
            break

    with codecs.open(batch_file, "w", common.charset) as f:
        for row in batch_table_list:
            data = row[0] + "," + str(row[1]) + "," + str(row[2]) +  "," + str(row[3]) + "," + row[4] +  newline
            f.write(data)
            table_list.remove(row)

    batch_cnt = batch_cnt +1
    partition_batches_on_size(table_list,common.batch_size,batch_cnt)

# Function to verify the IQ tables count to be unloaded
# with the lines count of iq_tables file generated
def verify_iq_table_file(connectstr):

    iqtables_complete = 0
    if os.path.isfile(iqtables_list):
        try:
            conn = pyodbc.connect(connectstr, timeout=0)
        except Exception as exp:
            sys.exit("Exception: %s"%str(exp))
        cursor = conn.cursor()
        cursor.execute("SELECT table_name,user_name,t.table_id FROM SYS.SYSTABLE t JOIN SYS.SYSUSER u ON u.user_id = t.creator JOIN SYS.SYSIQTAB it ON (t.table_id = it.table_id) WHERE t.table_type not like '%GBL TEMP%' and t.server_type = 'IQ' AND it.is_rlv = 'F' and lower(user_name) != 'dbo' and lower(user_name) != 'hdladmin' AND lower(user_name) NOT LIKE '_sap\\_%' ESCAPE '\\'")
        count_iq_tables = len(cursor.fetchall())
        cursor.close()
        conn.close()

        # Count the number of line in iq_tables file generated
        iqtables_file_count = 0
        with codecs.open(iqtables_list, "r", common.charset) as f:
            for line in f.readlines():
                iqtables_file_count = iqtables_file_count + 1

        # The iq_tables file is generated successfully
        if count_iq_tables == iqtables_file_count:
            iqtables_complete = 1

    return iqtables_complete

# Function which verify lines count in iqtables.list with
# the lines count of batches file generated and no_extraction.list
# If it matches, means batches file generated successfully
def verify_batches_generated_iq_file():

    batches_complete = 0

    # Count the number of line in iq_tables file generated
    iqtables_lines_count = 0
    with codecs.open(iqtables_list, "r", common.charset) as f:
        for line in f.readlines():
            iqtables_lines_count = iqtables_lines_count + 1

    batches_file_count = len(fnmatch.filter(os.listdir(migrationpath), 'iq_tables_Batch_*.list'))
    batches_lines_count = 0
    batch_num = 1
    while batch_num <= batches_file_count:
        iqtables_batch="%s%siq_tables_Batch_%s.list"%(migrationpath,path_sep,batch_num)
        if os.path.isfile(iqtables_batch):
            with codecs.open(iqtables_batch, "r", common.charset) as f:
                for line in f.readlines():
                    batches_lines_count = batches_lines_count + 1
        batch_num = batch_num + 1

    if os.path.isfile(no_extraction_file):
        with codecs.open(no_extraction_file, "r", common.charset) as f:
            for line in f.readlines():
                batches_lines_count = batches_lines_count + 1

    if iqtables_lines_count == batches_lines_count:
        batches_complete = 1

    return batches_complete

# Function which check if iq_tables.list exist or not
# if not exist then create file in given path
def check_and_create_iq_tables_file(connectstr):

    iq_table_start = datetime.datetime.now()
    resume_complete = 0
    # There can be three scenarios:
    # 1. File exists and it is resume mode, verification is true then do nothing
    # 2. File exists and it is resume mode, verification is false, call function again and after that again do verification
    # 3. File does not exist, call function and after that again do verification

    # In resume mode with file exist, verify the file
    if os.path.isfile(iqtables_list) and resume == True:
        iqtables_complete = verify_iq_table_file(connectstr)
        # The iq_table file is successfull and no need to call verify_iq_table_file again
        # hence setting resume_complete = 1 here
        if iqtables_complete == 1:
            logging.info("%s"%(common.dividerline))
            logging.info("%s file already exists. %sSkipping creation of this file as migration utility started in resume mode."%(iqtables_list,newline))
            resume_complete = 1
            if batch != 0:
                #If in batchwise extraction mode, check if all batches generated
                batches_complete = verify_batches_generated_iq_file()
                if batches_complete == 0:
                   # If doing batch wise extraction, then partition the iqtables list based on batch size provided
                    original_iqtables_generate_batches()

        else :
            generate_iqtablesize_withrowscount(connectstr)
    else:
        generate_iqtablesize_withrowscount(connectstr)

    # After generation of file, verify the file
    if resume_complete == 0:
        iqtables_complete = verify_iq_table_file(connectstr)
        if iqtables_complete == 0:
            sys.exit("IQ tables list file doesn't match with total IQ tables. Re-run the migration utility in resume mode.")
        else:
            logging.info("%s"%(common.dividerline))
            logging.info("%s file successfully created."%(iqtables_list))

    iq_total_elap_sec = common.elap_time(iq_table_start)
    days, hours, minutes, seconds = common.calculate_time(iq_total_elap_sec)
    logging.info("%s"%(common.dividerline))
    logging.info("Time taken to create IQ table list %s: %s%d days, %d hours, %d minutes and %d seconds" % ( iqtables_list, newline, days[0], hours[0], minutes[0], seconds[0]))
    logging.info("%s"%(common.dividerline))

# Function which adds entry in ExtractedTables.out on successful extraction of a table
def write_extractedTables_out(splits,batch):
    if batch != 0:
        global extractedTables_out
        extractedTables_out="%s%sExtractedTables_Batch_%s.out"%(migrationpath,path_sep,batch)
    with codecs.open(extractedTables_out, "a", common.charset) as f:
        if  splits[4]:
            f.write(  splits[0]  + "," + str(splits[1]) + "," + str(splits[3]) + "," + str(splits[4]) + newline)
        else:
            f.write(  splits[0]  + "," + str(splits[1]) + "," + str(splits[3]) + ",BASE" + newline)
    logging.info( "Adding entry in %s file %sfor table : %s [tableID:%s]"%(extractedTables_out,newline,splits[0],splits[3]))
    logging.info("%s"%(common.dividerline))

# call this function to form table list to be unloaded when the migration is not in resume mode
def formlist_tobeunloaded(batch):

    # Get all the table names into a list from iqtables.list file
    table_tobe_unloaded = []
    if batch != 0:
        global iqtables_list
        iqtables_list="%s%siq_tables_Batch_%s.list"%(migrationpath,path_sep,batch)
    with codecs.open(iqtables_list, "r", common.charset) as f:
        for line in f.readlines():
            line = line.rstrip(newline)
            splits = line.split(',')
            total_table.value = total_table.value + 1
            if(splits[1] != '0'):
               # Append the table names and its data into list to be processed for unload
               if(splits[4]):
                  table_tobe_unloaded.append((splits[0], splits[1], splits[2], splits[3], splits[4]))
               else:
                  table_tobe_unloaded.append((splits[0], splits[1], splits[2], splits[3] ,"BASE"))
            else:
               # Incresing the count for empty tables which extracted successfully
               tables_count.value = tables_count.value + 1
               write_extractedTables_out(splits,batch)
               empty_table_count.value += 1

    if empty_table_count.value != 0:
        str1 = "Number of Empty tables in database : %s %sNo need of extraction for empty tables."%(empty_table_count.value,newline)
        common.print_and_log(str1)

    divide_almost_equally(table_tobe_unloaded, nodes_count)

# call this function to form table list to be unloaded when the migration is in resume mode
def resume_formlist_tobeunloaded(batch):
    # Get all the table names already unloaded into a list
    table_already_unloaded = []

    # Get all the table names into a list
    original_iq_tablelist = []

    if batch !=0 :
        global extractedTables_out
        extractedTables_out="%s%sExtractedTables_Batch_%s.out"%(migrationpath,path_sep,batch)

    extract_file = codecs.open(extractedTables_out,'r', common.charset)
    lines = extract_file.readlines()
    for line in lines:
        line = line.rstrip(newline)
        splits = line.split(',')
        if len(splits) == 4:
            table_already_unloaded.append(splits[0])
    extract_file.close()

    if batch != 0:
        global iqtables_list
        iqtables_list="%s%siq_tables_Batch_%s.list"%(migrationpath,path_sep,batch)
    iqtable_file = codecs.open(iqtables_list,'r', common.charset)
    lines = iqtable_file.readlines()
    for line in lines:
        line = line.rstrip(newline)
        splits = line.split(',')
        original_iq_tablelist.append(splits[0])
    iqtable_file.close()

    table_already_extracted_count = len(table_already_unloaded)
    original_iq_tablelist_count = len(original_iq_tablelist)

    print("%s"%(common.dividerline))
    print("%s tables out of %s tables already successfully extracted by previous run of migration utility."%(table_already_extracted_count,original_iq_tablelist_count))

    delta = [item for item in original_iq_tablelist  if item not in table_already_unloaded]

    table_tobe_unloaded = []
    if not delta:
        logging.info("All IQ tables are already processed for data extraction")
    else:
        for i in delta:
            total_table.value = total_table.value + 1
            for line in lines:
                stripped_line = line.rstrip(newline)
                splits = stripped_line.split(',')
                if i == splits[0].strip() and (splits[1] != '0'):
                   if(splits[4]):
                      table_tobe_unloaded.append((splits[0], splits[1], splits[2], splits[3], splits[4]))
                   else:
                      table_tobe_unloaded.append((splits[0], splits[1], splits[2], splits[3] ,"BASE"))
                elif i == splits[0].strip():
                   # Incresing the count for tables which extracted successfully
                   tables_count.value = tables_count.value + 1
                   write_extractedTables_out(splits,batch)
                   empty_table_count.value += 1

        if empty_table_count.value != 0:
            str1 = "Number of Empty tables in database : %s %sNo need of extraction for empty tables.%s"%(empty_table_count.value,newline,newline)
            common.print_and_log(str1)
    divide_almost_equally(table_tobe_unloaded, nodes_count)

# Check for existence of ExtractedTables.out file or not
def get_unload_table_list(batch):
    if batch != 0:
        global extractedTables_out
        extractedTables_out="%s%sExtractedTables_Batch_%s.out"%(migrationpath,path_sep,batch)

    if os.path.isfile(extractedTables_out) and resume == True:
        logging.info("%s"%(common.dividerline))
        logging.info("Some tables are already extracted. Skipping extraction of those tables as migration utility started in resume mode." )
        logging.info("%s"%(common.dividerline))
        resume_formlist_tobeunloaded(batch)
    else:
        formlist_tobeunloaded(batch)

# Function which adds the entry of table in ExtractedTables.out on successfull extraction
def updateUnloadStatus(qSuccess,batch,total_table,tables_count,fail_count,file_write_lock):
    if batch != 0:
        global extractedTables_out
        extractedTables_out="%s%sExtractedTables_Batch_%s.out"%(migrationpath,path_sep,batch)

    with file_write_lock:
        with codecs.open(extractedTables_out, "a", common.charset) as f:
            while True:
                try:
                    owner,tableinfo= qSuccess.get_nowait()
                    splits = tableinfo[0].split('.')
                    table = splits[1]
                    tableid = tableinfo[3]
                    if  tableinfo[4]:
                        f.write(  tableinfo[0]  + "," + str(tableinfo[1]) + "," + str(tableid) + "," + str(tableinfo[4]) + newline)
                    else:
                        f.write(  tableinfo[0]  + "," + str(tableinfo[1]) + "," + str(tableid) + ",BASE" + newline)

                    logging.info("Extraction of table %s [tableID: %s]  was successful"%(tableinfo[0],tableid))
                    logging.info("%s"%(common.dividerline))
                    logging.info("Adding entry in %s file %sfor table: %s [tableID: %s]"%(extractedTables_out,newline,tableinfo[0],tableid))
                    f.flush()
                    tables_count.value += 1
                    progressBar(tables_count,fail_count,total_table)

                    if total_table.value == tables_count.value + fail_count.value:
                        break

                except Exception as exp:
                    break
        f.close()

# Function to display extraction progress
def progressBar(current,fail_count,total_table):
    if (((current.value + fail_count.value )% 20) == 0) or ((current.value + fail_count.value )== total_table.value):
        print("%s"%(common.dividerline))
        print("%s tables successfully extracted and %s tables failed out of total %s tables."%(current.value,fail_count.value,total_table.value))
        print("%s"%(common.dividerline))

# Function which will table failed with exception in failure file
def updateFailureStatus(qFail,batch,total_table,tables_count,fail_count,file_write_lock):
    if batch != 0:
        global extractedFailures_err
        extractedFailures_err = "%s%sextractFailure_Batch_%s.err"%(migrationpath,path_sep,batch)
    with file_write_lock:
        with codecs.open(extractedFailures_err, "a", common.charset) as f:
            while True:
                try:
                    owner,tableName,tableid,exp= qFail.get_nowait()
                    f.write("%s Failed to extract "%(newline)+ owner + "." + tableName + " [tableID: "+ tableid+"]" + newline)
                    f.write(str(exp))
                    f.flush()
                    logging.error("Adding entry in %s file %sfor table : %s.%s [tableID: %s]"%(extractedFailures_err,newline,owner,tableName,tableid))
                    f.close()
                    fail_count.value += 1
                    progressBar(tables_count,fail_count,total_table)
                    if total_table.value == tables_count.value + fail_count.value:
                        break
                except Exception as exp:
                    break
        f.close()

# Function to extract data using temporary options
# which takes queue and get tablename to be extracted from that queue
# and connection string from a list
# This function is not for 16.1 SP01 and 16.0 SP11 versions
def extract_single(q, connstr_port,total_table,batch,log_q,qSuccess,qFail,tables_count,fail_count,file_write_lock):
    global compressed_data
    if log_q:
        qh = QueueHandler(log_q)
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        logger.addHandler(qh)
    while True:
        tableName = ""
        count = 0
        try:
            table_withsize = q.get_nowait()
            splits = table_withsize[0].split('.')
            tableName = splits[1]
            owner = splits[0]
            tableid = table_withsize[3]
            connectstr = connstr_port[0]
            hostport = connstr_port[1]
            is_table_failed = False
            try:
                conn = pyodbc.connect(connectstr, timeout=0)
                cursor = conn.cursor()
                strt = datetime.datetime.now()
                logging.info( "Starting extraction of table: %s.%s [tableID:%s] by : %s"%(owner,tableName,tableid,hostport))
                logging.info("%s"%(common.dividerline))

                cursor.execute("select setting from SYSOPTIONS where \"option\"='string_rtruncation';")
                string_truncation = cursor.fetchone()[0]
                if string_truncation.lower() == "on":
                    cursor.execute("set temporary option STRING_RTRUNCATION = 'off'")
                cursor.execute("""select count(*) from SYS.SYSDOMAIN d JOIN SYS.SYSCOLUMN c ON
                            (c.domain_id = d.domain_id) JOIN SYS.SYSTABLE t ON (c.table_id = t.table_id )
                                     JOIN SYS.SYSUSER u ON (u.user_id = t.creator) where t.table_name='%s'
                     and u.user_name='%s'and (d.domain_name='long varchar' or d.domain_name='long binary');"""%(tableName, owner))
                count = cursor.fetchone()[0]
                # make the directory corresponding to each table with directory name as owner.tablename
                # in the datapath directory
                folder = "%s%s%s"%(datapath,path_sep,tableid)
                if not os.path.isdir(folder):
                    os.mkdir(folder)

                npath = folder
                if platform.system() == "Windows" and npath.startswith("\\"):
                    npath1 = os.path.join("\\", npath)
                    cursor.execute("set temporary option temp_extract_directory = '%s'"%(npath1))
                else:
                    cursor.execute("set temporary option temp_extract_directory = '%s'"%(npath))
            except Exception as exp:
                is_table_failed = True
                qFail.put((owner,tableName,tableid,exp))

            if count != 0:
                if platform.system() == "Windows" and npath.startswith("\\"):
                    text =  form_select_for_lobbfile(table_withsize,conn,npath1)
                else:
                    text =  form_select_for_lobbfile(table_withsize,conn,npath)
                # If the table has LOB datatypes either long varchar or long binary
                # then extraction of table should be done using BFILE() method
                cursor.execute("SET TEMPORARY OPTION temp_extract_file_prefix ='%s'"%(tableid))
                cursor.execute("set temporary option Temp_Extract_File_Extension = 'txt'")
                cursor.execute("set temporary option Temp_Extract_Max_Parallel_Degree = 64")
                cursor.execute("set temporary option Temp_Extract_Quotes = 'on'")
                cursor.execute("set temporary option Temp_Extract_Escape_Quotes = 'on'")
                cursor.execute("set temporary option Temp_Extract_Row_Delimiter = '\n'")
                try:
                    cursor.execute(text).fetchall()
                except Exception as exp:
                    is_table_failed = True
                    qFail.put((owner,tableName,tableid,exp))

                cursor.execute("SET TEMPORARY OPTION temp_extract_file_prefix =''")

                if platform.system() == "Windows" and npath.startswith("\\"):
                    text1 = bfile_select_stmt(table_withsize,conn,npath1)
                else:
                    text1 = bfile_select_stmt(table_withsize,conn,npath)
                try:
                    cursor.execute(text1).fetchall()
                    qSuccess.put((owner,table_withsize))
                except Exception as exp:
                    is_table_failed = True
                    qFail.put((owner,tableName,tableid,exp))

                try:
                    form_load_table_bfilesequential(table_withsize, conn, npath,1)
                except Exception as exp:
                    is_table_failed = True
                    qFail.put((owner,tableName,tableid,exp))

            else:
                cursor.execute("SET TEMPORARY OPTION temp_extract_file_prefix ='%s'"%(tableid))
                if compressed_data == 1:
                    cursor.execute("set temporary option Temp_Extract_File_Extension = 'gz'")
                    cursor.execute("set temporary option Temp_Extract_Compress = 'on'")
                    cursor.execute("set temporary option TEMP_EXTRACT_GZ_COMPRESSION_LEVEL = '1'")
                else:
                    cursor.execute("set temporary option Temp_Extract_File_Extension = 'inp'")

                cursor.execute("set temporary option Temp_Extract_Max_Parallel_Degree = 64")
                cursor.execute("SET TEMPORARY OPTION Temp_Extract_Binary = 'on'")
                cursor.execute("select column_name from SYS.SYSCOLUMN c JOIN SYS.SYSTABLE t ON (c.table_id = t.table_id ) JOIN SYS.SYSUSER u ON (u.user_id = t.creator) where  t.table_name='%s' and u.user_name='%s'"%(tableName, owner ))
                collst = []
                for col in cursor:
                    collst.append(col[0])
                column_string = ""
                n = len(collst)
                for i in range(n-1):
                    column_string = column_string + '\"%s\"'%(collst[i]) + ", "
                column_string = column_string + '\"%s\"'%(collst[n-1])

                select_query = """Select %s FROM "%s"."%s";"""%(column_string,owner,tableName)
                try:
                    cursor.execute(select_query).fetchall()
                    qSuccess.put((owner,table_withsize))
                except Exception as exp:
                    is_table_failed = True
                    qFail.put((owner,tableName,tableid,exp))

                cursor.execute("SET TEMPORARY OPTION Temp_Extract_Binary = 'off'")
                try:
                    form_load_table_stmt(table_withsize, conn, npath, 1)
                except Exception as exp:
                    is_table_failed = True
                    qFail.put((owner,tableName,tableid,exp))

            cursor.execute("SET TEMPORARY OPTION temp_extract_file_prefix = ''")
            if compressed_data == 1:
                cursor.execute("set temporary option Temp_Extract_Compress = 'off'")

            lock.acquire()
            try:
                PATH = '%s%s%s%s%sextractinfo'%(datapath,path_sep,tableid,path_sep,tableid)
                if (os.path.isfile(PATH) and os.access(PATH, os.R_OK)):
                    updateUnloadStatus(qSuccess,batch,total_table,tables_count,fail_count,file_write_lock)
                if is_table_failed:
                    updateFailureStatus(qFail,batch,total_table,tables_count,fail_count,file_write_lock)

            finally:
                lock.release() #release lock

            elap_sec = common.elap_time(strt)
            days, hours, minutes, seconds = common.calculate_time(elap_sec)

            logging.info("Time taken to unload table: %s [tableID: %s] is : %d days, %d hours, %d minutes and %d seconds\n" % (tableName, tableid,days[0], hours[0], minutes[0], seconds[0]))
            logging.info("%s"%(common.dividerline))
            cursor.close()
            conn.close()
        except Exception as exp:
            #TODO : Need to fix of NUll entries in the queue
            # Also add try/catch blocks all where needed
            #logging.info("Exception occurred while extracting table: %s"%(tableName))
            if str(exp) != "":
                logging.error("Unexpected error in extract_single() reported while extracting data: %s"%str(exp))
                qFail.put((owner,tableName,tableid,exp))
                updateFailureStatus(qFail,batch,total_table,tables_count,fail_count,file_write_lock)
            else:
                return

# For version 16.0 SP11 if a table has LOB datatypes then extraction should be done using BFILE() method
# Function which form select query during bfile extraction
def form_select_for_lobbfile(tablename,conn,path_to_copy):
    cursor3 = conn.cursor()
    splits = tablename[0].split('.')
    table = splits[1]
    owner = splits[0]
    tableid = tablename[3]
    cursor3.execute("select c.column_name,d.domain_name,c.column_id  from SYS.SYSDOMAIN d JOIN SYS.SYSCOLUMN c ON (c.domain_id = d.domain_id) JOIN SYS.SYSTABLE t ON (c.table_id = t.table_id ) JOIN SYS.SYSUSER u ON (u.user_id = t.creator) where  t.table_name='%s' and u.user_name='%s'"%(table, owner ))

    column_domain_list = []
    for i in cursor3:
        column_domain_list.append(i)
    obj_path = "hdlfs:///%s/Extracted_Data/%s/"%(common.hdlfs_directory,tableid)

    cmd = "SELECT "
    filelst = []
    for i in column_domain_list:
        if i[1] == 'long varchar' or i[1] == 'long binary':
            filelst.append( "CASE WHEN " + i[0] + " IS NOT NULL THEN" + "('" + obj_path + "' + '" + tableid + "_" + "row' + " + "string(rowid(\"" + table + "\" )) + '.' + '" + str(i[2]) + "') " + "ELSE NULL END" )
        else:
            filelst.append( i[0])

    my_string = " ,".join(filelst)

    command1 = " FROM \"%s\".\"%s\""%(owner,table)
    cmd = cmd + my_string + command1 + ";"
    cursor3.close()
    return cmd

# For version 16.0 SP11 if a table has LOB datatypes then extraction should be done using BFILE() method
# Function which form select query during bfile extraction
def bfile_select_stmt(tablename,conn,path_to_copy):
    cursor3 = conn.cursor()
    splits = tablename[0].split('.')
    table = splits[1]
    owner = splits[0]
    tableid = tablename[3]
    cursor3.execute("select c.column_name,d.domain_name,c.column_id from SYS.SYSDOMAIN d JOIN SYS.SYSCOLUMN c ON (c.domain_id = d.domain_id) JOIN SYS.SYSTABLE t ON (c.table_id = t.table_id ) JOIN SYS.SYSUSER u ON (u.user_id = t.creator) where  t.table_name='%s' and u.user_name='%s'"%(table, owner ))

    column_domain_list = []
    for i in cursor3:
        column_domain_list.append(i)
    cmd = "SELECT "
    filelst = []
    for i in column_domain_list:
        if i[1] == 'long varchar' or i[1] == 'long binary':
            filelst.append( "BFILE('" + path_to_copy + "/' + '" + tableid + "_" + "row' + " + "string(rowid(\"" + table + "\" )) + '.' + '" + str(i[2]) + "', " + i[0] + ")")

    my_string = " ,".join(filelst)
    command1 = " FROM \"%s\".\"%s\""%(owner,table)
    cmd = cmd + my_string + command1 + ";"
    cursor3.close()
    return cmd

# For version 16.0 SP11 if a table has LOB datatypes then extraction should be done using BFILE() method
# Function which form load table statement for tables having LOB datatypes
def form_load_table_bfilesequential(tablename, conn, path_to_copy,parl):
    splits = tablename[0].split('.')
    table = splits[1]
    owner = splits[0]
    tableid = tablename[3]
    with codecs.open("%s%s%s.sql"%(path_to_copy,path_sep,tableid), "w", common.charset) as f1:
        f1.write("set temporary option \"auto_commit\" = 'OFF';"+ "%s%s"%(newline,newline))
        f1.write("set temporary option disable_ri_check= 'on';"+ "%s%s"%(newline,newline))
        f1.write("COMMIT;" + newline)
        f1.write("BEGIN TRANSACTION;" + newline)

        cursor3 = conn.cursor()
        cursor3.execute("select setting from SYSOPTIONS where \"option\"='string_rtruncation';")
        string_truncation = cursor3.fetchone()[0]
        if string_truncation.lower() == "on":
            cursor3.execute("set temporary option STRING_RTRUNCATION = 'off'")
        cursor3.execute("select \"default\" from SYS.SYSCOLUMN c JOIN SYS.SYSTABLE t ON (c.table_id = t.table_id ) JOIN SYS.SYSUSER u ON (u.user_id = t.creator) where  t.table_name='%s' and u.user_name='%s'"%(table, owner ))
        idtcol = []
        idtflag = 0
        for idt in cursor3:
            idtcol.append(idt[0])
        if 'Identity/Autoincrement' in idtcol or 'autoincrement' in idtcol:
            idtflag = 1
        if owner.lower() == 'dba':
            command1 = "LOAD TABLE \"HDLADMIN\".\"%s\""%(table)
            f1.write("sp_iqlogtoiqmsg('HDL-Migration: " + command1 + "');" + newline + newline)
            if idtflag == 1:
                f1.write("set temporary option \"identity_insert\" = \"HDLADMIN.%s\";"%(table)+ newline + newline)
            cntstmt = "SELECT count(*) into cnt FROM  \"HDLADMIN\".\"%s\""%(table)
        else:
            command1 = "LOAD TABLE \"%s\".\"%s\""%(owner,table)
            f1.write("sp_iqlogtoiqmsg('HDL-Migration: " + command1 + "');" + newline + newline)
            if idtflag == 1:
                f1.write("set temporary option \"identity_insert\" = \"%s.%s\";"%(owner,table)+ newline + newline)
            cntstmt = "SELECT count(*) into cnt FROM  \"%s\".\"%s\""%(owner,table)
        cursor3.execute("select c.column_name,d.domain_name,c.nulls  from SYS.SYSDOMAIN d JOIN SYS.SYSCOLUMN c ON (c.domain_id = d.domain_id) JOIN SYS.SYSTABLE t ON (c.table_id = t.table_id ) JOIN SYS.SYSUSER u ON (u.user_id = t.creator) where  t.table_name='%s' and u.user_name='%s'"%(table, owner ))
        l = []
        for i in cursor3:
            l.append(i)

        command2 = ""
        filelst = []
        for i in l:
            if i[1] == 'long varchar' :
                filelst.append( i[0] + " ASCII FILE (',') NULL('NULL')")
            elif i[1] == 'long binary':
                filelst.append( i[0] + " BINARY FILE (',') NULL('NULL')")
            elif i[2] == 'Y':
                filelst.append( i[0] + " NULL('NULL')")
            else:
                filelst.append( i[0] )

        command2 = ','.join(map(str, filelst))
        command = " " + command1 + "( " + command2 + " )"
        f1.write(command)
        cursor3.close()
        tab = "     "
        f1.write(newline + tab + "FROM" + " ")
        filelst = list()
        l = list()
        if parl == 1:

            l = getfilelist_fromesinfo(tableid,path_to_copy)
        else:
            i = 1
            while i < 9:
                var = '%s_%s.txt'%(tableid,i)
                l.append(var)
                i = i + 1

        obj_path = "hdlfs:///%s/Extracted_Data/%s/"%(common.hdlfs_directory,tableid)
        for lst in l:
            filelst.append("'"+obj_path+str(lst)+"'")

        my_string = ','.join(map(str, filelst))
        f1.write(my_string )

        f1.write(newline + tab + "escapes off quotes on")
        f1.write(newline + tab + "row delimited by \'\\" + "n" "'"+";")

        rowCountVerifyInHDL(tablename,path_to_copy,cntstmt,f1)

def size_query(ext,tableid):
    size_const = 3990000000

    l1 = []
    l2 = []

    i = 1
    while i < 9:
        l1.append ("SET TEMPORARY OPTION Temp_Extract_Name%s ='%s_%s.%s';"%(i,tableid,i,ext))
        l1.append ("SET TEMPORARY OPTION Temp_Extract_Size%s = %s;"%(i,size_const))
        l2.append ("SET TEMPORARY OPTION Temp_Extract_Name%s ='';"%(i))
        l2.append ("SET TEMPORARY OPTION Temp_Extract_Size%s = 0;"%(i))
        i = i + 1

    return l1,l2

# Function to extract data using temporary options
# which takes queue and get tablename to be extracted from that queue
# and connection string from a list
# This function is for 16.1 SP01 and 16.0 SP11 versions
# The implementation is different since parallel extraction is not in above versions.
def extract_single_sequential(q, connstr_port,total_table,batch,log_q,qSuccess,qFail,tables_count,fail_count):
    if log_q:
        qh = QueueHandler(log_q)
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        logger.addHandler(qh)
    while True:
        try:
            table_withsize = q.get_nowait()
            connectstr = connstr_port[0]
            hostport = connstr_port[1]
            splits = table_withsize[0].split('.')
            tableName = splits[1]
            owner = splits[0]
            tableid = table_withsize[3]
            conn = pyodbc.connect(connectstr, timeout=0)
            cursor = conn.cursor()
            strt = datetime.datetime.now()
            logging.info( "Starting extraction of table: %s.%s [tableID:%s] by : %s"%(owner,tableName,tableid,hostport))
            logging.info("%s"%(common.dividerline))

            cursor.execute("""select count(*) from SYS.SYSDOMAIN d JOIN SYS.SYSCOLUMN c ON
                            (c.domain_id = d.domain_id) JOIN SYS.SYSTABLE t ON (c.table_id = t.table_id )
                                     JOIN SYS.SYSUSER u ON (u.user_id = t.creator) where t.table_name='%s'
                     and u.user_name='%s'and (d.domain_name='long varchar' or d.domain_name='long binary');"""%(tableName, owner))

            count = cursor.fetchone()[0]
            cursor.execute("select @@version;")
            version = cursor.fetchone()[0]
            # make the directory corresponding to each table with directory name as owner.tablename
            # in the Extract_Path/Migration_Data/Extracted_Data directory
            folder = "%s%s%s"%(datapath,path_sep,tableid)
            if not os.path.isdir(folder):
                os.mkdir(folder)

            npath = folder
            if platform.system() == "Windows" and npath.startswith("\\"):
                npath1 = os.path.join("\\", npath)
                cursor.execute("set temporary option temp_extract_directory = '%s'"%(npath1))
            else:
                cursor.execute("set temporary option temp_extract_directory = '%s'"%(npath))
            is_table_failed = False
            if count != 0:
                if 'SAP IQ/16.0.' in version:
                    if platform.system() == "Windows" and npath.startswith("\\"):
                        text =  form_select_for_lobbfile(table_withsize,conn,npath1)
                    else:
                        text =  form_select_for_lobbfile(table_withsize,conn,npath)
                    # If the table has LOB datatypes either long varchar or long binary on 16.0 SP11
                    # then extraction of table should be done using BFILE() method
                    l1 ,l2 = size_query('txt',tableid)
                    for i in l1:
                        cursor.execute(i)

                    cursor.execute("set temporary option Temp_Extract_Quotes = 'on'")
                    cursor.execute("set temporary option Temp_Extract_Escape_Quotes = 'on'")
                    cursor.execute("set temporary option Temp_Extract_Row_Delimiter = '\n'")
                    try:
                        cursor.execute(text).fetchall()
                    except Exception as exp:
                        is_table_failed = True
                        qFail.put((owner,tableName,tableid,exp))

                    for i in l2:
                        cursor.execute(i)
                    if platform.system() == "Windows" and npath.startswith("\\"):
                        text1 = bfile_select_stmt(table_withsize,conn,npath1)
                    else:
                        text1 = bfile_select_stmt(table_withsize,conn,npath)
                    try:
                        cursor.execute(text1).fetchall()
                        qSuccess.put((owner,table_withsize))
                    except Exception as exp:
                        is_table_failed = True
                        qFail.put((owner,tableName,tableid,exp))

                try:
                    form_load_table_bfilesequential(table_withsize, conn, npath,0 )
                except Exception as exp:
                    is_table_failed = True
                    qFail.put((owner,tableName,tableid,exp))

            else:
                l1 ,l2 = size_query('inp',tableid)
                for i in l1:
                    cursor.execute(i)

                cursor.execute("SET TEMPORARY OPTION Temp_Extract_Binary = 'on'")
                cursor.execute("select column_name from SYS.SYSCOLUMN c JOIN SYS.SYSTABLE t ON (c.table_id = t.table_id ) JOIN SYS.SYSUSER u ON (u.user_id = t.creator) where  t.table_name='%s' and u.user_name='%s'"%(tableName, owner ))
                collst = []
                for col in cursor:
                    collst.append(col[0])
                column_string = ""
                n = len(collst)
                for i in range(n-1):
                    column_string = column_string + '\"%s\"'%(collst[i]) + ", "
                column_string = column_string + '\"%s\"'%(collst[n-1])

                select_query = """Select %s FROM "%s"."%s";"""%(column_string,owner,tableName)
                try:
                    cursor.execute(select_query).fetchall()
                    qSuccess.put((owner,table_withsize))
                except Exception as exp:
                    is_table_failed = True
                    qFail.put((owner,tableName,tableid,exp))

                for i in l2:
                    cursor.execute(i)

                cursor.execute("SET TEMPORARY OPTION Temp_Extract_Binary = 'off'")
                try:
                    form_load_table_sequential(table_withsize, conn, npath, 1)
                except Exception as exp:
                    is_table_failed = True
                    qFail.put((owner,tableName,tableid,exp))

            lock.acquire()
            try:
                PATH = '%s%s%s%s%s_1.inp'%(datapath,path_sep,tableid,path_sep,tableid)
                PATH1 = '%s%s%s%s%s_1.txt'%(datapath,path_sep,tableid,path_sep,tableid)

                if (((os.path.isfile(PATH)) or (os.path.isfile(PATH1))) and is_table_failed == False):
                    updateUnloadStatus(qSuccess,batch,total_table,tables_count,fail_count,file_write_lock)
                if is_table_failed :
                    updateFailureStatus(qFail,batch,total_table,tables_count,fail_count,file_write_lock)
            finally:
                lock.release() #release lock

            elap_sec = common.elap_time(strt)
            days, hours, minutes, seconds = common.calculate_time(elap_sec)

            logging.info("Time taken to unload table: %s [tableID:%s] is : \n%d days, %d hours, %d minutes and %d seconds" % (table_withsize[0], tableid, days[0], hours[0], minutes[0], seconds[0]))
            logging.info("%s\n"%(common.dividerline))
            cursor.close()
            conn.close()
        except Exception as exp:
            if str(exp) != "":
                logging.error("Unexpected error reported in extract_single_sequential() while extracting data: \n%s"%str(exp))
                updateFailureStatus(qFail,batch,total_table,tables_count,fail_count,file_write_lock)
            else:
                return

# Function to form connect strings for each node based on
# value of Client_Num_Conn provided in json config file
def connect_list(connectstr):
    connection_sets = {}
    for i in range(nodes_count):
        connection_sets[i] = []

    node_connect_str = []
    conn = pyodbc.connect(connectstr, timeout=0)
    cursor = conn.cursor()
    coord = "host=" + str(common.hostname) + ":" +  str(common.port)
    node_connect_str.append((connectstr,coord))

    cursor.execute("select connection_info from sp_iqmpxinfo() where (role= 'reader' and status='included' and inc_state='active');")
    connrecords = cursor.fetchall()
    logging.info("%s"%(common.dividerline))
    logging.info("Total number of active MPX reader nodes: %s"% len(connrecords))

    for i in connrecords:
        full_host = i[0].replace(f"host={common.hostname.split('.')[0]}:", f"host={common.hostname}:")
        connvarstr = 'DRIVER={%s};%s;UID=%s;PWD=%s;ENC=%s' % (common.driver, full_host, common.userid, common.password, common.enc_string)
        node_connect_str.append((connvarstr, full_host))

    cursor.execute("select connection_info from sp_iqmpxinfo() where (role= 'writer' and status='included' and inc_state='active');")
    connrecords = cursor.fetchall()
    logging.info("Total number of active MPX writer nodes: %s"% len(connrecords))
    logging.info("%s"%(common.dividerline))

    for i in connrecords:
        full_host = i[0].replace(f"host={common.hostname.split('.')[0]}:", f"host={common.hostname}:")
        connvarstr = 'DRIVER={%s};%s;UID=%s;PWD=%s;ENC=%s' % (common.driver, full_host, common.userid, common.password, common.enc_string)
        node_connect_str.append((connvarstr, full_host))

    for i in range(len(node_connect_str)):
        total_connection_per_node = []
        for j in range(int(common.conn_num)):
            total_connection_per_node.append(node_connect_str[i])
        connection_sets[i] = total_connection_per_node

    for i in range(nodes_count):
        connection_list.append(connection_sets[i])

    cursor.close()
    conn.close()

# Function which display the CLI to be used to copy extracted data to object store
def display_clicopy_command(batch):
    logging.info("%s"%(common.double_divider_line))
    logging.info("Next Steps:%s%s1. Copy data on Object store."%(newline,newline))
    logging.info("%sSample command to copy the data on data lake Files object store:"%(newline))
    if is_windows:
        copy_cmd = "hdlfscli -cert %s -key %s -s %s upload %s /%s -log"%(common.hdlfs_cert_path, common.hdlfs_key_path, common.hdlfs_files_endpoint, migrationpath, common.hdlfs_directory)
        logging.info("%s%s "%(newline,copy_cmd))
        logging.info("NOTE: Due to a known HDLFS limitation, the hdlfscli utility does not support copying data file larger than 95 GB. At present, no official workaround exists but an alternate solution is in progress.")
    else:
        logging.info("%spython3 copy_hdlfs.py --config_file <migration config file path>"%newline)
    logging.info("%s"%(common.double_divider_line))
    batches_count = count_batches_generated_failed()
    if batch == 0 or batch == batches_count:
        failure_backup_count = count_failure_backup_files()
        if not os.path.isfile('%s%sno_extraction.list'%(migrationpath,path_sep)) and failure_backup_count == 0:
            logging.info("%s2. Run load utility (load_schema_and_data.py)"%newline)
            if onlydata == 'y':
                logging.info("%sSample command to run load utility for data load :"%newline)
                logging.info("%spython3 load_schema_and_data.py --config_file <config file path> --onlydata y"%newline)
            if fullextraction == 'y':
                logging.info("%sSample command to run load utility for schema and data load :"%newline)
                logging.info("%spython3 load_schema_and_data.py --config_file <config file path> --fullload y"%newline) 
            logging.info("%s"%(common.double_divider_line))
        elif os.path.isfile('%s%sno_extraction.list'%(migrationpath,path_sep)) and failure_backup_count == 0:
            common.print_and_log(no_extraction_warning_msg)
            logging.info("%s"%(common.double_divider_line))
            logging.info("%s2. Run load utility (load_schema_and_data.py)"%newline)
            if onlydata == 'y':
                logging.info("%sSample command to run load utility for data load :"%newline)
                logging.info("%spython3 load_schema_and_data.py --config_file <config file path> --onlydata y"%newline)
            if fullextraction == 'y':
                logging.info("%sSample command to run load utility for schema and data load :"%newline)
                logging.info("%spython3 load_schema_and_data.py --config_file <config file path> --fullload y"%newline)
            logging.info("%s"%(common.double_divider_line))

# Function which read extractedTables_out file into a list and returns its count
def extracted_tables_count(f):
    extracted_list = list()
    table_cnt = 0
    if not os.path.isfile(extractedTables_out):
        table_cnt = 0
    else:
        with codecs.open(f, "r", common.charset) as f:
            for line in f.readlines():
                line = line.rstrip(newline)
                splits = line.split(',')
                if len(splits) == 4:
                    # splits[0] is of form <owner>.<tablename>
                    extracted_list.append((splits[0]))
        table_cnt = len(extracted_list)
    return table_cnt

# Function to check the status of migration
# It will compare the IQ table list with the list of unloaded tables
# If both lists are equal then extraction of all tables done successfully
# f1 :iqtables_list, f2 : extractedTables_out and f3 : extractedFailures_err
def check_migration_status(f1,f2,f3,batch):
    iqtableslist = list()
    with codecs.open(f1, "r", common.charset) as f:
        for line in f.readlines():
            line = line.rstrip(newline)
            splits = line.split(',')
            iqtableslist.append((splits[0]))

    if len(iqtableslist) == 0:
        logging.info("%s"%(common.double_divider_line))
        logging.info("The Database has no IQ tables. No need of Extraction")
        logging.info("%s"%(common.double_divider_line))

    elif not os.path.isfile(f2):
        logging.info("================================================================================")
        logging.info("%s file does not exist. Extraction of all tables failed."%f2)
        logging.info("================================================================================")

    else:
        extractedtablelist = list()

        with codecs.open(f2, "r", common.charset) as f:
            for line in f.readlines():
                line = line.rstrip(newline)
                splits = line.split(',')
                if len(splits) == 4:
                    extractedtablelist.append((splits[0]))

        failed_list = [item for item in iqtableslist  if item not in extractedtablelist]

        if len(failed_list) == 0:
            logging.info("%s"%(common.double_divider_line))
            if batch == 0:
                logging.info("Extraction of all tables is successful")
            else:
                logging.info("Extraction of all tables is successful for Batch = %s"%batch)
            logging.info("%s"%(common.double_divider_line))
            display_clicopy_command(batch)
        else:
            fail_count = len(failed_list)
            succ_count = len(extractedtablelist)
            str1 = "Extraction of %s tables failed."%fail_count
            common.print_and_log(str1)
            str1 = "Extraction of %s tables successful."%succ_count
            common.print_and_log(str1)
            logging.info("Please check %s file for extraction failures. %sRerun Migration Utility in resume mode to extract remaining tables."%(f3,newline))
            logging.info("%s"%(common.double_divider_line))

# Function which to get byteorder on the basis of verification of host
def get_byteorder():
    global byteorder
    val = verify_hostname()
    if val ==1:
        byteorder=sys.byteorder
    else:
        import paramiko
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect( common.hostname, username=common.client_id, password=common.client_pwd)

        stdin, stdout, stderr = client.exec_command('python -c "import sys;print sys.byteorder"',get_pty=True)
        byteorder=stdout.readline()

# Function which will do parallel extraction and extract data of tables
# At the end it will also check if all tables are extracted or not
def extract_main(batch):

    global q_listener, log_q, logger
    if not is_windows:
        log_q = None

    if len(extract_list) != 0:
        str1 = "Starting data unload"
        common.print_and_log(str1)
        logging.info("%s" % (common.dividerline))
        print("For unload progress of tables, please check file : %s%s" % (newline, migration_log))

    start = datetime.datetime.now()

    qSuccess = multiprocessing.Queue()
    qFail = multiprocessing.Queue()
    file_write_lock = multiprocessing.Lock()
    connect_list(connectstr)

    extract_fun = extract_single

    if batch != 0:
        global extractedFailures_err
        extractedFailures_err = "%s%sextractFailure_Batch_%s.err" % (migrationpath, path_sep, batch)

    if os.path.isfile(extractedFailures_err):
        os.remove(extractedFailures_err)

    process_trackers = []
    restart_counts = {}  # Track restarts for each conn_info
    RESTART_LIMIT = 3    # Max restarts allowed per connection

    for i in range(nodes_count):
        node_connect_list = connection_list[i]
        node_extract_list = extract_list[i]
        nodesqueue = multiprocessing.Queue()
        for item in node_extract_list:
            nodesqueue.put(item)

        node_processes = []

        def start_worker(conn_info):
            p = multiprocessing.Process(
                target=extract_fun,
                args=(nodesqueue, conn_info, total_table, batch, log_q, qSuccess, qFail, tables_count, fail_count, file_write_lock)
            )
            p.start()
            return p

        for conn_info in node_connect_list:
            p = start_worker(conn_info)
            node_processes.append((p, conn_info))
            restart_counts[conn_info] = 0  # Initialize restart count

        process_trackers.append((nodesqueue, node_processes))

    # Monitor and restart dead processes
    while any(nodesqueue.qsize() > 0 for nodesqueue, _ in process_trackers):
        for nodesqueue, node_processes in process_trackers:
            for idx, (proc, conn_info) in enumerate(node_processes):
                if not proc.is_alive():
                    if nodesqueue.qsize() > 0:
                        if restart_counts[conn_info] < RESTART_LIMIT:
                            logging.warning(f"Restarting dead process for {conn_info} (attempt {restart_counts[conn_info] + 1})")
                            new_proc = multiprocessing.Process(
                                target=extract_fun,
                                args=(nodesqueue, conn_info, total_table, batch, log_q, qSuccess, qFail, tables_count, fail_count, file_write_lock)
                            )
                            new_proc.start()
                            node_processes[idx] = (new_proc, conn_info)
                            restart_counts[conn_info] += 1
                        else:
                            logging.error(f"Restart limit exceeded for connection: {conn_info}. No further restart attempts.")
        time.sleep(5)

    # Ensure all processes have completed
    for _, node_processes in process_trackers:
        for proc, _ in node_processes:
            proc.join()


    total_elap_sec = common.elap_time(start)
    days, hours, minutes, seconds = common.calculate_time(total_elap_sec)

    logging.info("Data unload completed.")

    if batch != 0:
        global extractedTables_out
        extractedTables_out="%s%sExtractedTables_Batch_%s.out"%(migrationpath,path_sep,batch)

    table_cnt = extracted_tables_count(extractedTables_out)
    str1 = "Total number of unloaded tables = %s"%table_cnt
    common.print_and_log(str1)
    logging.info("Total Time taken in data unload : %s%d days, %d hours, %d minutes and %d seconds" % ( newline, days[0], hours[0], minutes[0], seconds[0]))
    logging.info("%s"%(common.dividerline))

    if batch != 0:
        extractedTables_out="%s%sExtractedTables_Batch_%s.out"%(migrationpath,path_sep,batch)
        global iqtables_list
        iqtables_list="%s%siq_tables_Batch_%s.list"%(migrationpath,path_sep,batch)
        extractedFailures_err = "%s%sextractFailure_Batch_%s.err"%(migrationpath,path_sep,batch)
    check_migration_status(iqtables_list, extractedTables_out,extractedFailures_err,batch)

# Count the number of iq_tables_Batch_ files to determine the total batches count
def count_batches_generated():
    count = len(fnmatch.filter(os.listdir(migrationpath), 'iq_tables_Batch_*.list'))
    return count

# Function to delete all the previous batch data files in datapath except the sql file
def delete_previous_extracted_batch_data():
    for dirpath, dirnames, filenames in os.walk(datapath):
        for f in filenames:
            if not f.endswith('.sql'):
                try:
                    os.remove(os.path.join(dirpath,f))
                except Exception as e:
                    logging.info( e )

# In rerun case when all the batches done but we have failure file or no_extraction list,
# to again generate batches, we need count how many already exists so we can start from next count
def count_batches_generated_failed():
    prev_count = len(fnmatch.filter(os.listdir(migrationpath), 'iq_tables_Batch_*.list'))
    return prev_count

# Count the number of failure backup files
def count_failure_backup_files():
    count = len(fnmatch.filter(os.listdir(migrationpath), 'extractFailure_Batch_*.err.bk'))
    return count

def max_size_of_no_extraction_list():
    table_no_unloaded = []

    no_extract_file = codecs.open(no_extraction_file,'r', common.charset)
    lines = no_extract_file.readlines()
    for line in lines:
        line = line.rstrip(newline)
        splits = line.split(',')
        table_no_unloaded.append(splits[2])
    no_extract_file.close()

    max_value = 0
    for i in table_no_unloaded:
        if int(i) > max_value:
            max_value = int(i)

    # Convert max_value to GB
    max_value_GB = math.ceil(max_value / (1024*1024*1024))
    return max_value_GB

# Function which print summary of batchwise table extraction
def print_batch_extraction_summary():
    iq_tables_batches_count = count_batches_generated()
    str1 = "Total Batches generated for iq_tables.list = %s"%iq_tables_batches_count
    common.print_and_log(str1)
    failure_backup_files_count = count_failure_backup_files()
    if not os.path.isfile(no_extraction_file):
        no_extraction_file_count = 0
    else:
        no_extraction_file_count = 1
        max_value_GB = max_size_of_no_extraction_list()

    str1 = "%sBatchwise Table Extraction Summary"%newline
    common.print_and_log(str1)
    str1 = "%sTotal number of Batches generated for iq_tables = %s"%(newline,iq_tables_batches_count)
    common.print_and_log(str1)

    str1 = "%sTotal number of failure backup files generated = %s"%(newline,failure_backup_files_count)
    common.print_and_log(str1)

    if no_extraction_file_count == 1:
        str1 = "%sSome tables could not be extracted due to batch_size limit. Please increase Batch_Size_GB limit to = %sGB in config file"%(newline,max_value_GB)
        common.print_and_log(str1)

    str1 = "%sTo verify successfully extracted tables, refer to %s"%(newline,extractedTables_out)
    common.print_and_log(str1)

# Function which do batchwise table extraction
def extracted_batch_file_exist():
    count = count_batches_generated()
    str1 = "Total Batches generated for iq_tables.list = %s"%count
    common.print_and_log(str1)
    batch = 1
    global no_extraction_warning_msg
    no_extraction_warning_msg = ''.join(("Some tables could not be extracted due to Batch_Size_GB limit. Please refer to the file 'no_extraction.list'.%s"%newline ,
                                    "You may either increase the Batch_Size_GB limit in config file and rerun migration utility, or proceed with the load.%s"%newline ,
                                    "If you choose to do the latter, you will have to manually extract and load the tables that could not be automatically extracted.%s"%newline ,
                                    "Preferred way would to complete all table extraction before proceeding to the load phase.%s"%newline ,
                                    "Please refer to the user documentation for more details."))
    while batch  <= count:
        extractedTables_out_btch="%s%sExtractedTables_Batch_%s.out"%(migrationpath,path_sep,batch)
        if os.path.isfile(extractedTables_out_btch):

             # If extracted_out batch file also exist and no failure exist, then proceed to next batch
             extractedFailures_err_batch = "%s%sextractFailure_Batch_%s.err"%(migrationpath,path_sep,batch)
             if not os.path.isfile(extractedFailures_err_batch):
                 # If all the batches have been done
                 if batch == count :
                     # check for number of failure backup files
                     failure_backup_count = count_failure_backup_files()
                     combine_extracted_output(count)

                     # If no failure backup files generated and no_extraction.list, all batches tables extracted successfully
                     if not os.path.isfile(no_extraction_file) and  failure_backup_count == 0:
                         print_batch_extraction_summary()
                         str1 = "%sExtraction of tables from all batches have been completed successfully.Please copy the last batch data to object store and proceed to load."%newline
                         common.print_and_log(str1)

                     else:
                         str1 = "%sAll the batches have been completed and summary is generated. %sDo you want to create new batches for failed tables or no_extracted tables or both if exists? Enter Y/N :"%(newline,newline)
                         if(sys.version[0:2] == '2.'):
                             val = str(raw_input(str1))
                         else:
                             val = str(input(str1))
                         if val.lower() == "y":
                             failure_and_noextraction_file_batches()
                             count = count_batches_generated()
                             if count > batch:
                                 str1 = "%sTotal new batches generated from failed and no_extraction tables = %s"%(newline,count)
                                 common.print_and_log(str1)
                                 batch = batch + 1
                                 # After generating new batches, in same run execute extraction for next batch
                                 current_batch_extraction(batch,count)
                             else:
                                 str1 = "%sNo new batches have been generated from failed and remaining no_extraction tables. Please increase Batch_Size_GB limit in config file and Run Migration Utility in resume mode."%newline
                                 common.print_and_log(str1)
                                 common.print_and_log(no_extraction_warning_msg)
                                 logging.info("%s"%(common.double_divider_line))

                         elif val.lower() == "n":
                             print("%s"%(common.dividerline))
                             sys.exit("%sRerun the migration utility  in resume mode which will create new batches by reading Batch_Size_GB in config file"%newline)
                         else:
                             print("%s"%(common.dividerline))
                             sys.exit("%sExiting as wrong input provided. Supported values are Y and N. Rerun the Migration utility in resume mode."%newline)

                 batch = batch + 1

             else:
                 if batch == count:
                     str1 = "%sLast batch = %s has some failures as %s file exists.%sDo you want to %sY - Resume(Existing batch will run in resume mode) %sN - Proceed to exit batch extraction? (Y/N): "%(newline,batch,extractedFailures_err_batch,newline,newline,newline)
                 else:
                     str1 = "%sPrevious batch = %s has some failures as %s file exists.%sDo you want to %sY - Resume(Existing batch will run in resume mode) %sN - Proceed to next batch extraction? (Y/N): "%(newline,batch,extractedFailures_err_batch,newline,newline,newline)
                 if(sys.version[0:2] == '2.'):
                     val = str(raw_input(str1))
                 else:
                     val = str(input(str1))
                 if val.lower() == "y":
                     str1 = "%sStarted extraction of batch = %s in resume mode"%(newline,batch)
                     common.print_and_log(str1)
                     resume_formlist_tobeunloaded(batch)
                     extract_main(batch)
                     break
                 elif val.lower() == "n":
                     # Copy the contents of batch error to one backup batch failure file, else it will ask input for each batch
                     extractedFailures_err_bk = "%s%sextractFailure_Batch_%s.err.bk"%(migrationpath,path_sep,batch)
                     if batch == count:
                         str1 = "%sAnalyse the failure backup file %s. All the batches have been completed and summary is generated. Run Migration Utility in resume mode."%(newline,extractedFailures_err_bk)
                     else:
                         str1 = "%sAnalyse the failure backup file %s and extraction will be done for next batch"%(newline,extractedFailures_err_bk)
                     common.print_and_log(str1)
                     shutil.copyfile(extractedFailures_err_batch,extractedFailures_err_bk)
                     os.remove(extractedFailures_err_batch)
                     if batch == count:
                         print_batch_extraction_summary()
                         combine_extracted_output(count)
                         break
                     else:
                         batch = batch + 1

                 else:
                     sys.exit("%s%sExiting as wrong input provided. Supported values are Y and N. Rerun the Migration utility in resume mode."%(newline,newline))

        else:
             current_batch_extraction(batch,count)
             break

# Function which deletes previous batch data and run extraction for next batch
def current_batch_extraction(batch,count):
    # We should not ask for this input when migration utility is running for restart mode, since it is first time, hence this condition
    if resume == True and batch > 1:
        str1 = "%sPrevious batch = %s has been completed.%sPlease copy all the contents of Migration_Data folder to object store before proceeding.If data has been successfully copied,%sEnter Y - Yes or N - No: "%(newline,batch-1,newline,newline)
        if(sys.version[0:2] == '2.'):
            val = str(raw_input(str1))
        else:
            val = str(input(str1))
        if val.lower() == "y":
            str2 = "%sData has been successfully copied to object store. Started extraction of batch = %s "%(newline,batch)
            common.print_and_log(str2)
        elif val.lower() == "n":
            sys.exit("%sPlease copy data of Migration_Data folder to object store before proceeding."%newline)
        else:
            sys.exit("%sEnter correct input value. Supported values are Y(Yes) and N(No)."%newline)

    str1= "%sMigration utility will run for batch = %s and iq_tables_Batch_%s.list"%(newline,batch,batch)
    common.print_and_log(str1)
    if batch > 1 :
        str1 = "%sDeleting already extracted data for tables in batch = %s and in file %s%siq_tables_Batch_%s.list"%(newline,batch-1,migrationpath,path_sep,batch-1)
        common.print_and_log(str1)

    delete_previous_extracted_batch_data()
    formlist_tobeunloaded(batch)
    extract_main(batch)
    str1 = "%sMigration completed for batch = %s. Please refer to file %s%sExtractedTables_Batch_%s.out"%(newline,batch,migrationpath,path_sep,batch)
    common.print_and_log(str1)
    # If the last batch is successfully, print summary for that
    if batch == count :
        extractedFailures_err_batch = "%s%sextractFailure_Batch_%s.err"%(migrationpath,path_sep,batch)
        if not os.path.isfile(extractedFailures_err_batch):
            print_batch_extraction_summary()
            combine_extracted_output(count)
            # If last batch is successfully extracted, then checking condition for proceed to load
            # check for number of failure backup files
            failure_backup_count = count_failure_backup_files()
            # If no failure backup files generated and no_extraction.list, all batches tables extracted successfully
            if not os.path.isfile('%s%sno_extraction.list'%(migrationpath,path_sep)) and failure_backup_count == 0:
                str1 = "%sAll the batches have been completed successfully.Please copy the last batch data and proceed to load."%newline
                common.print_and_log(str1)

# Function which will combine all batches extractedout into single file
def combine_extracted_output(count):
    batchnum = 1
    batches_extracted_list = list()
    combine_extractedTables_out = "%s%sExtractedTables.out"%(migrationpath,path_sep)
    while batchnum  <= count:
        extractedTables_out_btch="%s%sExtractedTables_Batch_%s.out"%(migrationpath,path_sep,batchnum)
        if os.path.isfile(extractedTables_out_btch):
            with codecs.open("%s"%(extractedTables_out_btch), "r", common.charset) as f:
                for line in f.readlines():
                    batches_extracted_list.append(line)

        batchnum = batchnum + 1

    with codecs.open("%s"%(combine_extractedTables_out), "w", common.charset) as f:
        for line in batches_extracted_list:
            f.write(line)

# Function which will make batches from failure files and no_extraction.list file
# It will also read the batch_size again, so if change, it will make batches according to that.
def failure_and_noextraction_file_batches():
    # Get all the table names already unloaded into a list
    table_already_unloaded = []

    # Get all the table names into a list
    original_iq_tablelist = []

    extract_file = codecs.open(extractedTables_out,'r', common.charset)
    lines = extract_file.readlines()
    for line in lines:
        line = line.rstrip(newline)
        splits = line.split(',')
        table_already_unloaded.append(splits[0])
    extract_file.close()

    iqtable_file = codecs.open(iqtables_list,'r', common.charset)
    lines = iqtable_file.readlines()
    for line in lines:
        line = line.rstrip(newline)
        splits = line.split(',')
        original_iq_tablelist.append(splits[0])
    iqtable_file.close()

    table_already_extracted_count = len(table_already_unloaded)
    original_iq_tablelist_count = len(original_iq_tablelist)

    print("%s"%(common.dividerline))
    print("%s tables out of %s tables already successfully extracted by previous Batch run of migration utility."%(table_already_extracted_count,original_iq_tablelist_count))

    delta = [item for item in original_iq_tablelist  if item not in table_already_unloaded]

    if not delta:
        logging.info("All IQ tables are already processed for data extraction")
    else:
        for i in delta:
            total_table.value = total_table.value + 1
            for line in lines:
                stripped_line = line.rstrip(newline)
                splits = stripped_line.split(',')
                if i == splits[0].strip() and (splits[1] != '0'):
                   if(splits[4]):
                      extract_list.append((splits[0], splits[1], splits[2], splits[3], splits[4]))
                   else:
                      extract_list.append((splits[0], splits[1], splits[2], splits[3] ,"BASE"))

    # remove the no_extraction.list if it exist
    if os.path.isfile(no_extraction_file):
        os.remove(no_extraction_file)

    # delete all the failure backup files
    failed_list = fnmatch.filter(os.listdir(migrationpath), 'extractFailure_Batch_*.err.bk')
    for i in failed_list:
        os.remove('%s%s%s'%(migrationpath,path_sep,i))

    generate_batches(extract_list,'failed')

# Main function which runs the utility
if __name__ == '__main__':
    # detect the current working directory and print it
    path = os.getcwd()
    print ("The current working directory: %s%s" % (newline,path))
    print("%s"%(common.dividerline))
    total_strt = datetime.datetime.now()
    global batch

    if onlyschema == 'y':
        # Schema-only execution
        get_inputs(config_file)
        mpx_verify(connectstr)
        version_verify(connectstr)
        charset_verify()
        get_byteorder()
        if dbmode == 'r':
            db_readonly_verify(connectstr)
        schema_unload_and_modify(connectstr)

    elif onlydata == 'y':
        # Data-only execution (ask user if schema is already unloaded)
        confirmation = input("Have you already unloaded the schema? (yes/no): ").strip().lower()
        if confirmation != 'yes':
            sys.exit("Error: Please unload schema first before running data-only mode.")

        get_inputs(config_file)
        check_create_dataextractdir()

        #global batch
        batch = 0
        if common.batch_size != 0:
            batch = 1
        check_and_create_iq_tables_file(connectstr)

        if batch != 0:
            extracted_batch_file_exist()
        else:
            get_unload_table_list(batch)
            extract_main(batch)

    elif fullextraction == 'y':
        # Full extraction mode (schema + data)
        get_inputs(config_file)
        mpx_verify(connectstr)
        version_verify(connectstr)
        charset_verify()
        get_byteorder()
        if dbmode == 'r':
            db_readonly_verify(connectstr)

        check_create_dataextractdir()
        schema_unload_and_modify(connectstr)

        #global batch
        batch = 0
        if common.batch_size != 0:
            batch = 1
        check_and_create_iq_tables_file(connectstr)

        if batch != 0:
            extracted_batch_file_exist()
        else:
            get_unload_table_list(batch)
            extract_main(batch)

    print("%s"%(common.dividerline))
    print("Migration path : %s%s"%(newline,migrationpath))
    print("%s"%(common.dividerline))
    if fullextraction == 'y' or onlydata == 'y' :  
       print("Data Extraction path : %s%s"%(newline,datapath))
       print("%s"%(common.dividerline))
    print("Migration Utility completed. %sPlease check file for details :%s%s"%(newline,newline,migration_log))
    print("%s"%(common.dividerline))
    total_elaptime = common.elap_time(total_strt)
    days, hours, minutes, seconds = common.calculate_time(total_elaptime)
    if onlyschema == 'y' :
        logging.info("%s"%(common.double_divider_line))
        logging.info("Next Steps:%s%s1. Run load utility (load_schema_and_data.py) for schema load."%(newline,newline))
        logging.info("%sSample command to run load utility for schema load :"%newline)
        logging.info("%spython3 load_schema_and_data.py --config_file <config file path> --onlyschema y"%newline)
        logging.info("%s"%(common.double_divider_line))
    logging.info("Total Time taken in migration utility : %s%d days, %d hours, %d minutes and %d seconds" % ( newline, days[0], hours[0], minutes[0], seconds[0]))
    print("Total Time taken in migration utility : %s%d days, %d hours, %d minutes and %d seconds" % ( newline, days[0], hours[0], minutes[0], seconds[0]))
    logging.info("%s"%(common.dividerline))
    print("%s"%(common.dividerline))
