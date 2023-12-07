# ----------------------------------------------------------------------
# @(#)Migration                      2021              SAP
# ----------------------------------------------------------------------
# Migration utilities to migrate SAP IQ on SAP datalake IQ.
# ----------------------------------------------------------------------
#
# ***************************************************************************
# Copyright (c) 2021 SAP SE or an SAP affiliate company. All rights reserved.
# ***************************************************************************
import getpass
import platform
import time
import os,re,socket
import logging
import json
import sys,getopt
import datetime
from sys import byteorder

global double_divider_line
double_divider_line = "=========================================================================================="

global dividerline
dividerline = "------------------------------------------------------------------------------------------"

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

def file_input(config_file,util):
    # Opening JSON file
    f = open('%s'%(config_file),)

    global config_valid_str
    config_valid_str = ""
    try:
        # returns JSON object as a dictionary
        global data
        data = json.load(f)
    except ValueError:
        sys.exit("%sInvalid json config file: %s. %sCorrect the file and rerun %s utility."%(newline,newline,config_file,util)) # in case json is invalid
    else:
        config_valid_str = "\nInput json config file: %s is valid" %(config_file) # in case json is valid

# Common function for printing a string in console and in log file
def print_and_log(string):
    print("%s"%(dividerline))
    logging.info("%s"%(dividerline))
    print("%s"%(string))
    logging.info("%s"%(string))

def elap_time(start):
    fins = datetime.datetime.now()
    elap_sec = (fins - start).total_seconds()
    return elap_sec

def calculate_time(elap_sec):
    days    = divmod(elap_sec, 86400)
    hours   = divmod(days[1], 3600)               # Use remainder of days to calc hours
    minutes = divmod(hours[1], 60)                # Use remainder of hours to calc minutes
    seconds = divmod(minutes[1], 1)               # Use remainder of minutes to calc seconds

    return days, hours, minutes, seconds

# Read the config.json file and get all values
def get_inputs(config_file,util):

    file_input(config_file,util)
    global hostname
    hostname = data['Host_Name']

    # checking if hostname is empty or just contain spaces
    if (not (hostname and hostname.strip())) :
        sys.exit("Please enter valid string value for Host_Name in %s file"%config_file)
    if (not (hostname and hostname.strip())) :
        sys.exit("Please enter valid string value for Host_Name in %s file"%config_file)

    global port
    port = data['Port_Number']

    if type(port) != int:
        sys.exit("Please enter integer value for Port_Number in %s file"%config_file)

    global userid
    userid = data['DBA_User']

    if (not (userid and userid.strip())) :
        sys.exit("Please enter valid string value for DBA_User in %s file"%config_file)

    global password
    if ('DBA_Pwd' in data) :
        password = data['DBA_Pwd']
        if (not (password and password.strip()) or password.startswith('<Optional')) :
            password = getpass.getpass("Enter DBA login password: ")
    else :
        password = getpass.getpass("Enter DBA login password: ")

    global enc_string
    enc_string = 'NONE'

    if ('ENC' in data):
        enc_string = data['ENC']
        if (enc_string.lower() == 'none' or not enc_string.strip() or enc_string.startswith('<Optional')):
            enc_string = 'None'

    global driver
    if ('IQ_Version' in data) :
        global version
        version = data['IQ_Version']
        if not version:
            sys.exit("Error: IQ_Version needs to be set in %s file"%config_file)
        if version == "16.1":
            driver = "libdbodbc17.so"
        elif version == "16.0":
            driver = "libdbodbc16.so"
        else:
            sys.exit("Error: Invalid IQ_Version in %s file. Supported values : [16.0 or 16.1]"%(config_file))

    global driv
    driv = platform.system()
    if driv == "Windows":
        driver = "Sybase IQ"

    global sybase_path
    if ('IQ_Server_Install_Path' in data):
        sybase_path = data['IQ_Server_Install_Path']
        if sybase_path.startswith('<Path'):
            sys.exit("Error: IQ_Server_Install_Path needs to be set in %s file"%config_file)

    global host
    global ipaddress
    global fullhostname
    host = socket.gethostname()
    ipaddress = socket.gethostbyname(host)
    fullhostname = socket.getfqdn(ipaddress)

def object_store_azure(config_file):

    global w
    w = list()
    w.append('Azure_Account_Name')
    w.append('Azure_Account_Key')
    w.append('Container_Name')

    global t
    t = list()
    for i in data['Hyperscaler_Details']['Credentials']:
        t.append(i)

    w.sort()
    t.sort()

    if not w == t:
        sys.exit("Hyperscaler_Details are not correct in in %s file"%config_file)

    global az_container_name

    global connection_string
    for i in data['Hyperscaler_Details']['Credentials']:
        value = data['Hyperscaler_Details']['Credentials'][i]
        if i == 'Azure_Account_Name':
            if (not (value and value.strip())) :
                sys.exit("Please enter valid string value for Azure_Account_Name in %s file"%config_file)
            else:
                az_account_name = data['Hyperscaler_Details']['Credentials'][i]
        elif i == 'Azure_Account_Key':
            if (not (value and value.strip())) :
                sys.exit("Please enter valid string value for Azure_Account_Key in %s file"%config_file)
            else:
                az_key = data['Hyperscaler_Details']['Credentials'][i]
        else:
            if (not (value and value.strip())) :
                sys.exit("Please enter valid string value for Container_Name in %s file"%config_file)
            else:
                az_container_name = data['Hyperscaler_Details']['Credentials'][i]
    connection_string = "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net"%(az_account_name,az_key)

def object_store_aws(config_file):

    global w
    w = list()
    w.append('AWS_Access_Key_Id')
    w.append('AWS_Secret_Access_Key')
    w.append('AWS_Region')
    w.append('Bucket_Name')

    global t
    t = list()
    for i in data['Hyperscaler_Details']['Credentials']:
        t.append(i)

    w.sort()
    t.sort()

    if not w == t:
        sys.exit("Hyperscaler_Details are not correct in in %s file"%config_file)

    global aws_access_key
    global aws_secret_key
    global aws_region
    global aws_bucket
    for i in data['Hyperscaler_Details']['Credentials']:
        value = data['Hyperscaler_Details']['Credentials'][i]
        if i == 'AWS_Access_Key_Id':
            if (not (value and value.strip())) :
                sys.exit("Please enter valid string value for AWS_Access_Key_Id in %s file"%config_file)
            else:
                aws_access_key = data['Hyperscaler_Details']['Credentials'][i]
        elif i == 'AWS_Secret_Access_Key':
            if (not (value and value.strip())) :
                sys.exit("Please enter valid string value for AWS_Secret_Access_Key in %s file"%config_file)
            else:
                aws_secret_key = data['Hyperscaler_Details']['Credentials'][i]
        elif i == 'AWS_Region':
            if (not (value and value.strip())) :
                sys.exit("Please enter valid string value for AWS_Region in %s file"%config_file)
            else:
                aws_region = data['Hyperscaler_Details']['Credentials'][i]
        else:
            if (not (value and value.strip())) :
                sys.exit("Please enter valid string value for Bucket_Name in %s file"%config_file)
            else:
                aws_bucket = data['Hyperscaler_Details']['Credentials'][i]

def host_validation(config_file,util):
    import pyodbc
    file_input(config_file,util)

    global same_host
    same_host = True
    if ('IQ_Server_On_Same_Host' in data):
        same_server_host = data['IQ_Server_On_Same_Host']
        if not same_server_host.lower() in ("yes","no"):
            sys.exit("Error: IQ_Server_On_Same_Host needs to be set either Yes/No in %s file"%config_file)

        if same_server_host.lower() == "no":
            same_host = False
        else:
            same_host = True

    global conn_str
    global conn
    global cursor
    global hostname
    global port
    global userid
    global password
    global enc_string
    global driver

    # establish db connection
    if same_host == False:
        try:
            if version == "16.0":
                driver = "libdbodbc17.so"
            conn_str = 'DRIVER={%s};host=%s:%i;UID=%s;PWD=%s;ENC=%s' % \
                (driver, hostname, port, userid, password, enc_string )
            conn = pyodbc.connect(conn_str, timeout=0)
        except Exception as exp:
            errno, strerror = exp.args
            if errno == "01000":
                sys.exit("Exception: %s.\nPlease source <Datalake_Client_Install_Path>/<IQ.sh/IQ.csh> and rerun %s.py"%str(strerror,util))
            else:
                sys.exit("Exception: %s"%str(strerror))
    else:
        try:
            conn_str = 'DRIVER={%s};host=%s:%i;UID=%s;PWD=%s;ENC=%s' % \
                (driver, hostname, port, userid, password, enc_string )
            conn = pyodbc.connect(conn_str, timeout=0)
        except Exception as exp:
            errno, strerror = exp.args
            if errno == "01000":
                sys.exit("Exception: %s.\nPlease source <IQ_Server_Install_Path>/<IQ.sh/IQ.csh> and rerun %s.py"%str(strerror,util))
            else:
                sys.exit("Exception: %s"%str(strerror))

    cursor = conn.cursor()

    cursor.execute("select @@servername")
    global server_name
    server_name = cursor.fetchone()[0]

    cursor.execute("select count(*) from SYS.SYSIQMPXSERVER")
    mpx_count = cursor.fetchone()[0]
    if mpx_count > 0:
        cursor.execute("select role from SYS.SYSIQMPXSERVER where server_name='%s'"%(server_name))
        role = cursor.fetchone()[0]
        #Role 0 indicated coordinator node
        if role != 0:
            sys.exit("Please enter the value of port_number as coordinator port in %s file"%config_file)
 
def mig_inputs(config_file,util):

    file_input(config_file,util)

    global same_host
    global shared_path
    shared_path = data['Extract_Path']

    if (not (shared_path and shared_path.strip())) :
        sys.exit("Please enter valid string value for Extract_Path in %s file"%config_file)

    if driv == "Windows" and shared_path.startswith("\\"):
        shared_path = "\{}".format(shared_path)

    global conn_num
    conn_num = data['Client_Num_Conn']

    if type(conn_num) != int or conn_num < 2:
        sys.exit("Please enter integer value greater than 1 for Client_Num_Conn in %s file"%config_file)

    global batch_size
    # Input of batch_size is in GB, convert into Bytes
    convert_to_bytes = 1024*1024*1024
    batch_size_config = data['Batch_Size_GB']
    if (str(batch_size_config).startswith('<Optional')):
        batch_size = 0
    else:
        batch_size = data['Batch_Size_GB']*convert_to_bytes

    batch_size_100GB = 100 * 1024* 1024 * 1024
    if type(batch_size) != int or ( batch_size < batch_size_100GB and batch_size != 0 ):
        sys.exit("Please enter integer value greater than or equal to 100GB for Batch_Size in %s file"%config_file)

    global object_store
    object_store = data['Hyperscaler_Details']['Name']

    if (not (object_store and object_store.strip())) :
        sys.exit("Please enter valid string value for Hyperscaler_Details in %s file"%config_file)
    # Validation of right hyperscaler names supported
    elif not object_store.lower() == 'azure' and not object_store.lower() == 'aws':
        sys.exit("Please select Hyperscaler_Details as Azure or AWS")

    if object_store.lower() == 'azure':
        object_store_azure(config_file)
    elif object_store.lower() == 'aws':
        object_store_aws(config_file)

    if same_host == False:
        global client_id
        client_id = data['IQ_Host_Login_Id']

        if (not (client_id and client_id.strip()) or client_id.startswith('<Optional')) :
            sys.exit("Please enter valid string value for IQ_Host_Login_Id in %s file"%config_file)

        global client_pwd
        if ('IQ_Host_Login_Pwd' in data) :
            client_pwd = data['IQ_Host_Login_Pwd']
            if (not (client_pwd and client_pwd.strip()) or client_pwd.startswith('<Optional')) :
                client_pwd = getpass.getpass("Enter IQ host login password: ")
        else :
            client_pwd = getpass.getpass("Enter IQ host login password: ")

    cursor = conn.cursor()

    cursor.execute("select db_name()")
    global dbname
    dbname = cursor.fetchone()[0]

    cursor.execute("select db_property('Collation');")
    global collation
    collation = cursor.fetchone()[0]

    cursor.execute("select trim( db_property( 'Charset' ));")
    global charset
    charset = cursor.fetchone()[0]
    if charset == "Extended_UNIX_Code_Packed_Format_for_Japanese":
        logging.info("Warning: Python doesn't support charset %s.%sSwitching to charset EUC-JP."%(charset,newline))
        charset = "EUC-JP"

    cursor.close()
    conn.close()

def load_inputs(config_file,util):

    import sqlanydb
    file_input(config_file,util)
    port = "443"
    global user
    try:
        user = data['HDLADMIN_User']
    except KeyError:
        sys.exit("Error: Please add HDLADMIN_User key in %s file"%config_file)

    if (not (user and user.strip())) or (user.strip().lower() != "hdladmin"):
        sys.exit("Error: Please enter valid string value for HDLADMIN_User in %s file"%config_file)

    global password
    if ('HDLADMIN_Pwd' in data) :
        password = data['HDLADMIN_Pwd']
        if (not (password and password.strip()) or password.startswith('<Optional')) :
            password = getpass.getpass("Enter HDLADMIN login password: ")
    else :
        password = getpass.getpass("Enter HDLADMIN login password: ")

    global coord_host
    try:
        coord_host = data['HDL_Coord_Endpoint']
    except KeyError:
        sys.exit("Error: Please add HDL_Coord_Endpoint key in %s file"%config_file)

    if (not (coord_host and coord_host.strip())) :
        sys.exit("Error: Please enter valid string value for HDL_Coord_Endpoint in %s file"%config_file)

    coord_host = data['HDL_Coord_Endpoint'] + ":" + port

    global writer_host
    try:
        writer_host = data['HDL_Writer_Endpoint']
    except KeyError:
        sys.exit("Error: Please add HDL_Writer_Endpoint key in %s file"%config_file)

    if (not (writer_host and writer_host.strip())) :
        sys.exit("Error: Please enter valid string value for HDL_Writer_Endpoint in %s file"%config_file)

    writer_host = data['HDL_Writer_Endpoint'] + ":" + port

    global writer_conn_num
    try:
        writer_conn_num = data['HDL_Num_Writer_Conn']
    except KeyError:
        sys.exit("Error: Please add HDL_Num_Writer_Conn key in %s file"%config_file)

    if type(writer_conn_num) != int or (writer_conn_num < 0):
        sys.exit("Error: Please enter positive integer value for HDL_Num_Writer_Conn in %s file"%config_file)

    # coord_conn_num is optional field
    global coord_conn_num
    try:
        coord_conn_num = data['HDL_Num_Coord_Conn']
        if (str(coord_conn_num).startswith('<Optional')):
            coord_conn_num = 0
        elif ((not coord_conn_num) or (coord_conn_num < 0) ):
            coord_conn_num = 0
    except KeyError:
        coord_conn_num = 0
        pass

    if type(coord_conn_num) != int:
        sys.exit("Error: Please enter positive integer value for HDL_Num_Coord_Conn in %s file"%config_file)

    global extract_path
    try:
        extract_path = data['Extract_Path']
    except KeyError:
        sys.exit("Error: Please add Extract_Path key in %s file"%config_file)

    if platform.system() == "Windows" and extract_path.startswith("\\"):
        extract_path = "\{}".format(extract_path)

    global Datalake_Client_Install_Path
    try:
        Datalake_Client_Install_Path = data['Datalake_Client_Install_Path']
    except ValueError:
        sys.exit("Error: Please add Datalake_Client_Install_Path Value in %s file"%config_file)
    if (not (Datalake_Client_Install_Path and Datalake_Client_Install_Path.strip())) :
        sys.exit("Please enter valid Datalake_Client_Install_Path in %s file"%config_file)

    global object_store
    object_store = data['Hyperscaler_Details']['Name']

    if (not (object_store and object_store.strip())) :
        sys.exit("Please enter valid string value for Hyperscaler_Details in %s file"%config_file)
    # Validation of right hyperscaler names supported
    elif not object_store.lower() == 'azure' and not object_store.lower() == 'aws':
        sys.exit("Please select Hyperscaler_Details as Azure or AWS")

    if object_store.lower() == 'azure':
        object_store_azure(config_file)
    elif object_store.lower() == 'aws':
        object_store_aws(config_file)

    global Object_Store_Copy_Validation
    try:
        Object_Store_Copy_Validation = data['Object_Store_Copy_Validation']
    except KeyError:
        sys.exit("Error: Please add Object_Store_Copy_Validation key in %s file"%config_file)

    global charset
    global conn
    ENC="tls(tls_type=rsa;direct=yes)"
    conn = sqlanydb.connect( uid=user, pwd=password, host=coord_host, enc=ENC )

    cursor = conn.cursor()
    cursor.execute("select trim( db_property( 'Charset' ));")
    charset = cursor.fetchone()[0]
    cursor.close()
    conn.close()
