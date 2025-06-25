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
import getpass
import time
import datetime
import os,re,socket
import csv
import logging
import json
import sys,getopt
from sys import byteorder
import platform
import psutil
import sys
argv = sys.argv[1:]

# total arguments passed
n = len(sys.argv)
if not(n==3 or n==2):
    sys.exit("Error: Incorrect/Invalid number of arguments. Run pre_migration.py -h or --help for help")

try:
    opts, args = getopt.getopt(argv,"ht:f:",["help","config_file="])
except getopt.GetoptError:
    print ('Error : Unsupported option/values. Run pre_migration.py -h or --help for help')
    sys.exit(2)
for opt, arg in opts:
    if opt in ("-h", "--help"):
        print ('usage:\npre_migration.py --config_file <config file path>')
        print ('which is the same as:\npre_migration.py -f <config file path>')
        print ('Switch --config_file or -f denote utilizing the config file to access parameters from.')
        sys.exit()
    elif opt in ("-f", "--config_file"):
        config_file = arg


def setup_logger(logger_name, log_file, level=logging.INFO):
    l = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(message)s')
    fileHandler = logging.FileHandler(log_file, mode='w')
    fileHandler.setFormatter(formatter)
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)

    l.setLevel(level)
    l.addHandler(fileHandler)

# Read the config.json file and get all values
def get_inputs(config_file):
    strt = datetime.datetime.now()

    print ('Reading Config File: %s' %(config_file))
    print("%s"%(common.dividerline))

    # Opening JSON file
    log_file.info("\nReading and Verification of config file : \n%s started"%(config_file))
    log_file.info("\n%s"%(common.dividerline))

    common.get_inputs(config_file,'pre_migration')
    common.host_validation(config_file,'pre_migration')
    common.premig_inputs(config_file,'pre_migration')
    log_file.info("IQ Server Hostname : %s\n"%(common.hostname))
    log_file.info("Client Hostname : %s , ip-address : %s , full-hostname : %s\n"%(common.host,common.ipaddress,common.fullhostname))

    log_file.info("Python version: %s\n"%sys.version)
    global conn
    conn = common.conn

    elap_sec = common.elap_time(strt)
    log_file.info("%s"%(common.dividerline))
    log_file.info("Reading and Verification of config file : \n%s completed in %s seconds"%(config_file,elap_sec))

def get_size(bytes, suffix="B"):
    """
    Scale bytes to its proper format
    e.g:
        1253656 => '1.20MB'
        1253656678 => '1.17GB'
    """
    factor = 1024
    for unit in ["", "K", "M", "G", "T", "P"]:
        if bytes < factor:
            return "{:.2f}{}{}".format(bytes, unit, suffix)
        bytes /= factor


# Verify the dbspaces present in DB.
def dbspace_verify(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of the dbspaces present in DB started.")

    cursor = conn.cursor()

    # dbspace verification
    cursor.execute("Set temporary  Option String_rtruncation = 'off'")
    cursor.execute("select count(*) from sp_iqdbspace() where DBSpaceType='MAIN';")
    count = cursor.fetchone()[0]
    if count > 1:
        features_list.append(('Multiple_DBSpaces', f'To be merged into one dbspace.'))
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of the dbspaces present in DB completed in %s seconds"%(elap_sec))

def output_logging(str1,str2):
    fixed_str = "{:<40} {:<10}".format(str('%s')%(str1),str('%s')%(str2))
    out_file.info(fixed_str)

# Calculate the sum of size of all IQ tables.
def dbspacesize_verify(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of the dbspace size started.")

    output_logging('IQ_SYSTEM_MAIN DBSPACE USAGE','%s GB'%(round(systemDbUsage,3)))

    output_logging('USER DBSPACES USAGE','%s GB'%(round(userDbUsage,3)))

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of the dbspace size completed in %s seconds"%(elap_sec))

# Calculate all user dbspace usage and store that in pre_migration_tablelist
def dbtables_size_calc(conn, dbspace):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nCalculation of all IQ table size and storing that in pre_migration_tablelist started for %s dbspace."%dbspace)

    print ("Calculating %s dbspace usage"%dbspace)
    cursor = conn.cursor()
    cursor.execute("set temporary option STRING_RTRUNCATION = 'off'")
    if dbspace == 'main':
       cursor.execute("select Usage, totalSize from sp_iqdbspace() where DBSpaceType = 'MAIN' and DBSpaceName = 'IQ_SYSTEM_MAIN'")
    elif dbspace == 'user':
       cursor.execute("select Usage, totalSize from sp_iqdbspace() where DBSpaceType = 'MAIN' and DBSpaceName != 'IQ_SYSTEM_MAIN'")
    elif dbspace == 'temp':
       cursor.execute("select Usage, totalSize from sp_iqdbspace() where DBSpaceType = 'TEMPORARY' and DBSpaceName = 'IQ_SYSTEM_TEMP'")
    elif dbspace == 'sharedtemp':
       cursor.execute("select Usage, totalSize from sp_iqdbspace() where DBSpaceType = 'SHARED_TEMP' and DBSpaceName = 'IQ_SHARED_TEMP'")
    else:
       return 0.0

    records = cursor.fetchall()

    #global dbsize
    dbsize = 0.0
    for row in records:
        usage = row[0]
        dbspacerSize = row[1]
        unit = dbspacerSize[-1]
        sizeStr = dbspacerSize[:-1]
        size = float(sizeStr)
        if unit == 'B':
           size = size / (1024*1024*1024)
        elif unit == 'K':
           size = size / (1024*1024)
        elif unit == 'M':
           size = size / 1024
        elif unit == 'G':
           size = size
        elif unit == 'T':
           size = size * 1024
        elif unit == 'P':
           size = size * 1024 *1024
        dbsize = dbsize + size*float(usage)/100
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nCalculation of all IQ table size and \nstoring that in pre_migration_tablelist for %s dbspace completed in %s seconds"%(dbspace,elap_sec))
    return dbsize

# Fetch database properties
def dbproperties_verify(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nFetching of database properties started.")

    cursor = conn.cursor()
    cursor.execute("set temporary option STRING_RTRUNCATION = 'off'")

    # Server Type and No. of writers
    cursor.execute("select count(*) from sp_iqmpxinfo()")
    server_count = cursor.fetchone()[0]
    if server_count > 0 :
        output_logging('SERVER TYPE', 'MULTIPLEX') 
        cursor.execute("select count(*) from sp_iqmpxinfo() where role='writer'")
        wcount = cursor.fetchone()[0]
        output_logging('MPX WRITERS PRESENT', '%s'%(wcount))
    else:
        output_logging('SERVER TYPE', 'SIMPLEX')

    # Database CHAR Collation check
    cursor.execute("select db_property('Collation');")
    collation = cursor.fetchone()[0]
    output_logging('DB CHAR COLLATION','%s'%(collation))

    # CaseSensitivity check
    cursor.execute("select CASE DB_PROPERTY('caseSensitive') WHEN 'Off' THEN 'ignore' WHEN 'On' THEN 'respect' ELSE '' END")
    caseSensitive = cursor.fetchone()[0]
    output_logging('CASESENSITIVE','%s'%(caseSensitive))

    # Blank Padding check
    #cursor.execute("select trim( db_property( 'BlankPadding' ));")
    #blankpadding = cursor.fetchone()[0]
    #output_logging('BLANK PADDING','%s'%(blankpadding))

    # Database Encryption check
    #cursor.execute("select db_property('Encryption');")
    #encryption = cursor.fetchone()[0]
    #output_logging('DB Encryption','%s'%(encryption))

    # IQ Page Size check
    #cursor.execute("select substring(Value, 1,charindex('/', Value)-1) from  sp_iqstatus() where Name like '%Page Size%';")
    #iqpagesize = cursor.fetchone()[0]
    #output_logging('IQ PAGE SIZE','%s BYTES'%(iqpagesize))

    # Endianess check
    #output_logging('ENDIANESS','%s'%(byteorder))

    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nFetching of database properties completed in %s seconds"%(elap_sec))

# Verify if RLV is enabled
def rlv_verify(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of RLV enablement started.")

    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM SYS.SYSTABLE t JOIN SYS.SYSIQTAB it ON (t.table_id = it.table_id) WHERE it.is_rlv = 'T'")
    rlv_table_count = cursor.fetchone()[0]

    cursor.execute("select count(*) from sp_iqdbspace() where DBSpaceType = 'RLV';")
    rlv_dbspace_count = cursor.fetchone()[0]

    if (rlv_table_count != 0) or (rlv_dbspace_count != 0) :
        action_required_list.append(('RLV_Support', f'RLV is not supported in SAP HANA Cloud, Data Lake Relational Engine.'))
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of RLV enablement completed in %s seconds"%(elap_sec))

# Check current IQ version
def verify_iq_version(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of current IQ version started.")

    cursor = conn.cursor()

    # IQ version verification
    cursor.execute("select @@version")
    ver = cursor.fetchone()[0]
    version = re.search('16.1', ver)
    if not version:
        action_required_list.append(('Current_IQ_Version not 16.1', f'Upgrade required as Paralellization in extraction is not supported in below IQ versions.'))
    #if 'SAP IQ/16.0.' in ver:
        #Document link needs to be updated with HDLFS not supported for IQ below 16.0 version
        #features_list.append(('Current_IQ_Version', f'Not Supported with HDLFS, Upgrade to 16.1_SP01 required.'))
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of current IQ version completed in %s seconds"%(elap_sec))

#Check if HSR feature is enabled
def verify_HSR(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of Hana System Replication feature enablement started.")

    cursor = conn.cursor()

    # HSR Enabled or not
    cursor.execute("select db_property('ReplicationEnabled')")
    enabled = cursor.fetchone()[0]
    if enabled == "Yes":
        action_required_list.append(('HSR_Enabled', f'To be disabled.'))
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of Hana System Replication feature completed in %s seconds"%(elap_sec))

# Check if Encryption enabled in on-prem database
def verify_encryptiondb(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of On-prem Database Encryption started.")

    cursor = conn.cursor()
    cursor.execute("select db_property('Encryption')")
    encryptiondb = cursor.fetchone()[0]
    if encryptiondb != 'None':
        # Document link need to be updated with this information
        features_list.append(('DB_Encryption Enabled', f'On-prem encryption key will not work in SAP HANA Cloud, Data Lake Relational Engine.'))
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of On-prem Database Encryption completed in %s seconds"%(elap_sec))

#Check if SAP unsupported remote sources present
def verify_sap_supported_remote_services(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of SAP supported remote sources started.")
    supported_list = ['aseodbc','hanaodbc','iqodbc','saodbc']
    cursor = conn.cursor()
    cursor.execute("select srvclass from sysserver")
    rows = cursor.fetchall()
    for i in rows:
        if i[0] not in supported_list:
            features_list.append(('Non-SAP remote source', f'HDLRE does not have access to clients for non-SAP data sources.'))
        elif i[0] in supported_list:
            action_required_list.append(('SAP remote source', f'Automated migration of SAP(ASE, HANA, IQ, SQL Anywhere) remote sources is not supported.'))
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of SAP supported remote sources completed in %s seconds"%(elap_sec))

# Check if external UDFs are used
def external_udf_verify(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of external udf started.")

    cursor = conn.cursor()
    cursor.execute("select count(*) from sys.sysprocedure where proc_defn like '%external name%';")
    count = cursor.fetchone()[0]
    if count != 0:
        action_required_list.append(('External_UDF', f'External UDFs not supported in SAP HANA Cloud, Data Lake Relational Engine.'))
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of external udf completed in %s seconds"%(elap_sec))

# Check if reserved data lake Relational Engine users are in use (saptu, sapsupport, custadmin, hdladmin )
def username_verify(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of username started.")

    cursor = conn.cursor()
    cursor.execute("select count(*) from sys.sysuser where (user_name='saptu') OR (user_name='sapsupport') OR (user_name='custadmin') OR (user_name='hdladmin')")
    count = cursor.fetchone()[0]
    if count != 0:
        # Document needs to be updated for reserved users
        features_list.append(('Reserved_User_Names', f'Reserved usernames(saptu, sapsupport, custadmin, hdladmin) to be deleted/renamed \n\t\t\t\t\t as not supported in SAP HANA Cloud, Data Lake Relational Engine.'))
    cursor.execute("select count(*) from sys.sysuser where (user_name='DBA')")
    count = cursor.fetchone()[0]
    if count != 0:
        features_list.append(('DBA_user', f'To be replaced by \"hdladmin\".'))
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of username completed in %s seconds"%(elap_sec))

# Check for security certificates in use
def certificates_verify(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of security certificates started.")

    cursor = conn.cursor()
    cursor.execute("select count(*) from SYS.SYSCERTIFICATE")
    count = cursor.fetchone()[0]
    if count != 0:
        features_list.append(('Certificates', f'Security Certificates to be deleted as not supported in SAP HANA Cloud, Data Lake Relational Engine..'))
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of security certificates completed in %s seconds"%(elap_sec))


# External Environments
def externalenv_verify(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of external environments started.")

    cursor = conn.cursor()
    cursor.execute("select count(*) from SYS.SYSEXTERNENV")
    count = cursor.fetchone()[0]
    if count != 0:
        action_required_list.append(('External_Environment', f'External Environment not supported in SAP HANA Cloud, Data Lake Relational Engine.'))
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of external environments completed in %s seconds"%(elap_sec))

# External Environments objects
def externalenvobj_verify(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of external environment objects started.")

    cursor = conn.cursor()
    cursor.execute("select count(*) from SYS.SYSEXTERNENVOBJECT")
    count = cursor.fetchone()[0]
    if count != 0:
        action_required_list.append(('External_Environment_Objects', f'External Environment Objects not supported in SAP HANA Cloud, Data Lake Relational Engine.'))
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of external environment objects completed in %s seconds"%(elap_sec))

# SPATIAL UNIT OF MEASURE
def spatialunit_verify(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of spatial unit started.")

    cursor = conn.cursor()
    cursor.execute("select count(*) from SYS.SYSSPATIALREFERENCESYSTEM")
    count = cursor.fetchone()[0]
    if count != 0:
        action_required_list.append(('Spatial_Unit_Of_Measure', f'Geospatial features not supported in SAP HANA Cloud, Data Lake Relational Engine.'))
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of spatial unit completed in %s seconds"%(elap_sec))

# SQL Anywhere tables
def sa_tables_verify(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of SQL Anywhere tables started.")

    cursor = conn.cursor()
    cursor.execute("select count(*) from SYS.SYSTABLE JOIN SYS.SYSUSER ON user_id = creator WHERE user_name not in ('SYS','rs_systabgroup','SA_DEBUG','dbo') AND table_type = 'BASE' and server_type='SA';")
    count = cursor.fetchone()[0]
    if count != 0:
        action_required_list.append(('SQLA_Tables', f'SQLA Catalog Tables not supported in SAP HANA Cloud, Data Lake Relational Engine.'))
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of SQL Anywhere tables completed in %s seconds"%(elap_sec))

# LD INDEXES
def ldindex_verify(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of LD indexes started.")

    cursor = conn.cursor()
    cursor.execute("select count(*) from SYS.SYSINDEX where index_type='LD'")
    count = cursor.fetchone()[0]
    if count != 0:
        features_list.append(('LD_INDEXES', f'LD_Indexes not supported in SAP HANA Cloud, Data Lake Relational Engine.'))
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of LD indexes completed in %s seconds"%(elap_sec))

# LF INDEXES
def lfindex_verify(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of LF indexes  started.")

    cursor = conn.cursor()
    cursor.execute("select count(*) from SYS.SYSINDEX where index_type='LF'")
    count = cursor.fetchone()[0]
    if count != 0:
        features_list.append(('LF_INDEXES', f'LF indexes are considered obsolete and the use of \n\t\t\t\t\t default FP indexes is now recommended in SAP HANA Cloud, Data Lake Relational Engine.'))
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of LF indexes completed in %s seconds"%(elap_sec))


# HNG INDEXES
def hngindex_verify(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of HNG indexes  started.")

    cursor = conn.cursor()
    cursor.execute("select count(*) from SYS.SYSINDEX where index_type='HNG'")
    count = cursor.fetchone()[0]
    if count != 0:
        features_list.append(('HNG_INDEXES', f'HNG indexes are considered obsolete and the use of \n\t\t\t\t\t default FP indexes is now recommended in SAP HANA Cloud, Data Lake Relational Engine.'))
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of HNG indexes completed in %s seconds"%(elap_sec))

# Local items
def localitem_verify(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of local items started.")

    cursor = conn.cursor()
    cursor.execute("select count(*) from SYS.SYSIQFILE WHERE segment_type = 'Local'")
    count = cursor.fetchone()[0]
    if count != 0:
        features_list.append(('LOCAL_DBFILES', f'To be merged into one dbspace.'))
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of local items completed in %s seconds"%(elap_sec))

# Check if Logical servers are configured.
def logicalserver_verify(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of logical server started.")

    cursor = conn.cursor()
    cursor.execute("select count(*) from SYSIQLOGICALSERVER where ls_id > 10000")
    count = cursor.fetchone()[0]
    if count != 0:
        action_required_list.append(('Logical_Servers', f'Logical Servers not supported in SAP HANA Cloud, Data Lake Relational Engine.'))
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of logical server completed in %s seconds"%(elap_sec))

# Check if DQP is enabled and used.
def dqpenable_verify(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of DQP enablement started.")

    cursor = conn.cursor()
    cursor.execute("select count(*) from SYSOPTION where \"option\" like 'DQP_Enabled%' AND setting = 'ON'")
    count = cursor.fetchone()[0]
    if count != 0:
        features_list.append(('DQP_Enabled', f'Not Supported in SAP HANA Cloud, Data Lake Relational Engine.'))
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of DQP enablement completed in %s seconds"%(elap_sec))

# Check if IQ_SHARED_TEMP present or not
def sharedtemp_verify(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of IQ_SHARED_TEMP started.")

    cursor = conn.cursor()
    cursor.execute("select count(*) from sp_iqdbspace() where DBSpaceName = 'IQ_SHARED_TEMP'")
    count = cursor.fetchone()[0]
    if count != 0:
        # Document needs to be updated for IQ_SHARED_TEMP not supported on HDLRE
        features_list.append(('IQ_SHARED_TEMP', f'Not Supported in SAP HANA Cloud, Data Lake Relational Engine.'))
    cursor.close()

    fins = datetime.datetime.now()
    elap_sec = (fins - strt).total_seconds()
    log_file.info("\nVerification of IQ_SHARED_TEMP completed in %s seconds"%(elap_sec))

# Check if any CORE_Options set and to be disabled in data lake Relational Engine
def coreoptions_verify(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of CORE_Options started.")

    cursor = conn.cursor()
    cursor.execute("select \"option\" from SYSOPTION where \"option\" like 'CORE_Options%' AND setting = 'ON' ")
    rows = cursor.fetchall()
    for i in rows:
        # Document needs to be updated for CORE_OPTIONS not supported on HDLRE
        action_required_list.append((i[0], f'Not Supported in SAP HANA Cloud, Data Lake Relational Engine.'))
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of CORE_Options completed in %s seconds"%(elap_sec))

# Check if any MPX_options/MPX_test_options set and to be disabled in data lake Relational Engine
def mpxoptions_verify(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of MPX_options/MPX_test_options started.")

    cursor = conn.cursor()
    cursor.execute("""select \"option_name\" from sp_iqcheckoptions() where Option_name like 'MPX_options%' and User_name = 'PUBLIC'""")
    rows = cursor.fetchall()
    for i in rows:
        features_list.append(('MPX_options', f'MPX Options not supported in SAP HANA Cloud, Data Lake Relational Engine.'))

    cursor.execute("""select \"option_name\" from sp_iqcheckoptions() where Option_name like 'MPX_test_options%' and User_name = 'PUBLIC'""")
    rows = cursor.fetchall()
    for i in rows:
        features_list.append(('MPX_test_options', f'MPX Test Options not supported in SAP HANA Cloud, Data Lake Relational Engine.'))
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of MPX_options/MPX_test_options completed in %s seconds"%(elap_sec))

# Check if Temp_Extract_Directory is set and used.
def tempextractdir_verify(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of Temp_Extract_Directory option started.")

    cursor = conn.cursor()
    cursor.execute("select count(*) from SYSOPTION where \"option\" like 'Temp_Extract_Directory%' AND setting != ''")
    count = cursor.fetchone()[0]
    if count != 0:
        features_list.append(('Temp_Extract_Directory', f'It would be reset as it should be object store path in SAP HANA Cloud, Data Lake Relational Engine.'))
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of Temp_Extract_Directory option completed in %s seconds"%(elap_sec))

#Verification for Webservices
def webservice_verify(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of webservice started.")

    cursor = conn.cursor()
    cursor.execute("select count(*) from SYS.SYSWEBSERVICE")
    count = cursor.fetchone()[0]
    if count != 0:
        features_list.append(('Web_Services', f'Web Services not supported in SAP HANA Cloud, Data Lake Relational Engine.'))
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of webservice completed in %s seconds"%(elap_sec))

# Gather all data lake Relational Engine unsupported SQL Anywhere server options
def sa_server_options(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of not-settable customer_dbopts started.")

    path = os.getcwd()
    PATH = '%s%s..%sCommon%sdbopts_noncustomer.csv'%(pwd,path_sep,path_sep,path_sep)
    if not(os.path.isfile(PATH) and os.access(PATH, os.R_OK)):
        sys.exit("dbopts_noncustomer.csv file does not exists in %s directory"%PATH)

    l = list()
    # Add dbopts_noncustomer.csv for comment in list
    with open("%s%s..%sCommon%sdbopts_noncustomer.csv"%(pwd,path_sep,path_sep,path_sep), "r") as f:
        for line in f.readlines():
            line = line.rstrip('\n')
            l.append((line))

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of not-settable customer_dbopts completed in %s seconds"%(elap_sec))

    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of SQL Anywhere server options started.")

    t = list()
    cursor = conn.cursor()
    cursor.execute("select \"option_name\" from sp_iqcheckoptions() where sp_iqcheckoptions.Option_type = 'Permanent'")
    rows = cursor.fetchall()
    for i in rows:
        t.append(i[0])
    cursor.close()

    # Remove duplicates from list
    t = list(set(t))

    # take common elements of sp_iqcheckoptions() list and dbopts_noncustomer.csv list
    if (set(t) & set(l)):
        t = set(t) & set(l)

    for i in t:
        features_list.append((i, f'HDL unsupport SQL Anywhere server options.'))

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of SQL Anywhere server options completed in %s seconds"%(elap_sec))

# Gather all data lake Relational Engine unsupported IQ server options
def iq_server_options(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of not-settable customer_hosparams started.")

    path = os.getcwd()
    PATH = '%s%s..%sCommon%shosparams_noncustomer.csv'%(pwd,path_sep,path_sep,path_sep)
    if not(os.path.isfile(PATH) and os.access(PATH, os.R_OK)):
        sys.exit("hosparams_noncustomer.csv file does not exists in %s directory"%PATH)

    l = list()
    # Add dbopts_noncustomer.csv for comment in list
    with open("%s%s..%sCommon%shosparams_noncustomer.csv"%(pwd,path_sep,path_sep,path_sep), "r") as f:
        for line in f.readlines():
            line = line.rstrip('\n')
            l.append((line))

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of not-settable customer_hosparams completed in %s seconds"%(elap_sec))

    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of IQ server options started.")

    cursor = conn.cursor()
    t = list()
    cursor = conn.cursor()
    cursor.execute("select \"option_name\" from sp_iqcheckoptions() where sp_iqcheckoptions.Option_type = 'Permanent'")
    rows = cursor.fetchall()
    for i in rows:
        t.append(i[0])
    cursor.close()

    # Remove duplicates from list
    t = list(set(t))

    # take common elements of sp_iqcheckoptions() list and dbopts_noncustomer.csv list
    if (set(t) & set(l)):
        t = set(t) & set(l)

    for i in t:
        features_list.append((i, f'HDL unsupport IQ server options.'))

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of IQ server options completed in %s seconds"%(elap_sec))

# Function to find MPX readers present in on-prem
def verify_readers_present(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of presence of MPX readers started.")

    cursor = conn.cursor()
    cursor.execute("select count(*) from sp_iqmpxinfo() where role='reader'")
    count = cursor.fetchone()[0]
    if count > 0:
        # Document need to be updated with MPX readers need to be replaced with MPX writers on HDLRE
        features_list.append(('MPX_Readers_Present', f'All HDLRE worker nodes are provisioned as Writer nodes.'))
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of presence of MPX readers completed in %s seconds"%(elap_sec))

# Function to find -gm value and report warning for IQ Startup options
def verify_startup_options(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of IQ startup options started.")

    cursor = conn.cursor()
    cursor.execute("select db_property('MaxConnections')")
    MaxConnections = cursor.fetchone()[0]

    # Document needs to be updated to mention On-prem IQ startup options will be replaced based on t-shirt sizes on HDLRE
    features_list.append(('StartupOptions', f'HDLRE startup options are set based on node t-shirt size.'))
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of IQ startup options completed in %s seconds"%(elap_sec))

# Prints System Configuration Information 
def system_configuration():
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nSystem Configuration Information started.")

    #print("System Configuration Information")
    #print("%s"%(common.dividerline))
    #out_file.info("%s"%(common.double_divider_line))
    #out_file.info(" ******** System Information ********")
    #out_file.info("%s"%(common.double_divider_line))
    #uname = platform.uname()
    #output_logging('System', '%s' % (uname.system))
    #output_logging('Node Name', '%s' % (uname.node))
    #output_logging('Release', '%s' % (uname.release))
    #output_logging('Version', '%s' % (uname.version))
    #output_logging('Machine', '%s' % (uname.machine))
    #output_logging('Processor', '%s' % (uname.processor))

    
    # let's out_file.info CPU information
    out_file.info("%s"%(common.double_divider_line))
    out_file.info(" ******** CPU Information ********")
    out_file.info("%s"%(common.double_divider_line))
    # Number of cores
    output_logging('Physical cores', '%s' % (psutil.cpu_count(logical=False)))
    output_logging('Total cores', '%s' % (psutil.cpu_count(logical=True)))

    # CPU frequencies
    #cpufreq = psutil.cpu_freq()
    #output_logging('Max Frequency', '%.2f Mhz' % (cpufreq.max))
    #output_logging('Min Frequency', '%.2f Mhz' % (cpufreq.min))
    #output_logging('Current Frequency', '%.2f Mhz' % (cpufreq.current))

    # CPU usage
    #output_logging('Total CPU Usage', '%.2f%%' % (psutil.cpu_percent()))

    # Memory Information
    out_file.info("%s"%(common.double_divider_line))
    out_file.info(" ******** Memory Information ********")
    out_file.info("%s"%(common.double_divider_line))
    # get the memory details
    svmem = psutil.virtual_memory()
    output_logging('Total', '%s' % (get_size(svmem.total)))
    output_logging('Available', '%s' % (get_size(svmem.available)))
    output_logging('Used', '%s' % (get_size(svmem.used)))
    output_logging('Percentage', '%.2f%%' % (svmem.percent))
    out_file.info("%s\n"%(common.dividerline))

    elap_sec = common.elap_time(strt)
    log_file.info("\nSystem Configuration Information completed in %s seconds"%(elap_sec))

# Prints Recommended HDLRE Configuration Settings
def hdlre_settings():
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nRecommending SAP HANA Cloud, Data Lake Relational Engine Configuration Settings started.")

    print("Recommended SAP HANA Cloud, Data Lake Relational Engine Configuration Settings")
    print("%s"%(common.dividerline))
    out_file.info("%s"%(common.double_divider_line))
    out_file.info(" ******** Recommended SAP HANA Cloud, Data Lake Relational Engine Configuration Settings ********")
    out_file.info("%s"%(common.double_divider_line))
    out_file.info("Configure to be most compatible with SAP IQ")
    out_file.info("%s"%(common.double_divider_line))
    output_logging('Properties','Value')
    out_file.info("%s"%(common.double_divider_line))
    output_logging('Collation','UTF8BIN')
    output_logging('CaseSensitivity','RESPECT')
    out_file.info("%s\n"%(common.dividerline))

    elap_sec = common.elap_time(strt)
    log_file.info("\nRecommending SAP HANA Cloud, Data Lake Relational Engine Configuration Settings completed in %s seconds"%(elap_sec))

# Prints all Server and dbspace property information used in current system in log file
def server_properties(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification server properties and dbspace properties started.")

    print("%s"%(common.dividerline))
    print("Verifying server properties and dbspace properties")
    print("%s"%(common.dividerline))
    out_file.info("%s"%(common.double_divider_line))
    out_file.info(" ******** Server Properties and dbspace Properties ********")
    out_file.info("%s"%(common.double_divider_line))
    output_logging('Properties','Value')
    out_file.info("%s"%(common.double_divider_line))
    dbproperties_verify(conn)
    dbspacesize_verify(conn)
    out_file.info("%s"%(common.dividerline))

    elap_sec = common.elap_time(strt)
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of server properties and dbspace properties completed in %s seconds"%(elap_sec))

# Gather and prints all unsupported features in current IQ DB system.
def feature_properties(conn):
    dbspace_verify(conn)
    rlv_verify(conn)
    verify_iq_version(conn)
    verify_encryptiondb(conn)
    verify_sap_supported_remote_services(conn)
    external_udf_verify(conn)
    username_verify(conn)
    certificates_verify(conn)
    externalenv_verify(conn)
    externalenvobj_verify(conn)
    spatialunit_verify(conn)
    sa_tables_verify(conn)
    lfindex_verify(conn)
    hngindex_verify(conn)
    localitem_verify(conn)
    logicalserver_verify(conn)
    dqpenable_verify(conn)
    sharedtemp_verify(conn)
    webservice_verify(conn)
    verify_readers_present(conn)
    verify_startup_options(conn)
    print("Verifying Incompatible features and properties")
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification incompatible features and properties started.")

    out_file.info("\n%s"%(common.double_divider_line))
    url = "https://help.sap.com/docs/SAP_HANA_DATA_LAKE/f70c0f7e072d4f969d3c8b4bc95b4214/18253c30c28a4407a61592b9067b56b8.html"

    out_file.info(f"For detailed information on the features that are changed or no longer supported,")
    out_file.info(f"See this topic: ({url})")
    out_file.info("%s"%(common.dividerline))

    out_file.info("\n%s"%(common.double_divider_line))
    out_file.info(" ******** Informational Messages ********")
    out_file.info("%s"%(common.double_divider_line))

    fixed_str = "{:<40} {:<15}".format( 'Feature', 'Comment')
    out_file.info(fixed_str)
    out_file.info("%s"%(common.double_divider_line))
    for row in features_list:
        fixed_str = "{:<40} {:<15}".format(str(row[0]), str(row[1]))
        out_file.info(fixed_str)
        out_file.info("%s"%(common.dividerline))

    sa_server_options(conn)
    iq_server_options(conn)
    mpxoptions_verify(conn)
    tempextractdir_verify(conn)

    out_file.info("\n%s"%(common.double_divider_line))
    out_file.info(" ******** Customer Action Required ********")
    out_file.info("%s"%(common.double_divider_line))

    fixed_str = "{:<40} {:<15}".format( 'Feature', 'Comment')
    out_file.info(fixed_str)
    out_file.info("%s"%(common.double_divider_line))

    for row in action_required_list:
        fixed_str = "{:<40} {:<20}".format(str(row[0]), str(row[1]))
        out_file.info(fixed_str)
        out_file.info("%s"%(common.dividerline))

    out_file.info("\n%s"%(common.dividerline))
    out_file.info( "WARNING : Try to fix Incompatibilities else they will be auto deleted/modified in the Migration phase")
    out_file.info("%s"%(common.dividerline))

    elap_sec = common.elap_time(strt)
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of incompatible features and properties completed in %s seconds"%(elap_sec))


# detect the current working directory and print it
pwd = os.getcwd()
print ("The current working directory: %s" % pwd)
global path_sep
if(platform.system() == "Windows"):
    path_sep = "\\"
else:
    path_sep = "/"
sys.path.insert(0, '%s%s..%sCommon%s'%(pwd,path_sep,path_sep,path_sep))
import common

#setup logger for pre_migration.out
print("%s"%(common.dividerline))
premigration_out = "%s%spre_migration.out"%(pwd,path_sep)
setup_logger('out_file',premigration_out)
out_file = logging.getLogger('out_file')

#setup logger for pre_migration.log
premigration_log = "%s%spre_migration.log"%(pwd,path_sep)
setup_logger('log_file',premigration_log)
log_file = logging.getLogger('log_file')

log_file.info("\n******************************************************************")
log_file.info("[%s] : Pre-Migration Verification started."%(datetime.datetime.now()))
log_file.info("******************************************************************")

# Read config file
get_inputs(config_file)

start_time = datetime.datetime.now()

system_configuration()

# calcaulate table sizes for all IQ tables before creating any tables for
# pre_migration table.
global systemDbUsage
systemDbUsage = dbtables_size_calc(conn,'main')
global userDbUsage
userDbUsage = dbtables_size_calc(conn, 'user')
global tempDbUsage
tempDbUsage = dbtables_size_calc(conn, 'temp')
global sharedTempDbUsage
sharedTempDbUsage = dbtables_size_calc(conn, 'sharedtemp')
action_required_list = list()
features_list = list()

server_properties(conn)
feature_properties(conn)

conn.close()

elap_sec_time = common.elap_time(start_time)
log_file.info("\n%s"%(common.dividerline))
log_file.info("\nPre-Migration Verification completed in %s seconds"%(elap_sec_time))
log_file.info("\n%s"%(common.dividerline))

print("%s"%(common.dividerline))
print("Pre-Migration utility logs are present at: \n%s"%(premigration_log))
print("%s"%(common.dividerline))
print("Pre-Migration Verification complete. Please check :\n%s file for details"%(premigration_out))
print("%s"%(common.dividerline))
