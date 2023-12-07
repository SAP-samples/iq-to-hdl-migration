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
    log_file.info("IQ Server Hostname : %s\n"%(common.hostname))
    log_file.info("Client Hostname : %s , ip-address : %s , full-hostname : %s\n"%(common.host,common.ipaddress,common.fullhostname))

    log_file.info("Python version: %s\n"%sys.version)
    global conn
    conn = common.conn

    elap_sec = common.elap_time(strt)
    log_file.info("%s"%(common.dividerline))
    log_file.info("Reading and Verification of config file : \n%s completed in %s seconds"%(config_file,elap_sec))

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
        features_list.append(('Multiple_DBSpaces', 'Warning', 'To be merged into one dbspace'))
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

    output_logging('COORD TEMP DBSPACES USAGE','%s GB'%(round(tempDbUsage,3)))

    output_logging('SHARED TEMP DBSPACES USAGE','%s GB'%(round(sharedTempDbUsage,3)))

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
    # Page Size check
    cursor.execute("select db_property('PageSize');")
    pagesize = cursor.fetchone()[0]
    output_logging('PAGE SIZE','%s BYTES'%(pagesize))

    # Database CHAR Collation check
    cursor.execute("select db_property('Collation');")
    collation = cursor.fetchone()[0]
    output_logging('DB CHAR COLLATION','%s'%(collation))

    # CaseSensitivity check
    cursor.execute("select CASE DB_PROPERTY('caseSensitive') WHEN 'Off' THEN 'ignore' WHEN 'On' THEN 'respect' ELSE '' END")
    caseSensitive = cursor.fetchone()[0]
    output_logging('CASESENSITIVE','%s'%(caseSensitive))

    # NCHAR Collation check
    cursor.execute("select db_property('NCHARCollation');")
    ncharcollation = cursor.fetchone()[0]
    output_logging('NCHAR COLLATION','%s'%(ncharcollation))

    # Nchar CaseSensitivity check
    cursor.execute("select UPPER(DB_EXTENDED_PROPERTY( 'NcharCollation', 'CaseSensitivity'))")
    caseSensitivity = cursor.fetchone()[0]
    output_logging('NCHAR CASESENSITIVITY','%s'%(caseSensitivity))

    # Nchar AccentSensitivity check
    cursor.execute("select UPPER(DB_EXTENDED_PROPERTY( 'NcharCollation', 'AccentSensitivity'))")
    accentSensitivity = cursor.fetchone()[0]
    output_logging('NCHAR ACCENTSENSITIVITY','%s'%(accentSensitivity))

    # Nchar PunctuationSensitivity check
    cursor.execute("select UPPER(DB_EXTENDED_PROPERTY( 'NcharCollation', 'PunctuationSensitivity'))")
    punctuationSensitivity = cursor.fetchone()[0]
    output_logging('NCHAR PUNCTUATIONSENSITIVITY','%s'%(punctuationSensitivity))

    # Nchar SortType check
    cursor.execute("select UPPER(DB_EXTENDED_PROPERTY( 'NcharCollation', 'SortType'))")
    sortType = cursor.fetchone()[0]
    if sortType:
        output_logging('NCHAR SORTTYPE','%s'%(sortType))

    # Nchar Locale check
    cursor.execute("select DB_EXTENDED_PROPERTY( 'NcharCollation', 'Specification' )")
    locale = cursor.fetchone()[0]
    ncharLocale = (locale.partition("Locale=")[2].partition(";")[0] or locale.partition("Locale=")[2].partition(")")[0])
    if ncharLocale:
        output_logging('NCHAR LOCALE','%s'%(ncharLocale))

    # Blank Padding check
    cursor.execute("select trim( db_property( 'BlankPadding' ));")
    blankpadding = cursor.fetchone()[0]
    output_logging('BLANK PADDING','%s'%(blankpadding))

    # Database Encryption check
    cursor.execute("select db_property('Encryption');")
    encryption = cursor.fetchone()[0]
    output_logging('DB Encryption','%s'%(encryption))

    # IQ Page Size check
    cursor.execute("select substring(Value, 1,charindex('/', Value)-1) from  sp_iqstatus() where Name like '%Page Size%';")
    iqpagesize = cursor.fetchone()[0]
    output_logging('IQ PAGE SIZE','%s BYTES'%(iqpagesize))

    # Endianess check
    output_logging('ENDIANESS','%s'%(byteorder))

    # Charset check
    cursor.execute("select trim( db_property( 'Charset' ));")
    charset = cursor.fetchone()[0]
    output_logging('CHARSET','%s'%(charset))
    if charset == "Extended_UNIX_Code_Packed_Format_for_Japanese":
        features_list.append(('CHARSET', 'Warning', '%s to be replaced by EUC-JP'%(charset)))
        charset = "EUC-JP"
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
        features_list.append(('RLV_Support', 'Error', 'To be disabled'))
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
    version = re.search('16.0|16.1', ver)
    if not version:
        features_list.append(('Current_IQ_Version', 'Fatal', 'Upgrade required'))
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
        features_list.append(('HSR_Enabled', 'Error', 'To be disabled'))
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
        features_list.append(('DB_Encryption', 'Warning', 'On-prem encryption key will not work in HDL.'))
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
            features_list.append(('Non SAP remote-source: %s'%(i[0]), 'Error', 'To be deleted'))
        elif i[0] in supported_list:
            features_list.append(('SAP remote-source: %s'%(i[0]), 'Warning', 'Automated migration of SAP(ASE,HANA,IQ,SA) remote sources is not supported'))
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
        features_list.append(('External_UDF', 'Error', 'To be deleted'))
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of external udf completed in %s seconds"%(elap_sec))

# Check if reserved HDL users are in use (saptu, sapsupport, custadmin, hdladmin )
def username_verify(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of username started.")

    cursor = conn.cursor()
    cursor.execute("select count(*) from sys.sysuser where (user_name='saptu') OR (user_name='sapsupport') OR (user_name='custadmin') OR (user_name='hdladmin')")
    count = cursor.fetchone()[0]
    if count != 0:
        features_list.append(('Reserved_User_Names', 'Error', 'To be Deleted'))
    cursor.execute("select count(*) from sys.sysuser where (user_name='DBA')")
    count = cursor.fetchone()[0]
    if count != 0:
        features_list.append(('DBA_user', 'Warning', 'To be replaced by \"hdladmin\"'))
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
        features_list.append(('Certificates', 'Error', 'To be deleted'))
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
        features_list.append(('External_Environment', 'Error', 'To be deleted'))
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
        features_list.append(('External_Environment_Objects', 'Error', 'To be deleted'))
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
        features_list.append(('Spatial_Unit_Of_Measure', 'Error', 'To be deleted'))
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
        features_list.append(('SA_Tables', 'Error', 'To be deleted'))
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
        features_list.append(('LD_INDEXES', 'Error', 'To be deleted'))
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
        features_list.append(('LF_INDEXES', 'Error', 'To be deleted'))
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
        features_list.append(('HNG_INDEXES', 'Error', 'To be deleted'))
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
        features_list.append(('LOCAL_DBFILES', 'Error', 'To be deleted'))
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
        features_list.append(('Logical_Servers', 'Error', 'To be deleted'))
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
        features_list.append(('DQP_Enabled', 'Error', 'To be disabled'))
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of DQP enablement completed in %s seconds"%(elap_sec))

# Check if IQ_SHARED_TEMP present or not
def sharedtemp_verify(conn):
    strt = datetime.datetime.now()
    log_file.info("\n--------------------------------------------------------------------")
    log_file.info("\nVerification of IQ_SHARED_TEMP started.")

    cursor = conn.cursor()
    cursor.execute("select count(*) from sp_iqdbspace() where DBSpaceName = 'IQ_SHARED_TEMP'")
    count = cursor.fetchone()[0]
    if count != 0:
        features_list.append(('IQ_SHARED_TEMP', 'Error', 'Not Supported in Datalake IQ'))
    cursor.close()

    fins = datetime.datetime.now()
    elap_sec = (fins - strt).total_seconds()
    log_file.info("\nVerification of IQ_SHARED_TEMP completed in %s seconds"%(elap_sec))

# Check if any CORE_Options set and to be disabled in HDL
def coreoptions_verify(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of CORE_Options started.")

    cursor = conn.cursor()
    cursor.execute("select \"option\" from SYSOPTION where \"option\" like 'CORE_Options%' AND setting = 'ON' ")
    rows = cursor.fetchall()
    for i in rows:
        options_list.append((i[0],'Error', 'To be disabled'))
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of CORE_Options completed in %s seconds"%(elap_sec))

# Check if any MPX_options/MPX_test_options set and to be disabled in HDL
def mpxoptions_verify(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification of MPX_options/MPX_test_options started.")

    cursor = conn.cursor()
    cursor.execute("""select \"option_name\" from sp_iqcheckoptions() where Option_name like 'MPX_options%' and User_name = 'PUBLIC'""")
    rows = cursor.fetchall()
    for i in rows:
        options_list.append((i[0],'Error', 'No', 'To be disabled'))

    cursor.execute("""select \"option_name\" from sp_iqcheckoptions() where Option_name like 'MPX_test_options%' and User_name = 'PUBLIC'""")
    rows = cursor.fetchall()
    for i in rows:
        options_list.append((i[0],'Error', 'To be disabled'))
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
        options_list.append(('Temp_Extract_Directory','Warning', 'It would be reset in HDL as it should be object store path in HDL'))
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
        features_list.append(('Web_Services', 'Error', 'To be deleted'))
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of webservice completed in %s seconds"%(elap_sec))

# Gather all HDL unsupported SQL Anywhere server options
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
        options_list.append((i,'Error', 'To be disabled'))

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of SQL Anywhere server options completed in %s seconds"%(elap_sec))

# Gather all HDL unsupported IQ server options
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
        options_list.append((i,'Error', 'To be disabled'))

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
        features_list.append(('MPX_Readers_Present', 'Error', 'To be dropped'))
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

    features_list.append(('StartupOptions', 'Warning','All startup options will be changed on HDL based on t-shirt size'))
    cursor.close()

    elap_sec = common.elap_time(strt)
    log_file.info("\nVerification of IQ startup options completed in %s seconds"%(elap_sec))

# Prints all Server and dbspace property information used in current system in log file
def server_properties(conn):
    strt = datetime.datetime.now()
    log_file.info("\n%s"%(common.dividerline))
    log_file.info("\nVerification server properties and dbspace properties started.")

    print("%s"%(common.dividerline))
    print("Verifying server properties and dbspace properties")
    print("%s"%(common.dividerline))
    out_file.info("%s"%(common.double_divider_line))
    out_file.info(" ******** Server properties and dbspace properties ********")
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
    verify_HSR(conn)
    verify_encryptiondb(conn)
    verify_sap_supported_remote_services(conn)
    external_udf_verify(conn)
    username_verify(conn)
    certificates_verify(conn)
    externalenv_verify(conn)
    externalenvobj_verify(conn)
    spatialunit_verify(conn)
    sa_tables_verify(conn)
    ldindex_verify(conn)
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

    out_file.info("\n%s"%(common.dividerline))
    out_file.info("******** Number of HDL Incompatible Feature/Properties found in IQ Database: %d ********", len(features_list))
    out_file.info("%s"%(common.dividerline))
    out_file.info("\n%s"%(common.double_divider_line))
    out_file.info(" ******** Incompatible features and properties ********")
    out_file.info("%s"%(common.double_divider_line))
    fixed_str = "{:<40} {:<15} {:<5}".format( 'Feature_Name', 'Severity', 'Action')
    out_file.info(fixed_str)
    out_file.info("%s"%(common.double_divider_line))
    for row in features_list:
        fixed_str = "{:<40} {:<15} {:<5}".format(str(row[0]), str(row[1]), str(row[2]))
        out_file.info(fixed_str)
        out_file.info("%s"%(common.dividerline))

    sa_server_options(conn)
    iq_server_options(conn)
    coreoptions_verify(conn)
    mpxoptions_verify(conn)
    tempextractdir_verify(conn)

    out_file.info("\n\n%s"%(common.dividerline))
    out_file.info("******** Number of HDL Unsupported Server Options found in IQ Database: %s ********", len(options_list))
    out_file.info("%s"%(common.dividerline))

    out_file.info("\n%s"%(common.double_divider_line))
    out_file.info(" ******** Unsupported Server Options ********")
    out_file.info("%s"%(common.double_divider_line))

    fixed_str = "{:<40} {:<20} {:<5}".format( 'Option_Name', 'Severity', 'Action')
    out_file.info(fixed_str)
    out_file.info("%s"%(common.double_divider_line))

    for row in options_list:
        fixed_str = "{:<40} {:<20} {:<5}".format(str(row[0]), str(row[1]), str(row[2]))
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
options_list = list()
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
