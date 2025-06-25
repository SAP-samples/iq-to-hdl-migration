### Overview
The premigration utility identifies incompatible features, configuration information, artifacts, server options, users, etc. in your existing on-premise SAP IQ database before migrating it to data lake Relational Engine.

#### Prerequisite
The following should be installed on the host on which the migration utilities will be run:
    - Python 3.10.x and Above
    - Pyodbc
    - Minimum Supported IQ Server version - 16.1_SP01(Older SAP IQ versions must be upgraded to atleast 16.1_SP01 before migration)

**_NOTE:_**
You may need to install additional Python modules required by specific parts of the migration scripts.

#### Migration Utility Scripts Location
- Create directory for copying Migration utility scripts. Ex: <utility_scripts_dir>
- Please clone or download scripts from [iq-to-hdl-migration](https://github.com/SAP-samples/iq-to-hdl-migration) to <utility_scripts_dir>.

#### Input
The file below is used as input during the premigration phase:
Path for json file : `<utility_scripts_dir>/iq-to-hdl-migration/Common`

    - `premigration_config.json`: Configuration options file to provide the options required to run the premigration utility.

#### Files used by PreMigration Utility
The following files are used by the `pre_migration.py` utility:
Path for files: `<utility_scripts_dir>/iq-to-hdl-migration/Common`

    - `dbopts_noncustomer.csv`: Used by the premigration utility to identify different SAP SQLAnywhere server options that are not supported in data lake Relational Engine.
    - `hosparams_noncustomer.csv`: Used by the premigration utility to identify different SAP IQ server options which are not supported in data lake Relational Engine.
    - `common.py`: Contains common functionality used by the premigration, migration, and load utilities.

#### Output
The following files are generated as output of the premigration phase:
Path for logs: `<utility_scripts_dir>/iq-to-hdl-migration/Pre_Migration`

    - `pre_migration.log`: Contains execution logs of the premigration utility.
    - `pre_migration.out`: Contains the report of all on-premise SAP IQ database artifacts, options, features that are not supported or are supported differently in data lake Relational Engine. It also displays system configuration information and recommended data lake Relational Engine configuration settings.

#### Requirement
Download and install the data lake Relational Engine client.

**_NOTE:_**
	For Windows Migration,the data lake client cannot be installed on the same host as the SAP IQ server. 

##### Use the standalone premigration verification utility(pre_migration.py) to identify any SAP IQ on-premise database artifacts that are incompatible with data lake Relational Engine. Check [Migrate On-Premise SAP IQ](https://help.sap.com/docs/hana-cloud-data-lake/migration-guide/migrate-on-premise-sap-iq-to-data-lake-relational-engine?state=DRAFT&version=2025_2_QRC)

#### Steps to run the premigration utility as below:

Linux:

    a. Source IQ.sh/IQ.csh
      - If you are running the premigration utility on a different host than the IQ server host, source IQ.sh or IQ.csh from <Datalake_Client_Install_Path>.
		OR
      - If you are running the premigration utility on the same host as the IQ server host, source IQ.sh or IQ.csh from <IQ_Server_Install_Path>.

    b. cd `<utility_scripts_dir>/iq-to-hdl-migration/Pre_Migration`

    c. Run the premigration utility:
	    python3 pre_migration.py --config_file ../Common/premigration_config.json
    	OR
	    python3 pre_migration.py -f ../Common/premigration_config.json

Windows:

    a. In Windows PowerShell, enter the following command at the prompt:

        cd `<utility_scripts_dir>/iq-to-hdl-migration/Pre_Migration`

    b. Run the premigration utility:

    	python3 pre_migration.py -f ../Common/premigration_config.json

    To run this command along with its associated help, run:
	    python3 pre_migration.py -h
    	OR
	    python3 pre_migration.py --help

**_NOTE:_**: You can monitor the progress of premigration to data lake Relational Engine by checking (or tail) `<utility_scripts_dir>/iq-to-hdl-migration/Pre_Migration/pre_migration.log` file.

#### Sample for Linux premigration_config.json file
```
{
"Host_Name": "iqSrver",
"Port_Number": 4567,
"DBA_User": "DBA",
"DBA_Pwd": "password",

"IQ_Server_On_Same_Host": "Yes",
"IQ_Host_Login_Id":"",
"IQ_Host_Login_Pwd":"",
"ENC": "tls(fips=yes;tls_type=rsa;skip_certificate_name_check=yes;trusted_certificate=/iqSrver1/iqtesttrust_RSA.pem;identity=/iqSrver1/iqtestcert_RSA.pem;identity_password=test)",

"IQ_Version": "16.1",
"IQ_Server_Install_Path": "/iqSrver1/install/iq-16.1",
"Datalake_Client_Install_Path": "/iqSrver1/install/client"
}
```
