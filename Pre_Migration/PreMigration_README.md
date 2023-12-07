1. Overview
The pre-migration utility identifies incompatible features, artifacts, server options, users etc. in your existing on-premise SAP IQ database before migrating it to SAP HANA Data Lake (data lake).

2. Prerequisite
The following should be installed on the host on which the migration utilities are run:
    - Python 3
    - Pyodbc

3. Input
The files discussed in this section are taken as input during the pre-migration and migration phases.

    - config_aws.json: Configuration options file to provide the options required to run the pre-migration, migration and load utilities. Use this file if your data lake instance is created on Amazon Web Services (AWS).

    - config_azure.json: Configuration options file to provide the options required to run the pre-migration, migration and load utilities. Use this file if your data lake instance is created on Microsoft Azure.

4. Uses
The following files are required by the pre-migration.py utility:

    - dbopts_noncustomer.csv: Used by the pre-migration utility to identify different SQLAnywhere server options which are not supported in data lake.

    - hosparams_noncustomer.csv: Used by the pre-migration utility to identify different SAP IQ server options which are not supported in data lake.

    - common.py: Contains common functionalities used by the pre-migration, migration, and load utilities for example methods to read .json configuration files

5. Output
The following files are generated as output of the pre-migration phase:

    - pre_migration.log: Contains execution logs of the pre-migration utility.

    - pre_migration.out: Contains the report of all on-premise SAP IQ database artifacts/options/features which are not supported or are supported differently in data lake.

6. Help

PreMigration_README.md

7. Use the standalone pre-migration verification utility  - pre_migration.py to alert and to know if any SAP IQ on-premise database artifacts are incompatible with data lake.

Steps to run the pre-migration verification pre_migration.py utility as under:

    i.  Run the following command at the prompt:
    python3 pre_migration.py

    To run this command along with its associated help, run as follows:

    python3 pre_migration.py -h
    OR
    python3 pre_migration.py --help

    ii. Provide values for the following parameters in the configuration file applicable to you/based on your hyperscaler  config_aws.json or config_azure.json. This is required to run the pre_migration.py utility:

    "Host_Name": "<(Coordinator / Simplex server) hostname>",
    "Port_Number": <Port number of (IQ coordinator / IQ simplex server)>,
    "DBA_User": "<DBA user name>",
    "DBA_Pwd": "<Optional: DBA user password.Please provide the value or empty string as shown in ReadMe files.>",
    "Batch_Size_GB": "<Optional: Batch size in GB if batch extraction is enabled, else set it to 0 or leave this parameter unchanged to go with normal (non-batch) extraction mode. Provide positive integer value without quotes.>"
    "IQ_Server_On_Same_Host": "<SAP IQ Server on same host or not, Valid values:(Yes/No)>",
    "IQ_Host_Login_Id":"<Optional: SAP IQ host login id to establish an ssh connection if your installations of SAP IQ Server and the migration utilities (17.1 data lake client installation) are on different hosts.Please provide the value or empty string as shown in ReadMe files.>",
    "IQ_Host_Login_Pwd":"<Optional: SAP IQ host login password to establish an ssh connection if your installations of SAP IQ Server and the migration utilities (17.1 data lake client installation) are on different hosts.Please provide the value or empty string as shown in ReadMe files.>",
    "IQ_Version": "<Your major SAP IQ version in use: Should be either SAP IQ 16.0 or 16.1>",
    "ENC": "<Optional: ENC string based on Encryption setting on SAP IQ server (None/Simple/TLS(<TLS Options>).Please provide the value or empty string as shown in ReadMe files.>",
    "IQ_Server_Install_Path": "<Path of SAP IQ Server installation directory>",
    "Datalake_Client_Install_Path": "<Path of 17.1 SAP Data lake Client Installation directory>",

    iii. source IQ.sh/IQ.csh

    Source IQ.sh or IQ.csh from <Datalake_Client_Install_Path> if you are running migration utilities on different host than IQ server host.

    Source IQ.sh or IQ.csh from <IQ_Server_Install_Path> if you are running migration utilities on same host as IQ server host.

    iv.  Run the pre_migration.py utility along with your hyperscaler-specific configuration file as follows:

    python3 pre_migration.py --config_file <config_file_path>
    OR
    python3 pre_migration.py -f <config_file_path>

Note: You can monitor the progress of pre-migration on data lake by checking (or tail) <Datalake_Client_Install_Path>/IQ-17_1/HDL_Migration/Pre_Migration/pre_migration.log file.

8. Sample config_aws.json file

    {"Host_Name": "iqSrver",
    "Port_Number": 4567,
    "DBA_User": "DBA",
    "DBA_Pwd": "password",
    "Extract_Path": "/iqSrver1/Migration_Extract_Demo",
    "Client_Num_Conn": 2,
    "IQ_Server_On_Same_Host": "Yes",
    "IQ_Host_Login_Id":"",
    "IQ_Host_Login_Pwd":"",
    "Batch_Size_GB": 100,
    "IQ_Version": "16.1",
    "ENC": "tls(fips=yes;tls_type=rsa;skip_certificate_name_check=yes;trusted_certificate=/iqSrver1/iqtesttrust_RSA.pem;identity=/iqSrver1/iqtestcert_RSA.pem;identity_password=test)",
    "IQ_Server_Install_Path": "/iqSrver1/install/iq-16.1",
    "Datalake_Client_Install_Path": "/iqSrver1/install/client",
    "HDLADMIN_User": "HDLADMIN",
    "HDLADMIN_Pwd": "password12345",
    "HDL_Coord_Endpoint": "a1234567-123b-4567-abcd-abcd123456e9-coord.iq.demohdl.com",
    "HDL_Writer_Endpoint": "a1234567-123b-4567-abcd-abcd123456e9.iq.demohdl.com",
    "HDL_Num_Writer_Conn": 2,
    "HDL_Num_Coord_Conn": 0,
    "Object_Store_Copy_Validation": "No",
    "Hyperscaler_Details": {
        "Name": "Aws",
        "Credentials": {
            "AWS_Access_Key_Id": "ABCDEFGHIJKLMNOPQRS1",
            "AWS_Secret_Access_Key" : "aBCDefghijKL01234mnopQRST879jklmxyz+YYcd",
            "AWS_Region" : "us-east-1",
            "Bucket_Name" : "migration-demo"
            }
        }
    }

9. Sample config_azure.json file

    {"Host_Name": "iqSrver",
    "Port_Number": 4567,
    "DBA_User": "DBA",
    "DBA_Pwd": "password",
    "Extract_Path": "/iqSrver1/Migration_Extract_Demo",
    "Client_Num_Conn": 2,
    "Batch_Size_GB": 100,
    "IQ_Server_On_Same_Host": "Yes",
    "IQ_Host_Login_Id":"",
    "IQ_Host_Login_Pwd":"",
    "IQ_Version": "16.1",
    "ENC": "tls(fips=yes;tls_type=rsa;skip_certificate_name_check=yes;trusted_certificate=/iqSrver1/iqtesttrust_RSA.pem;identity=/iqSrver1/iqtestcert_RSA.pem;identity_password=test)",
    "IQ_Server_Install_Path": "/iqSrver1/install/iq-16.1",
    "Datalake_Client_Install_Path": "/iqSrver1/install/client",
    "HDLADMIN_User": "HDLADMIN",
    "HDLADMIN_Pwd": "password12345",
    "HDL_Coord_Endpoint": "a1234567-123b-4567-abcd-abcd123456e9-coord.iq.demohdl.com",
    "HDL_Writer_Endpoint": "a1234567-123b-4567-abcd-abcd123456e9.iq.demohdl.com",
    "HDL_Num_Writer_Conn": 2,
    "HDL_Num_Coord_Conn": 0,
    "Object_Store_Copy_Validation": "No",
    "Hyperscaler_Details": {
        "Name": "Azure",
        "Credentials": {
             "Azure_Account_Name": "AzureBlobStore",
             "Azure_Account_Key" : "aBCDefghijKL01234mnopQRST879jklmxyz+YYcd",
             "Container_Name"    : "migration-demo"
            }
        }
    }
