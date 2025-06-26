# SAP IQ to SAP HANA Cloud, Data Lake Migration Utility
[![REUSE status](https://api.reuse.software/badge/github.com/SAP-samples/iq-to-hdl-migration)](https://api.reuse.software/info/github.com/SAP-samples/iq-to-hdl-migration)

## Description
This is a utility for SAP IQ to SAP HANA Cloud, Data Lake migration. These sample utilities can be used to migrate existing schema and data of SAP IQ to SAP HANA Cloud Data Lake.
These utilities have been verified for migration to SAP Hana Clould, Data Lake via data lake Files only. 
It has 2 different utilities.

1. Pre-Migration utility - The pre-migration utility identifies incompatible features, artifacts, server options, users etc. in your existing on-premise SAP IQ database before migrating it to SAP HANA Cloud, Data Lake.

2. Migration Utility - The Migration utility has three scripts:
- migration.py: Extracts SAP IQ schema and data to a specified local location.
- copy_hdlfs.py: Copies the extracted data files from local disk to the target SAP HANA Cloud, Data Lake File Store (HDLFS).
- load_schema_and_data.py: Loads the schema and copied data into SAP HANA Cloud, Data Lake.
  
For more details on these utilities, please refer to individual utilities readMe files available on respective utilities folders (Pre_Migration and Migration folders).

Utilities are spread across following folders:

1. **Common** :

It will have following files:
- DB_Artifacts.list: This file is used by the migration utility to identify different artifacts that are not supported in SAP Hana Cloud, Data Lake.

- common.py: This file contains common functionalities used by the pre-migration, migration, and load utilities for example,  it has methods to read different json configuration files.

- dbopts_noncustomer.csv: This file is used by the pre-migration utility to identify different SQLAnywhere server options which are not supported in SAP Hana Cloud, Data Lake.

- hosparams_noncustomer.csv: This file is used by the pre-migration utility to identify different SAP IQ server options which are not supported in SAP Hana Cloud, Data Lake.
  
- load_config.json: Configuration options file to provide the options required to run the load utility.
   
- login_policy.csv: This file is used by the migration utility to identify different login policy-related options which are not supported in data lake.

- migration_config.json: Configuration options file to provide the options required to run the migration utility.

- premigration_config.json: Configuration options file to provide the options required to run the pre-migration utility.

2. **Pre_Migration**

It will have following files:

- PreMigration_README.md: Readme file for Pre_Migration utility. Please refer to this README file to run Pre_Migration utility.
- pre_migration.py: The pre-migration utility identifies incompatible features, artifacts, server options, users etc. in your existing on-premise SAP IQ database before migrating it to SAP HANA Cloud, Data Lake.

3. **Migration**

It will have following files:
- Load_README.md: Readme file for Load utility. Please refer to this README file to run load utility.
  
- Migration_README.md: Readme file for Migration utility. Please refer to this README file to run migration utility.

- copy_data_to_hdlfs.sh: Shell script to be used by copy_hdlfs.py with necessary parameters and environment setup.

- copy_hdlfs.py: Python utility to recursively copy extracted data files from local storage to HDLFS using REST APIs.

- load_schema.sh: Shell script used by load_schema_and_data.py utility to load database schema into SAP Hana Cloud, Data Lake.

- load_schema_and_data.py: Python utility to load SAP IQ extracted Schema and data into SAP Hana Cloud, Data Lake.

- load_table.sh: Shell script used by load_schema_and_data.py utility to load table data SAP Hana Cloud, Data Lake.

- migration.py: Migration utility to extract schema and data from SAP IQ.
 
## Requirements
- Python 3 (3.10.x or higher version)
- Pyodbc
- Minimum Supported IQ Server version - 16.1_SP01
- Paramiko(Python module)

## Download and Installation
Install python 3.10.x or higher version and pyodbc. 
Clone this repo
You can then run Pre_Migration and Migration utilities.

## Known Issues

## Limitations

## How to obtain support
[Create an issue](https://github.com/SAP-samples/iq-to-hdl-migration/issues) in this repository if you find a bug or have questions about the content.
 
For additional support, [ask a question in SAP Community](https://answers.sap.com/questions/ask.html).

## Contributing
If you wish to contribute code, offer fixes or improvements, please send a pull request. Due to legal reasons, contributors will be asked to accept a DCO when they create the first pull request to this project. This happens in an automated fashion during the submission process. SAP uses [the standard DCO text of the Linux Foundation](https://developercertificate.org/).

## License
Copyright (c) 2023 SAP SE or an SAP affiliate company. All rights reserved. This project is licensed under the Apache Software License, version 2.0 except as noted otherwise in the [LICENSE](LICENSE) file.

