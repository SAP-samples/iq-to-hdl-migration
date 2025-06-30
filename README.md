# SAP IQ to SAP HANA Cloud, Data Lake Relational Engine Migration Utility
[![REUSE status](https://api.reuse.software/badge/github.com/SAP-samples/iq-to-hdl-migration)](https://api.reuse.software/info/github.com/SAP-samples/iq-to-hdl-migration)

## Description
This is a utility for SAP IQ to SAP HANA Cloud, Data Lake migration. These sample utilities can be used to migrate existing schema and data of SAP IQ to SAP HANA Cloud data lake Relational Engine.
These utilities have been verified for migration to SAP HANA Cloud data lake Relational Engine via data lake Files only. 
It has 3 different utilities.

1. Premigration utility - The premigration utility identifies incompatible features, artifacts, server options, users etc. in your existing on-premise SAP IQ database before migrating it to SAP HANA Cloud data lake Relational Engine.

2. Migration Utility - The migration utility has three scripts:
- migration.py: Extracts SAP IQ schema and data to a specified local location.
- copy_hdlfs.py: Copies the extracted data files from local disk to the target SAP HANA Cloud, Data Lake File Store (HDLFS).
- load_schema_and_data.py: Loads the schema and copied data into SAP HANA Cloud data lake Relational Engine.
  
For more information on these utilities, please refer to README files within the  respective Premigration and Migration folders.

1. **Common Folder** :

It will have following files:
- DB_Artifacts.list: Used by the migration utility to identify different artifacts that are not supported in data lake Relational Engine.

- common.py: This file contains common functionalities used by the premigration, migration, and load utilities.

- dbopts_noncustomer.csv: Used by the premigration utility to identify any SQLAnywhere server options that are not supported in data lake Relational Engine.

- hosparams_noncustomer.csv: Used by the premigration utility to identify different SAP IQ server options that are not supported in data lake Relational Engine.
  
- load_config.json: Configuration options file to provide the options required to run the load utility.
   
- login_policy.csv: This file is used by the migration utility to identify different login policy-related options which are not supported in data lake.

- migration_config.json: Configuration options file to provide the options required to run the migration utility.

- premigration_config.json: Configuration options file to provide the options required to run the premigration utility.

2. **Pre_Migration Folder**

It will have following files:

- PreMigration_README.md: README file for the premigration utility. Please refer to the SAP HANA Cloud, Data Lake Migration Guide of assistance on running the utility.
  
- pre_migration.py: Used by the premigration utility to identify different login policy-related options that are not supported in data lake Relational Engine.

3. **Migration Folder**

It will have following files:
- Load_README.md: Readme file for Load utility. Please refer to this README file to run load utility.
  
- Migration_README.md: README file for the Migration utility. Please refer to the SAP HANA Cloud, Data Lake Migration Guide of assistance on running the utility.

- copy_data_to_hdlfs.sh: Shell script to be used by copy_hdlfs.py with necessary parameters and environment setup.

- copy_hdlfs.py: Python utility to recursively copy extracted data files from local storage to HDLFS using REST APIs.

- load_schema.sh: Shell script used by the load_schema_and_data.py utility to load the database schema into data lake Relational Engine.

- load_schema_and_data.py: Python utility to load the SAP IQ extracted schema and data into the data lake Relational Engine.

- load_table.sh: Shell script used by the load_schema_and_data.py utility to load table data into data lake Relational Engine.

- migration.py: Python utility to extract schema and data from SAP IQ.
 
## Requirements
- Python 3 (3.10.x or higher version)
- Pyodbc
- Minimum Supported IQ Server version - 16.1_SP01
- Paramiko(Python module)

## Download and Installation
Install python 3.10.x or higher version and pyodbc. 
Clone this repo
You can then run Premigration and Migration utilities.

## Known Issues

## Limitations
Due to a known HDLFS limitation, the `hdlfscli` utility does not support copying data file larger than 95 GB.
At present, no official workaround exists for windows platform but an alternate solution is in progress which will be fixed in next release.

## How to obtain support
[Create an issue](https://github.com/SAP-samples/iq-to-hdl-migration/issues) in this repository if you find a bug or have questions about the content.
 
For additional support, [ask a question in SAP Community](https://answers.sap.com/questions/ask.html).

## Contributing
If you wish to contribute code, offer fixes or improvements, please send a pull request. Due to legal reasons, contributors will be asked to accept a DCO when they create the first pull request to this project. This happens in an automated fashion during the submission process. SAP uses [the standard DCO text of the Linux Foundation](https://developercertificate.org/).

## License
Copyright (c) 2023 SAP SE or an SAP affiliate company. All rights reserved. This project is licensed under the Apache Software License, version 2.0 except as noted otherwise in the [LICENSE](LICENSE) file.

