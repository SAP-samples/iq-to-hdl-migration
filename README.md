# SAP-samples/iq-to-hdl-migration
This is SAP IQ to SAP Hana Cloud, Data Lake Migration Utility.

# Containing Files and Folders

1. The LICENSE file:
In most cases, the license for SAP sample projects is `Apache 2.0`.

2. The .reuse/dep5 file: 
The [Reuse Tool](https://reuse.software/) must be used for your samples project. You can find the .reuse/dep5 in the project initial. Please replace the parts inside the single angle quotation marks < > by the specific information for your repository.

3. The README.md file (this file):
Provides information of Migration utility.

4. Common folder:
It will have following files:
DB_Artifacts.list: This file is used by the migration utility to identify different artifacts that are not supported in SAP Hana Cloud, Data Lake.
common.py: This file contains common functionalities used by the pre-migration, migration, and load utilities for example,  it has methods to read different json configuration files. 
config_aws.json: Configuration options file to provide the options required to run the pre-migration, migration and load utilities. Use this file if your data lake instance is created on Amazon Web Services (AWS) Hyperscaler based Landscapes.
config_azure.json: Configuration options file to provide the options required to run the pre-migration, migration and load utilities. Use this file if your data lake instance is created on Microsoft Azure Hyperscaler based Landscapes. 
dbopts_noncustomer.csv: This file is used by the pre-migration utility to identify different SQLAnywhere server options which are not supported in SAP Hana Cloud, Data Lake.
hosparams_noncustomer.csv: This file is used by the pre-migration utility to identify different SAP IQ server options which are not supported in SAP Hana Cloud, Data Lake.
login_policy.csv: This file is used by the migration utility to identify different login policy-related options which are not supported in data lake.

5. Pre_Migration folder
It will have following files:
PreMigration_README.md: Readme file for Pre_Migration utility. Please refer to this README file to run Pre_Migration utility.
pre_migration.py: The pre-migration utility identifies incompatible features, artifacts, server options, users etc. in your existing on-premise SAP IQ database before migrating it to SAP HANA Cloud, Data Lake.

6. The Migration folder
It will have following files:
Migration_README.md: Readme file for Migration utility. Please refer to this README file to run migration and load utilities.
load_schema.sh: Shell script used by load_schema_and_data.py utility to load database schema into SAP Hana Cloud, Data Lake.
load_schema_and_data.py: Python utility to load SAP IQ extracted Schema and data into SAP Hana Cloud, Data Lake.
load_table.sh: Shell script used by load_schema_and_data.py utility to load table data SAP Hana Cloud, Data Lake.
migration.py: Migration utility to extract schema and data from SAP IQ.

# SAP IQ to SAP HANA Cloud, Data Lake Migration Utility

## Description
This sample utility can be used to Migrate existing Schema and Data of SAP IQ to SAP Hana Cloud Data Lake. 
 

## Requirements
Python 3
Pyodbc

## Download and Installation
Install python 3.9 or higher version and pyodbc. 
Clone this repo
You can then run Pre_Migration and Migration utilities.

## Known Issues


## How to obtain support
[Create an issue](https://github.com/SAP-samples/iq-to-hdl-migration/issues) in this repository if you find a bug or have questions about the content.
 
For additional support, [ask a question in SAP Community](https://answers.sap.com/questions/ask.html).

## Contributing
If you wish to contribute code, offer fixes or improvements, please send a pull request. Due to legal reasons, contributors will be asked to accept a DCO when they create the first pull request to this project. This happens in an automated fashion during the submission process. SAP uses [the standard DCO text of the Linux Foundation](https://developercertificate.org/).

## License
Copyright (c) 2023 SAP SE or an SAP affiliate company. All rights reserved. This project is licensed under the Apache Software License, version 2.0 except as noted otherwise in the [LICENSE](LICENSE) file.

