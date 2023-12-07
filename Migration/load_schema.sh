# ----------------------------------------------------------------------
# @(#)Migration                      2021              SAP
# ----------------------------------------------------------------------
# Migration utilities to migrate SAP IQ on SAP datalake IQ.
# ----------------------------------------------------------------------
#
# ***************************************************************************
# Copyright (c) 2021 SAP SE or an SAP affiliate company. All rights reserved.
# ***************************************************************************
export SYBASE=$5
source $SYBASE/IQ.sh

log_file=$4'/HDL_LoadSchema.log'
echo -e "\n------------------------------------------------------------------------------------------" >> $log_file
echo $6" connection log" >> $log_file
echo "------------------------------------------------------------------------------------------" >> $log_file
charset="'$8'"
dbisql  -nogui  -c 'uid='$1';pwd='$2';host='$3';ENC=tls(fips=NO;tls_type=rsa;skip_certificate_name_check=1; direct=yes;);log='$log_file  READ ENCODING $charset $4$7 -onerror continue >> $log_file

