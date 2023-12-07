# ----------------------------------------------------------------------
# @(#)Migration                      2021              SAP
# ----------------------------------------------------------------------
# Migration utilities to migrate SAP IQ on SAP datalake IQ.
# ----------------------------------------------------------------------
#
# ***************************************************************************
# Copyright (c) 2021 SAP SE or an SAP affiliate company. All rights reserved.
# ***************************************************************************
#!/bin/bash
export SYBASE=$6
source $SYBASE/IQ.sh

uid=$1
password=$2
host=$3
tableid=$7
table=$5
owner=${5%.*}
tableName=${5#*.}
sql_file=$4'/Extracted_Data/'$7'/'$7'.sql'
log_file=$4'/HDL_Conn_Logs/'$7'_load.log'
out_file=$7'_out'
charset="'$8'"
already_processed=$9
expected_rowcount=${10}

owner=${owner,,}
if [ $owner = "dba" ]; then
   owner="HDLADMIN"
   table=$owner'.'$tableName
else
   table=$5
fi

get_rowcount() {
    m='"'
    query="select count(*) from ${m}$owner${m}.${m}$tableName${m}"
    echo -e "\n------------------------------------------------------------------------------------------" >> $log_file
    echo "Data verification connection log for already processed table" >> $log_file
    echo "------------------------------------------------------------------------------------------" >> $log_file
    ret=$(dbisql  -nogui  -c 'uid='$uid';pwd='$password';host='$host';ENC=tls(fips=NO;tls_type=rsa;skip_certificate_name_check=1; direct=yes;);log='$log_file $query -onerror continue)
}

if [[ "$already_processed" == True ]] ; then
   get_rowcount
fi

IFS=$'\n' read -rd '' -a strarr <<<"$ret"
load_rowcount=${strarr[2]}
is_loaded_successfully=false

if [[ "$load_rowcount" -eq "$expected_rowcount" ]]; then
    is_loaded_successfully=true
fi

if [[ "$is_loaded_successfully" == true ]] ; then
   echo OK
   echo $ret
else
   echo -e "\n------------------------------------------------------------------------------------------" >> $log_file
   echo "Data load connection log" >> $log_file
   echo "------------------------------------------------------------------------------------------" >> $log_file

   dbisql  -nogui  -c 'uid='$1';pwd='$2';host='$3';ENC=tls(fips=NO;tls_type=rsa;skip_certificate_name_check=1; direct=yes;);log='$log_file'' READ ENCODING $charset $sql_file -onerror exit >$out_file 2>>$log_file

   retval=$?

   row_missmatch=0
   if  grep "$table" $out_file >>$log_file; then
      row_missmatch=1
   fi


   if [[ $retval -eq 0  &&  $row_missmatch -ne 1 ]]; then
      echo OK
   else
      echo FAIL
   fi

   if [ -f "$out_file" ] ; then
      rm "$out_file"
   fi

   get_rowcount
   echo $ret
fi
