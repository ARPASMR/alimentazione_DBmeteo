#!/bin/bash
#=============================================================================
export https_proxy="http://proxy2:8080"
export http_proxy="http://proxy2:8080"

numsec=600 # 10 minuti 
./getcsv_from_ftp_rem2.sh
sleep $numsec
while [ 1 ]
do
  if [ $SECONDS -ge $numsec ]
  then
    SECONDS=0
    ./getcsv_from_ftp_rem2.sh
    sleep $numsec
  fi
done
