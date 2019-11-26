#!/bin/bash
#=============================================================================
numsec=600 # 10 minuti 
./getcsv_from_ftp_rem2.sh
sleep $numsec
while [ 1 ]
do
  if [ $SECONDS -ge $numsec ]
  then
    ./getcsv_from_ftp_rem2.sh
    SECONDS=0
    sleep $numsec
  fi
done
