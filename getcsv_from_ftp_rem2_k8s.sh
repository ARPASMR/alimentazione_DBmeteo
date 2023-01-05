#!/bin/bash
#===============================================================================
#  alimentazione del db METEO
#  scarica i files .csv con i dati dal server ftp.
# 2015/11/23  MS & MR - Codice originale
# 2015-2016   MM - inserimento logger e controllo pidof
# 2019/11/26  MR - dockerizzazione 
###############################################################################

DIRDATA=data
FTP=/usr/bin/ftp
SUBS="---> QUIT"
FTP_LOG=ftp_rem2.log
ERRE=/usr/bin/R
AGGIORNAMENTO_FTP=aggiornamento_ftp_rem2.R
#AGGIORNAMENTO_FTP_LOG=aggiornamento_ftp_rem2.log
nomescript=${0##*/}
# modifica per evitare esagerati accessi al dB
#nomescript=${0##*/}
#if pidof -o %PPID -x $nomescript >/dev/null; then
#    echo "Process already running"
#    logger -is -p user.warning "$nomescript: processo attivo -> esco" -t "PREVISORE"
#    exit 1
#fi
# Info di Log
echo "#~### getcsv_from_ftp_rem2.sh "`date  '+%Y/%m/%d %H:%M:%S'`" ###~#"
echo "getcsv_from_ftp_rem2.sh "`date  '+%Y/%m/%d %H:%M:%S'`" > scarico files .csv dall'ftp server di ARPA Lombardia"
echo "getcsv_from_ftp_rem2.sh "`date  '+%Y/%m/%d %H:%M:%S'`" >       ftp-server: "$FTP_SERV
echo "getcsv_from_ftp_rem2.sh "`date  '+%Y/%m/%d %H:%M:%S'`" >          ftp-usr: "$FTP_USR
echo "getcsv_from_ftp_rem2.sh "`date  '+%Y/%m/%d %H:%M:%S'`" > directory remota: "$FTP_DIR
echo "getcsv_from_ftp_rem2.sh "`date  '+%Y/%m/%d %H:%M:%S'`" > directory locale: "$DIRDATA
NUM=0
# ----
cd $DIRDATA
# Scarica i dati e salvali sul PC locale
rm -f $FTP_LOG
NUM=`ls -1 SMR_REM2*.csv | wc -l`
ncftpget -u $FTP_USR -p $FTP_PWD -d $FTP_LOG -t 60 ftp://$FTP_SERV/$FTP_DIR/SMR_REM2*.csv
if [ "$?" -ne 0 ]
then
  echo "getcsv_from_ftp_rem2.sh "`date  '+%Y/%m/%d %H:%M:%S'`" > ERRORE di connessione con ftp-server!"
 # logger -is -p user.err "$nomescript "`date  '+%Y/%m/%d %H:%M:%S'`" > ERRORE di connessione con ftp-server!" -t "PREVISORE"
  echo "getcsv_from_ftp_rem2.sh "`date  '+%Y/%m/%d %H:%M:%S'`" > Ulteriori dettagli dal file di log di ftp:"
  echo "getcsv_from_ftp_rem2.sh "`date  '+%Y/%m/%d %H:%M:%S'`" > INIZIO: "
{ while read RIGA
  do
    echo "ftp LOG  > "$RIGA
  done
} < $FTP_LOG
  echo "getcsv_from_ftp_rem2.sh "`date  '+%Y/%m/%d %H:%M:%S'`" > FINE. "
  exit 1
fi
#
NUM1=`ls -1 SMR_REM2*.csv | wc -l`
NUM2=$(( NUM1-NUM ))
echo "getcsv_from_ftp_rem2.sh "`date  '+%Y/%m/%d %H:%M:%S'`" > numero di files scaricati dall'FTP server = "$NUM2
echo "getcsv_from_ftp_rem2.sh "`date  '+%Y/%m/%d %H:%M:%S'`" > numero di files presenti nella directory  = "$NUM1

# Aggiornamento archivio MySQL
if [ "$NUM1" -gt 0 ]
then
 # logger -is -p user.info "$nomescript: presenti  su sinergico $NUM1 file(s) e scaricati $NUM2 file(s)"
  echo "getcsv_from_ftp_rem2.sh "`date  '+%Y/%m/%d %H:%M:%S'`" > Avvio applicativo R che aggiorna l'archivio MySQL"
  echo "getcsv_from_ftp_rem2.sh "`date  '+%Y/%m/%d %H:%M:%S'`" > vvvvvvvvv~vvvvv~vvvvvvv~vvvvvv~~vvvv~~v~vvvvvvvvv"
  $ERRE --vanilla < ../$AGGIORNAMENTO_FTP
#  $ERRE --no-save --no-restore < $AGGIORNAMENTO_FTP
  if [ "$?" -ne 0 ]
  then
  # logger -is -p user.err "$nomescript "`date  '+%Y/%m/%d %H:%M:%S'`" > ERRORE nell'esecuzione dell'istruzione :"$AGGIORNAMENTO_FTP
    echo "getcsv_from_ftp_rem2.sh "`date  '+%Y/%m/%d %H:%M:%S'`" > ERRORE nell'esecuzione dell'istruzione :"
    echo "getcsv_from_ftp_rem2.sh "`date  '+%Y/%m/%d %H:%M:%S'`" > "$ERRE" --save --no-restore < "$AGGIORNAMENTO_FTP
  fi
  #cat $AGGIORNAMENTO_FTP_LOG
    echo "getcsv_from_ftp_rem2.sh "`date  '+%Y/%m/%d %H:%M:%S'`" > ^^^^~^^^^^^^^^^^^^^^^^^^^^~^^^^^^^^^^^^^^^^^^^^~^"
  #  logger -is -p user.notice "$nomescript: esecuzione terminata con successo per $NUM2 file(s)" -t "DATI"
else
 # logger -is -p user.warning "$nomescript: nessun file presente in ftp REM" -t "PREVISORE"
 echo "nessun file presente in ftp REM"
fi
# Fine con successo
exit 0
