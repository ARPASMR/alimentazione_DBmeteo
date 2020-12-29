##############################################      
###  alimentazione del db METEO            ###
###  con la migrazioni delle flag DQC      ###
###                                        ###
### MR & MS             24/05/2016         ###
### MR-dockerizzazione  26/11/2019         ###
###                                        ###
##############################################      

library(DBI)
library(RMySQL)
#_____________________________
# ricavo soglie da DB

soglia_min_da_DB <- function(tipp){
                                    query <- paste("select Rif1 from M_",tipologia[tipp],"_DQCinfo where Test='P1a'",sep="")
                                    q <- try( dbGetQuery(conn,query),silent=TRUE )
                                    if (inherits(q,"try-error")) {
                                     quit(status=1)
                                    }
return(q)
}

soglia_max_da_DB <- function(tipp){
                                    query <- paste("select Rif2 from M_",tipologia[tipp],"_DQCinfo where Test='P1a'",sep="")
                                    q <- try( dbGetQuery(conn,query),silent=TRUE )
                                    if (inherits(q,"try-error")) {
                                     quit(status=1)
                                    }
return(q)
}

soglia_min_da_DB_RG_notte<-function() {
                                    query <- paste("select Rif1 from M_RadiometriG_DQCinfo where Test='P1b'",sep="")
                                    q <- try( dbGetQuery(conn,query),silent=TRUE )
                                    if (inherits(q,"try-error")) {
                                     quit(status=1)
                                    }
return(q)
}
#_____________________________

file_log        <- 'aggiornamento_ftp_rem2.log'
neverstop<-function(){
  cat("EE..ERRORE durante l'esecuzione dello script!! Messaggio d'Errore prodotto:\n",file=file_log,append=T)
  quit()
}
options(show.error.messages=TRUE,error=neverstop)

# ASSEGNAZIONE PARAMETRI
tipologia <- c("Termometri","RadiometriG","RadiometriN","Pluviometri","Igrometri","Barometri","AnemometriVV","AnemometriDV","Nivometri","TPvisibilita","TPpioggia","TPneve","AnemometriVR","AnemometriDR")
test <- 'P1a' # = Allarme del test di soglia P1a
fallimento <- 'F' # = Allarme del test di soglia P1a
nome_tavola_appoggio    <- "AUX_table"
nome_tavola_DQCinDBUNICO <- "DQCinDBUNICO_dati"

cat ( "INSERIMENTO DATI METEO IN TABELLE RECENTI", date()," \n\n" , file = file_log)

#___________________________________________________
#    COLLEGAMENTO AL DB
#___________________________________________________

cat("collegamento al DB...",file=file_log,append=T)
drv<-dbDriver("MySQL")
conn<-try(dbConnect(drv, user=as.character(Sys.getenv("MYSQL_USR")), password=as.character(Sys.getenv("MYSQL_PWD")), dbname=as.character(Sys.getenv("MYSQL_DBNAME")), host=as.character(Sys.getenv("MYSQL_HOST"))))

if (inherits(conn,"try-error")) {
  print( "ERRORE nell'apertura della connessione al DB \n")
  print( "chiusura connessione malriuscita ed uscita dal programma \n")
  dbDisconnect(conn)
  rm(conn)
  dbUnloadDriver(drv)
  quit(status=1)
}

#______________________________________________________
# INTERROGAZIONE DB PER ESTRARRE ANAGRAFICA 
#______________________________________________________
cat("interrogo DB per estrarre le info di anagrafica...",file=file_log,append=T)
q_anagrafica <- try( dbGetQuery(conn,"select * from A_Sensori where Importato='Yes'"),silent=TRUE )
#
# chiedo ID SOLO PER LE STAZIONI DI METEOSVIZZERA, per ora.
#q_anagrafica <- try( dbGetQuery(conn,"select * from A_Sensori , A_Stazioni where ProprietaStazione in ('Meteosvizzera','Canton Ticino', 'CGP') and A_Stazioni.IDstazione=A_Sensori.IDstazione"),silent=TRUE )
#print(q_anagrafica)
if (inherits(q_anagrafica,"try-error")) {
  #cat(q_anagrafica,"\n",file=file_log,append=T)
  quit(status=1)
}

#___________________________________________________
#   PROCESSO FILE NUOVI DALLA DIRECTORY CSV_FTP 
#___________________________________________________
comando <- paste("ls *.csv",sep="")
nomefile <- try(readLines(pipe(comando)),silent=TRUE)
if (inherits(nomefile,"try-error")) {
  cat(nomefile,"\n",file=file_log,append=T)
  quit(status=1)
}

numerofiles<-length(nomefile)
cat("numero di files con dati da importare nell'archivio : ", numerofiles,"\n",file=file_log,append =T)

# ciclo sui files
cat("Inizio ciclo sui files\n",file=file_log,append=T)

i<-1
while( i <= length(nomefile) ){
  cat("\n ############## \n\n",i," sto elaborando il file : ",nomefile[i],"\n",file=file_log,append=T)
#print(nomefile[i])
# chiudo connessioni precedenti
  closeAllConnections()
#___________________________________________________
# leggo info
#___________________________________________________
  cat("lettura\n",file=file_log,append=T)
  lettura<-NULL
  lettura <- try(read.csv(nomefile[i],
                  header=F, 
                  as.is=T,
                  fill=T,
                  na.string=c("","","-999","-999","-999"),
                  colClasses=c("integer","character","numeric","numeric","integer"),
                  col.names=c("ID","tempo","Variabile","codice_val","funzione")),silent=TRUE)
  if (inherits(lettura,"try-error")) {
    cat(lettura,"\n",file=file_log,append=T)
    cat("..............................................................................\n",file=file_log,append=T)
    cat(paste("Non e' possibile leggere il file ",nomefile[i]," passiamo al file successivo \n",sep=""),file=file_log,append=T)
    cat("..............................................................................\n",file=file_log,append=T)
    i<-i+1
    next 
  }
  sensore_nel_file <- NULL
  tempo            <- NULL
  variabile        <- NULL
  man              <- NULL
  funzione         <- NULL
#
  sensore_nel_file   <- lettura$ID
  tempo     <- as.POSIXct(strptime(lettura$tempo,format="%Y/%m/%d %H:%M"),"GMT")
# tempo_min <- min(tempo)
  variabile <- lettura$Variabile
  man       <- lettura$codice_val
  funzione  <- lettura$funzione
#  auto      <- lettura$codice_auto
# Controllo: File vuoto o anomalie strane?
  if ( (length(sensore_nel_file) == 0) |
       (length(tempo)==0)              |
       (length(variabile)==0)          |
       (length(man)==0)                |
       (length(funzione)==0) ) {
    cat ( " ATTENZIONE! il file e' (a) vuoto o (b) mal formattato ! Passo al file successivo \n", file= file_log, append=T)
    i<-i+1
    next 
  }
# Controllo: stampa su file di log tutti i valori letti - inizio
#  if ( length(sensore_nel_file) > 0) {
#    cat ( " DEBUG! Ecco tutti i valori contenuti nel file \n", file= file_log, append=T)
#    cat ( rbind( "# ",sensore_nel_file[],   " # ",
#                      format(tempo[],"%Y/%m/%d %H:%M","UTC"),  " # ",
#                      variabile[],          " # ",
#                      man[],                " #\n"
#               ) , file= file_log, append=T)
#  }
# Controllo: stampa su file di log tutti i valori letti - fine 
# Controllo su valori "NA" nelle misure: inizio 
#   (verra' ripetuto poi per verificare la consistenza nel passaggio dei vari array)
  if ( length(variabile[is.na(as.numeric(variabile))]) > 0 ) {
    aux_NA<-NULL
    aux_NA<-is.na(as.numeric(variabile)) 
    cat ( " ATTENZIONE! (tot) segnalo record/s nel file di input con NA nel campo \"Misura\" \n", file= file_log, append=T)
#    cat ( rbind( "# ",sensore_nel_file[aux_NA], " # ",
#                      format(tempo[aux_NA],"%Y/%m/%d %H:%M","UTC"),  " # ",
#                      variabile[aux_NA],        " # ",
#                      man[aux_NA],              " #\n"
#               ) , file= file_log, append=T)
  }
# Controllo su valori "NA" nelle misure: fine
# Controllo su valori "NA" in altri campi: inizio
  if ( length(sensore_nel_file[is.na(as.numeric(sensore_nel_file))]) > 0) {
    aux_NA<-NULL
    aux_NA<-is.na(as.numeric(sensore_nel_file)) 
    cat ( " ATTENZIONE! (tot) segnalo record/s nel file di input con NA nel campo \"IDsensore\" \n", file= file_log, append=T)
    cat ( " ATTENZIONE! questa e' una grave anomalia nel formato del file che puo' avere ripercussioni sul buon esito dell'applicazione \n", file= file_log, append=T)
#    cat ( rbind( "# ",sensore_nel_file[aux_NA], " # ",
#                      format(tempo[aux_NA],"%Y/%m/%d %H:%M","UTC"),  " # ",
#                      variabile[aux_NA],        " # ",
#                      man[aux_NA],              " #\n"
#               ) , file= file_log, append=T)
  }
#
  if ( length(tempo[is.na(as.numeric(tempo))]) > 0) {
    aux_NA<-NULL
    aux_NA<-is.na(as.numeric(tempo)) 
    cat ( " ATTENZIONE! (tot) segnalo record/s nel file di input con NA nel campo \"tempo\" \n", file= file_log, append=T)
    cat ( " ATTENZIONE! questa e' una grave anomalia nel formato del file che puo' avere ripercussioni sul buon esito dell'applicazione \n", file= file_log, append=T)
#    cat ( rbind( "# ",sensore_nel_file[aux_NA], " # ",
#                      format(tempo[aux_NA],"%Y/%m/%d %H:%M","UTC"),  " # ",
#                      variabile[aux_NA],        " # ",
#                      man[aux_NA],              " #\n"
#               ) , file= file_log, append=T)
  }
#
  if ( length(man[is.na(as.numeric(man))]) > 0) {
    aux_NA<-NULL
    aux_NA<-is.na(as.numeric(man)) 
    cat ( " ATTENZIONE! (tot) segnalo record/s nel file di input con NA nel campo \"flag\" \n", file= file_log, append=T)
    cat ( " ATTENZIONE! questa e' una grave anomalia nel formato del file che puo' avere ripercussioni sul buon esito dell'applicazione \n", file= file_log, append=T)
}
  if ( length(funzione[is.na(as.numeric(funzione))]) > 0) {
    aux_NA<-NULL
    aux_NA<-is.na(as.numeric(funzione)) 
    cat ( " ATTENZIONE! (tot) segnalo record/s nel file di input con NA nel campo \"funzione\" \n", file= file_log, append=T)
    cat ( " ATTENZIONE! questa e' una grave anomalia nel formato del file che puo' avere ripercussioni sul buon esito dell'applicazione \n", file= file_log, append=T)
#    cat ( rbind( "# ",sensore_nel_file[aux_NA], " # ",
#                      format(tempo[aux_NA],"%Y/%m/%d %H:%M","UTC"),  " # ",
#                      variabile[aux_NA],        " # ",
#                      man[aux_NA],              " #\n"
#               ) , file= file_log, append=T)
  }
# Controllo su valori "NA" in altri campi: fine
#___________________________________________________
#  INSERISCO RECORD PROCEDENDO PER TIPOLOGIA 
#___________________________________________________
# ciclo sulle tipologie di sensori
  tip<-1
  while( tip <= length(tipologia) ){
#print(tipologia[tip])
    cat ( "\n\n",tipologia[tip],"\n",sep=" ", file = file_log, append=T)
    indice_sensori<-NULL
    sensori_in_anagrafica<-NULL
# estraggo ID dei sensori di questa tipologia
    if(tip==1){
      indice  <- NULL
      indice1  <- NULL
      indice2  <- NULL
      indice_sensori <- which( q_anagrafica$NOMEtipologia=="T" |
                               q_anagrafica$NOMEtipologia=="TV")
      # "sensori_in_anagrafica" conterra' la lista dei sensori in anagrafica
      #                         appartenenti alla tipologia in esame    
      sensori_in_anagrafica <- q_anagrafica$IDsensore[indice_sensori]
      # incrocio con anagrafica per selezionare solo quelli della tipologia giusta
      indice1 <- which(sensore_nel_file %in% sensori_in_anagrafica) 
      indice2 <- which(funzione==1) 
      indice  <- indice1[which(indice1%in%indice2==T)]
    }
    if(tip==2){
      indice  <- NULL
      indice1  <- NULL
      indice2  <- NULL
      indice_sensori <- which( q_anagrafica$NOMEtipologia=="RG" )
      sensori_in_anagrafica <- q_anagrafica$IDsensore[indice_sensori]
      indice1 <- which(sensore_nel_file %in% sensori_in_anagrafica) 
      indice2 <- which(funzione==1) 
      indice  <- indice1[which(indice1%in%indice2==T)]
    }
    if(tip==3){
      indice  <- NULL
      indice1  <- NULL
      indice2  <- NULL
      indice_sensori <- which( q_anagrafica$NOMEtipologia=="RN" )
      sensori_in_anagrafica <- q_anagrafica$IDsensore[indice_sensori]
      indice1 <- which(sensore_nel_file %in% sensori_in_anagrafica) 
      indice2 <- which(funzione==1) 
      indice  <- indice1[which(indice1%in%indice2==T)]
    }
    if(tip==4){
      indice  <- NULL
      indice1  <- NULL
      indice2  <- NULL
      indice_sensori <- which( q_anagrafica$NOMEtipologia=="PP" | 
                               q_anagrafica$NOMEtipologia=="PPR")
      sensori_in_anagrafica <- q_anagrafica$IDsensore[indice_sensori]
      indice1 <- which(sensore_nel_file %in% sensori_in_anagrafica) 
      indice2 <- which(funzione==4) 
      indice  <- indice1[which(indice1%in%indice2==T)]
    }
    if(tip==5){
      indice  <- NULL
      indice1  <- NULL
      indice2  <- NULL
      indice_sensori <- which( q_anagrafica$NOMEtipologia=="UR" )
      sensori_in_anagrafica <- q_anagrafica$IDsensore[indice_sensori]
      indice1 <- which(sensore_nel_file %in% sensori_in_anagrafica) 
      indice2 <- which(funzione==1) 
      indice  <- indice1[which(indice1%in%indice2==T)]
    }
    if(tip==6){
      indice  <- NULL
      indice1  <- NULL
      indice2  <- NULL
      indice_sensori <- which( q_anagrafica$NOMEtipologia=="PA" )
      sensori_in_anagrafica <- q_anagrafica$IDsensore[indice_sensori]
      indice1 <- which(sensore_nel_file %in% sensori_in_anagrafica) 
      indice2 <- which(funzione==1) 
      indice  <- indice1[which(indice1%in%indice2==T)]
    }
    if(tip==7){
      indice  <- NULL
      indice1  <- NULL
      indice2  <- NULL
      indice_sensori <- which( q_anagrafica$NOMEtipologia=="VV" | 
                               q_anagrafica$NOMEtipologia=="VVP" | 
                               q_anagrafica$NOMEtipologia=="VVQ" |
                               q_anagrafica$NOMEtipologia=="VVS")
      sensori_in_anagrafica <- q_anagrafica$IDsensore[indice_sensori]
      indice1 <- which(sensore_nel_file %in% sensori_in_anagrafica) 
      indice2 <- which(funzione==1) 
      indice  <- indice1[which(indice1%in%indice2==T)]
    }
    if(tip==8){
      indice  <- NULL
      indice1  <- NULL
      indice2  <- NULL
      indice_sensori <- which( q_anagrafica$NOMEtipologia=="DV" | 
                               q_anagrafica$NOMEtipologia=="DVP" |
                               q_anagrafica$NOMEtipologia=="DVQ" |
                               q_anagrafica$NOMEtipologia=="DVS")
      sensori_in_anagrafica <- q_anagrafica$IDsensore[indice_sensori]
      indice1 <- which(sensore_nel_file %in% sensori_in_anagrafica) 
      indice2 <- which(funzione==1) 
      indice  <- indice1[which(indice1%in%indice2==T)]
    }
    if(tip==9){
      indice  <- NULL
      indice1  <- NULL
      indice2  <- NULL
      indice_sensori <- which( q_anagrafica$NOMEtipologia=="N" )
      sensori_in_anagrafica <- q_anagrafica$IDsensore[indice_sensori]
      indice1 <- which(sensore_nel_file %in% sensori_in_anagrafica) 
      indice2 <- which(funzione==1) 
      indice  <- indice1[which(indice1%in%indice2==T)]
    }
    if(tip==10){
      indice  <- NULL
      indice1  <- NULL
      indice2  <- NULL
      indice_sensori <- which( q_anagrafica$NOMEtipologia=="TPV" ) 
      sensori_in_anagrafica <- q_anagrafica$IDsensore[indice_sensori]
      indice1 <- which(sensore_nel_file %in% sensori_in_anagrafica) 
      indice2 <- which(funzione==1) 
      indice  <- indice1[which(indice1%in%indice2==T)]
    }
    if(tip==11){
      indice  <- NULL
      indice1  <- NULL
      indice2  <- NULL
      indice_sensori <- which( q_anagrafica$NOMEtipologia=="TPP" ) 
      sensori_in_anagrafica <- q_anagrafica$IDsensore[indice_sensori]
      indice1 <- which(sensore_nel_file %in% sensori_in_anagrafica) 
      indice2 <- which(funzione==4) 
      indice  <- indice1[which(indice1%in%indice2==T)]
    }
    if(tip==12){
      indice  <- NULL
      indice1  <- NULL
      indice2  <- NULL
      indice_sensori <- which( q_anagrafica$NOMEtipologia=="TPN" ) 
      sensori_in_anagrafica <- q_anagrafica$IDsensore[indice_sensori]
      indice1 <- which(sensore_nel_file %in% sensori_in_anagrafica) 
      indice2 <- which(funzione==4) 
      indice  <- indice1[which(indice1%in%indice2==T)]
    }
    if(tip==13){
      indice  <- NULL
      indice1  <- NULL
      indice2  <- NULL
      indice_sensori <- which( q_anagrafica$NOMEtipologia=="VV" ) 
      sensori_in_anagrafica <- q_anagrafica$IDsensore[indice_sensori]
      indice1 <- which(sensore_nel_file %in% sensori_in_anagrafica) 
      indice2 <- which(funzione==3) 
      indice  <- indice1[which(indice1%in%indice2==T)]
    }
    if(tip==14){
      indice  <- NULL
      indice1  <- NULL
      indice2  <- NULL
      indice_sensori <- which( q_anagrafica$NOMEtipologia=="DV" ) 
      sensori_in_anagrafica <- q_anagrafica$IDsensore[indice_sensori]
      indice1 <- which(sensore_nel_file %in% sensori_in_anagrafica) 
      indice2 <- which(funzione==3) 
      indice  <- indice1[which(indice1%in%indice2==T)]
    }


    cat(" numero osservazioni = ",length(indice),"\n",sep="",file=file_log, append=T)
#proseguo solo se ci sono sensori appartenenti a questa tipologia 
#da inserire:
    if(length(indice)!=0){

##### @@@@@@@@@@ 
#  da "tempo" devo estrarre gli anni e ciclare perchè posso avere misure 
#  relative ad anni diversi nello stesso file (e quindi devo riempire tabelle diverse) 

      nel_1987  <- NULL 
      nel_1988  <- NULL 
      nel_1989  <- NULL 
      nel_1990  <- NULL 
      nel_1991  <- NULL 
      nel_1992  <- NULL 
      nel_1993  <- NULL 
      nel_1994  <- NULL 
      nel_1995  <- NULL 
      nel_1996  <- NULL 
      nel_1997  <- NULL 
      nel_1998  <- NULL 
      nel_1999  <- NULL 
      nel_2000  <- NULL 
      nel_2001  <- NULL 
      nel_2002  <- NULL 
      nel_2003  <- NULL 
      nel_2004  <- NULL 
      nel_2005  <- NULL 
      nel_2006  <- NULL 
      nel_2007  <- NULL 
      nel_2008  <- NULL 
      nel_2009  <- NULL 
      nel_2010  <- NULL 
      nel_2011  <- NULL 
      nel_2012  <- NULL 
      nel_2013  <- NULL 
      nel_2014  <- NULL 
      nel_2015  <- NULL 
      nel_2016  <- NULL 
      nel_2017  <- NULL 
      nel_2018  <- NULL 
      nel_2019  <- NULL 
      nel_2020  <- NULL
      nel_2021  <- NULL
      nel_1987  <- which(format(tempo[indice],"%Y") == "1987")
      nel_1988  <- which(format(tempo[indice],"%Y") == "1988")
      nel_1989  <- which(format(tempo[indice],"%Y") == "1989")
      nel_1990  <- which(format(tempo[indice],"%Y") == "1990")
      nel_1991  <- which(format(tempo[indice],"%Y") == "1991")
      nel_1992  <- which(format(tempo[indice],"%Y") == "1992")
      nel_1993  <- which(format(tempo[indice],"%Y") == "1993")
      nel_1994  <- which(format(tempo[indice],"%Y") == "1994")
      nel_1995  <- which(format(tempo[indice],"%Y") == "1995")
      nel_1996  <- which(format(tempo[indice],"%Y") == "1996")
      nel_1997  <- which(format(tempo[indice],"%Y") == "1997")
      nel_1998  <- which(format(tempo[indice],"%Y") == "1998")
      nel_1999  <- which(format(tempo[indice],"%Y") == "1999")
      nel_2000  <- which(format(tempo[indice],"%Y") == "2000")
      nel_2001  <- which(format(tempo[indice],"%Y") == "2001")
      nel_2002  <- which(format(tempo[indice],"%Y") == "2002")
      nel_2003  <- which(format(tempo[indice],"%Y") == "2003")
      nel_2004  <- which(format(tempo[indice],"%Y") == "2004")
      nel_2005  <- which(format(tempo[indice],"%Y") == "2005")
      nel_2006  <- which(format(tempo[indice],"%Y") == "2006")
      nel_2007  <- which(format(tempo[indice],"%Y") == "2007")
      nel_2008  <- which(format(tempo[indice],"%Y") == "2008")
      nel_2009  <- which(format(tempo[indice],"%Y") == "2009")
      nel_2010  <- which(format(tempo[indice],"%Y") == "2010")
      nel_2011  <- which(format(tempo[indice],"%Y") == "2011")
      nel_2012  <- which(format(tempo[indice],"%Y") == "2012")
      nel_2013  <- which(format(tempo[indice],"%Y") == "2013")
      nel_2014  <- which(format(tempo[indice],"%Y") == "2014")
      nel_2015  <- which(format(tempo[indice],"%Y") == "2015")
      nel_2016  <- which(format(tempo[indice],"%Y") == "2016")
      nel_2017  <- which(format(tempo[indice],"%Y") == "2017")
      nel_2018  <- which(format(tempo[indice],"%Y") == "2018")
      nel_2019  <- which(format(tempo[indice],"%Y") == "2019")
      nel_2020  <- which(format(tempo[indice],"%Y") == "2020")
      nel_2021  <- which(format(tempo[indice],"%Y") == "2021")
##### @@@@@@@@@@ 
# CICLO SUGLI ANNI  
      for (anno in seq(1987,2021, by=1)) {
##### @@@@@@@@@@ !!!!!!!!
        index <- NULL
        index <- indice[eval(parse(text=paste("nel_",anno,sep="")))]
##### @@@@@@@@@@ !!!!!!!! 

       if(length(index)!=0){ #proseguo solo se esistono misure da inserire relative a questo anno

          cat ( " sto elaborando anno (num obs) > ", anno, " (",length(index),")\n", file= file_log, append=T)

          Sensore   <- NULL 
          Tempo     <- NULL
          Variabile <- NULL
          Man       <- NULL
          Sensore   <- sensore_nel_file[index]
          Tempo     <- tempo[index]
          Variabile <- variabile[index]
          Man       <- man[index]
          Funzione  <- funzione[index]


# Controllo su valori "NA" nelle misure: inizio
#   (si ripete qui per verificare la consistenza nel passaggio dei vari array)
          if ( length(Variabile[is.na(as.numeric(Variabile))]) > 0) {
            aux_NA<-NULL
            aux_NA<-is.na(as.numeric(Variabile)) 
            cat ( " ATTENZIONE! (chck1) segnalo record/s nel file di input con NA nel campo \"Misura\" \n", file= file_log, append=T)
#            cat ( rbind( "# ",Sensore[aux_NA],   " # ",
#                              format(Tempo[aux_NA],"%Y/%m/%d %H:%M","UTC"),  " # ",
#                              Variabile[aux_NA], " # ",
#                              Man[aux_NA],       " #\n"
#                       ) , file= file_log, append=T)
          }
# Controllo su valori "NA" nelle misure: fine
#_______________________________________________________
# DEFINISCO VARIABILI 
#______________________________________________________

          IDsensore            <- NULL 
          Data_e_ora           <- NULL 
          Misura               <- NULL 
          Flag_manuale_DBunico <- NULL 
          Flag_automatica      <- NULL 
          Flag_manuale         <- NULL 
          Autore               <- NULL 
          Data                 <- NULL 
          IDsensore            <- Sensore
          Data_e_ora           <- paste("'",Tempo,"'",sep="")
          Misura               <- Variabile
          Flag_manuale_DBunico <- Man
          Flag_automatica      <- vector(                  length=length(IDsensore))
          Flag_manuale         <- vector(                  length=length(IDsensore))
          Autore               <- vector(mode="character", length=length(IDsensore))
          Data                 <- vector(                  length=length(IDsensore))
          Flag_manuale[]       <- "'M'"
          Flag_automatica[]    <- "'P'"
          Autore[]             <- "'aggiornamento_ftp'"
          Data[]               <- paste("'",as.character(Sys.time()),"'",sep="") 

# definizione data.frame 
          riga <- NULL
          riga <- data.frame( IDsensore            ,
                              Data_e_ora           ,
                              Misura               ,
                              Flag_manuale_DBunico ,
                              Flag_manuale         ,
                              Flag_automatica      ,
                              Autore               ,
                              Data                 )

# impongo di essere vector e non factor (altrimenti ho Levels: 'P')
          riga$IDsensore       <- as.vector(riga$IDsensore)
          riga$Data_e_ora      <- as.vector(riga$Data_e_ora)
          riga$Flag_automatica <- as.vector(riga$Flag_automatica)
          riga$Flag_manuale    <- as.vector(riga$Flag_manuale)

#_______________________________________________________
# FILTRO CON I TEST DI RANGE 
#______________________________________________________
#
          s_min <- soglia_min_da_DB(tip) 
          s_max <- soglia_max_da_DB(tip)
          if (tip==2) {
            soglia_notte <- NULL
            s_notte <- soglia_min_da_DB_RG_notte()
            soglia_notte<-s_notte$Rif1
          } 
          soglia_min <- NULL
          soglia_max <- NULL
          soglia_min <- s_min$Rif1
          soglia_max <- s_max$Rif2

# impongo flag "F" ai fuori soglia
          sotto_min <- NULL
          sotto_min <- which(riga$Misura < soglia_min)
     # se direzione del vento contemplo codici
          sopra_max <- NULL
          if(tip==8){
            sopra_max <- which(riga$Misura > soglia_max & riga$Misura != 777
                                                        & riga$Misura != 7777
                                                        & riga$Misura != 888
                                                        & riga$Misura != 999
                                                        & riga$Misura != 8888
                                                        & riga$Misura != 9999
                                                        & riga$Misura != 1000
                                                        & riga$Misura != 1001
                                                        & riga$Misura != 1002)
          }else{
            sopra_max <- which(riga$Misura > soglia_max)
          }
#
          sopra_notte<-NULL
          if (tip==2) {
            ore<-NULL
            ore<-as.numeric(substr(riga$Data_e_ora,13,14))
            sopra_notte<- which( (riga$Misura>soglia_notte) & (ore<=4 | ore>=23) & !is.na(riga$Misura) )
          }
      # sostituisco i fuori soglia 
          if(length(sotto_min)!=0)   riga$Flag_automatica[sotto_min[]]   <- "'F'"
          if(length(sopra_max)!=0)   riga$Flag_automatica[sopra_max[]]   <- "'F'"
          if(length(sopra_notte)!=0) riga$Flag_automatica[sopra_notte[]] <- "'F'"
# scrivi info su file di log
          cat("numero obs sotto soglia min= ",length(sotto_min),"\n",file=file_log,append=T)
#          if(length(sotto_min)!=0) {
#            cat( rbind( "# "  , riga$IDsensore[sotto_min[]],            " # ",
#                                riga$Data_e_ora[sotto_min[]],           " # ",
#                                riga$Misura[sotto_min[]],               " # ",
#                                riga$Flag_manuale_DBunico[sotto_min[]], " # ",
#                                riga$Flag_automatica[sotto_min[]],      " # ",
#                                riga$Flag_manuale[sotto_min[]],         " # ",
#                                riga$Autore[sotto_min[]],               " # ",
#                                riga$Data[sotto_min[]],                 " #\n" 
#                       ), file=file_log,append=T)
#          }
          cat("numero obs sopra soglia max: ",length(sopra_max),"\n",file=file_log,append=T)
#          if(length(sopra_max)!=0) {cat(riga$Misura[sopra_max[]],"\n",file=file_log,append=T)
#            cat( rbind( "# ", riga$IDsensore[sopra_max[]],            " # ",
#                                riga$Data_e_ora[sopra_max[]],           " # ",
#                                riga$Misura[sopra_max[]],               " # ",
#                                riga$Flag_manuale_DBunico[sopra_max[]], " # ",
#                                riga$Flag_automatica[sopra_max[]],      " # ",
#                                riga$Flag_manuale[sopra_max[]],         " # ",
#                                riga$Autore[sopra_max[]],               " # ",
#                                riga$Data[sopra_max[]],                 " #\n" 
#                       ), file=file_log,append=T)
#          }
          if (tip==2) {
            cat("numero obs sopra soglia notturna: ",length(sopra_notte),"\n",file=file_log,append=T)
#            if(length(sopra_notte)!=0) {cat(riga$Misura[sopra_notte[]],"\n",file=file_log,append=T)
#              cat( rbind( "# ", riga$IDsensore[sopra_notte[]],            " # ",
#                                  riga$Data_e_ora[sopra_notte[]],           " # ",
#                                  riga$Misura[sopra_notte[]],               " # ",
#                                  riga$Flag_manuale_DBunico[sopra_notte[]], " # ",
#                                  riga$Flag_automatica[sopra_notte[]],      " # ",
#                                  riga$Flag_manuale[sopra_notte[]],         " # ",
#                                  riga$Autore[sopra_notte[]],               " # ",
#                                  riga$Data[sopra_notte[]],                 " #\n" 
#                         ), file=file_log,append=T)
#            }
          }

#______________________________________________________
# SEGNALO DATI MANCANTI 
#______________________________________________________
#
#print("segnalo mancanti")
          dati_mancanti <- which(is.na(riga$Misura) == T)
#print(dati_mancanti)
#      if(riga$Misura[dati_mancanti[]] != NA) quit(status=1)
          if(length(dati_mancanti)!=0) {
            riga$Flag_automatica[dati_mancanti[]] <- "'M'"
# Controllo su valori "NA" nelle misure: inizio
#   (si ripete qui per verificare la consistenza nel passaggio dei vari array)
            cat ( " ATTENZIONE! (chck2) segnalo record/s nel file di input con NA nel campo \"Misura\" \n", file= file_log, append=T)
#            cat ( rbind( "# ",riga$IDsensore[dati_mancanti[]],            " # ",
#                                riga$Data_e_ora[dati_mancanti[]],           " # ",
#                                riga$Misura[dati_mancanti[]],               " # ",
#                                riga$Flag_manuale_DBunico[dati_mancanti[]], " # ",
#                                riga$Flag_manuale[dati_mancanti[]], " #\n",
#                       ) , file= file_log, append=T)
# Controllo su valori "NA" nelle misure: fine 
          }

##########  sovrascrivo la FLag_manuale_DBunico con -1 solo per i dati completi che passano i test, e con 
##########  5 per qualunque dato non valutato dall'operatore che non passa il test 
          test_passato <- which(riga$Flag_automatica=="'P'" & riga$Flag_manuale_DBunico == 0)
          test_fallito <- which(riga$Flag_automatica=="'F'" & (abs(riga$Flag_manuale_DBunico) < 5) )
         # cat("########## test #########\n",file=file_log,append=T)
         # cat(riga$IDsensore[test_passato] ,"\n",file=file_log,append=T)
         # cat(riga$Flag_automatica[] ,"\n",file=file_log,append=T)
         # cat(riga$Flag_manuale_DBunico[] ,"\n",file=file_log,append=T)
              riga$Flag_manuale_DBunico[test_passato] <- -1
              riga$Flag_manuale_DBunico[test_fallito] <- 5
         # cat(riga$Flag_manuale_DBunico[] ,"\n",file=file_log,append=T)
##########
          valori <-vector(length=length(riga$IDsensore))

          valori[] <- paste ( "(",
                              riga$IDsensore[]           , ","  , 
                              riga$Data_e_ora[]          , ","  , 
                              riga$Misura[]              , ","  , 
                              riga$Flag_manuale_DBunico[], ","  , 
                              riga$Flag_manuale[]        , ","  , 
                              riga$Flag_automatica[]     , ","  , 
                              riga$Autore[]              , ","  , 
                              riga$Data[]                , ",NULL)" ,sep="")

          stringa<-NULL
          stringa <- toString(valori[])

#sostituisco eventuali NA
          stringa <- gsub("NA","NULL",stringa)

#______________________________________________________
# SCRITTURA IN TAVOLA DI APPOGGIO
#______________________________________________________
          cat("inserimento righe in tavola di appoggio: ", file=file_log, append=T)
          cat(nome_tavola_appoggio,"\n", file=file_log, append=T)
#__________________________________
# svuoto tabella di appoggio
          query_cancella<-paste("delete from ",nome_tavola_appoggio," where IDsensore>0",sep="")
          cat ( " query cancella > ", query_cancella," \n", file= file_log, append=T)
#print(query_cancella)
          q_canc <- try(dbGetQuery(conn, query_cancella),silent=TRUE)
          if (inherits(q_canc,"try-error")) {
            cat(q_canc,"\n",file=file_log,append=T)
            quit(status=1)
          }
# preparo query e inserisco record
# attenzione, non c'e' controllo sul fatto che in ftp possono esserci piu' misure relative a stesso sensore/istante.
          query_inserimento_riga<-paste("insert ignore into ",nome_tavola_appoggio,
                                        " values ", stringa, sep="")
#          cat ( " query inserisci > ", query_inserimento_riga," \n", file= file_log, append=T)
          cat ( " effetuo query inserimento \n", file= file_log, append=T)
#cat(query_inserimento_riga,"\n", file=file_log, append=T)
          inserimento_riga <- try(dbGetQuery(conn,query_inserimento_riga),silent=TRUE)
          if (inherits(inserimento_riga,"try-error")) {
           cat(inserimento_riga,"\n",file=file_log,append=T)
           quit(status=1)
          }
#______________________________________________________
# SCRITTURA IN TAVOLA DQCinDBUNICO_dati per invio al REM
#______________________________________________________
# preparo query e inserisco record (ricordarsi che nella stringa "stringa" ci sono anche i dati incompleti 
# arrivati dal REM con flag 2 e 1 e che ora hanno flag -1 o 5)  
          cat("inserimento righe in tavola DQCinDBUNICO: ",nome_tavola_DQCinDBUNICO,"\n", file=file_log, append=T)

          query_inserimento_riga<-paste("insert into ",nome_tavola_DQCinDBUNICO,
                                        " values ", stringa,
                                        " on duplicate key update Autore=values(Autore), Data=values(Data)", sep="")
#         cat ( " query inserisci > ", query_inserimento_riga," \n", file= file_log, append=T)
          cat ( " effetua query inserimento \n", file= file_log, append=T)
# cat(query_inserimento_riga,"\n", file=file_log, append=T)
          inserimento_riga <- try(dbGetQuery(conn,query_inserimento_riga),silent=TRUE)
          if (inherits(inserimento_riga,"try-error")) {
            cat(inserimento_riga,"\n",file=file_log,append=T)
            quit(status=1)
          }

#______________________________________________________
# SCRITTURA IN TAVOLA ANNUALE
#______________________________________________________
# preparo query e inserisco record
          nome_tavola_anno    <- paste("M_",tipologia[tip],"_",anno,sep="")
          cat("inserimento righe in tavola annuale: ",nome_tavola_anno,"\n", file=file_log, append=T)

          query_inserimento_riga<-paste("insert into ",nome_tavola_anno,
                                        " values ", stringa,
                                        " on duplicate key update Autore=values(Autore), Data=values(Data)", sep="")
#          cat ( " query inserisci > ", query_inserimento_riga," \n", file= file_log, append=T)
          cat ( " effetua query inserimento \n", file= file_log, append=T)
# cat(query_inserimento_riga,"\n", file=file_log, append=T)
          inserimento_riga <- try(dbGetQuery(conn,query_inserimento_riga),silent=TRUE)
          if (inherits(inserimento_riga,"try-error")) {
            cat(inserimento_riga,"\n",file=file_log,append=T)
            quit(status=1)
          }
#_____________________________________________________
# SCRITTURA IN TAVOLA RECENTE 
#______________________________________________________
# preparo query e inserisco record
#print("inserimento righe in tavola recenti")
          nome_tavola_recente <- paste("M_",tipologia[tip],sep="")
          cat("inserimento righe in tavola recenti: ", nome_tavola_recente,"\n", file=file_log, append=T)
          query_inserimento_riga<-paste("insert into ",nome_tavola_recente,
                                        " values ", stringa,
                                        " on duplicate key update Autore=values(Autore), Data=values(Data)", sep="")
#          cat ( " query inserisci > ", query_inserimento_riga," \n", file= file_log, append=T)
          cat ( " effetua query inserimento \n", file= file_log, append=T)
#print(query_inserimento_riga)
          inserimento_riga <- try(dbGetQuery(conn,query_inserimento_riga),silent=TRUE)
          if (inherits(inserimento_riga,"try-error")) {
            cat(inserimento_riga,"\n",file=file_log,append=T)
            quit(status=1)
          }
#_____________________________________________________
# cerco record per cui è cambiata la misura
#_____________________________________________________
#          query_misura <- paste("select * from AUX_table where AUX_table.Misura not in (select ",
#                                nome_tavola_anno, ".Misura from ",nome_tavola_anno, 
#                                " where ", nome_tavola_anno, ".IDsensore=AUX_table.IDsensore and ",
#                                nome_tavola_anno,".Data_e_ora=AUX_table.Data_e_ora)", 
#                                sep="")

          query_misura <- paste("select AUX_table.IDsensore, ",
                                       "AUX_table.Data_e_ora, ",
                                       "AUX_table.Misura,",
                                       "AUX_table.Flag_manuale_DBunico, ",
                                       "AUX_table.Flag_manuale,",
                                       "AUX_table.Flag_automatica,",
                                       "AUX_table.Autore,",
                                       "AUX_table.Data from AUX_table,", nome_tavola_anno, 
                                       " where ( ",
                                                "(AUX_table.Misura!=",nome_tavola_anno,".Misura) or ",
                                                "(AUX_table.Misura IS NULL and ",nome_tavola_anno,".Misura IS NOT NULL) or ",
                                                "(AUX_table.Misura IS NOT NULL and ",nome_tavola_anno,".Misura IS NULL)",
                                            "  ) and AUX_table.IDsensore=",nome_tavola_anno,".IDsensore ",
                                                "and AUX_table.Data_e_ora=",nome_tavola_anno,".Data_e_ora",sep="")
#print(query_misura)
#
          q_misura <- NULL
          q_misura <- try(dbGetQuery(conn,query_misura),silent=TRUE)
          if (inherits(q_misura,"try-error")) {
            cat(q_misura,"\n",file=file_log,append=T)
            quit(status=1)
          }
# Controllo su valori con il campo \"Misura\" in input diverso da record in tabella: inizio 
          if ( length(q_misura$Misura)!=0 ) {
            cat ( " ATTENZIONE! (chck3) segnalo record/s nel file di input con il campo \"Misura\" diverso da record in tabella\n",
                  file= file_log, append=T)
#            cat ( rbind( "# ",q_misura$IDsensore,            " # ",
#                                q_misura$Data_e_ora,           " # ",
#                                q_misura$Misura,               " # ",
#                                q_misura$Flag_manuale_DBunico, " #\n ",
#                                q_misura$Flag_manuale, " #\n",
#                       ) , file= file_log, append=T)
#           cat( "fuori")
          }
# Controllo su valori con il campo \"Misura\" in input diverso da record in tabella: fine 
#__________________________________________________________________________
# CANCELLO DA DQC EVENTUALI SEGNALAZIONI PRECEDENTI di qualunque test !?! 
#__________________________________________________________________________
          nome_tavola_DQC     <- paste("M_",tipologia[tip],"DQC",sep="")

          if(length(q_misura$IDsensore!=0)){
            cat ( " (chck4) queries corrispondenti per eliminare record da tabella: ",
                  nome_tavola_DQC, "\n", file= file_log, append=T)
            uu <- 1
            while( uu <= length(q_misura$IDsensore) ){
              query_delete <- paste( "delete from ", nome_tavola_DQC, 
                                     " where IDsensore=", q_misura$IDsensore[uu], 
                                     " and Data_e_ora='", q_misura$Data_e_ora[uu],"'",sep="")
#             cat ( " count query > ",uu," ", query_delete," \n", file= file_log, append=T)
              q_delete <- try(dbGetQuery(conn,query_delete),silent=TRUE)
              if (inherits(q_delete,"try-error")) {
                cat(q_delete,"\n",file=file_log,append=T)
                quit(status=1)
              }
              uu <- uu + 1
            }
          }
#__________________________________________________________________________________________________
# UPDATE dei campi (Misura, Flag_manDBunico, Flag_auto) nelle tabelle dei dati (annuale e recente)
###  aggiornamento 2015: update anche della tabella DQCinDBUNICO
#__________________________________________________________________________________________________

## DQCinDBUNICO
          if(length(q_misura$IDsensore!=0)){
            cat ( " (chck5) queries corrispondenti per update record da tabella: ",
                  nome_tavola_DQCinDBUNICO, "\n", file= file_log, append=T)
            uu <- 1
            while( uu <= length(q_misura$IDsensore) ){
              if (!is.na(q_misura$Misura[uu])) {
              #  query_update <- paste("update ", nome_tavola_DQCinDBUNICO, 
              #                        " set Misura=", q_misura$Misura[uu], 
              #                        ", Flag_manuale_DBunico=", q_misura$Flag_manuale_DBunico[uu],
              #                        ", Flag_manuale ='"  , q_misura$Flag_manuale[uu],"'",
              #                        ", Flag_automatica='",q_misura$Flag_automatica[uu], 
              #                        "' where IDsensore=" , q_misura$IDsensore[uu], 
              #                        " and Data_e_ora='"  , q_misura$Data_e_ora[uu],"'",
              #                        sep="")
                query_replace <- paste("replace into ", nome_tavola_DQCinDBUNICO, 
                                      " values (",
                                      q_misura$IDsensore[uu],",", 
                                      " '", q_misura$Data_e_ora[uu],"',",
                                      q_misura$Misura[uu],",", 
                                      q_misura$Flag_manuale_DBunico[uu],",",
                                      "'", q_misura$Flag_manuale[uu],"',",
                                      "'",q_misura$Flag_automatica[uu],"',", 
                                      "'aggiornamento_ftp',",
                                      "'",as.character(Sys.time()),"',NULL)",
                                      sep="")
              } else {
               cat ( " misura nulla  \n", file= file_log, append=T)
               # query_update <- paste("update ", nome_tavola_DQCinDBUNICO, 
               #                       " set Misura=NULL", 
               #                       ", Flag_manuale_DBunico=", q_misura$Flag_manuale_DBunico[uu],
               #                       ", Flag_manuale ='", q_misura$Flag_manuale[uu],"'",
               #                       ", Flag_automatica='",q_misura$Flag_automatica[uu], 
               #                       "' where IDsensore=", q_misura$IDsensore[uu], 
               #                       " and Data_e_ora='", q_misura$Data_e_ora[uu],"'",
               #                       sep="")
                query_replace <- paste("replace into ", nome_tavola_DQCinDBUNICO, 
                                      " values (",
                                      q_misura$IDsensore[uu],",", 
                                      " '", q_misura$Data_e_ora[uu],"',",
                                      "NULL ,", 
                                      q_misura$Flag_manuale_DBunico[uu],",",
                                      "'", q_misura$Flag_manuale[uu],"',",
                                      "'",q_misura$Flag_automatica[uu],"',", 
                                      "'aggiornamento_ftp',",
                                      "'",as.character(Sys.time()),"',NULL)",
                                      sep="")
              }
             cat ( " count query REPLACE> ",uu," ", query_replace," \n", file= file_log, append=T)
              q_replace <- try(dbGetQuery(conn,query_replace),silent=TRUE)
              if (inherits(q_replace,"try-error")) {
                cat(q_replace,"\n",file=file_log,append=T)
                quit(status=1)
              }
              uu <- uu + 1
            }
          }

## ANNUALE
          if(length(q_misura$IDsensore!=0)){
            cat ( " (chck5) queries corrispondenti per update record da tabella: ",
                  nome_tavola_anno, "\n", file= file_log, append=T)
            uu <- 1
            while( uu <= length(q_misura$IDsensore) ){
              if (!is.na(q_misura$Misura[uu])) {
                query_update <- paste("update ", nome_tavola_anno, 
                                      " set Misura=", q_misura$Misura[uu], 
                                      ", Flag_manuale_DBunico=", q_misura$Flag_manuale_DBunico[uu],
                                      ", Flag_manuale ='"  , q_misura$Flag_manuale[uu],"'",
                                      ", Flag_automatica='",q_misura$Flag_automatica[uu], 
                                      "' where IDsensore=" , q_misura$IDsensore[uu], 
                                      " and Data_e_ora='"  , q_misura$Data_e_ora[uu],"'",
                                      sep="")
              } else {
                query_update <- paste("update ", nome_tavola_anno, 
                                      " set Misura=NULL", 
                                      ", Flag_manuale_DBunico=", q_misura$Flag_manuale_DBunico[uu],
                                      ", Flag_manuale ='", q_misura$Flag_manuale[uu],"'",
                                      ", Flag_automatica='",q_misura$Flag_automatica[uu], 
                                      "' where IDsensore=", q_misura$IDsensore[uu], 
                                      " and Data_e_ora='", q_misura$Data_e_ora[uu],"'",
                                      sep="")
              }
#             cat ( " count query > ",uu," ", query_update," \n", file= file_log, append=T)
              q_update <- try(dbGetQuery(conn,query_update),silent=TRUE)
              if (inherits(q_update,"try-error")) {
                cat(q_update,"\n",file=file_log,append=T)
                quit(status=1)
              }
              uu <- uu + 1
            }
          }

## RECENTE
          if(length(q_misura$IDsensore!=0)){
            cat ( " (chck6) queries corrispondenti per update record da tabella: ",
                  nome_tavola_recente, "\n", file= file_log, append=T)
            uu <- 1
            while( uu <= length(q_misura$IDsensore) ){
              if (!is.na(q_misura$Misura[uu])) {
                query_update <- paste("update ", nome_tavola_recente, 
                                      " set Misura=", q_misura$Misura[uu],
                                      ", Flag_manuale_DBunico=", q_misura$Flag_manuale_DBunico[uu],
                                      ", Flag_manuale ='", q_misura$Flag_manuale[uu],"'",
                                      ", Flag_automatica='",q_misura$Flag_automatica[uu], 
                                      "' where IDsensore=", q_misura$IDsensore[uu],
                                      " and Data_e_ora='", q_misura$Data_e_ora[uu],"'",
                                      sep="")
              } else {
                query_update <- paste("update ", nome_tavola_anno, 
                                      " set Misura=NULL", 
                                      ", Flag_manuale_DBunico=", q_misura$Flag_manuale_DBunico[uu],
                                      ", Flag_manuale ='", q_misura$Flag_manuale[uu],"'",
                                      ", Flag_automatica='",q_misura$Flag_automatica[uu], 
                                      "' where IDsensore=", q_misura$IDsensore[uu], 
                                      " and Data_e_ora='", q_misura$Data_e_ora[uu],"'",
                                      sep="")
              }
#             cat ( " count query > ",uu," ", query_update," \n", file= file_log, append=T)
              q_update <- try(dbGetQuery(conn,query_update),silent=TRUE)
              if (inherits(q_update,"try-error")) {
                cat(q_update,"\n",file=file_log,append=T)
                quit(status=1)
              }
              uu <- uu + 1
            }
          }
#__________________________________________________________
# cerco record per cui è cambiata la Flag manuale_DBunico:
#__________________________________________________________
#          query_flag <- paste("select * from AUX_table where AUX_table.Flag_manuale_DBunico not in (select ",
#                              nome_tavola_anno, ".Flag_manuale_DBunico from ",nome_tavola_anno, 
#                              " where ", nome_tavola_anno, ".IDsensore=AUX_table.IDsensore and ",
#                              nome_tavola_anno,".Data_e_ora=AUX_table.Data_e_ora)", 
#                              sep="")
          query_flag <- paste("select AUX_table.IDsensore, ",
                                       "AUX_table.Data_e_ora, ",
                                       "AUX_table.Misura,",
                                       "AUX_table.Flag_manuale_DBunico, ",
                                       "AUX_table.Flag_manuale,",
                                       "AUX_table.Flag_automatica,",
                                       "AUX_table.Autore,",
                                       "AUX_table.Data from AUX_table,", nome_tavola_anno, 
                                       " where ( ",
                                                "(AUX_table.Flag_manuale_DBunico!=",nome_tavola_anno,".Flag_manuale_DBunico) or ",
                                                "(AUX_table.Flag_manuale_DBunico IS NULL and ",nome_tavola_anno,".Flag_manuale_DBunico IS NOT NULL) or ",
                                                "(AUX_table.Flag_manuale_DBunico IS NOT NULL and ",nome_tavola_anno,".Flag_manuale_DBunico IS NULL)",
                                            "  ) and AUX_table.IDsensore=",nome_tavola_anno,".IDsensore ",
                                                "and AUX_table.Data_e_ora=",nome_tavola_anno,".Data_e_ora",sep="")

          q_flag <- try(dbGetQuery(conn,query_flag),silent=TRUE)
          if (inherits(q_flag,"try-error")) {
            cat(q_flag,"\n",file=file_log,append=T)
            quit(status=1)
          }
# Controllo su valori con il campo \"Flag_manuale_DBunico\" in input diverso da record in tabella: inizio 
          if ( length(q_flag$Misura)!=0 ) {
            cat ( " ATTENZIONE! (chck7) segnalo record/s nel file di input con il campo \"Flag_maunale_DBunico\" diverso da record in tabella\n",
                  file= file_log, append=T)
#            cat ( rbind( "# ",q_flag$IDsensore,            " # ",
#                                q_flag$Data_e_ora,           " # ",
#                                q_flag$Misura,               " # ",
#                                q_flag$Flag_manuale_DBunico, " # ",
#                                q_flag$Flag_manuale, " #\n"
#                       ) , file= file_log, append=T)
          }
# Controllo su valori con il campo \"Flag_manuale_DBunico\" in input diverso da record in tabella: fine 

# sono sicura che è cambiata SOLO la flag_manDB,
# perchè se fosse cambiata anche la misura le righe di codice
# qua sopra avrebbero riallineato le tabelle con un update sulle tabelle dei dati.
# Quindi: update solo del campo Flag_manDBunico.

#_____________________________________________________________________________
# UPDATE del campo Flag_manDBunico nelle tabelle dei dati (annuale e recente)
###  aggiornamento 2015: update anche della tabella DQCinDBUNICO
###  aggiornamento 2015: update in tutte e tre le tabelle anche delle Flag Manuali DBmeteo 
#_____________________________________________________________________________

## DQCinDBUNICO
# M&M cambiata questa query in una delete del record per evitare che la invalidazione manuale fatta
# nel REM torni al REM che già lo sa.
          if(length(q_flag$IDsensore!=0)){
            cat ( " (chck8) queries corrispondenti per update record da tabella: ",
                  nome_tavola_DQCinDBUNICO, "\n", file= file_log, append=T)
            uu <- 1
            while( uu <= length(q_flag$IDsensore) ){
            #
             if (abs(q_flag$Flag_manuale_DBunico[uu]) < 6) {
             # se il cambio della flag dal rem è dovuta al completamento di un dato (nessun intervento manuale)
             # allora faccio un update come nelle tabelle dati
               if(q_flag$Flag_manuale_DBunico[uu] %in% c(-1,0,1,2,5)){flag="M"}
               if(q_flag$Flag_manuale_DBunico[uu] %in% c(100,101,102)){flag="E"}
               if(q_flag$Flag_manuale_DBunico[uu] %in% c(-100,-101,-102)){flag="G"}
               #query_update <- paste("update ", nome_tavola_DQCinDBUNICO,
               #                      " set Flag_manuale_DBunico=", q_flag$Flag_manuale_DBunico[uu],
               #                      ", Flag_manuale='", flag, "'",
               #                      " where IDsensore=", q_flag$IDsensore[uu],
               #                      " and Data_e_ora='", q_flag$Data_e_ora[uu],"'",
               #                      sep="")
               query_replace <- paste("replace into ", nome_tavola_DQCinDBUNICO, 
                                      " values (",
                                      q_flag$IDsensore[uu],",", 
                                      " '", q_flag$Data_e_ora[uu],"',",
                                      q_flag$Misura[uu],",", 
                                      q_flag$Flag_manuale_DBunico[uu],",",
                                      "'", flag,"',",
                                      "'",q_flag$Flag_automatica[uu],"',", 
                                      "'aggiornamento_ftp',",
                                      "'",as.character(Sys.time()),"',NULL)",
                                      sep="")
              q_replace<- try(dbGetQuery(conn,query_replace),silent=TRUE)
              if (inherits(q_replace,"try-error")) {
                cat(q_replace,"\n",file=file_log,append=T)
                quit(status=1)
              }
             }else{
             # se invece il cambio della flag dal rem è dovuta ad un giudizio manuale 
             # allora faccio una delete perchè l'informazione è già nel rem e non devo rimandarla
              query_delete <- paste("delete from ", nome_tavola_DQCinDBUNICO,
                                    " where IDsensore=", q_flag$IDsensore[uu],
                                    " and Data_e_ora='", q_flag$Data_e_ora[uu],"'",
                                    sep="")
#              cat ( " count query > ",uu," ", query_delete," \n",
#                    file= file_log, append=T)
              q_delete <- try(dbGetQuery(conn,query_delete),silent=TRUE)
              if (inherits(q_delete,"try-error")) {
                cat(q_delete,"\n",file=file_log,append=T)
                quit(status=1)
              }
             }
             uu <- uu + 1
            }
          } 
## ANNUALE
          if(length(q_flag$IDsensore!=0)){
            cat ( " (chck8) queries corrispondenti per update record da tabella: ",
                  nome_tavola_anno, "\n", file= file_log, append=T)
            uu <- 1
            while( uu <= length(q_flag$IDsensore) ){
              if(q_flag$Flag_manuale_DBunico[uu] %in% c(-1,0,1,2,5)){flag="M"}
              if(q_flag$Flag_manuale_DBunico[uu] %in% c(100,101,102)){flag="E"}
              if(q_flag$Flag_manuale_DBunico[uu] %in% c(-100,-101,-102)){flag="G"}
              query_update <- paste("update ", nome_tavola_anno, 
                                    " set Flag_manuale_DBunico=", q_flag$Flag_manuale_DBunico[uu],  
                                    ", Flag_manuale='", flag, "'",
                                    " where IDsensore=", q_flag$IDsensore[uu],
                                    " and Data_e_ora='", q_flag$Data_e_ora[uu],"'",
                                    sep="")
#              cat ( " count query > ",uu," ", query_update," \n",
#                    file= file_log, append=T)
              q_update <- try(dbGetQuery(conn,query_update),silent=TRUE)
              if (inherits(q_update,"try-error")) {
                cat(q_update,"\n",file=file_log,append=T)
                quit(status=1)
              }

              uu <- uu + 1
            }
          } 

## RECENTE
          if(length(q_flag$IDsensore!=0)){
            cat ( " (chck9) queries corrispondenti per update record da tabella: ",
                  nome_tavola_recente, "\n", file= file_log, append=T)
            uu <- 1
            while( uu <= length(q_flag$IDsensore) ){
              if(q_flag$Flag_manuale_DBunico[uu] %in% c(-1,0,1,2,5)){flag="M"}
              if(q_flag$Flag_manuale_DBunico[uu] %in% c(100,101,102)){flag="E"}
              if(q_flag$Flag_manuale_DBunico[uu] %in% c(-100,-101,-102)){flag="G"}
              query_update <- paste("update ", nome_tavola_recente, 
                                    " set Flag_manuale_DBunico=", q_flag$Flag_manuale_DBunico[uu], 
                                    ", Flag_manuale='", flag, "'",
                                    " where IDsensore=", q_flag$IDsensore[uu],
                                    " and Data_e_ora='", q_flag$Data_e_ora[uu],"'",
                                    sep="")
#              cat ( " count query > ",uu," ", query_update," \n", file= file_log, append=T)
              q_update <- try(dbGetQuery(conn,query_update),silent=TRUE)
              if (inherits(q_update,"try-error")) {
                cat(q_update,"\n",file=file_log,append=T)
                quit(status=1)
              }
              uu <- uu + 1
            }
          }
#_______________________________________________________
# SCRIVO IN TABELLA DQC
#______________________________________________________
#
# sopra max
          if(length(sopra_max)!=0){
            cat ( " (chck10-sopra_max) queries corrispondenti per insert record in tabella: ",
                  nome_tavola_DQC, "\n", file= file_log, append=T)
            pp <- 1
            while( pp <= length(sopra_max) ){
              stringa_per_DQC <- paste("(",
                                       riga$IDsensore[sopra_max[pp]],
                                       ",", riga$Data_e_ora[sopra_max[pp]],",'",
                                       test,"','",fallimento,
                                       "','aggiornamento_ftp', ",
                                       paste("'",as.character(Sys.time()),"',NULL)",sep=""),
                                       sep="")
              query_insert <- paste ("insert into " , nome_tavola_DQC, 
                                     " values " , stringa_per_DQC, 
                                     " on duplicate key update Result=values(Result), Autore=values(Autore), Data=values(Data)", sep="")
#              cat ( " count query > ",pp," ", query_insert," \n", file= file_log, append=T)
              q_insert <- try(dbGetQuery(conn, query_insert),silent=TRUE)
              if (inherits(q_insert,"try-error")){
                quit(status=1)
              }
              pp <- pp + 1
            }
          }

# sotto min
          if(length(sotto_min)!=0){
            cat ( " (chck11-sotto_min) queries corrispondenti per insert record in tabella: ",
                  nome_tavola_DQC, "\n", file= file_log, append=T)
            pp <- 1
            while( pp <= length(sotto_min) ){
              stringa_per_DQC <- paste("(",
                                       riga$IDsensore[sotto_min[pp]],
                                       ",", riga$Data_e_ora[sotto_min[pp]],",'",
                                       test,"','",fallimento,
                                       "','aggiornamento_ftp', ",
                                       paste("'",as.character(Sys.time()),"',NULL)",sep=""),
                                       sep="")
              query_insert <- paste ("insert into " , nome_tavola_DQC,
                                     " values " , stringa_per_DQC,
                                     " on duplicate key update Result=values(Result), Autore=values(Autore), Data=values(Data)", sep="")
#              cat ( " count query > ",pp," ", query_insert," \n", file= file_log, append=T)
              q_insert <- try(dbGetQuery(conn, query_insert),silent=TRUE)
              if (inherits(q_insert,"try-error")){
                quit(status=1)
              }
              pp <- pp + 1
            }
          }

# sopra notte
          if(length(sopra_notte)!=0){
            cat ( " (chck12-sopra_notte) queries corrispondenti per insert record in tabella: ",
                  nome_tavola_DQC, "\n", file= file_log, append=T)
            pp <- 1
            while( pp <= length(sopra_notte) ){
              stringa_per_DQC <- paste("(",
                                           riga$IDsensore[sopra_notte[pp]], ",",
                                           riga$Data_e_ora[sopra_notte[pp]],",",
                                           "'P1b'",
                                           ",'",fallimento,"',",
                                           "'aggiornamento_ftp',",
                                           "'",as.character(Sys.time()),"',NULL",
                                       ")", sep="")
              query_insert <- paste ("insert into " , nome_tavola_DQC,
                                     " values " , stringa_per_DQC,
                                     " on duplicate key update Result=values(Result), Autore=values(Autore), Data=values(Data)", sep="")
#              cat ( " count query > ",pp," ", query_insert," \n", file= file_log, append=T)
              q_insert <- try(dbGetQuery(conn, query_insert),silent=TRUE)
              if (inherits(q_insert,"try-error")){
                quit(status=1)
              }
              pp <- pp + 1
            }
          }

        } # fine esistenza dati da inserire relativi all'anno
      } # fine ciclo for sulle annate 

############  CANCELLO DA TAVOLA RECENTI EVENTUALI RECORD RELATIVI A ISTANTI PRECEDENTI AI 15 GIORNI 
      query_cancella_riga<-paste("delete from ",nome_tavola_recente ," where Data_e_ora<'",Sys.Date()-15,"'", sep="")
      q_canc_riga <- try(dbGetQuery(conn, query_cancella_riga),silent=TRUE)
      if (inherits(q_canc_riga,"try-error")) {
        #cat(q_canc_riga,"\n",file=file_log,append=T)
        quit(status=1)
      }
############  CANCELLO DA TAVOLA RECENTI EVENTUALI RECORD RELATIVI A ISTANTI SUCCESSIVI AL PRESENTE (NEL FUTURO) 
      query_cancella_riga<-paste("delete from ",nome_tavola_recente ," where Data_e_ora>'",Sys.Date()+1,"'", sep="")
      q_canc_riga <- try(dbGetQuery(conn, query_cancella_riga),silent=TRUE)
      if (inherits(q_canc_riga,"try-error")) {
        #cat(q_canc_riga,"\n",file=file_log,append=T)
        quit(status=1)
      }
###########
#_________________________________________________________
# ELIMINA EVENTUALI SEGNALAZIONI "SOLITARIE" DEL TEST T1a 
#_________________________________________________________
      cat ( " elimina le segnalazioni T1a solitarie della tipologia > ", tipologia[tip]," INIZIO \n", file= file_log, append=T)
      cat ( " scrematura dei sensori con 1 sola segnalazione T1a \n", file= file_log, append=T)
# PRIMA cerco ed elimino solo i sensori che appaiono 1 volta sola in tutta la tabella
# M...DQC per il test T1a
      query_elimina_T1a<-paste("select IDsensore,count(Data_e_ora) from M_",tipologia[tip],"DQC",
                               "  where Test=\"T1a\" and Data_e_ora>'",Sys.Date()-20,
                               "'  group by IDsensore",sep="")
#     cat ( " query > ", query_elimina_T1a,"\n", file= file_log, append=T)
      q_elimina_T1a <- try(dbGetQuery(conn, query_elimina_T1a),silent=TRUE)
      if (inherits(q_elimina_T1a,"try-error")) {
        #cat(q_canc_riga,"\n",file=file_log,append=T)
        quit(status=1)
      }
      aux_IDsens_T1a<-NULL
      aux_IDsens_T1a<-q_elimina_T1a$IDsensore
      length_aux_IDsens_T1a<-length(aux_IDsens_T1a)
      length_aux<-0
      aux<-NULL
      length_aux<-length(q_elimina_T1a$IDsensore[q_elimina_T1a$count==1])
      aux<-which(q_elimina_T1a$count==1)
      if (length_aux > 0) {
        pp<-1
        while(pp<=length_aux) {
          query_del_T1a<-paste("delete from M_",tipologia[tip],"DQC where ",
                               "Data_e_ora>'",Sys.Date()-20,"' ",
                               "and IDsensore=",q_elimina_T1a$IDsensore[aux[pp]],
                               sep="")
#         cat ( " cont query > ", pp, " ", query_del_T1a,"\n", file= file_log, append=T)
          q_del_T1a <- try(dbGetQuery(conn, query_del_T1a),silent=TRUE)
          if (inherits(q_del_T1a,"try-error")) {
            #cat(q_canc_riga,"\n",file=file_log,append=T)
            quit(status=1)
          }
          pp<-pp+1
        }
      }
# POI cerco ed elimino i sensori che appaiono anche piu' 1 volta sola in tutta la tabella
# M...DQC per il test T1a ma con segnalazioni "solitarie"
      cat ( " scrematura dei sensori con piu' di 1 segnalazione T1a \n", file= file_log, append=T)
      q_elimina_T1a <- NULL 
      query_elimina_T1a<-paste("select * from M_", tipologia[tip],"DQC",
                               " where Test=\"T1a\" and Data_e_ora>'",Sys.Date()-20,
                               "' order by IDsensore, Data_e_ora", sep="")
#     cat ( " query > ", query_elimina_T1a,"\n", file= file_log, append=T)
      q_elimina_T1a <- try(dbGetQuery(conn, query_elimina_T1a),silent=TRUE)
      if (inherits(q_elimina_T1a,"try-error")) {
        #cat(q_canc_riga,"\n",file=file_log,append=T)
        quit(status=1)
      }
      ii<-1
      while (ii<=length_aux_IDsens_T1a) {
        length_aux<-length(q_elimina_T1a$IDsensore[q_elimina_T1a$IDsensore==aux_IDsens_T1a[ii]])
        cat ( " elaborazione: sensore/#record in tabella > ",aux_IDsens_T1a[ii]," ",length_aux,"\n", file= file_log, append=T)
        if (length_aux>0) {
          pp<-1
          aux_T1a_01<-q_elimina_T1a$IDsensore[q_elimina_T1a$IDsensore==aux_IDsens_T1a[ii]]
          aux_T1a_02<-q_elimina_T1a$Data_e_ora[q_elimina_T1a$IDsensore==aux_IDsens_T1a[ii]]
          while(pp<=length_aux){
            hour_diff_prev<-100.
            hour_diff_next<-100.
            if ( (pp-1)>0 ) hour_diff_prev <- as.numeric( as.POSIXct(aux_T1a_02[pp],"UTC")
                                                         -as.POSIXct(aux_T1a_02[pp-1],"UTC") )
            if ( (pp+1)<=length_aux ) hour_diff_next <- as.numeric( as.POSIXct(aux_T1a_02[pp+1],"UTC")
                                                                   -as.POSIXct(aux_T1a_02[pp],"UTC") )
            if ( (hour_diff_prev!=1) & (hour_diff_next!=1)  ) {
              query_del_T1a<-paste("delete from M_",tipologia[tip],"DQC where ",
                                   "Data_e_ora='",aux_T1a_02[pp],"' ",
                                   "and IDsensore=",aux_T1a_01[pp],
                                   sep="")
#             cat ( " query > ", query_del_T1a,"\n", file= file_log, append=T)
              q_del_T1a <- try(dbGetQuery(conn, query_del_T1a),silent=TRUE)
              if (inherits(q_del_T1a,"try-error")) {
                #cat(q_canc_riga,"\n",file=file_log,append=T)
                quit(status=1)
              }
            }
            pp<-pp+1
          }
        }
        ii<-ii+1
      }
      cat ( " elimina le segnalazioni T1a solitarie della tipologia > ", tipologia[tip]," FINE \n", file= file_log, append=T)
#_________________________________________________________
# ELIMINA EVENTUALI SEGNALAZIONI "SOLITARIE" DEL TEST T1b (solo per i Pluviometri) 
#_________________________________________________________
      if (tipologia[tip]=="Pluviometri") {
        cat ( " elimina le segnalazioni T1b solitarie della tipologia > ", tipologia[tip]," INIZIO \n", file= file_log, append=T)
        cat ( " scrematura dei sensori con 1 sola segnalazione T1b \n", file= file_log, append=T)
# PRIMA cerco ed elimino solo i sensori che appaiono 1 volta sola in tutta la tabella
# M...DQC per il test T1b
        query_elimina_T1b<-paste("select IDsensore,count(Data_e_ora) from M_",tipologia[tip],"DQC",
                                 "  where Test=\"T1b\" and Data_e_ora>'",Sys.Date()-20,
                                 "'  group by IDsensore",sep="")
#       cat ( " query > ", query_elimina_T1b,"\n", file= file_log, append=T)
        q_elimina_T1b <- try(dbGetQuery(conn, query_elimina_T1b),silent=TRUE)
        if (inherits(q_elimina_T1b,"try-error")) {
          #cat(q_canc_riga,"\n",file=file_log,append=T)
          quit(status=1)
        }
        aux_IDsens_T1b<-NULL
        aux_IDsens_T1b<-q_elimina_T1b$IDsensore
        length_aux_IDsens_T1b<-length(aux_IDsens_T1b)
        length_aux<-0
        aux<-NULL
        length_aux<-length(q_elimina_T1b$IDsensore[q_elimina_T1b$count==1])
        aux<-which(q_elimina_T1b$count==1)
        if (length_aux > 0) {
          pp<-1
          while(pp<=length_aux) {
            query_del_T1b<-paste("delete from M_",tipologia[tip],"DQC where ",
                                 "Data_e_ora>'",Sys.Date()-20,"' ",
                                 "and IDsensore=",q_elimina_T1b$IDsensore[aux[pp]],
                                 sep="")
#           cat ( " cont query > ", pp, " ", query_del_T1b,"\n", file= file_log, append=T)
              q_del_T1b <- try(dbGetQuery(conn, query_del_T1b),silent=TRUE)
            if (inherits(q_del_T1b,"try-error")) {
              #cat(q_canc_riga,"\n",file=file_log,append=T)
              quit(status=1)
            }
            pp<-pp+1
          }
        }
# POI cerco ed elimino i sensori che appaiono anche piu' 1 volta sola in tutta la tabella
# M...DQC per il test T1b ma con segnalazioni "solitarie"
        cat ( " scrematura dei sensori con piu' di 1 segnalazione T1b \n", file= file_log, append=T)
        q_elimina_T1b <- NULL 
        query_elimina_T1b<-paste("select * from M_", tipologia[tip],"DQC",
                                 " where Test=\"T1b\" and Data_e_ora>'",Sys.Date()-20,
                                 "' order by IDsensore, Data_e_ora", sep="")
#        cat ( " query > ", query_elimina_T1b,"\n", file= file_log, append=T)
        q_elimina_T1b <- try(dbGetQuery(conn, query_elimina_T1b),silent=TRUE)
        if (inherits(q_elimina_T1b,"try-error")) {
          #cat(q_canc_riga,"\n",file=file_log,append=T)
          quit(status=1)
        }
        ii<-1
        while (ii<=length_aux_IDsens_T1b) {
          length_aux<-length(q_elimina_T1b$IDsensore[q_elimina_T1b$IDsensore==aux_IDsens_T1b[ii]])
          cat ( " elaborazione: sensore/#record in tabella > ",aux_IDsens_T1b[ii]," ",length_aux,"\n", file= file_log, append=T)
          if (length_aux>0) {
            pp<-1
            aux_T1b_01<-q_elimina_T1b$IDsensore[q_elimina_T1b$IDsensore==aux_IDsens_T1b[ii]]
            aux_T1b_02<-q_elimina_T1b$Data_e_ora[q_elimina_T1b$IDsensore==aux_IDsens_T1b[ii]]
            while(pp<=length_aux){
              hour_diff_prev<-100.
              hour_diff_next<-100.
              if ( (pp-1)>0 ) hour_diff_prev <- as.numeric( as.POSIXct(aux_T1b_02[pp],"UTC")
                                                           -as.POSIXct(aux_T1b_02[pp-1],"UTC") )
              if ( (pp+1)<=length_aux ) hour_diff_next <- as.numeric( as.POSIXct(aux_T1b_02[pp+1],"UTC")
                                                                     -as.POSIXct(aux_T1b_02[pp],"UTC") )
              if ( (hour_diff_prev!=1) & (hour_diff_next!=1)  ) {
                query_del_T1b<-paste("delete from M_",tipologia[tip],"DQC where ",
                                     "Data_e_ora='",aux_T1b_02[pp],"' ",
                                     "and IDsensore=",aux_T1b_01[pp],
                                     sep="")
#               cat ( " query > ", query_del_T1b,"\n", file= file_log, append=T)
#MM                q_del_T1b <- try(dbGetQuery(conn, query_del_T1b),silent=TRUE)
#MM                if (inherits(q_del_T1b,"try-error")) {
#MM                  #cat(q_canc_riga,"\n",file=file_log,append=T)
#MM                  quit(status=1)
#MM                }
              }
              pp<-pp+1
            }
          }
          ii<-ii+1
        }
        cat ( " elimina le segnalazioni T1b solitarie della tipologia > ", tipologia[tip]," FINE \n", file= file_log, append=T)
      }

    } # fine "se ci sono record per questa tipologia"

    tip <- tip + 1
  }  # fine ciclo sulle tipologie di sensori

#__________________________________
#zippo file di dati inseriti 
  comando<-paste("gzip ", nomefile[i], sep=" ")
  print(paste("@@@",comando,sep=""))
  rizzippo <-try(readLines(pipe(comando)),silent=TRUE)
  if (inherits(rizzippo,"try-error")) {
    cat(rizzippo,"\n",file=file_log,append=T)
    quit(status=1)
  }
#__________________________________

  i <- i + 1  #fine if sul file

} 

#___________________________________________________
#    DISCONNESSIONE DAL DB
#___________________________________________________

# chiudo db
cat ( "chiudo DB \n" , file = file_log , append = TRUE )
dbDisconnect(conn)
rm(conn)
dbUnloadDriver(drv)

cat ( "PROGRAMMA ESEGUITO CON SUCCESSO alle ", date()," \n" , file = file_log , append = TRUE )
quit(status=0)



