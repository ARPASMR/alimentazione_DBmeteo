FROM arpasmr/r-base
COPY . /usr/local/src/myscripts
# COPY .vimrc / #funziona così?
WORKDIR /usr/local/src/myscripts
RUN apt-get install -y ncftp
RUN chmod a+x launcher.sh
RUN chmod a+x getcsv_from_ftp_rem2.sh
RUN mkdir /usr/local/src/myscripts/data
CMD ["./launcher.sh"]
