FROM arpasmr/r-base
COPY . /usr/local/src/myscripts
WORKDIR /usr/local/src/myscripts
RUN chmod a+x launcher.sh
RUN chmod a+x getcsv_from_ftp_rem2.sh
RUN mkdir /usr/local/src/myscripts/data
CMD ["./launcher.sh"]
