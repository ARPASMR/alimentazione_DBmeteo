FROM arpasmr/r-base
COPY . /usr/local/src/myscripts
WORKDIR /usr/local/src/myscripts
CMD ["./getcsv_from_ftp_rem2.sh"]
