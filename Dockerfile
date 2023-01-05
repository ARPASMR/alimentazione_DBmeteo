FROM debian:11-slim
# modalita' non interattiva
RUN echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections
# cambio i timeout
RUN echo 'Acquire::http::Timeout "240";' >> /etc/apt/apt.conf.d/180Timeout
# installo gli aggiornamenti ed i pacchetti necessari
 RUN apt-get update
 RUN apt-get -y install curl git locales dnsutils openssh-client smbclient procps util-linux build-essential ncftp rsync
 RUN apt-get -y install nfs-common openssl libjpeg-dev libpng-dev libreadline-dev r-base r-base-dev libmariadb-dev
 RUN apt-get -y install -y libpq-dev vim
 RUN R -e "install.packages('RMySQL', repos = 'http://cran.us.r-project.org')"
 RUN R -e "install.packages('RPostgreSQL', repos = 'http://cran.us.r-project.org')"
 RUN R -e "install.packages('lubridate', repos = 'http://cran.us.r-project.org')"
 RUN R -e "install.packages('curl', repos = 'http://cran.us.r-project.org')"
 COPY . /usr/local/src/myscripts
 WORKDIR /usr/local/src/myscripts
 RUN chmod a+x getcsv_from_ftp_rem2_k8s.sh
 RUN mkdir /usr/local/src/myscripts/data
 CMD ["./getcsv_from_ftp_rem2_k8s.sh"]
