FROM rocker/verse:3.5.0
RUN mkdir /tmp/output/
RUN R -e "options(repos =  list(CRAN = 'https://cran.microsoft.com/snapshot/2020-04-10/')); \
          pkgs <- c('vegawidget','parsedate','logging', 'Hmisc', 'ggplot2','glue','DBI','bigrquery','gargle','data.table','knitr','rmarkdown'); \
          install.packages(pkgs,dep=TRUE);"

RUN apt-get update && apt-get install -y \
        bzr \
        gnupg2 \
        cvs \
        git \
        curl \
        mercurial \
        subversion

RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && apt-get update -y && apt-get install google-cloud-sdk -y

COPY . /webrender_intel_win10_nightly 

CMD  /bin/bash /webrender_intel_win10_nightly/run.sh


    

