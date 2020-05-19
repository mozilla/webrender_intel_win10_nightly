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

RUN git clone https://github.com/mozilla/webrender_intel_win10_nightly.git && cd webrender_intel_win10_nightly  && git reset --hard b7e834724a4967e66d20b69da2f513c5eaf69fd1


CMD  Rscript -e "setwd('/webrender_intel_win10_nightly');rmarkdown::render('driver.Rmd');rmarkdown::render('dashboard.Rmd',output_dir='/tmp/output/')"  && gsutil -m  rsync -r -d /tmp/output/   gs://moz-fx-data-prod-analysis/sguha/ds_283/

    
