#!/bin/bash
set -e
BUCKET=${BUCKET:-gs://moz-fx-data-prod-analysis}



if [ -z "$GOOGLE_APPLICATION_CREDENTIALS" ]
then
    echo "Creds Set"
else
    gcloud auth activate-service-account --key-file /app/.credentials
fi
    
if [ -z "$RUNFAST" ]
then
    ## Complete run if RUNFAST environment is missing
    Rscript -e "setwd('/webrender_intel_win10_nightly');library(rmarkdown); render('driver.Rmd',param=list(args=list(use_email='use')));render('dashboard.Rmd',output_dir='/tmp/output/')"
    gsutil -m  rsync -r -d /tmp/output/   $BUCKET/sguha/ds_283/
    gsutil -m  rsync -r  /webrender_intel_win10_nightly/driver.html   $BUCKET/sguha/ds_283/
else
    ## Quick Check if RUNFAST environment is passed
    Rscript -e "setwd('/webrender_intel_win10_nightly'); source('query.R');g=bq(use_email='no'); g$q('select client_id from `moz-fx-data-shared-prod`.telemetry.main where date(submission_timestamp)=\'2020-01-01\' limit 10')"
    bq query --use_legacy_sql=false "select client_id from \`moz-fx-data-shared-prod\`.telemetry.main where date(submission_timestamp)='2020-01-01' limit 10"
    #gsutil cp /webrender_intel_win10_nightly/driver.Rmd $BUCKET/sguha/ds_283/
fi

