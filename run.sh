#!/bin/bash
set -e
export BUCKET=${BUCKET:-gs://moz-fx-data-prod-analysis}
export PROJECT_ID=${PROJECT_ID:-moz-fx-data-bq-data-science}
export DATASET=${DATASET:-sguha}

if [[ -z "${GOOGLE_APPLICATION_CREDENTIALS}" ]]; then
    echo "creds set, not activating"
else
    echo "Activating Credentials"
    gcloud auth activate-service-account --key-file /app/.credentials
fi
    
if [[ -z "${RUNFAST}" ]]; then   
    ## Complete run if RUNFAST environment is missing
    echo "Running SLOW"
    gsutil -m  rsync -r -d /tmp/output/   $BUCKET/sguha/ds_283/
    gsutil -m  rsync -r  /webrender_intel_win10_nightly/driver.html   $BUCKET/sguha/ds_283/
    Rscript /webrender_intel_win10_nightly/runner.R
else
    ## Quick Check if RUNFAST environment is passed
    echo "Running FAST"
    bq query --project_id="$PROJECT_ID" --use_legacy_sql=false "select client_id from \`moz-fx-data-shared-prod\`.telemetry.main where date(submission_timestamp)='2020-01-01' limit 10"
    gsutil cp /webrender_intel_win10_nightly/driver.Rmd $BUCKET/sguha/ds_283/
    Rscript /webrender_intel_win10_nightly/test.R 
fi

