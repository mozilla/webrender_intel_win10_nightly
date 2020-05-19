#!/bin/bash

BUCKET=moz-fx-data-prod-analysis
gcloud auth activate-service-account --key-file /app/.credentials

Rscript -e "setwd('/webrender_intel_win10_nightly');library(rmarkdown); render('driver.Rmd',param=list(args=list(use_email=FALSE)));render('dashboard.Rmd',output_dir='/tmp/output/')"
gsutil -m  rsync -r -d /tmp/output/   gs://$BUCKET/sguha/ds_283/
gsutil -m  rsync -r  /webrender_intel_win10_nightly/driver.html   gs://$BUCKET/sguha/ds_283/
