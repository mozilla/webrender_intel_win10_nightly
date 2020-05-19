#!/bin/bash

Rscript -e "setwd('/webrender_intel_win10_nightly');library(rmarkdown); render('driver.Rmd');render('dashboard.Rmd',output_dir='/tmp/output/')"
gsutil -m  rsync -r -d /tmp/output/   gs://moz-fx-data-prod-analysis/sguha/ds_283/
gsutil -m  rsync -r  /webrender_intel_win10_nightly/driver.html   gs://moz-fx-data-prod-analysis/sguha/ds_283/
