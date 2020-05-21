setwd('/webrender_intel_win10_nightly');
source("query.R")
print(c(project=Sys.getenv("PROJECT_ID"), dataset=Sys.getenv("DATASET")))
g = bq(project=Sys.getenv("PROJECT_ID"), dataset=Sys.getenv("DATASET"),path=Sys.getenv('GOOGLE_APPLICATION_CREDENTIALS'))

max.build.id = if(Sys.getenv("START_BUILD")=="") NULL else Sys.getenv("START_BUILD")
## max.build.id needs to be of the form YYYYMMDD
render('driver.Rmd',param=list(args=list(max.build.id = max.build.id )))
render('dashboard.Rmd',output_dir='/tmp/output/')
