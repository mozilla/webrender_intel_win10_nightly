## stard date: 
library(data.table)
library(bigrquery)
library(DBI)
library(glue)
library(boot)
library(Hmisc)
library(logging)
library(parsedate)
library(vegawidget)
library(rmarkdown)

ds_283_tablename <- "`moz-fx-data-shared-prod`.analysis.sguha_ds_283"
ds_283_tablename_tmp  <- "`moz-fx-data-shared-prod`.analysis.sguha_ds_283_tmp"

REPLICATES <- 500
REPLICATES.2 <- 1000
experiment.start <- as.Date("2020-04-01")
slug <- "bug-1622934-pref-webrender-continued-v2-nightly-only-nightly-76-80"
SEVEN <- 10

Emean <- function(x) exp(mean(log(1+x)))-1

if(!exists("query.R")){
    ## executed only once
    basicConfig()
    query.R <- TRUE
}

bq <- function(
                                        # project = 'moz-fx-data-derived-datasets'#
                                        # project = 'moz-fx-data-shared-prod'
               project='moz-fx-data-bq-data-science'
              ,dataset = 'sguha'
              ,path = ""
               ){
    if(is.null(path) || path==""){
        message("Using Email Auth")
        bq_auth(email='sguha@mozilla.com' ,use_oob =TRUE )
    } else {
        message("Using Service Credentials")
        bq_auth(path=path)
    }
    ocon <- dbConnect(
        bigrquery::bigquery()
      , project = project
      , dataset = dataset
    )
    w <- dbListTables(ocon)
    adhoc <- function(s,n=200,con=NULL){
        ## be careful with n=-1 !!
        if(is.null(con)) con <- ocon
        data.table(dbGetQuery(con, glue(s), n = n))
    }
    f <- list(w=w,con=ocon, query=adhoc)
    class(f) <- "bqh"
    return(f)
}

expandlims <- function(s,p=0.05){
    r <- range(s)
    r+c(-1,1)*diff(r)*p/2
}

toShellFromSQLName <- function(s){
    ## `moz-fx-data-shared-prod`.analysis.sguha_ds_283
    ## moz-fx-data-shared-prod:analysis.sguha_ds_283
    y <- gregexpr("^`[a-zA-Z-]+`",s)[[1]]
    if(y > 0){
        s2 <- attr(y,"match.length")
        u <- substr(s,s2+2, nchar(s))
        o <- substr(s,y+1,s2-1)
        return( glue("{o}:{u}"))
    }else stop(glue("Could not find the thing i want in {s}"))
}

mAndCi <- function(d,var,R=REPLICATES,fac=1,oper=median){
    CC <- 1/3600
    g1 <- boot(d, function(x,i){
        d <- x[i,]
        c1 <-  d[branch=="disabled", oper(get(var))]
        t1 <- d[branch=="enabled", oper(get(var))]
        if(c1<= .Machine$double.eps) H <- 0 else H <- (t1-c1)/c1
        c(c1,t1,H)
    }, R=R, strata=factor(d$branch))
    c1ci <- boot.ci(g1,type='perc', index=1)
    t1ci <- boot.ci(g1,type='perc', index=2)
    rdci <- boot.ci(g1,type='perc', index=3)
    nreporting.dis <- length(unique(d[branch=='disabled',id]));
    nreporting.ena <- length(unique(d[branch=='enabled',id]));
  ex <- function(n,s,fac=1,N) data.table(what=n,nreporting=N,low = s$perc[[4]]*fac, est = as.numeric(s$t0)*fac, high = fac*s$perc[[5]])
   rbindlist( list(ex("disabled",c1ci,fac=fac,nreporting.dis),
                    ex("enabled",t1ci,fac=fac,nreporting.ena),
                    ex("reldiff(TvsC) %",rdci,fac=100,nreporting.ena+nreporting.dis )))
}

count.avg.popln.size <- function(builds,starts,ends,slug,debug=TRUE){
     dq <- glue("(",glue(paste(as.character(unlist(Map(function(b,s,e){
        glue("( DATE(submission_timestamp)>='{s}' and DATE(submission_timestamp)<='{e}' and substr(application.build_id,1,8) = '{b}')")
    }, builds, starts,ends))),collapse="\nOR\n")),")")
z=g$q(s<-glue("
with a as (
select
 --DATE(submission_timestamp),
substr(application.build_id,1,8),
count(distinct(client_id)) as n,
       count(distinct(case when `moz-fx-data-shared-prod`.udf.get_key(environment.experiments,'{slug}') is not null then client_id else null end)) as nexp
  FROM
        `moz-fx-data-shared-prod`.telemetry.main
    WHERE
        DATE(submission_timestamp)>='{min(starts)}' and DATE(submission_timestamp)<='{max(ends)}' AND
{dq}
  AND normalized_channel = 'nightly'
        and environment.system.gfx.features.wr_qualified.status='available'
        AND environment.system.os.name ='Windows_NT'
        AND STARTS_WITH(environment.system.os.version,'10') = TRUE
        AND environment.system.gfx.adapters[OFFSET(0)].vendor_id = '0x8086'
        AND substr(metadata.uri.app_version,1,2)>='76' and substr(metadata.uri.app_version,1,2)<='89'
group by 1
) select avg(n) as popln, avg(nexp) as popexp,avg(nexp/n) as wa  from a 
"),-1)
     if(debug) cat(s)
     z
}

get.data <- function(builds,starts,ends,tbl,slug,debug=FALSE){
    dq <- glue("(",glue(paste(as.character(unlist(Map(function(b,s,e){
        glue("( DATE(submission_timestamp)>='{s}' and DATE(submission_timestamp)<='{e}' and substr(application.build_id,1,8) = '{b}')")
    }, builds, starts,ends))),collapse="\nOR\n")),")")

    make.clients <- glue("
CREATE OR REPLACE TABLE {tbl}
OPTIONS(
  expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 168 HOUR)
) AS
(
with
myclients AS (
    SELECT
        client_id                                                                           AS cid,
        submission_timestamp                                                                AS date,
        payload.info.session_id                                                             AS session_id,
        `moz-fx-data-shared-prod`.udf.get_key(environment.experiments,'{slug}').branch                                AS branch,
        substr(application.build_id,1,8)                                                    AS buildid,
        payload.processes.parent.scalars.browser_engagement_active_ticks                    AS active_ticks,
        payload.info.subsession_length                                                      AS subsession_length,
        payload.processes.parent.scalars.browser_engagement_total_uri_count                 AS total_uri,
        json_extract(payload.processes.gpu.histograms.content_frame_time_vsync,'$.values')  AS content_frame_time_vsync,
        json_extract(payload.histograms.fx_tab_switch_composite_e10s_ms,'$.values')         AS tab_switch_ms,
        json_extract(payload.histograms.fx_page_load_ms_2,'$.values')                       AS page_load_ms,
        json_extract(payload.processes.gpu.histograms.content_full_paint_time,'$.values')   AS content_full_paint_time,
        json_extract(payload.processes.content.histograms.content_paint_time,'$.values')    AS content_paint_time,
        json_extract(payload.processes.gpu.histograms.composite_time,'$.values')            AS composite_time,
        json_extract(payload.processes.gpu.histograms.content_frame_time,'$.values')        AS content_frame_time,
        json_extract(payload.processes.gpu.histograms.checkerboard_severity,'$.values')     AS checkerboard_severity,
        json_extract(payload.processes.content.histograms.device_reset_reason,'$.values')   AS device_reset_reason_content,
        json_extract(payload.histograms.device_reset_reason,'$.values')                     AS device_reset_reason_parent,
        json_extract(payload.processes.gpu.histograms.device_reset_reason,'$.values')       AS device_reset_reason_gpu
    FROM
        `moz-fx-data-shared-prod`.telemetry.main
    WHERE
        DATE(submission_timestamp)>='{min(starts)}' and DATE(submission_timestamp)<='{max(ends)}'
        AND {dq}
        AND `moz-fx-data-shared-prod`.udf.get_key(environment.experiments,'{slug}').branch IS NOT NULL
        AND normalized_channel = 'nightly'
        AND environment.system.os.name ='Windows_NT'
        AND STARTS_WITH(environment.system.os.version,'10') = TRUE
        AND environment.system.gfx.adapters[OFFSET(0)].vendor_id = '0x8086'
        AND substr(metadata.uri.app_version,1,2)>='76' and substr(metadata.uri.app_version,1,2)<='89'
        AND substr(application.build_id,1,8)>='20200401'
)
,client_rank AS (
    SELECT  cid,
            dense_rank() OVER ( ORDER BY cid) AS id
    FROM myclients
),
client_rank2 AS (    SELECT cid, max(id) as id from client_rank group by 1)
,joined2  AS (
    SELECT
         a.*,
        client_rank2.id AS id
    FROM myclients a
    JOIN client_rank2
    ON
        a.cid  = client_rank2.cid )
,final as (
SELECT 
    * 
    --EXCEPT(cid) 
FROM joined2)

-- select branch,count(*),count(distinct(cid)) from myclients group by 1
select * from final
)
")
    if(debug) cat(make.clients)
    g$q(make.clients)
}


## Check Client Enrollments Are Correct
## Needs to be 50:50

check.enrollment.status <- function(tbl){
   f <-  g$q(glue("
with a as (
select
buildid,
branch,
count(distinct(id)) as n
from {tbl}
group by 1,2
),
b as (
select buildid, sum(n) as nt from a group by 1),
c as ( select a.buildid, branch, n, n/nt as prop from a join b on a.buildid=b.buildid)
select * from c
order by buildid, branch
"),-1)
   f[, data.table(buildid,what=branch,nreporting=n, low=0, est=0,high=0,label='enrollement_status')]
}

### As per https://dbc-caf9527b-e073.cloud.databricks.com/#notebook/55754/command/55758
### CMD 4, line 67,
### severea_checkerboarding is >500

check.usage.length.and.crash <- function(tbl,starts,ends,debug=FALSE){
    ## Lets assume branch switching is neglibible
        qq <- glue("
CREATE TEMP FUNCTION VALUE_SUM(x STRING,gt FLOAT64) AS (
(with a as (
 select  `moz-fx-data-shared-prod`.udf.json_extract_int_map(x) as Y
)
select sum(coalesce(value,0))  from a cross join unnest(Y) where key> gt
)
);
with a as (
select
    buildid,
    cid,
    id,
    branch,
    GREATEST(0,LEAST(sum(coalesce(active_ticks,0))*5/3600)) as active_hours,
    GREATEST(0,LEAST(25,sum(coalesce(subsession_length,0)/3600))) as total_hours,
    sum(coalesce(total_uri,0)) as total_uri,
    sum(coalesce(VALUE_SUM(device_reset_reason_content,0),0)+
        coalesce(VALUE_SUM(device_reset_reason_parent,0),0)+
        coalesce(VALUE_SUM(device_reset_reason_gpu,0),0))      as device_resets,
    sum(coalesce(VALUE_SUM(checkerboard_severity,500)))        as checkerboard_severity
from {tbl}
group by  1,2,3,4
)
,crash1 AS (
    select
        client_id as cid,
        substr(application.build_id ,1,8)                                                                        AS buildid,
        coalesce((countif(C.payload.process_type = 'main' or C.payload.process_type is null)),0)                 AS ncrash_main,
        coalesce((countif(regexp_contains(C.payload.process_type, 'content')
                 and not regexp_contains(
           coalesce(C.payload.metadata.ipc_channel_error, ''), 'ShutDownKill'))),0)                              AS ncrash_content,
        coalesce(countif(regexp_contains(coalesce(C.payload.metadata.ipc_channel_error, ''), 'ShutDownKill')),0) AS ncrash_shutdown,
        coalesce(countif(C.payload.process_type = 'gpu'),0)                                                      AS ncrash_gpu,
        coalesce(countif(C.payload.metadata.oom_allocation_size is not null),0)                                  AS ncrash_oom
    FROM
        `moz-fx-data-shared-prod`.telemetry.crash C
    WHERE
        DATE(submission_timestamp)>='{min(starts)}' and DATE(submission_timestamp)<='{max(ends)}'  
        AND normalized_channel = 'nightly'
        AND environment.system.os.name ='Windows_NT'
        AND STARTS_WITH(environment.system.os.version,'10') = TRUE
        AND environment.system.gfx.adapters[OFFSET(0)].vendor_id = '0x8086'
        AND substr(metadata.uri.app_version,1,2)>='76' and substr(metadata.uri.app_version,1,2)<='80'
        AND substr(application.build_id,1,8)>='20200401'
    GROUP BY 1,2
),
j1 as (
select
*
except(cid)
from a left join crash1 using (cid, buildid)
),
j2 as (
select
buildid, id,branch,
active_hours,
total_hours,
total_uri,
device_resets,
-- https://sql.telemetry.mozilla.org/queries/70011/source
LEAST(13,coalesce(ncrash_main,0)) as ncrash_main,
LEAST(18,coalesce(ncrash_content,0)) as ncrash_content,
LEAST(61,coalesce(ncrash_shutdown,0)) as ncrash_shutdown,
LEAST(5,coalesce(ncrash_gpu,0)) as ncrash_gpu,
LEAST(10,coalesce(ncrash_oom,0)) as ncrash_oom,
LEAST(27,coalesce(ncrash_main,0)+coalesce(ncrash_content,0)+coalesce(ncrash_gpu,0)) as ncrash_all,
coalesce(checkerboard_severity,0) as checkerboard_severe_events
from j1)
select * from j2
")
    if(debug) cat(qq)
    g$q(qq,-1)
    
}



dirch <- function(v, REP){
    t(replicate(REP,{
        a <- sapply(v, function(s) rgamma(1,shape=s,scale=1))
        a/sum(a)
    }))
}

histogram.aggregate.raw <- function(H,table,n=-1){
    base1 <- "a as (
select
id, buildid,branch,
`moz-fx-data-shared-prod`.udf.json_extract_int_map({H}) as x
from {table}
),
b as ( select id, buildid,branch,key,coalesce(value,0)  as value  from a cross join unnest(x)),
c as ( select id, buildid, branch, key, coalesce(sum(value),0)  as value from b group by 1,2,3,4), -- STAGE 1
k as (select * from c order by buildid, branch,key)
"
    query1 <- glue("with ", base1, "select * from k")
    g$q(query1,n)
}


histogram.aggregate.all <- function(H,table, n=-1){
    ## under a suitable definition, assuming every client has the same prior theta
    ## we can assume each follow Multinomial(n=1, p=(p1,p2,...pk))
    ## what statisticians call Categorical
    ## then the posterior of p is
    ## Dirichlet( vector of (1/k+sum(pi's)  and the means would be (1/K+psum)/(nreporting+1)and so on)
    base1 <- "a as (
select
id, buildid,branch,
`moz-fx-data-shared-prod`.udf.json_extract_int_map({H}) as x
from {table}
),
b0 as ( select  id,buildid,branch,key,sum(coalesce(value,0)) as value from a cross join unnest(x)  group by 1,2,3,4 ),
b1 as ( select  id,buildid,branch, sum(value) as npings from b0 group by 1,2,3),
b  as ( select  b0.id,b0.buildid,b0.branch, key, value/npings as p from b1 join b0 on
        b0.id=b1.id and b0.buildid=b1.buildid and b0.branch=b1.branch
      ),
c1 as ( select buildid,branch, count(distinct(id)) as nreporting,count(distinct(key)) as K  from b group by 1,2),
c  as ( select b.buildid,b.branch,key, max(nreporting) as nreporting,max(K) as K,--max is just to get one value
        sum(p) as psum
        from b join c1 on b.buildid = c1.buildid and b.branch = c1.branch group by 1,2,3),
d  as (select buildid, branch,key, nreporting,1/K+psum as psum,K as K,
              (1/K+psum)/(nreporting+1)  as p
      from c),
k as (select buildid, branch,key, nreporting,K,psum, p
       from d
     )
"
    query1 <- glue("with ", base1, "select * from k")
    g$q(query1,n)
}    

histogram.bin.errs <- function(histo,REP=5000){
   z <-  histo[,{
        mean_estimates <- dirch(psum, REP) 
        stats <- data.table(key,
                            lower = as.numeric(apply(mean_estimates,2,quantile,0.05/2)),
                            upper=as.numeric(apply(mean_estimates,2,quantile,1-0.05/2)))
    } ,by=list(buildid=buildid,branch)][order(buildid, branch),]
    merge(histo,z, by=c("buildid","branch","key"))
}
    

## histogram.summary <- function(histo, REP=5000){
##     histo[,{
##         mean_estimates <- dirch(psum, REP) %*% key
##         stats <- c(avg = mean(mean_estimates),
##                    lower = as.numeric(quantile(mean_estimates,0.05/2)),
##                    upper=as.numeric(quantile(mean_estimates,1-0.05/2)))
##         data.table(nreporting=nreporting[1],
##                    low=stats[['lower']],
##                    est=stats[['avg']],
##                    high=stats[['upper']])
##     } ,by=list(buildid=buildid,what=branch)][order(buildid, what),]
## }

histogram.summary.cuts <- function(histo, REP=5000,CUT){
    getGTProb <- function(keys,probs,CUT){
        w <- keys >= CUT
        w1 <- keys < CUT
        l <- tail(probs[w1],1)
        l1 <- tail(keys[w1],1)
        r1 <- head(keys[w],1)
        ## some edge cases i havent thought of
        base.p <- sum(probs[w]) + (r1-CUT)/(r1-l1)*l
        return(base.p)
    }
    br <- histo[,{
        mean_estimates <- apply(dirch(psum,REP), 1, function(s){
                                        #mean(sample(key, 5000, replace=TRUE, prob = s) >= CUT)
            getGTProb(key,s, CUT)
        })
        stats <- c(avg   = mean(mean_estimates),
                   lower = as.numeric(quantile(mean_estimates,0.05/2)),
                   upper = as.numeric(quantile(mean_estimates,1-0.05/2)))
        data.table(nreporting=nreporting[1],
                   low=stats[['lower']],
                   est=stats[['avg']],
                   high=stats[['upper']])
    } ,by=list(buildid=buildid,what=branch)][order(buildid, what),]

    rel <- histo[,{
        dis <- .SD[branch=='disabled',]
        ena <- .SD[branch=='enabled',]
        dis_mean_estimates <- apply(dirch(dis$psum,REP), 1, function(s){
            getGTProb(dis$key,s, CUT)
        })
        ena_mean_estimates <- apply(dirch(ena$psum,REP), 1, function(s){
            getGTProb(ena$key,s, CUT)
        })
        mean_estimates <- (ena_mean_estimates - dis_mean_estimates)/dis_mean_estimates*100
        stats <- c(avg   = mean(mean_estimates,na.rm=TRUE),
                   lower = as.numeric(quantile(mean_estimates,0.05/2,na.rm=TRUE)),
                   upper = as.numeric(quantile(mean_estimates,1-0.05/2,na.rm=TRUE)))
        data.table(what='reldiff(TvsC) %',nreporting=nreporting[1],
                   low=stats[['lower']],
                   est=stats[['avg']],
                   high=stats[['upper']])
    } ,by=list(buildid=buildid)][order(buildid, what),]
    rbind(br,rel)[order(buildid,what),]
}


histogram.summary.percentiles <- function (histo, perc, REP=5000)
{
    br <-  histo[, {
        D <- dirch(psum, REP)
        mean_estimates <- as.numeric(apply(D, 1, function(k) {
            ##wtd.quantile(key, k, perc,type="i/(n+1)",normwt=TRUE)
            quantile(sample(key,10000, prob=k, replace=TRUE),perc)
            }))
        stats <- c(avg = mean(mean_estimates),
                   lower = as.numeric(quantile(mean_estimates,0.05/2)),
                   upper = as.numeric(quantile(mean_estimates, 1-0.05/2)))
        data.table(nreporting = nreporting[1], 
            low = stats[["lower"]], est = stats[["avg"]], high = stats[["upper"]])
    }, by = list(buildid = buildid, what = branch)][order(buildid, what), ]

    rel <- histo[, {
        dis <- .SD[branch=='disabled',]
        ena <- .SD[branch=='enabled',]
        Ddis <- dirch(dis$psum, REP)
        Dena <- dirch(ena$psum, REP)
        dis_mean_estimates <- as.numeric(apply(Ddis, 1, function(k) {
            quantile(sample(dis$key,10000, prob=k, replace=TRUE),perc)
        }))
        ena_mean_estimates <- as.numeric(apply(Dena, 1, function(k) {
            quantile(sample(ena$key,10000, prob=k, replace=TRUE),perc)
        }))
        mean_estimates <- (ena_mean_estimates - dis_mean_estimates)/dis_mean_estimates*100
        stats <- c(avg = mean(mean_estimates,na.rm=TRUE),
                   lower = as.numeric(quantile(mean_estimates,0.05/2,na.rm=TRUE)),
                   upper = as.numeric(quantile(mean_estimates, 1-0.05/2,na.rm=TRUE)))
        data.table(what='reldiff(TvsC) %',nreporting = nreporting[1], 
            low = stats[["lower"]], est = stats[["avg"]], high = stats[["upper"]])
    }, by = list(buildid = buildid)][order(buildid, what), ]
    rbind(br,rel)[order(buildid,what),]
}


log.sm <- list(
    t = function(x,...) exp(mean(log(1+x),...))-1,
    i = function(x) x
)

histogram.summary <- function (histo, REP=500,sm = list(t= mean,i=function(s) s))
{
    br <- histo[, {
        D <- dirch(psum, REP)
        mean_estimates <- as.numeric(apply(D, 1, function(k) {
            sm$t(sample(key,10000, prob=k, replace=TRUE))
            }))
        stats <- c(avg = mean(sm$i(mean_estimates)),
                   lower = sm$i(as.numeric(quantile(mean_estimates,0.05/2))),
                   upper = sm$i(as.numeric(quantile(mean_estimates, 1-0.05/2))))
        data.table(nreporting = nreporting[1], 
            low = stats[["lower"]], est = stats[["avg"]], high = stats[["upper"]])
    }, by = list(buildid = buildid, what = branch)][order(buildid,
        what), ]

    rel <- histo[, {
        dis <- .SD[branch=='disabled',]
        ena <- .SD[branch=='enabled',]
        Ddis <- dirch(dis$psum, REP)
        Dena <- dirch(ena$psum, REP)
        dis_mean_estimates <- as.numeric(apply(Ddis, 1, function(k) {
            sm$i(sm$t(mean(sample(dis$key,10000, prob=k, replace=TRUE))))
        }))
        ena_mean_estimates <- as.numeric(apply(Dena, 1, function(k) {
            sm$i(sm$t(mean(sample(ena$key,10000, prob=k, replace=TRUE))))
        }))
        mean_estimates <- (ena_mean_estimates - dis_mean_estimates)/dis_mean_estimates*100
        stats <- c(avg = mean(mean_estimates,na.rm=TRUE),
                   lower = as.numeric(quantile(mean_estimates,0.05/2,na.rm=TRUE)),
                   upper = as.numeric(quantile(mean_estimates, 1-0.05/2,na.rm=TRUE)))
        data.table(what='reldiff(TvsC) %',nreporting = nreporting[1], 
            low = stats[["lower"]], est = stats[["avg"]], high = stats[["upper"]])
    }, by = list(buildid = buildid)][order(buildid, what), ]
    rbind(br,rel)[order(buildid,what),]

}



plotFigure <- function(f, title,width=NULL,height=NULL,pointSize=10,ydomains,yaxislab='Estimate',LA=0){
    ## Need error bars
    ## Jitter the points to not overlap(use dtatetime and plot the x axis as Date, make Label also date)
    ## For enabled, hoverover, keep Rel. Diff wrt Disabled - for Disabled
    u3 <- list(
        `$schema` = vega_schema(),
        data = list(values = f),
        config = list( legend = list(direction='horizontal',orient='top',title=NULL)),
        title = list(text=glue(title), anchor='start',fontWeight = 'normal'),
        width = if(is.null(width)) 1200 else width,
        height = if(is.null(height)) 300 else height,
        autosize = list(type='fit',contains='content'),
        layer = list(            
            list(
                selection = list(grid = list(type= "interval", bind="scales") ),
                transform = list(list(calculate="format(datum.est,'.2f')+' ('+format(datum.low,'.2f')+','+format(datum.high,'.2f')+')'", as="Estimate")),
                mark = list(type="line",point='true'), #'transparent'),
                encoding = list(
                    x = list(field="buildid2",type='temporal',timeUnit="yearmonthdatehoursminutes",
                             axis=list(title="BuildIDs on this Date"
                                     , titleFontWeight='light'
                                     , tickExtra=FALSE
#                                     , tickCount=length(unique(xx$buildid))
                                     , grid=FALSE
                                     , labelOverlap='parity'
                                     , format="%Y%m%d"
                                     , labelAngle=LA)
                             , scale= list(type= "utc")
                             ),
                    y = list(field="est"
                             ,scale= list(zero=FALSE) #domain=ydomains)
                             ,type="quantitative"
                            ,axis=list(title=yaxislab
                                      ,titleFontSize=11)),
                    color = list(field = "Branch", type = "nominal",scale=list(scheme="set1")),
                    tooltip=list(list(field = "buildid",type="nominal"),
                                 list(field = 'Branch', type='ordinal'),
                                 list(field = 'Estimate', type='ordinal'),
                                 list(field = 'UsingDataTill', type='temporal'),
                                 list(field= 'RelativeDiff', type='nominal')
                                 )
                )),
            list(
                mark = list(type="errorband"), #,"strokeDash"=list(4, 2)),
                encoding = list(
                    x = list(field="buildid2",type='temporal',timeUnit="yearmonthdatehoursminutes"
                             ,axis=list(title="BuildIDs on this Date",grid=FALSE,labelOverlap='parity', format="%Y%m%d", labelAngle=360)
                             ,scale= list(type= "utc")
                             ),
                    y = list(field="low",type="quantitative", axis=list(title=""), grid=FALSE, scale=list(zero=FALSE)),
                    y2 = list(field="high",type="quantitative",axis=list(title=""),  grid=FALSE),
                    color = list(field = "Branch", type = "nominal"),
                    opacity = list(value=0.15),
                    tooltip =  NULL
                )
            )
        )
    )
    u3
}

create.figure <- function(LL, title,width=NULL,height=NULL, yaxislab='Estimate',bqtable,g,
                          pointSize=65,expandFrac=0.05,LA=0,
                          buildLimit=NULL){
    w=g$q(glue("select * from {bqtable} where label='{LL}' order by buildid, what"),-1)
    if(!is.null(buildLimit)){
        w <- w[buildid >= strftime((as.Date(buildLimit,"%Y%m%d")-as.difftime(6,"months")),'%Y%m%d')]
    }
    w <- w[,{
        reld <- .SD[what=='reldiff(TvsC) %',]
        X <- .SD[what!='reldiff(TvsC) %',]
        if(sign(reld$low)==sign(reld$high)){
            ch <- "✅"
        }else ch <- "⁓"
        X <- X[order(what),][, RelativeDiff:= c( "not applicable",glue("{e}% ({l},{h}) {ch}",e=round(reld$est,1),l=round(reld$low,1),h=round(reld$high,1)))]
        X
    } ,by=list(date_computed, buildid)]
    xx <- w[, list(UsingDataTill=date_computed,buildid = parse_date(buildid),
                   Branch = what,
                   est,low,high, RelativeDiff )]
    xx <- xx[Branch=='enabled',  buildid2 := format_iso_8601(buildid-as.difftime(0,units='mins'))]
    xx <- xx[Branch=='disabled', buildid2 := format_iso_8601(buildid+as.difftime(0,units='mins'))]
    MYDM <- expandlims( xx[ , c(est,low,high)],expandFrac)
    xy <- plotFigure(xx, title=title,width=width,height=height,yaxislab=yaxislab,pointSize=pointSize, LA=LA,ydomains=MYDM)
}


vw <- function(xy,actions=TRUE){
    vegawidget(as_vegaspec(xy),embed=vega_embed(actions=actions,renderer='canvas'))
}



## plotFigure <- function(f, title,width=NULL,height=NULL,pointSize=65,ydomains,yaxislab='Estimate',LA=0){
##     ## Need error bars
##     ## Jitter the points to not overlap(use dtatetime and plot the x axis as Date, make Label also date)
##     ## For enabled, hoverover, keep Rel. Diff wrt Disabled - for Disabled
##     u3 <- list(
##         `$schema` = vega_schema(),
##         data = list(values = f),
##         title = list(text=glue(title), anchor='start',fontWeight = 'normal'),
##         width = if(is.null(width)) 1200 else width,
##         height = if(is.null(height)) 300 else height,
##         autosize = list(type='fit',contains='content'),
##         layer = list(            
##             list(
##                 selection = list(
##                     grid = list(type= "interval", bind="scales") #,resolve=list(scale=list(y='indepedent')))
##                 ),
##                 transform = list(list(calculate="format(datum.est,'.2f')+' ('+format(datum.low,'.2f')+','+format(datum.high,'.2f')+')'", as="Estimate")
##                                  ),
##                 mark = list(type="point",filled=TRUE),
##                 encoding = list(
##                     size = list(value=pointSize),
##                     x = list(field="buildid2",type='temporal',timeUnit="yearmonthdatehoursminutes",
##                              axis=list(title="BuildIDs on this Date",titleFontWeight='light',
##                                        tickExtra=FALSE,tickCount=length(unique(xx$buildid)),
##                                        grid=FALSE,labelOverlap='parity', format="%Y%m%d", labelAngle=LA),
##                              scale= list(type= "utc")
##                              ),
##                     y = list(field="est",
##                              scale= list(domain=ydomains),
##                              type="quantitative",axis=list(title=yaxislab,grid=TRUE,
##                                                                       titleFontSize=11)),
##                     color = list(field = "Branch", type = "nominal",scale=list(scheme="set1"),
##                                  legend = list(direction='horizontal',orient='top',title=NULL)
##                                  ),
##                     tooltip=list(list(field = "buildid",type="nominal"),
##                                  list(field = 'Branch', type='ordinal'),
##                                  list(field = 'Estimate', type='ordinal'),
##                                  list(field = 'UsingDataTill', type='temporal'),
##                                  list(field= 'RelativeDiff', type='nominal')
##                                  )
##                 )),
##             list(
##                 mark = list(type="rule"), #,"strokeDash"=list(4, 2)),
##                 encoding = list(
##                     size = list(value=1),
##                     opacity = list(value=0.3),
##                     x = list(field="buildid2",type='temporal',timeUnit="yearmonthdatehoursminutes",
##                              axis=list(title="BuildIDs on this Date",grid=FALSE,labelOverlap='parity', format="%Y%m%d", labelAngle=360),
##                              scale= list(type= "utc")
##                              ),
##                     y = list(field="low",type="quantitative",  grid=FALSE),
##                     y2 = list(field="high",type="quantitative",  grid=FALSE),
##                     color = list(field = "Branch", type = "nominal",scale=list(scheme="grey"),
##                                  legend = list(direction='horizontal',orient='top',title=NULL)
##                                  ),
##                     tooltip =  NULL
##                 ))
##         )
##     )
##     u3
## }
