# Dashboard

NOTICE: This dashboard has been decomissioned as per [bug 1701054](https://bugzilla.mozilla.org/show_bug.cgi?id=1701054).

The published dashboard lives at https://metrics.mozilla.com/public/sguha/ds-283/dashboard.html.

The CI on this repo publishes a Docker container to GCR on commits to master.

The container is [executed nightly by Airflow](https://github.com/mozilla/telemetry-airflow/blob/master/dags/webrender.py).

Rendered HTML is published to `gs://moz-fx-data-prod-analysis/sguha/ds_283/`.

tdsmith has a cronjob on hala that runs
`/usr/bin/gsutil rsync gs://moz-fx-data-prod-analysis/sguha/ds_283/ /data/www/metrics.mozilla.com/public/sguha/ds-283/ > /dev/null`
to publish the results to the canonical metrics.mozilla.com URL daily at 11am UTC.

# Development

To create the docker image, run

```
sudo docker build -t ds_283_prod .
```


## With Service Account

If you have a service account, then

```
sudo docker run -it -v PATH_TO_SERVICE_CREDENITIALS.json:/app/.credentials -e GOOGLE_APPLICATION_CREDENTIALS=/app/.credentials ds_283_prod
```


And that ought be it! The dashboard is copied to `gs://moz-fx-data-prod-analysis/sguha/ds_283/`.



## With Email Based Credentials

Now for testing, I mount my `gargle` cached authentifications(and other google auths too)  into the container and then

```

sudo docker run -it  -v ~/.R:/root/.R  -v ~/.config:/root/.config -e USE_EMAIL=1 ds_283_prod

```
