import os
import time
import shutil
from datetime import datetime, timedelta

import pendulum
import requests
import boto3

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

from firebolt.db import connect
from firebolt.client import DEFAULT_API_URL
from firebolt.service.manager import ResourceManager
from firebolt.service.types import EngineStatus
from firebolt.common import Settings


def create_url(current_date):
    year = str(current_date.year)
    month = str(current_date.month).zfill(2)
    day = str(current_date.day).zfill(2)

    url = f'https://dumps.wikimedia.org/other/pageview_complete/{year}/{year}-{month}/pageviews-{year}{month}{day}-user.bz2'
    return url


def download_file(url):
    local_filename = url.split('/')[-1]
    with requests.get(url, stream=True) as r:
        with open(local_filename, 'wb') as f:
            shutil.copyfileobj(r.raw, f)

    return local_filename


def upload_file(file_name):
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(file_name, 'RAW_DATA_BUCKET', file_name)


def delete_file(file_name):
    os.remove(file_name)


with DAG(
    dag_id='firebolt',
    schedule_interval="0 5 * * *",
    start_date=pendulum.datetime(2022, 6, 8, tz="UTC"),
    end_date=pendulum.datetime(2022, 8, 23, tz="UTC"),
    catchup=True,
    tags=['cube_firebolt_tutorial'],
) as dag:
    
    @task(task_id="download_logs_from_wikipedia", wait_for_downstream=True, depends_on_past=True)
    def copy_logs_to_s3(ds=None, **kwargs):
        current_date = kwargs['yesterday_ds']
        current_date = datetime.fromisoformat(str(current_date))
        
        url = create_url(current_date)
        local_file_name = download_file(url)
        upload_file(local_file_name)
        delete_file(local_file_name)

    copy_to_s3 = copy_logs_to_s3()
    
    convert_to_firebolt = BashOperator(
        task_id='convert_to_firebolt_format',
        bash_command="""aws s3 cp s3://RAW_DATA_BUCKET/pageviews-{{ yesterday_ds_nodash }}-user.bz2 ~/pageviews-{{ yesterday_ds_nodash }}-user.bz2 || exit 1
pwd
bzcat ~/pageviews-{{ yesterday_ds_nodash }}-user.bz2 > ~/unbzipped.tmp

sed 's/,/_/g' ~/unbzipped.tmp > ~/text_file.tmp
sed "s/'/_/g" ~/text_file.tmp > ~/text_file.tmp2
sed 's/"/_/g' ~/text_file.tmp2 > ~/text_file.tmp3
sed 's/ /,/g' ~/text_file.tmp3 > ~/pageviews-{{ yesterday_ds_nodash }}-user.csv

gzip ~/pageviews-{{ yesterday_ds_nodash }}-user.csv

aws s3 cp ~/pageviews-{{ yesterday_ds_nodash }}-user.csv.gz s3://PROCESSED_DATA_BUCKET/wikilogs/pageviews-{{ yesterday_ds_nodash }}-user.csv.gz || exit 1

rm ~/pageviews-{{ yesterday_ds_nodash }}-user.csv.gz
""",
    )
        
    copy_to_s3 >> convert_to_firebolt
    
    
    def start_firebolt_engine():
        settings = Settings(
            user="",
            password="",
            server="api.app.firebolt.io",
            default_region="us-east-1"
        )

        rm = ResourceManager(settings=settings)

        all_engines = rm.engines.get_many()
        for engine in all_engines:
            if engine.name == 'ENGINE_NAME':
                print(f'Engine status: {engine.current_status}')
                if engine.current_status not in [EngineStatus.ENGINE_STATUS_RUNNING_REVISION_STARTING, EngineStatus.ENGINE_STATUS_RUNNING_REVISION_SERVING]:
                    print(f'Starting engine. Current status: {engine.current_status}')
                    engine.start()

    @task(task_id='start_firebolt_engine_if_disabled', wait_for_downstream=True, depends_on_past=True)
    def start_firebolt(ds=None, **kwargs):
        start_firebolt_engine()

    start_engine = start_firebolt()

    convert_to_firebolt >> start_engine
        
    def connect_to_firebolt():
        engine_name = 'ENGINE_NAME'
        database_name = "dev"
        username = ""
        password = ""
        api_endpoint = DEFAULT_API_URL


        connection = connect(
            engine_name=engine_name,
            database=database_name,
            username=username,
            password=password,
            api_endpoint=api_endpoint,
        )

        return connection.cursor()


    @task(task_id="ingest_into_firebolt", wait_for_downstream=True, depends_on_past=True, retries = 2)
    def ingest_into_firebolt(ds=None, **kwargs):
        current_date = kwargs['yesterday_ds_nodash']
        
        sql = f"""insert into cube_fact_pageviews
        select wiki_code, article_title, client, daily_total, to_date(concat(year, '-', month, '-', day)) as log_date
        from (
            select
            wiki_code,
            article_title,
            client,
            daily_total,
            source_file_name,
            SUBSTR(source_file_name, 20, 4) as year,
            SUBSTR(source_file_name, 24, 2) as month,
            SUBSTR(source_file_name, 26, 2) as day
            from cube_ex_wikipedia_pageviews
            where source_file_name = 'wikilogs/pageviews-{current_date}-user.csv.gz'
        )
        """
        connection = connect_to_firebolt()
        connection.execute(sql)
        
    ingest_pageviews = ingest_into_firebolt()
    
    start_engine >> ingest_pageviews

    
    @task(task_id='calculate_aggregations', wait_for_downstream=True, depends_on_past=True, retries = 2)
    def calculate_aggregations(ds=None, **kwargs):
        current_date = str(kwargs['yesterday_ds'])
        current_date = datetime.strptime(current_date, "%Y-%m-%d").date()
        delta = timedelta(days=30)
        month_ago = current_date - delta
        
        current_date = current_date.strftime("%Y-%m-%d")
        month_ago = month_ago.strftime("%Y-%m-%d")
        sql = f"""insert into cube_pageviews_aggregates
select
wiki_code,
article_title,
avg(daily_total) as average,
coalesce(stddev_samp(daily_total), 0) as st_dev,
max(log_date) as range_end_date
from 
    select wiki_code, article_title, daily_total, log_date from cube_fact_pageviews where log_date BETWEEN '{month_ago}' and '{current_date}'
group by wiki_code, article_title
        """
        
        connection = connect_to_firebolt()
        connection.execute(sql)
        
    calculate_pageview_aggregations = calculate_aggregations()    
    ingest_pageviews >> calculate_pageview_aggregations

    @task(task_id='add_aggregation_mapping', wait_for_downstream=True, depends_on_past=True, retries = 2)
    def add_aggregation_mapping(ds=None, **kwargs):
        current_date = str(kwargs['yesterday_ds'])
        sql = f"""insert into cube_pageview_days
select wiki_code, article_title, range_end_date, range_end_date+1 as next_day from second_load_cube_pageviews_aggregates where range_end_date = '{current_date}'
"""
        connection = connect_to_firebolt()
        connection.execute(sql)
        
    insert_aggregation_mapping = add_aggregation_mapping()
    calculate_pageview_aggregations >> insert_aggregation_mapping
    
    
    @task(task_id='detect_outliers', wait_for_downstream=True, depends_on_past=True, retries = 2)
    def detect_outliers(ds=None, **kwargs):
        current_date = str(kwargs['yesterday_ds'])
        current_date = datetime.strptime(current_date, "%Y-%m-%d").date()
        delta = timedelta(days=1)
        day_before = current_date - delta
        current_date = current_date.strftime("%Y-%m-%d")
        day_before = day_before.strftime("%Y-%m-%d")


        sql = f"""insert into cube_outliers
with dd as (
select * from cube_pageview_days where range_end_date = '{day_before}'
),
v as (
select * from cube_fact_pageviews where log_date = '{current_date}'
),
a as (
select * from cube_pageviews_aggregates where range_end_date = '{day_before}'
)
select v.wiki_code, v.article_title, v.log_date, v.daily_total, a.average, a.st_dev from dd
join v on dd.wiki_code = v.wiki_code and dd.article_title = v.article_title and dd.next_day = v.log_date
join a on dd.wiki_code = a.wiki_code and dd.article_title = a.article_title and dd.range_end_date = a.range_end_date
where v.daily_total >= a.average + 3*a.st_dev and v.log_date = '{current_date}'
"""
        
        connection = connect_to_firebolt()
        connection.execute(sql)
        
    run_outlier_detection = detect_outliers()
    insert_aggregation_mapping >> run_outlier_detection