from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import logging, json

from utils.s3 import S3
import utils.embed as embed
from utils.web_scrapping import WebScrapper
from utils.newsClassifier import NewsClassifier
from utils.snowflake_utils import SnowFlakeLoader


def ngetnews_to_s3(**context):
    """
        Scrapping nget news then load into S3 as Json
    """

    dt = context['execution_date']

    logging.info("Initiate Scrapping nget news page")

    crawler = WebScrapper()
    data_set = crawler.get_ngetnews()

    _s3 = S3(dt)
    logging.info("Loading in progress")
    _s3.load_json(data_set, "nget")


def sisanews_to_s3(**context):
    """
        Scrapping sisa news then load into S3 as Json
    """
    dt = context['execution_date']

    logging.info("Initiate Scrapping sisa news page")

    crawler = WebScrapper()
    data_set = crawler.get_sisanews()

    _s3 = S3(dt)
    logging.info("Loading in progress")
    _s3.load_json(data_set, "sisa")


def ynanews_to_s3(**context):
    """
        Scrapping yna news then load into S3 as Json
    """
    dt = context['execution_date']

    logging.info("Initiate Scrapping yna news page")

    crawler = WebScrapper()
    data_set = crawler.get_ynanews()

    _s3 = S3(dt)
    logging.info("Loading in progress")
    _s3.load_json(data_set, "yna")    


def donganews_to_s3(**context):
    """
        Scrapping donga news then load into S3 as Json
    """
    dt = context['execution_date']

    logging.info("Initiate Scrapping donga news page")

    crawler = WebScrapper()
    data_set = crawler.get_donganews()

    _s3 = S3(dt)
    logging.info("Loading in progress")
    _s3.load_json(data_set, "donga")    


def haninews_to_s3(**context):
    """
        Scrapping hani news then load into S3 as Json
    """
    dt = context['execution_date']

    logging.info("Initiate Scrapping hani news page")

    crawler = WebScrapper()
    data_set = crawler.get_haninews()

    _s3 = S3(dt)
    logging.info("Loading in progress")
    _s3.load_json(data_set, "hani")    


def s3_embed_s3(**context):
    """
        Read all json files then return merged json within execute day
        Then cleaning datasets and use embed(k bert) to scaled
        Next, store data as parquet in S3
    """

    dt = context['execution_date']

    logging.info("Reading Json in progress")
    _s3 = S3(dt)

    _json = _s3.read_json()

    df = embed.process_json(_json)

    logging.info("Saving parquet")

    _s3.load_parquet(df)


def applying_model(**context):
    """
        Read Parquet from S3 /embeded to apply deep learning model using
        title / content embeddings
        Then store parquet into S3/post model
    """

    dt = context['execution_date']

    logging.info("Reading Parquet from S3/Embeded")

    _s3 = S3(dt)
    _parquet = _s3.read_embeded()

    logging.info("Applying parquet data into model")

    model = NewsClassifier()
    df = model.apply_model(_parquet)

    logging.info("Loading post modeled parquet into S3/post_model")
    _s3.load_parquet(df, True)

    
def load_into_snowflake(**context):
    """
        Copy into Snowflake table
    """

    dt = context['execution_date']

    logging.info("Create and copy to Snowflake")

    _snowflake = SnowFlakeLoader(dt)
    _snowflake.copy_news_from_s3()
    _snowflake.merge_from_s3()


# DAG Init

with DAG(
    dag_id = 'ETL_news',
    start_date=datetime(2025,11,25),
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:

    # Empty Operator to announce Dag just initiated

    start = EmptyOperator(task_id = 'Start')

    # Crawling Tasks groups
    # Scrapping news from each news page then save as Json on S3
    with TaskGroup('Crawling') as crawling:
        nget = PythonOperator(
            task_id = 'nget_news_crawl',
            python_callable = ngetnews_to_s3,
        )

        sisa = PythonOperator(
            task_id = 'sisa_news_crawl',
            python_callable = sisanews_to_s3,
        )

        yna = PythonOperator(
            task_id = 'yna_news_crawl',
            python_callable = ynanews_to_s3
        )

        donga = PythonOperator(
            task_id = 'donga_news_crawl',
            python_callable = donganews_to_s3
        )

        hani = PythonOperator(
            task_id = 'hani_news_crawl',
            python_callable = haninews_to_s3
        )

        [nget, sisa, yna, donga, hani]


    # Cleaning and Scaled by Bert_embeded
    with TaskGroup('Preprocessing') as preprocessing:
        task = PythonOperator(
            task_id = 'Cleaning_and_embeding',
            python_callable = s3_embed_s3,
        )

        [task]


    # Applying model
    with TaskGroup('Modeled') as modeled:
        modeling = PythonOperator(
            task_id = "Applying_Model",
            python_callable = applying_model
        )

        [modeling]


    # Copying into Snowflake
    with TaskGroup('toSnowflake') as toSnowflake:
        copying = PythonOperator(
            task_id = "Copying_into_Snowflake",
            python_callable = load_into_snowflake
        )

    # Empty Operator to announce Dag just ended
    end = EmptyOperator(task_id = 'end')


    #Dependencies 
    start >> crawling >> preprocessing >> modeled >> toSnowflake >> end