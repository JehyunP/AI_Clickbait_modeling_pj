from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.hooks.base import BaseHook

class SnowFlakeLoader:
    """
        Load Parquet from S3 into Snowflake
    """

    def __init__(self, dt):

        self.hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        self.init_stage()

        y, m, d = dt.strftime("%Y-%m-%d").split('-')
        self.year, self.month, self.day = y, m, d


    def init_stage(self):
        aws_conn = BaseHook.get_connection("aws_conn")
        stage_cleaned = f"""
            create stage if not exists news_stage
            url='s3://news-clickbait/post_model'
            CREDENTIALS = (
                AWS_KEY_ID='{aws_conn.login}',
                AWS_SECRET_KEY='{aws_conn.password}'
            )
            FILE_FORMAT = (TYPE=PARQUET)
        """
        self.hook.run(stage_cleaned)



    def copy_news_from_s3(self):
        # Create table if not exist in news_clickbait schema
        create_table = """
            CREATE TABLE IF NOT EXISTS news_clickbait.final_news_predict (
                news STRING,
                date DATE,
                category STRING,
                title STRING,
                contents STRING,
                title_embedding VARIANT,
                content_embedding VARIANT,
                evaluate_probability FLOAT,
                evaluate INT
            );
        """
        self.hook.run(create_table)


    def merge_from_s3(self):

        sql = f"""
            MERGE INTO news_clickbait.final_news_predict t
            USING (
                SELECT 
                    $1:News::string       AS news,
                    TO_DATE($1:Date::string, 'YYYY.MM.DD')     AS date,
                    $1:Category::string   AS category,
                    $1:Title::string      AS title,
                    $1:Contents::string   AS contents,
                    $1:title_embedding    AS title_embedding,
                    $1:content_embedding  AS content_embedding,
                    $1:evaluate_probability::float AS evaluate_probability,
                    $1:evaluate::int      AS evaluate
                FROM @news_stage/{self.year}/{self.month}/{self.day}.parquet
            ) s
            ON t.date = s.date AND t.title = s.title
            WHEN NOT MATCHED THEN
                INSERT (
                    news, date, category, title, contents,
                    title_embedding, content_embedding,
                    evaluate_probability, evaluate
                )
                VALUES (
                    s.news, s.date, s.category, s.title, s.contents,
                    s.title_embedding, s.content_embedding,
                    s.evaluate_probability, s.evaluate
                );
        """
        self.hook.run(sql)