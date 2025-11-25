from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
import pyarrow.fs as pa_fs
import pyarrow.parquet as pq
import json

class S3:
    '''
        Plug-In Utill : S3
        Connect to S3 within Airflow to read and save files 
    '''

    def __init__(self, dt):
        self.hook = S3Hook(aws_conn_id = 'aws_conn')
        self.bucket = Variable.get('AWS_BUCKET')

        y, m, d = dt.split('-')
        self.year, self.month, self.day = y, m, d

        credential = self.hook.get_credentials()
        self.fs = pa_fs.S3FileSystem(
            access_key = credential.access_key,
            secret_key = credential.secret_key,
            region='ap-northeast-2'
        )

    def read_json(self):
        key = f'/Raw_data/news_json/{self.year}/{self.month}/{self.day}.json'
        
        # Read from S3 object
        obj = self.hook.get_key(key=key, bucket_name=self.bucket)
        data = obj.get()
        body = obj.get()['Body'].read().decode('utf-8')
        return json.loads(body)
    
    

    def read_embeded(self):
        key = f'{self.bucket}/Raw_data/embeded/{self.year}/{self.month}/{self.day}.parquet'
        table = pq.read_table(key, filesystem=self.fs)
        return table.to_pandas()
    

