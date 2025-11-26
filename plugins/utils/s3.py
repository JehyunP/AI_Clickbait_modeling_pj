from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
import pyarrow.fs as pa_fs
import pyarrow.parquet as pq
import pyarrow as pa
import json
from io import BytesIO

class S3:
    '''
        Plug-In Utill : S3
        Connect to S3 within Airflow to read and save files 
    '''

    def __init__(self, dt):
        self.hook = S3Hook(aws_conn_id = 'aws_conn')
        self.bucket = Variable.get('AWS_BUCKET')

        y, m, d = dt.strftime("%Y-%m-%d").split('-')
        self.year, self.month, self.day = y, m, d

        credential = self.hook.get_credentials()
        self.fs = pa_fs.S3FileSystem(
            access_key = credential.access_key,
            secret_key = credential.secret_key,
            region='ap-northeast-2'
        )

    def read_json(self):
        """
        Read Json file from crawlling in S3
        """
        prefix = f'Raw_data/news_json/{self.year}/{self.month}/{self.day}/'

        keys = self.hook.list_keys(
            bucket_name=self.bucket,
            prefix = prefix
        )

        merged = []
        
        for key in keys:
            obj = self.hook.get_key(key=key, bucket_name=self.bucket)
            body = obj.get()['Body'].read().decode('utf-8')

            try:
                data = json.loads(body)
                
                if isinstance(data, list):
                    merged.extend(data)

                else:
                    merged.append(data)

            except Exception as e:
                raise Exception(f"JSON parse error in {key}: {e}")
            
        return merged

    def load_json(self, data, news):
        """
        Load Json file from crawlling in S3
        """
        key = f'Raw_data/news_json/{self.year}/{self.month}/{self.day}/{news}.json'

        json_str = json.dumps(data, ensure_ascii=False, indent=2)
        
        self.hook.load_string(
            string_data=json_str,
            key=key,
            bucket_name=self.bucket,
            replace=True
        )
    

    def read_embeded(self):
        """
        Read parquet file from s3
        """
        key = f'Raw_data/embeded/{self.year}/{self.month}/{self.day}.parquet'
        s3_path = f'{self.bucket}/{key}'
        table = pq.read_table(s3_path, filesystem=self.fs)
        return table.to_pandas()
    

    def load_parquet(self, df, post=False):
        """
        Load parquet file in s3 
            post 
                True -> load at post_model
                False -> load at Raw
        """
        
        if post:
            key = f'post_model/{self.year}/{self.month}/{self.day}.parquet'
        else:
            key = f'Raw_data/embeded/{self.year}/{self.month}/{self.day}.parquet'

        table = pa.Table.from_pandas(df)
        buf = BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)

        # Save as parquet
        self.hook.load_bytes(
            bytes_data = buf.getvalue(),
            key=key,
            bucket_name=self.bucket,
            replace=True
        )

