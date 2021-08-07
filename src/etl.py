from sklearn.preprocessing import StandardScaler
from columns import numeric, categorical

import awswrangler as wr
import pandas as pd
import numpy as np
import itertools
import tempfile
import logging
import boto3
import sys

logging.basicConfig(level=logging.INFO)


class ETL:
    filename = None
    s3_session = boto3.Session()
    
    def read_data(self):
        logging.info('Reading data...')
        return pd.read_csv(self.filename, chunksize=50000, low_memory=False)
    
    def transform_data(self, data):
        logging.info('Transforming data...')
        data, scaler_data = itertools.tee(data)
        scaler = StandardScaler()
        
        logging.info('Fitting Scaler...')
        for scaler_chunk in scaler_data:
            scaler.partial_fit(scaler_chunk[numeric])
            break
        
        # NOTE: NULL issue date will appear with date 2000-01
        for chunk in data:
            transformed =  pd.get_dummies(chunk, columns=categorical)
            transformed[numeric] = scaler.transform(transformed[numeric])
            transformed['issue_d'] = pd.to_datetime(transformed['issue_d'], format='%b-%Y')
            transformed['issue_d'] = transformed['issue_d'].fillna('Jan-2000')
            transformed['year'] = transformed['issue_d'].apply(lambda x: int(x.year))
            transformed['month'] = transformed['issue_d'].apply(lambda x: int(x.month))
            yield transformed
            
    def insert_data(self, data):
        for chunk in data:
            wr.s3.to_parquet(df=chunk, 
                             boto3_session=self.s3_session,
                             path='s3://vn-project/', 
                             dataset=True, 
                             partition_cols=['year', 'month'])
            
        logging.info('Process finished...')
        
    def run_etl(self, filename):
        logging.info('Staring process...')
        self.filename = filename
        self.insert_data(self.transform_data(self.read_data()))
    
etl = ETL()
generator = etl.run_etl(str(sys.argv[1]))