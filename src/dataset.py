import datetime as dt
import gzip
import json
from typing import Dict
import subprocess
from IPython.display import HTML

import awswrangler as wr
import boto3
import pandas as pd
import smart_open
import warnings



AWS_ACCOUNT_ID = subprocess.run(['aws', 'sts', 'get-caller-identity', '--query', 'Account', '--output', 'text'], capture_output=True, text=True).stdout.strip()
BUCKET_NAME = f'de-c3w2lab1-{AWS_ACCOUNT_ID}-us-east-1-data-lake'
SCRIPTS_BUCKET_NAME = f'de-c3w2lab1-{AWS_ACCOUNT_ID}-us-east-1-glue-scripts'



def read_data_sample(bucket_name: str, s3_file_key: str) -> pd.DataFrame:
    """Reads review sample dataset

    Args:
        bucket_name (str): Bucket name
        s3_file_key (str): Dataset s3 key location

    Returns:
        pd.DataFrame: Read dataframe
    """
    s3_client = boto3.client('s3')
    source_uri = f's3://{bucket_name}/{s3_file_key}'
    json_list = []
    for json_line in smart_open.open(source_uri, transport_params={'client': s3_client}):
        json_list.append(json.loads(json_line))
    df = pd.DataFrame(json_list)
    return df


### START CODE HERE ### (1 line of code)
review_sample_df = read_data_sample(bucket_name=BUCKET_NAME, s3_file_key='staging/reviews_Toys_and_Games_sample.json.gz')
### END CODE HERE ###

review_sample_df.head(5)

review_sample_df.dtypes

### START CODE HERE ### (1 line of code)
metadata_sample_df = read_data_sample(bucket_name=BUCKET_NAME, s3_file_key='staging/meta_Toys_and_Games_sample.json.gz')
### END CODE HERE ###

metadata_sample_df.head(5)

metadata_sample_df.dtypes

def process_review(raw_df: pd.DataFrame) -> pd.DataFrame:    
    """Transformations steps for Reviews dataset

    Args:
        raw_df (DataFrame): Raw data loaded in dataframe

    Returns:
        DataFrame: Returned transformed dataframe
    """

    ### START CODE HERE ### (5 lines of code)
    raw_df['reviewTime'] = pd.to_datetime(raw_df['unixReviewTime'], unit='s')
    raw_df['year'] = raw_df['reviewTime'].dt.year
    raw_df['month'] = raw_df['reviewTime'].dt.month
    
    df_helpful = pd.DataFrame(raw_df['helpful'].to_list(), columns=['helpful', 'totalhelpful'])
    target_df = pd.concat([raw_df.drop(columns=['helpful']), df_helpful], axis=1)
    ### END CODE HERE ###
    
    return target_df

transformed_review_sample_df = process_review(raw_df=review_sample_df)
transformed_review_sample_df.head()


def process_metadata(raw_df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Function in charge of the transformation of the raw data of the
    Reviews Metadata.

    Args:
        raw_df (DataFrame): Raw data loaded in dataframe
        cols (list): List of columns to select

    Returns:
        DataFrame: Returned transformed dataframe
    """

    ### START CODE HERE ### (6 lines of code)
    tmp_df = raw_df.dropna(subset=["salesRank"], how="any")
    
    df_rank = pd.DataFrame([{"sales_category": key, "sales_rank": value} for d in tmp_df["salesRank"].tolist() for key, value in d.items()])
    
    target_df = pd.concat([tmp_df, df_rank], axis=1)
    target_df = target_df[cols]
    target_df = target_df.dropna(subset=["asin", "price", "sales_rank"], how="any")
    target_df = target_df.fillna("")
    ### END CODE HERE ###
    
    return target_df

# metadata_sample_df.head()
processed_metadata_df = process_metadata(raw_df=metadata_sample_df, 
                                         cols=['asin', 'description', 'title', 'price', 'brand','sales_category','sales_rank']
                                         )
processed_metadata_df.head()