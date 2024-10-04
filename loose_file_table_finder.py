import boto3
import csv
import os
from io import StringIO
import pandas as pd
import logging
import argparse
import datetime

def get_glue_catalog_table_metadata(database_name, s3_bucket_name, s3_key):
    # Initialize Glue and S3 clients
    glue = boto3.client('glue')
    s3 = boto3.client('s3')

    # Fetch all tables from the specified Glue database
    response = glue.get_tables(DatabaseName=database_name)
    tables = response['TableList']
    next_token = ""
    table_metadata_list = []
    df = pd.DataFrame([],columns=['TableName','DatabaseName','Owner','DEPRECATED_BY_CRAWLER','TableType','Location'])
    while True:
        response = glue.get_tables(
            DatabaseName=database_name,
            NextToken=next_token
        )

        tables = response['TableList']
        next_token = response.get('NextToken')
        table_metadata = [
        {
            'TableName': table['Name'],
            'DatabaseName': database_name,
            'Owner': table.get('Owner', ''),
            'DEPRECATED_BY_CRAWLER': table['Parameters'].get('DEPRECATED_BY_CRAWLER', ''),
            'TableType': table.get('TableType', ''),
            'Location': table['StorageDescriptor'].get('Location', '')
        }
        for table in tables if '_snappy_parquet' in table['Name']
        ]
        
        table_metadata_list.append(table_metadata)
        print(table_metadata)
        df_to_append = pd.DataFrame(table_metadata,columns=['TableName','DatabaseName','Owner','DEPRECATED_BY_CRAWLER','TableType','Location'])
        df = df._append(df_to_append,ignore_index=True)
        if next_token is None:
            break
            
    csv_buffer = df.to_csv(index=False)
    s3.put_object(Bucket=s3_bucket_name, Key=f"{s3_key}loose_file_table_metadata_{timestamp}.csv", Body=csv_buffer, ServerSideEncryption='AES256')
    logging.info(f"Table metadata saved!")
    logging.info(f"CSV file with table metadata uploaded to s3://{s3_bucket_name}/{s3_key}")


def delete_deprecated_tables(database_name, s3_bucket_name, s3_key):
    # Initialize Glue and S3 clients
    glue = boto3.client('glue')
    s3 = boto3.client('s3')
    # List objects in the S3 bucket matching the prefix
    objects = s3.list_objects(Bucket=s3_bucket_name, Prefix=f"{s3_key}loose_file_table_metadata_")
    csv_files_paths = []
    if 'Contents' in objects:
        for obj in objects['Contents']:
            csv_files_paths.append({'Key': obj['Key'], 'LastModified': obj['LastModified']})

    if not csv_files_paths:
        logging.info(f"No records found in the find phase for deletion or the find job for this table was not run! Check logs from find-phase.")
        return
    else:
        
        # Sort CSV files paths based on LastModified timestamp
        sorted_csv_files_paths = sorted(csv_files_paths, key=lambda x: x['LastModified'], reverse=True)
        logging.info(f"The sorted csv file paths are: {sorted_csv_files_paths}")

        # Get the path of the latest CSV file
        latest_csv_file_path = sorted_csv_files_paths[0]['Key']

    df = pd.read_csv(f"s3://{s3_bucket_name}/{latest_csv_file_path}")
    for index, row in df.iterrows():
        table_name = row['TableName']
        database_name = row['DatabaseName']
        deprecated_by_crawler = row['DEPRECATED_BY_CRAWLER']
        
        
        # Check if the table is deprecated
        if pd.isna(deprecated_by_crawler):
            logging.info(f"Deleting NON deprecated snappy parquet table: {database_name}.{table_name}")
            # Delete the table
            # glue.delete_table(DatabaseName=database_name, Name=table_name)
        elif isinstance(deprecated_by_crawler, float):
            logging.info(f"Deleting deprecated table: {database_name}.{table_name}")
            # Delete the table
            # glue.delete_table(DatabaseName=database_name, Name=table_name)
        else:
            logging.info("Error!")

   


# Customize your parameters
try:

    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    log_stream = StringIO()
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger().addHandler(logging.StreamHandler(log_stream))

    parser = argparse.ArgumentParser(description='Delete deprecated tables')
    
    parser.add_argument('--environment', choices=['prod', 'np'], required=True, help='Environment (prod or np)')
    parser.add_argument('--job', choices=['find', 'delete'], required=True, help='Job (find or delete)')
    parser.add_argument('--database_name', help='Name of the database')
    parser.add_argument('--s3_bucket_name', help='Name of the S3 bucket')

    args = parser.parse_args()
    database_name = args.database_name
    s3_bucket_name = args.s3_bucket_name
    job_name = args.job
    timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    s3_key = f'logs/glue_crawler_deprecated_tables/'

    if job_name == 'find':
        get_glue_catalog_table_metadata(database_name, s3_bucket_name, s3_key)
        log_file_name = f"{s3_key}log/find_phase_{timestamp}.log"
        s3 = boto3.resource("s3")
        s3.Object(s3_bucket_name, log_file_name).put(Body=log_stream.getvalue(), ServerSideEncryption="AES256")
    elif job_name == 'delete':
        delete_deprecated_tables(database_name, s3_bucket_name, s3_key)
        log_file_name = f"{s3_key}log/delete_phase_{timestamp}.log"
        s3 = boto3.resource("s3")
        s3.Object(s3_bucket_name, log_file_name).put(Body=log_stream.getvalue(), ServerSideEncryption="AES256")
    else:
        logging.info("Invalid job name. Select either find or delete")
    

except Exception as e:
        logging.error(f"Error occurred in loop at {e.__traceback__.tb_lineno}: {str(e)}", exc_info=True)