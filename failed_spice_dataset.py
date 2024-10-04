import boto3
from datetime import datetime, timedelta
import pandas as pd

# Create a QuickSight client
client = boto3.client('quicksight')
all_datasets = []
failed_datasets = []
# Get the current date and time
current_datetime = datetime.now()
# Calculate the datetime for yesterday
yesterday_datetime = current_datetime - timedelta(days=1)
datasets_response = client.list_data_sets(AwsAccountId=<INSERT YOUR AWS ACCOUNT ID>)
datasets = datasets_response.get('DataSetSummaries', [])
all_datasets.extend(datasets)


next_token = datasets_response.get('NextToken')

# List all datasets
while True:
    try:
        # Extract ingestions from the response
        datasets_response = client.list_data_sets(AwsAccountId=<INSERT YOUR AWS ACCOUNT ID>,NextToken=next_token)
        datasets = datasets_response.get('DataSetSummaries', [])
        all_datasets.extend(datasets)
        # Check if there are more ingestions to retrieve
        next_token = datasets_response.get('NextToken')
        if not next_token:
            break
    except Exception as e:
                print(f"Error getting dataset: {str(e)}")
                continue

print(f"Total datasets are: {len(all_datasets)}")

# Iterate through datasets and check for failures
for dataset in all_datasets:
    all_ingestions = []
    try:    
        dataset_id = dataset["DataSetId"]
        dataset_name = dataset["Name"]
        if dataset['ImportMode'] == "DIRECT_QUERY":
            continue
        response = client.list_ingestions(AwsAccountId=<INSERT YOUR AWS ACCOUNT ID>, DataSetId=dataset_id)
        try:
            # List ingestions for the dataset
            list_ingestions_response = client.list_ingestions(
                AwsAccountId=<INSERT YOUR AWS ACCOUNT ID>,
                DataSetId=dataset_id
            )

            # Extract ingestions from the response
            ingestions = list_ingestions_response.get('Ingestions', [])

            # Append ingestions to the list
            all_ingestions.extend(ingestions)
            timestamp_datetime = all_ingestions[0]['CreatedTime']
            last_refreshed_status = all_ingestions[0]['IngestionStatus']
            
            if last_refreshed_status == "FAILED":
                last_refreshed_status_error = all_ingestions[0]['ErrorInfo']['Message']
                failed_dataset_dict = {'dataset_id': dataset_id, 'dataset_name': dataset_name, 'last_refreshed_status': last_refreshed_status,'error': last_refreshed_status_error, 'date_error_occurred':  timestamp_datetime}
                failed_datasets.append(failed_dataset_dict)
            # if timestamp_datetime and timestamp_datetime.date() == yesterday_datetime.date():
                # print(f"The dataset: {dataset_name} with dataset_id: {dataset_id} has last refreshed status: {last_refreshed_status}")
            
                


        except Exception as e:
            #print(f"Error listing ingestions: {str(e)}")
            continue
    except Exception as e:
        print(e)
        continue

csv_file_path = f"QuicksightSpiceFailedDatasets/quicksight_spice_failed_datasets.csv"
# Save the batch DataFrame to a CSV file
df = pd.DataFrame(failed_datasets, columns = ['dataset_id', 'dataset_name', 'last_refreshed_status','error', 'date_error_occurred'])
csv_buffer = df.to_csv(index=False)
s3_client = boto3.client('s3')
s3_client.put_object(Bucket=<BUCKET NAME>, Key=csv_file_path, Body=csv_buffer.encode('utf-8'), ServerSideEncryption="AES256")