import os
import io
import boto3
import json
import csv

# grab environment variables
ENDPOINT_NAME = os.environ['ENDPOINT_NAME']
runtime= boto3.client('runtime.sagemaker')

cols = ['vendor_id', 'passenger_count', 'pickup_longitude', 'pickup_latitude',
       'dropoff_longitude', 'dropoff_latitude', 'store_and_fwd_flag',
       'trip_duration', 'pick_year', 'pick_month', 'pick_day', 'pick_hr',
       'pick_minute', 'pick_weekday', 'pickup_dropoff_loc', 'Temp', 'Precip',
       'snow', 'Visibility', 'id', 'pickup_datetime', 'dropoff_datetime',
       'log_trip_duration', 'prediction']

def lambda_handler(event, context):
    # TODO implement
    try:
        data = json.loads(json.dumps(event))
        val_list = data['data'].strip().split(',')
        payload = ",".join(val_list[:-4])
        response = runtime.invoke_endpoint(EndpointName=ENDPOINT_NAME,
                                          ContentType='text/csv',
                                          Body=payload)
        prediction = json.loads(response['Body'].read().decode())
        val_list = val_list + [prediction]
        
        res_dict = dict()
        for idx, col in enumerate(cols):
            res_dict[col] = val_list[idx]
    
    except Exception as e:
        # Send some context about this error to Lambda Logs
        print(e)
        raise e
    
    return res_dict