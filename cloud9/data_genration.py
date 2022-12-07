import time
import random
import boto3
import json
from random import random

STREAM_NAME = "ExampleInputStream"
my_session = boto3.session.Session()
my_region = my_session.region_name

kinesis_client = boto3.client('kinesis',region_name=my_region)

n_record_per_chuck = 50

if __name__ == '__main__':
    count = 0
    with open('realtime.csv') as f:
        f.readline()
        for data in f.readlines():
            count += 1
            print(data)
            kinesis_client.put_record(
                StreamName=STREAM_NAME,
                Data=json.dumps(data),
                PartitionKey="partitionkey")
            if count > n_record_per_chuck:
                pt = random() * 18
                print(f'Strop for {pt}')
                time.sleep(pt)
                count = 0