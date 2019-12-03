import boto3


client = boto3.client("logs")
events = client.get_log_events(
                logGroupName="/aws/batch/job",
                logStreamName="metaflow_fc737537ffe417107c27b472d5426ba9224c01c567a2203785c7c6e9/default/d9791171-d588-42c1-a0e1-9fde821adfb1")
for event in events['events']:
    print(type(event['message']))
    print(event['message'])
print(events)
