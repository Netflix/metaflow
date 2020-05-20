import json
import os

from metaflow.metaflow_config import get_authenticated_boto3_client, \
                                    SFN_DYNAMO_DB_TABLE


class DynamoDbClient(object):

    def __init__(self):
        self._client = get_authenticated_boto3_client('dynamodb', 
            params = {'region_name': self._get_instance_region()})
        self.name = SFN_DYNAMO_DB_TABLE

    def save_foreach_cardinality(self, 
                                 foreach_split_task_id, 
                                 foreach_cardinality,
                                 ttl):
        return self._client.put_item(
            TableName = self.name,
            Item = {
                'pathspec': {
                    'S': foreach_split_task_id
                },
                'for_each_cardinality': {
                    'NS': list(map(str, range(foreach_cardinality)))
                },
                'ttl': {
                    'N': str(ttl)
                }
            }
        )

    def save_parent_task_id_for_foreach_join(self, 
                                             foreach_split_task_id, 
                                             foreach_join_parent_task_id):
        return self._client.update_item(
            TableName = self.name,
            Key = {
                'pathspec': {
                    'S': foreach_split_task_id
                }
            },
            UpdateExpression = 'ADD parent_task_ids_for_foreach_join :val',
            ExpressionAttributeValues = {
                ':val': {
                    'SS': [foreach_join_parent_task_id] 
                }
            }
        )

    def get_parent_task_ids_for_foreach_join(self,
                                             foreach_split_task_id):
        response = self._client.get_item(
                TableName = self.name,
                Key = {
                    'pathspec': {
                        'S': foreach_split_task_id
                    }
                },
                ProjectionExpression = 'parent_task_ids_for_foreach_join',
                ConsistentRead = True
            )
        return response['Item']['parent_task_ids_for_foreach_join']['SS']

    def _get_instance_region(self):
        return os.popen(
            'curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone/'
            ).read()[:-1]