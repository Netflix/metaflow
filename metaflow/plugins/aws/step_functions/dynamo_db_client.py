import os
import requests
from metaflow.metaflow_config import SFN_DYNAMO_DB_TABLE


class DynamoDbClient(object):

    def __init__(self):
        from ..aws_client import get_aws_client
        self._client = get_aws_client(
            'dynamodb',
            params={'region_name': self._get_instance_region()})
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
        region = os.environ.get('AWS_REGION')
        # region is available as an env variable in AWS Fargate but not in EC2
        if region is not None:
            return region
        metadata_url = "http://169.254.169.254/latest/meta-data/placement/availability-zone/"
        r = requests.get(
            url = metadata_url
        )

        if r.status_code != 200:
            raise RuntimeError("Failed to query instance metadata. Url [%s]" % metadata_url +
                        " Error code [%s]" % str(r.status_code))

        return r.text[:-1]