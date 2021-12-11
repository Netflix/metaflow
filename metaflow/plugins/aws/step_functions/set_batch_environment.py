import json
import os
import sys

from .dynamo_db_client import DynamoDbClient

def export_parameters(output_file):
    input = json.loads(os.environ.get('METAFLOW_PARAMETERS', '{}'))
    params = json.loads(os.environ.get('METAFLOW_DEFAULT_PARAMETERS', '{}'))
    params.update(input)
    with open(output_file, 'w') as f:
        for k in params:
            # Replace `-` with `_` is parameter names since `-` isn't an
            # allowed character for environment variables. cli.py will
            # correctly translate the replaced `-`s.
            normalized_name = k.upper().replace('-', '_')
            dumps = json.dumps(params[k])
            value = f"'{dumps}'" if isinstance(params[k], dict) else dumps
            f.write(f"export METAFLOW_INIT_{normalized_name}={value}\n")
    os.chmod(output_file, 509)

def export_parent_task_ids(output_file):
    input = os.environ['METAFLOW_SPLIT_PARENT_TASK_ID']
    task_ids = DynamoDbClient().get_parent_task_ids_for_foreach_join(input)
    with open(output_file, 'w') as f:
        f.write('export METAFLOW_PARENT_TASK_IDS=%s' % ','.join(task_ids))
    os.chmod(output_file, 509)


# TODO: Maybe use click someday instead of conditional.
if __name__ == '__main__':
    if sys.argv[1] == 'parameters':
        export_parameters(sys.argv[2])
    elif sys.argv[1] == 'parent_tasks':
    	export_parent_task_ids(sys.argv[2])