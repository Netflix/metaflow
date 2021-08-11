import subprocess
from metaflow.decorators import StepDecorator
from metaflow.current import current
from metaflow.client import Task

class CardDecorator(StepDecorator):
    name='card'
    defaults = {
        'type':'basic'
    }
    def task_finished(self, step_name, flow, graph, is_task_ok, retry_count, max_user_code_retries):
        if not is_task_ok:
            # todo : What do we do when underlying `step` soft-fails. 
            # Todo : What do we do when underlying `@card` fails in some way?
            return 
        # todo : Launch as task 
        # todo : write command that will call the sub process CLI
        runspec = '/'.join([
            current.flow_name,
            current.run_id,
            current.step_name,
            current.task_id
        ])
        Task(runspec)
    
        