import subprocess
import os
import sys
from metaflow.decorators import StepDecorator
from metaflow.current import current

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
        self._run_cards_subprocess(flow,runspec)
        
    
    def _run_cards_subprocess(self,flow:FlowSpec,runspec):
        executable = sys.executable
        cmd = [
            executable,
            os.path.basename(sys.argv[0]),
            "card",
            "generate",
            "--card-type",
            self.attributes['type'],
            "--run-id-file",
            runspec
        ]
        response,fail = self._run_command(cmd,os.environ)
        if fail:
            print("Process Failed")
        print(response.decode('utf-8'))
    
    def _run_command(self,cmd,env):
        fail = False
        try:
            rep = subprocess.check_output(
                cmd,
                env=env,stderr=subprocess.STDOUT,
            )
        except subprocess.CalledProcessError as e:
            rep = e.output
            fail=True
        return rep,fail