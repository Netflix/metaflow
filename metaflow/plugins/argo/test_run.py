from metaflow.plugins.argo import trigger_live_run  # trigger_live_run
import time

flow_name = None  # "HelloArgoFlowTwo", "FailureFlow", "MissingFlow", None
template_name = "helloargoflowtwo"  # "helloargoflowtwo", "failureflow", "missingflow", None
run = trigger_live_run(
    plugin_name='Argo',
    flow_name=flow_name,
    alt_flow_id_info={'template_name': template_name},
    parameters=None,
    wait=False,
)

print(f"\nRunning flow {run.flow_name}")
start_time = time.time()
while run.is_running:
    print(f"Status (private): '{run._plugin_run._status}' after {int((time.time()-start_time)//1)} seconds")
    run._plugin_run._print_status()
    time.sleep(15 - (time.time() - start_time) % 15)

print(f"Final status (private): {run._plugin_run._status} after {int((time.time()-start_time)//1)} seconds")
run._plugin_run._print_status()
