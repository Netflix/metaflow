from metaflow.plugins.argo import trigger_live_run  # trigger_live_run
import time

flow_name = None  # "HelloArgoFlowTwo", "HelloJSONType", None
template_name = "hellojsontype"  # "helloargoflowtwo", "hellojsontype", None

parameters_valid = {'str_to_list': "[1, 2, 3, 4, 5]",
                    'str_to_dict': '{"c": [1, 2, 3], "d": 99, "is_input_param": "yes"}',
                    'list_param': ["this", "is", "a", "list"],
                    'dict_param': {"e": [4,5,6], "f": 22, "starts_as_dict": "yes"},
                    }
# NOTE: Metaflow Parameter objects only support complex data types in JSON formats.
parameters_invalid = {'set_param': {1, 2, 3, 4, 5},
                      'set_in_list_param': {"c": [1, 2, 3], "d": 99, "set_here": {"a"}},
                      }
parameters_alt = {'alt_param': [1, 2, 999]}

run = trigger_live_run(
    plugin_name='Argo',
    flow_name=flow_name,
    alt_flow_id_info={'template_name': template_name},
    parameters=parameters_valid,  # None, parameters_valid, parameters_invalid, parameters_alt
    wait=False,
)

print(f"\nRunning flow {run.flow_name}")
start_time = time.time()
while run.is_running:
    print(f"Status (private): '{run._plugin_run._status}' after "
          f"{int((time.time()-start_time)//1)} seconds")
    run._plugin_run._print_status()
    time.sleep(15 - (time.time() - start_time) % 15)

print(f"Final status (private): {run._plugin_run._status} after {int((time.time() - start_time) // 1)} seconds")
run._plugin_run._print_status()
