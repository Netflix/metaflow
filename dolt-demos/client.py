from metaflow import Flow, get_metadata
from doltpy.core import Dolt
from metaflow.datatools.dolt import get_flow_inputs_dolt, get_flow_writes_dolt


def print_data_map(data_map):
    for run_step in data_map.keys():
        for table in data_map[run_step]:
            print('{}, {}: {}'.format(run_step, table, data_map[run_step][table]))

print("Current metadata provider: %s" % get_metadata())

dolt = Dolt('path/to/dolt')
flow = Flow('DoltMLDemoFlow')
run = flow.latest_successful_run
print("Using run: %s" % str(run))

'''
Ex 1: Get all the data a given Flow read from Dolt
'''
data_map_for_flow = get_flow_inputs_dolt(dolt, flow)
print_data_map(data_map_for_flow)

'''
Ex 2: Filter by run
'''
data_map_for_run = get_flow_inputs_dolt(dolt, flow, [run.id])
print_data_map(data_map_for_run)

'''
Ex 3 Get all the outputs for a flow
'''
data_map_flow_outputs = get_flow_writes_dolt(dolt, flow, [run.id])
print_data_map(data_map_flow_outputs)
