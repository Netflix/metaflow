from metaflow import Flow, get_metadata
from metaflow.datatools.dolt import DoltDT


def print_data_map(data_map):
    for run_step in data_map.keys():
        for table in data_map[run_step]:
            print('{}, {}: {}'.format(run_step, table, data_map[run_step][table]))


print("Current metadata provider: %s" % get_metadata())
doltdb_path = 'path/to/dolt'
flow = Flow('DoltMLDemoFlow')
run = flow.latest_successful_run
print("Using run: %s" % str(run))

'''
Ex 1: Get all the inputs used by a flow across all runs
'''
doltdt = DoltDT(flow, doltdb_path, 'master')
data_map_for_flow = doltdt.get_reads()
print_data_map(data_map_for_flow)

'''
Ex 2: Get all the inputs used by a specific run of a flow
'''
doltdt = DoltDT(run, doltdb_path, 'master')
data_map_for_run = doltdt.get_reads()
print_data_map(data_map_for_run)

'''
Ex 3: Get all the inputs used by a specific step of a run of a flow
'''
doltdt = DoltDT(run, doltdb_path, 'master')
data_map_for_run = doltdt.get_reads(steps=['start'])
print_data_map(data_map_for_run)

'''
Ex 4 Ouputs are handled identically
'''
doltdt = DoltDT(run, doltdb_path, 'master')
data_map_flow_outputs = doltdt.get_writes(steps=['train'])
print_data_map(data_map_flow_outputs)


