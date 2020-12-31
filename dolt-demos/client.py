from metaflow import Flow, get_metadata
print("Current metadata provider: %s" % get_metadata())

run = Flow('DoltMLDemoFlow').latest_successful_run
print("Using run: %s" % str(run))

'''
Ex 1: Use the client library to determine which dataset was used first
'''
start_data = run['start'].task.data
db_name, repo_commit = start_data.dolt['db_name'], start_data.dolt['commit_hash']

print("Started dataset {} at commit {}".format(db_name, repo_commit))

'''
Ex 2: View the repo you added a table too.
'''
predict_data = run['predict'].task.data
db_name, table_used, db_name = predict_data.dolt['db_name'], predict_data.dolt['tables_accessed'][0], predict_data.dolt['commit_hash']

print("Write results to repo {} at commit {}".format(db_name, repo_commit))
print("Table {} was used.".format(table_used))

'''
Ex 3: Get the new commit hash at the end when you commit and push
'''
end_data = run['end'].task.data

db_name, repo_commit = end_data.dolt['db_name'], end_data.dolt['commit_hash']

print("Committed results to repo {} with commit {}".format(db_name, repo_commit))

'''
Ex 4: Match all the run's start with the repository and commit hash they used.
'''
desired_repo = 'iris-test'
for run in Flow('PlayListFlow').runs():
    if run.successful:
        db_name = run['start'].task.data.dolt.db_name

        if db_name == desired_repo:
            print(str(run))

'''
 Ex 5. Match the lineage of Two Dolt Flows. Asserts that at the start the two repos are 
 reading the same dataset at the same commit hash.
'''
# from metaflow import dolt_utils

# flow1 = Flow('DoltDemoFlow')
# flow2 = Flow('ComplexDemoFlow')

# print(dolt_utils.match_lineage(flow1, flow2))