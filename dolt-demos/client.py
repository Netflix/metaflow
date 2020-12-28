from metaflow import Flow, get_metadata
print("Current metadata provider: %s" % get_metadata())

run = Flow('DoltMLDemoFlow').latest_successful_run
print("Using run: %s" % str(run))

'''
Ex 1: Use the client library to determine which dataset was used first
'''
start_data = run['start'].task.data
repo_name = start_data.dolt['repo_name']
table_used = start_data.dolt['tables_accessed'][0]
repo_commit = start_data.dolt['commit_hash']

print("Started dataset {} at commit {}".format(repo_name, repo_commit))

'''
Ex 2: View the repo you added a table too.
'''
predict_data = run['predict'].task.data
repo_name = predict_data.dolt['repo_name']
table_used = predict_data.dolt['tables_accessed'][0]
repo_commit = predict_data.dolt['commit_hash']

print("Write results to repo {} at commit {}".format(repo_name, repo_commit))


'''
Ex 3: Get the new commit hash at the end when you commit and push
'''
end_data = run['end'].task.data

repo_name = end_data.dolt['repo_name']
repo_commit = end_data.dolt['commit_hash']

print("Commited results to repo {} with commit {}".format(repo_name, repo_commit))

'''
Ex 4: Match all the run's start with the repository and commit hash they used.
'''
for run in Flow('PlayListFlow').runs():
    if run.successful:
        start_data = run['start'].task.data
        repo_name = start_data.dolt.repo_name
        repo_commit = start_data.dolt.commit_hash
        print(repo_name, repo_commit)

'''
 Ex 5. Match the lineage of Two Dolt Flows. Asserts that at the start the two repos are 
 reading the same dataset at the same commit hash.
'''
# from metaflow import dolt_utils

# flow1 = Flow('DoltDemoFlow')
# flow2 = Flow('ComplexDemoFlow')

# print(dolt_utils.match_lineage(flow1, flow2))