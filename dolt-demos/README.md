# Dolt in Metaflow 

## Background 
Dolt is a SQL database with Git-like version control semantics for both data and schema. This folder contains demo code showing how to use the Dolt datatool inside Metaflow to get the benefits of Dolt's version control features.

## Goals
Assuming we have an instance of Dolt, in which we are storing some input data, and perhaps writing intermediate results to, we would like to do the following:
- given a Flow definition, stage, and run, succinctly retrieve the exact Dolt data used as input
- given a set of Flows, and one or more runs associated with each, easily verify they used the same input data
- given a Flow, a and a set of associated runs, succinctly obtain the data output to Dolt by each run


## Implementation
The `metaflow.datatools.DoltDT` class is implemented to effectively serve two use cases: acting as a context manager for reading and writing from Dolt in a running flow, and querying the data read and written by a run of a flow.

### Running Metaflow
The operations supported by `DoltDT` are reading and writing a Pandas `DataFrame` object to and from Dolt, and committing changes. Those operations are supported by three simple methods on `DoltDT`:
```python
def write_table(self, table_name: str, df: pd.DataFrame, pks: List[str]):
    """
    Writes the contents of the given DataFrame to the specified table. If the table exists it is updated, if it
    does not it is created.
    """

def read_table(self, table_name: str) -> pd.DataFrame:
    """
    Returns the specified tables as a DataFrame.
    """

def commit_table_writes(self, allow_empty=True):
    """
    Creates a new commit containing all the changes recorded in self.dolt_data.['table_writes'], meaning that the
    precise data can be reproduced exactly later on by querying self.flow_spec.
    """
```

The goal of this API is to integrate the Dolt commit graph with the Metaflow object hierchy, we do this with two objects:
```python
class DoltTableRead:
    def __init__(self, run_id: int, step: str, branch: str, commit: str, table_name: str):
        self.run_id = run_id
        self.step = step
        self.commit = commit
        self.branch = branch
        self.table_name = table_name


class DoltTableWrite:
    def __init__(self, run_id: int, step: str, table_name: str):
        self.run_id = run_id
        self.step = step
        self.commit = None
        self.branch = None
        self.table_name = table_name

    def set_commit_and_branch(self, commit: str, branch: str):
        self.commit = commit
        self.branch = branch
```

Every time read, write, or commit, are called `DoltTableRead` and `DoltTableWrite` objects record the state of the flow (`run_id`, `step`) and the state of Dolt (`branch`, `commit`, `table_name`), and store that information in the Metaflows data artifact tracking mechanism.

This means that given a Flow definition, possibly recovered from an S3 bucket, and a clone of the Dolt that Flow's runs were executed against, the reads and writes can be uniquely recovered.

### Querying Metaflow Runs
We ended the last section by pointing out that the read, write, and commit tracking inside the the Metaflow metadata tracking system led to a unique degree of reproducibility. `DoltDT` provides some tools for querying Dolt by using the stored metadata return data in a manner that ties neatly into the Metaflow object hierarchy:
```python
def get_reads(self, runs: List[int] = None, steps: List[str] = None) -> Mapping[str, Mapping[str, pd.DataFrame]]:
    """
    Returns a nested map of the form:
        {run_id/step: [{table_name: pd.DataFrame}]}
        
    That is, for a Flow or Run, a mapping from the run_id, and step, to a list of table names and table data read
    by the step associated identified by the key.
    """

def get_writes(self, runs: List[int] = None, steps: List[str] = None) -> Mapping[str, Mapping[str, pd.DataFrame]]:
    """
    Returns a nested map of the form:
        {run_id/step: [{table_name: pd.DataFrame}]}
        
    That is, for a Flow or Run, a mapping from the run_id, and step, to a list of table names and table data written
    by the step associated identified by the key.
    """

```

## Collaboration
WIP

## Questions and Open Items

### Questions
1. How could improve the UX to more neatly match the use-cases that currently exist in Netflix and the wider Metaflow community?
2. Does the way we have architected the `DoltDT` class make sense, and in particular is it the right approach to have that single class serve running flows and post-run querying of different elements of the flow object hierarchy?
3. Currently this is implemented assuming that the Dolt database being used exists on the filesystem of the machine/container that launches the flow, this might not be viable, and there are number of ways this could be handled

### To Do
1. For this to be robust and usable in a production setting there needs to be a lot more tests
2. There needs to be clear and detailed guidance on an appropriate workflow for collaboration with Dolt and Metaflow