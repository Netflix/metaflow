from doltpy.core import Dolt
from doltpy.core.write import import_df
from doltpy.core.read import read_table
from doltpy.core.read import read_table_sql
from .. import FlowSpec, Flow
from ..current import current
from typing import List, Mapping, Union
import pandas as pd


class DoltTableRead:
    def __init__(self, run_id: int, step: str, dolt: Dolt, branch: str, commit: str, table_name: str):
        self.run_id = run_id
        self.step = step
        self.commit = commit
        self.branch = branch
        self.table_name = table_name


class DoltTableWrite:
    def __init__(self, run_id: int, step: str, dolt: Dolt, table_name: str):
        self.run_id = run_id
        self.step = step
        self.commit = None
        self.branch = None
        self.table_name = table_name

    def set_commit_and_branch(self, commit: str, branch: str):
        self.commit = commit
        self.branch = branch


def get_flow_inputs_dolt(dolt: Dolt,
                         flow: Flow,
                         runs: List[int] = None,
                         steps: List[str] = None) -> Mapping[str, Mapping[str, pd.DataFrame]]:
    table_reads = _get_table_access_record_helper(flow, 'table_reads')
    return _get_tables_for_access_records(dolt, table_reads, runs, steps)


def get_flow_writes_dolt(dolt: Dolt,
                         flow: Flow,
                         runs: List[int] = None,
                         steps: List[str] = None) -> Mapping[str, Mapping[str, pd.DataFrame]]:
    table_writes = _get_table_access_record_helper(flow, 'table_writes')
    return _get_tables_for_access_records(dolt, table_writes, runs, steps)


def _get_tables_for_access_records(dolt: Dolt,
                                   access_records: Union[List[DoltTableRead], List[DoltTableWrite]],
                                   runs: List[int],
                                   steps: List[str]) -> Mapping[str, Mapping[str, pd.DataFrame]]:
    result = {}
    for access_record in access_records:
        if runs and access_record.run_id not in runs and steps and not access_record.step not in steps:
            pass
        else:
            run_step_path = '{}/{}'.format(access_record.run_id, access_record.step)
            df = _get_dolt_table_asof(dolt, access_record.table_name, access_record.commit)
            if run_step_path in result:
                result[run_step_path][access_record.table_name] = df
            else:
                result[run_step_path] = {access_record.table_name: df}

    return result


def _get_table_access_record_helper(flow: Flow, access_record_key: str):
    access_records = []
    for run in flow:
        for step in run:
            access_records.extend(run[step].task.data.dolt[access_record_key])

    return access_records


def _get_dolt_table_asof(dolt: Dolt, table_name: str, commit: str) -> pd.DataFrame:
    return read_table_sql(dolt, 'SELECT * FROM {} AS OF "{}"'.format(table_name, commit))


class DoltDT(object):
    def __init__(self, run=None, db_name='', branch: str = 'master'):
        self.db_name = db_name
        self.run = run
        self.branch = branch

        try:
            self.doltdb = Dolt(self.db_name)
            current_branch, _ = self.doltdb.branch()
            self.entry_branch = None
            if current_branch != self.branch:
                self.entry_branch = current_branch
                self.doltdb.checkout(branch, checkout_branch=True)
        except:
            return Exception('Dolt databsae not cloned locally')

        self.run.dolt = {}
        self.dolt_data = self.run.dolt
        self.dolt_data['table_reads'] = []
        self.dolt_data['table_writes'] = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self.entry_branch:
            self.doltdb.checkout(branch=self.entry_branch)

        self.close()

    def get_table_access(self, table: str) -> DoltTableRead:
        return DoltTableRead(current.run_id,
                             current.step_name,
                             self.doltdb,
                             self.branch,
                             self.get_latest_commit_hash(),
                             table)

    def get_table_write(self, table: str) -> DoltTableWrite:
        return DoltTableWrite(current.run_id, current.step_name, self.doltdb, table)

    def close(self):
        """
        Delete all temporary files downloaded in this context.
        """
        pass
    
    def get_latest_commit_hash(self) -> str:
        lg = self.doltdb.log()
        return lg.popitem(last=False)[0]

    def write_table(self, table_name: str, df: pd.DataFrame, pks: List[str]):
        import_df(repo=self.doltdb, table_name=table_name, data=df, primary_keys=pks)
        self.dolt_data['table_writes'].append(self.get_table_write(table_name))

    def get_table(self, table_name: str) -> pd.DataFrame:
        try:
            table = read_table(self.doltdb, table_name)
            self.dolt_data['tables_accesses'].append(self.get_table_access(table_name))
            return table
        except:
            Exception('Cant access table')

    def commit_table_writes(self, allow_empty=True):
        if isinstance(self.run, FlowSpec):
            self.doltdb.add([table_write.table_name for table_write in self.dolt_data['tables_writes']])
            self.doltdb.commit(message='Run {}'.format(current.run_id), allow_empty=allow_empty)
            commit_hash = self.get_latest_commit_hash()
            current_branch, _ = self.doltdb.branch()
            for table_write in self.dolt_data['tables_writes']:
                table_write.set_commit_and_branch(current_branch.name, commit_hash)
        else:
            Exception("This needs to be of instance FlowSpec")

