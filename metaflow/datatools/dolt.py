from doltpy.core import Dolt
from doltpy.core.write import import_df
from doltpy.core.read import read_table
from doltpy.core.read import read_table_sql
from .. import FlowSpec, Flow, Run
from ..current import current
from typing import List, Mapping, Union
import pandas as pd


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
    def __init__(self, flow_spec: FlowSpec, doltdb_path: str, branch: str = 'master'):
        """

        """
        self.doltdb = Dolt(doltdb_path)
        self.flow_spec = flow_spec
        self.branch = branch
        self.flow_spec.dolt = {}
        self.dolt_data = self.flow_spec.dolt
        self.dolt_data['table_reads'] = []
        self.dolt_data['table_writes'] = []

        current_branch, _ = self.doltdb.branch()
        self.entry_branch = None
        if current_branch != self.branch:
            self.entry_branch = current_branch
            self.doltdb.checkout(branch, checkout_branch=True)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        uncommitted_table_writes = [table_write.table_name for table_write in self.dolt_data['table_write']
                                    if not table_write.commit]
        if uncommitted_table_writes:
            # TODO what is the Metaflow way to log
            print('Warning, uncommitted table writes to the following tables {}'.format(uncommitted_table_writes))
        if self.entry_branch:
            self.doltdb.checkout(branch=self.entry_branch)

        self.close()

    def _get_table_read(self, table: str) -> DoltTableRead:
        return DoltTableRead(current.run_id, current.step_name, self.branch, self._get_latest_commit_hash(), table)

    def _get_table_write(self, table: str) -> DoltTableWrite:
        return DoltTableWrite(current.run_id, current.step_name, table)

    def close(self):
        """
        Delete all temporary files downloaded in this context.
        """
        pass
    
    def _get_latest_commit_hash(self) -> str:
        lg = self.doltdb.log()
        return lg.popitem(last=False)[0]

    def write_table(self, table_name: str, df: pd.DataFrame, pks: List[str]):
        """
        Writes the contents of the given DataFrame to the specified table. If the table exists it is updated, if it
        does not it is created.
        """
        import_df(repo=self.doltdb, table_name=table_name, data=df, primary_keys=pks)
        self.dolt_data['table_writes'].append(self._get_table_write(table_name))

    def read_table(self, table_name: str) -> pd.DataFrame:
        """
        Returns the specified tables as a DataFrame.
        """
        table = read_table(self.doltdb, table_name)
        self.dolt_data['tables_accesses'].append(self._get_table_read(table_name))
        return table

    def commit_table_writes(self, allow_empty=True):
        """
        Creates a new commit containing all the changes recorded in self.dolt_data.['table_writes'], meaning that the
        precise data can be reproduced exactly later on by querying self.flow_spec.
        """
        self.doltdb.add([table_write.table_name for table_write in self.dolt_data['tables_writes']])
        self.doltdb.commit(message='Run {}'.format(current.run_id), allow_empty=allow_empty)
        commit_hash = self._get_latest_commit_hash()
        current_branch, _ = self.doltdb.branch()
        for table_write in self.dolt_data['tables_writes']:
            table_write.set_commit_and_branch(current_branch.name, commit_hash)

