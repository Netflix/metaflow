import doltpy
from doltpy.core import Dolt
from doltpy.core.write import import_df
from doltpy.core.read import read_table
from .. import FlowSpec
from ..current import current


class DoltDT(object):
    def __init__(self, run=None, repoName=''):
        self.repoName = repoName
        self.run = run

        try:
            self.repo = Dolt(self.repoName)
        except:
            return Exception('Repo not cloned locally')

        self.run.dolt = {}

        self.dolt_data = self.run.dolt
        self.dolt_data['repo_name'] = self.repoName
        self.dolt_data['commit_hash'] = self.get_latest_commit_hash()
        self.dolt_data['tables_accessed'] = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        """
        Delete all temporary files downloaded in this context.
        """
        pass
    
    def get_latest_commit_hash(self):
        lg = self.repo.log()
        return lg.popitem(last=False)[0]

    
    def add_table(self, table_name, df, pks):
        # Format the table name and import the dataframe.
        table_name = '{}_{}'.format(current.step_name, table_name)
        import_df(repo=self.repo, table_name=table_name,
                  data=df, primary_keys=pks)
        
        # Attach the dolt imformation to this tables.
        self.dolt_data['tables_accessed'].append(table_name)

    # Return a dataframe
    def get_table(self, table_name):
        try:
            table = read_table(self.repo, table_name)
            self.dolt_data['tables_accessed'].append(table_name)
            return table
        except:
            Exception('Cant access table')


    def commit_and_push(self, allow_empty=True):
        if isinstance(self.run, FlowSpec):
            self.repo.add('.')
            self.repo.commit(message='Run {}'.format(current.run_id), allow_empty=allow_empty)
            self.repo.push('origin', 'master')
            self.dolt_data['commit_hash'] = self.get_latest_commit_hash()
        else:
            Exception("This needs to be of instance FlowSpec")