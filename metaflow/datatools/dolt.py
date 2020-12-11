import doltpy
from doltpy.core import Dolt
from doltpy.core.write import import_df
from .. import FlowSpec
from ..current import current


class MDolt(object):
    # TODO: Get rid of cloning and just do api calls.

    def __init__(self, run=None, repoOwner='', repoName=''):
        self.repoOwner = repoOwner
        self.repoName = repoName
        self.run = run

        self.combined = '{}/{}'.format(self.repoOwner, self.repoName)

        try:
            self.repo = Dolt.clone(self.combined)
        except:
            self.repo = Dolt(self.repoName)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        """
        Delete all temporary files downloaded in this context.
        """
        pass

    # TODO: Figure out how to do this all remotely.

    def add_table(self, table_name, df, pks):
        # Get the commit graph in here.
        table_name = '{}_{}'.format(current.step_name, table_name)
        import_df(repo=self.repo, table_name=table_name,
                  data=df, primary_keys=pks)


    def commit_and_push(self):
        if isinstance(self.run, FlowSpec):
            self.repo.add('.')
            self.repo.commit('Run {}'.format(current.run_id))
            self.repo.push('origin', 'master')
        else:
            Exception("This needs to be of instance FlowSpec")
