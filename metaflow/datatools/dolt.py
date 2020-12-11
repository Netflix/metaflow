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

        combined = '{}/{}'.format(self.repoOwner, self.repoName)

        try:
            self.repo = Dolt.clone(combined)
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

    def addTable(self, tableName, df, pks):
        if isinstance(self.run, FlowSpec):
            # print(current.flow_name)
            tableName = '{}_{}_{}'.format(current.run_id,
                                          current.step_name,
                                          tableName)

        import_df(repo=self.repo, table_name=tableName,
                  data=df, primary_keys=pks)
