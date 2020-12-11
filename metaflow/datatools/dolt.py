import doltpy
from doltpy.core import Dolt
from doltpy.core.write import import_df


class MDolt(object):
    # TODO: Get rid of cloning and just do api calls.

    def __init__(self, run=None, repoOwner='', repoName=''):
        self.repoOwner = repoOwner
        self.repoName = repoName

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

    # TODO: inference the run and flow name immediately.
    # Figure out how to do this all remotely.

    def addTable(self, tableName, df, pks):
        import_df(repo=self.repo, table_name=tableName,
                  data=df, primary_keys=pks)
