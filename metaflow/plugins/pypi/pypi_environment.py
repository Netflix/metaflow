from .conda_environment import CondaEnvironment


# To placate people who don't want to see a shred of conda in UX, we symlink
# --environment=pypi to --environment=conda
class PyPIEnvironment(CondaEnvironment):
    TYPE = "pypi"
