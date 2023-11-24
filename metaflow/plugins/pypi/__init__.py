from metaflow import metaflow_config
from metaflow.exception import MetaflowException

MAGIC_FILE = "conda.manifest"


# TODO: This can be lifted all the way into metaflow config
def _datastore_packageroot(datastore, echo):
    datastore_type = datastore.TYPE
    datastore_packageroot = getattr(
        metaflow_config,
        "CONDA_PACKAGE_{datastore_type}ROOT".format(
            datastore_type=datastore_type.upper()
        ),
        None,
    )
    if datastore_packageroot is None:
        datastore_sysroot = datastore.get_datastore_root_from_config(echo)
        if datastore_sysroot is None:
            # TODO: Throw a more evocative error message
            raise MetaflowException(
                msg="METAFLOW_DATASTORE_SYSROOT_{datastore_type} must be set!".format(
                    datastore_type=datastore_type.upper()
                )
            )
        datastore_packageroot = "{datastore_sysroot}/conda".format(
            datastore_sysroot=datastore_sysroot
        )
    return datastore_packageroot
