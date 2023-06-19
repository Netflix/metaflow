from metaflow.exception import MetaflowException


from metaflow import metaflow_config

MAGIC_FILE = "conda.manifest"


# TODO: This can be lifted all the way into metaflow config
def _datastore_packageroot(datastore_type):
    datastore_packageroot = getattr(
        metaflow_config,
        "CONDA_PACKAGE_{datastore_type}ROOT".format(
            datastore_type=datastore_type.upper()
        ),
    )
    if datastore_packageroot is None:
        datastore_sysroot = getattr(
            metaflow_config,
            "DATASTORE_SYSROOT_{datastore_type}".format(
                datastore_type=datastore_type.upper()
            ),
        )
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
