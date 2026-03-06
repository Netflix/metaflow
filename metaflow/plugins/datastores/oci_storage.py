import json
import os
import shutil
import uuid
from tempfile import mkdtemp

from metaflow.datastore.datastore_storage import DataStoreStorage, CloseAfterUse
from metaflow.exception import MetaflowException, MetaflowInternalError
from metaflow.metaflow_config import DATASTORE_SYSROOT_OCI, ARTIFACT_LOCALROOT


# ---------------------------------------------------------------------------
# Dependency check (same pattern as check_gs_deps in gcp/gs_utils.py)
# ---------------------------------------------------------------------------

class MetaflowOCIPackageError(MetaflowException):
    headline = "Missing OCI package"

    def __init__(self):
        super().__init__(
            "Could not import the 'oci' package. "
            "Install it with: pip install oci>=2.90.0"
        )


def _check_oci_deps():
    try:
        import oci  # noqa: F401
    except ImportError:
        raise MetaflowOCIPackageError()


def check_oci_deps(func):
    """Decorator that ensures the oci SDK is available before calling *func*."""

    def _wrapper(*args, **kwargs):
        _check_oci_deps()
        return func(*args, **kwargs)

    return _wrapper


# ---------------------------------------------------------------------------
# URI helpers
# ---------------------------------------------------------------------------

def parse_oci_uri(uri):
    """Parse ``oci://<bucket>@<namespace>/<prefix>`` and return
    ``(namespace, bucket, prefix-or-None)``.
    """
    from urllib.parse import urlparse

    scheme, netloc, path, _, _, _ = urlparse(uri)
    assert scheme == "oci", f"Expected 'oci' scheme, got '{scheme}'"
    assert "@" in netloc, (
        f"OCI URI must be oci://<bucket>@<namespace>/..., got '{uri}'"
    )
    bucket, namespace = netloc.split("@", 1)
    prefix = path.lstrip("/").rstrip("/") or None
    return namespace, bucket, prefix


# ---------------------------------------------------------------------------
# Auth helper
# ---------------------------------------------------------------------------

def _get_oci_client():
    """Return an ``oci.object_storage.ObjectStorageClient`` using the first
    auth method that succeeds:

    1. Instance Principals (OCI compute instances)
    2. Resource Principals  (OCI Functions / Container Instances)
    3. Config-based auth    (~/.oci/config fallback)
    """
    import oci

    # 1. Instance Principals
    try:
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        return oci.object_storage.ObjectStorageClient(config={}, signer=signer)
    except Exception:
        pass

    # 2. Resource Principals
    try:
        signer = oci.auth.signers.get_resource_principals_signer()
        return oci.object_storage.ObjectStorageClient(config={}, signer=signer)
    except Exception:
        pass

    # 3. Config file (~/.oci/config)
    config = oci.config.from_file()
    oci.config.validate_config(config)
    return oci.object_storage.ObjectStorageClient(config)


# ---------------------------------------------------------------------------
# OCIStorage
# ---------------------------------------------------------------------------

class OCIStorage(DataStoreStorage):
    TYPE = "oci"

    @check_oci_deps
    def __init__(self, root=None):
        super().__init__(root)
        self._client = None

    # Lazy – avoids auth overhead until actually needed
    @property
    def client(self):
        if self._client is None:
            self._client = _get_oci_client()
        return self._client

    # ---- helpers ----------------------------------------------------------

    def _parts(self, path=None):
        """Return (namespace, bucket, object_name) for *path* relative to
        datastore_root."""
        namespace, bucket, prefix = parse_oci_uri(self.datastore_root)
        if path is not None:
            path = path.lstrip("/")
            object_name = f"{prefix}/{path}" if prefix else path
        else:
            object_name = prefix
        return namespace, bucket, object_name

    # ---- DataStoreStorage interface ---------------------------------------

    @classmethod
    def get_datastore_root_from_config(cls, echo, create_on_absent=True):
        return DATASTORE_SYSROOT_OCI

    def is_file(self, paths):
        results = []
        for p in paths:
            ns, bucket, obj = self._parts(p)
            try:
                self.client.head_object(ns, bucket, obj)
                results.append(True)
            except Exception:
                results.append(False)
        return results

    def info_file(self, path):
        ns, bucket, obj = self._parts(path)
        try:
            resp = self.client.head_object(ns, bucket, obj)
            meta = {}
            if resp.headers:
                opc = resp.headers.get("opc-meta-metaflow-user-attributes")
                if opc:
                    meta = json.loads(opc)
            return True, meta or None
        except Exception:
            return False, None

    def size_file(self, path):
        ns, bucket, obj = self._parts(path)
        try:
            resp = self.client.head_object(ns, bucket, obj)
            return int(resp.headers.get("content-length", 0))
        except Exception:
            return None

    def list_content(self, paths):
        result = []
        for p in paths:
            ns, bucket, obj = self._parts(p)
            prefix = (obj.rstrip("/") + "/") if obj else ""
            next_start = None
            while True:
                resp = self.client.list_objects(
                    ns,
                    bucket,
                    prefix=prefix,
                    delimiter="/",
                    start=next_start,
                )
                listing = resp.data
                for o in listing.objects or []:
                    name = o.name
                    if prefix and name.startswith(prefix):
                        name = name[len(prefix):]
                    result.append(self.list_content_result(name, True))
                for pfx in listing.prefixes or []:
                    name = pfx
                    if prefix and name.startswith(prefix):
                        name = name[len(prefix):]
                    result.append(
                        self.list_content_result(name.rstrip("/"), False)
                    )
                next_start = listing.next_start_with
                if not next_start:
                    break
        return result

    def save_bytes(self, path_and_bytes_iter, overwrite=False, len_hint=0):
        for path, byte_stream in path_and_bytes_iter:
            metadata = None
            if isinstance(byte_stream, tuple):
                byte_stream, metadata = byte_stream
            ns, bucket, obj = self._parts(path)
            if not overwrite:
                try:
                    self.client.head_object(ns, bucket, obj)
                    continue  # already exists
                except Exception:
                    pass
            with byte_stream as bs:
                data = bs.read()
            opc_meta = None
            if metadata is not None:
                opc_meta = {
                    "metaflow-user-attributes": json.dumps(metadata),
                }
            self.client.put_object(
                ns,
                bucket,
                obj,
                data,
                opc_meta=opc_meta,
            )

    def load_bytes(self, keys):
        tmpdir = mkdtemp(dir=ARTIFACT_LOCALROOT, prefix="metaflow.oci.load_bytes.")
        try:
            items = []
            for key in keys:
                ns, bucket, obj = self._parts(key)
                tmp_filename = os.path.join(tmpdir, str(uuid.uuid4()))
                meta = None
                try:
                    resp = self.client.get_object(ns, bucket, obj)
                    with open(tmp_filename, "wb") as f:
                        for chunk in resp.data.raw.stream(1024 * 1024):
                            f.write(chunk)
                    # Read custom metadata
                    head = self.client.head_object(ns, bucket, obj)
                    opc = (head.headers or {}).get(
                        "opc-meta-metaflow-user-attributes"
                    )
                    if opc:
                        meta = json.loads(opc)
                except Exception:
                    tmp_filename = None
                items.append((key, tmp_filename, meta))
        except Exception:
            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir)
            raise

        class _Closer:
            @staticmethod
            def close():
                if os.path.isdir(tmpdir):
                    shutil.rmtree(tmpdir)

        return CloseAfterUse(iter(items), closer=_Closer)
