# pyright: strict, reportTypeCommentUsage=false

import errno
from genericpath import isfile
from hashlib import md5
import json
import os
import requests
import shutil
import stat
import subprocess
import tarfile
import tempfile
import time

from concurrent.futures import ThreadPoolExecutor, as_completed
from distutils.version import LooseVersion
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    NamedTuple,
    Optional,
    OrderedDict,
    Sequence,
    Set,
    Tuple,
    TypeVar,
    Union,
    cast,
)
from shutil import which
from urllib.parse import urlparse

from metaflow.datastore import DATASTORES
from metaflow.datastore.datastore_storage import DataStoreStorage
from metaflow.datastore.local_storage import LocalStorage
from metaflow.debug import debug
from metaflow.exception import MetaflowException, MetaflowInternalError
from metaflow.metaflow_config import (
    CONDA_DEPENDENCY_RESOLVER,
    CONDA_LOCAL_DIST_DIRNAME,
    CONDA_LOCAL_DIST,
    CONDA_LOCAL_PATH,
    CONDA_LOCK_TIMEOUT,
    CONDA_PACKAGE_DIRNAME,
    CONDA_PREFERRED_FORMAT,
    CONDA_PREFERRED_RESOLVER,
    CONDA_REMOTE_INSTALLER,
    CONDA_REMOTE_INSTALLER_DIRNAME,
)
from metaflow.metaflow_environment import InvalidEnvironmentException


from .utils import (
    CONDA_FORMATS,
    TRANSMUT_PATHCOMPONENT,
    CondaException,
    CondaStepException,
    arch_id,
    convert_filepath,
    get_conda_root,
    parse_explicit_url,
)

from .env_descr import (
    CachePackage,
    EnvID,
    PackageSpecification,
    ResolvedEnvironment,
    read_conda_manifest,
    write_to_conda_manifest,
)

_CONDA_DEP_RESOLVERS = ("conda", "mamba")

ParseURLResult = NamedTuple(
    "ParseURLResult",
    [("filename", str), ("format", str), ("hash", str), ("is_transmuted", bool)],
)


class Conda(object):
    _cached_info = None

    def __init__(
        self, echo: Callable[..., None], datastore_type: str, mode: str = "local"
    ):
        from metaflow.cli import logger

        if id(echo) != id(logger):

            def _modified_logger(*args: Any, **kwargs: Any):
                if "timestamp" in kwargs:
                    del kwargs["timestamp"]
                echo(*args, **kwargs)

            self._echo = _modified_logger
        else:
            self._echo = echo

        self._datastore_type = datastore_type
        self._mode = mode
        self._bins = None  # type: Optional[Dict[str, str]]
        self._dependency_solver = CONDA_DEPENDENCY_RESOLVER.lower()  # type: str
        self._have_micromamba = False  # True if the installer is micromamba
        self._use_conda_lock_to_resolve = (
            CONDA_PREFERRED_RESOLVER == "conda-lock"
        )  # type: bool
        self._find_conda_binary()

        # Figure out what environments we know about locally
        self._local_root = LocalStorage.get_datastore_root_from_config(
            echo
        )  # type: str
        self._cached_environment = read_conda_manifest(self._local_root)

        # Initialize storage
        if self._datastore_type != "local":
            # We will be able to cache things -- currently no caching for local
            storage_impl = DATASTORES[self._datastore_type]
            self._storage = storage_impl(
                get_conda_root(self._datastore_type)
            )  # type: Optional[DataStoreStorage]
        else:
            self._storage = None

    def binary(self, binary: str) -> Optional[str]:
        if self._bins:
            return self._bins.get(binary)
        return None

    def resolve(
        self,
        using_steps: Sequence[str],
        deps: Sequence[str],
        channels: Sequence[str],
        architecture: str,
        user_env_name: Optional[str] = None,
    ) -> ResolvedEnvironment:
        if self._mode != "local":
            # TODO: Maybe relax this later but for now assume that the remote environment
            # is a "lighter" conda.
            raise CondaException("Cannot resolve environments in a remote environment")

        try:
            if self._use_conda_lock_to_resolve:
                packages = self._resolve_env_with_conda_lock(
                    deps, channels, architecture
                )
            else:
                packages = self._resolve_env_with_conda(deps, channels, architecture)
            return ResolvedEnvironment(
                deps,
                channels,
                architecture,
                user_alias=user_env_name,
                all_packages=packages,
            )
        except CondaException as e:
            raise CondaStepException(e, using_steps)

    def create_for_step(
        self,
        step_name: str,
        env: ResolvedEnvironment,
        do_symlink: bool = False,
    ):
        try:
            # I am not 100% sure the lock is required but since the environments share
            # a common package cache, we will keep it for now
            env_name = self._env_directory_from_envid(env.env_id)
            return self.create_for_name(env_name, env, do_symlink)
        except CondaException as e:
            raise CondaStepException(e, [step_name])

    def create_for_name(
        self, name: str, env: ResolvedEnvironment, do_symlink: bool = False
    ):
        with CondaLock(self._env_lock_file(name)):
            self._create(env, name)
        if do_symlink:
            python_path = self.python(name)
            if python_path:
                os.symlink(python_path, os.path.join(os.getcwd(), "__conda_python"))

    def remove_for_step(self, step_name: str, env_id: EnvID):
        # Remove the conda environment
        try:
            env_name = self._env_directory_from_envid(env_id)
            return self.remove_for_name(env_name, env_id)

        except CondaException as e:
            raise CondaStepException(e, [step_name])

    def remove_for_name(self, name: str, env_id: EnvID):
        with CondaLock(self._env_lock_file(name)):
            self._remove(name, env_id)

    def python(self, env_desc: Union[EnvID, str]) -> Optional[str]:
        # Get Python interpreter for the conda environment
        env_path = None
        if isinstance(env_desc, EnvID):
            env_path = self.created_environment(env_desc)
            if env_path:
                env_path = env_path[1]
        else:
            env_paths = self._created_envs(env_desc, full_match=True)
            if len(env_paths) == 1:
                env_path = list(env_paths.values())[0]
                if len(env_path) == 1:
                    env_path = env_path[0]
                else:
                    env_path = None  # We don't do ambiguous
        if env_path:
            return os.path.join(env_path, "bin/python")
        return None

    def created_environment(
        self, env_desc: Union[EnvID, str]
    ) -> Optional[Tuple[EnvID, str]]:
        if isinstance(env_desc, EnvID):
            prefix = "metaflow_%s_%s" % (env_desc.req_id, env_desc.full_id)
        else:
            prefix = env_desc
        envs = self._created_envs(prefix, full_match=True)
        if len(envs) == 1:
            return [(k, v[0]) for k, v in envs.items()][0]
        return None

    def created_environments(
        self, req_id: Optional[str] = None
    ) -> Dict[EnvID, List[str]]:
        # List all existing metaflow environments; this can include environments that
        # were created with the `environment` command and therefore have a different
        # name
        prefix = "metaflow_%s_" % req_id if req_id else ""
        return {
            k: v
            for k, v in self._created_envs(prefix).items()
            if k.req_id != "_invalid"
        }

    def environment(
        self, env_id: EnvID, local_only: bool = False
    ) -> Optional[ResolvedEnvironment]:
        # First look if we have base_env_id locally
        env = self._cached_environment.env_for(*env_id)

        debug.conda_exec("%s%sfound locally" % (str(env_id), " " if env else " not "))

        if not local_only and not env and self._storage:
            env = self._remote_env_fetch([env_id])
            if env:
                env = env[0]
            else:
                env = None

        return env

    def environments(
        self, req_id: str, arch: Optional[str] = None, local_only: bool = False
    ) -> List[Tuple[EnvID, ResolvedEnvironment]]:
        arch = arch or arch_id()
        if not local_only and self._storage is not None:
            # If we are looking for remote stuff, we fetch all the full IDs the remote
            # side knows about and get all those in our cache first and then return
            # what we have
            base_path = self.get_datastore_path_to_env(EnvID(req_id, "_unknown", arch))
            base_path = "/".join(base_path.split("/")[:-2])
            full_ids = [
                os.path.basename(path.rstrip("/"))
                for path, is_file in self._storage.list_content([base_path])
                if not is_file
            ]  # type: List[str]
            self._remote_env_fetch(
                [EnvID(req_id=req_id, full_id=x, arch=arch) for x in full_ids]
            )

        return list(self._cached_environment.envs_for(req_id, arch))

    def set_default_environment(self, env_id: EnvID) -> None:
        self._cached_environment.set_default(env_id)

    def add_environments(self, resolved_envs: List[ResolvedEnvironment]) -> None:
        for resolved_env in resolved_envs:
            self._cached_environment.add_resolved_env(resolved_env)

    def cache_environments(
        self, resolved_envs: List[ResolvedEnvironment], cache_formats: List[str] = None
    ) -> None:
        # The logic behind this function is as follows:
        #  - check in the S3/Azure storage to see if the file exists
        #  - if it does, we are all good and we update the cache_urls
        #  - if it does not, check if the file is locally installed (if same arch)
        #    + if installed, check if it matches the MD5 hash and if all checks out, use to upload
        #    + if not, download it
        #  - at this point, we have the tarballs so upload to S3/Azure

        # We cache multiple environments at once because we can benefit from overlaps
        # between environments which is likely across multiple similar environments

        if self._storage is None:
            raise CondaException(
                "Cannot cache environments since no datastore configured"
            )

        cache_paths_to_check = []  # type: List[Tuple[str, str, str]]
        my_arch_id = arch_id()
        cache_formats = cache_formats or [CONDA_PREFERRED_FORMAT]
        # Contains the architecture, the list of packages that need the URL
        # and the list of formats needed
        url_to_pkgs = {}  # type: Dict[str, Tuple[str, List[PackageSpecification]]]

        for resolved_env in resolved_envs:
            # We are now going to try to figure out all the locations we need to check
            # in the cache for the presence of the files we need
            env_id = resolved_env.env_id
            for req_pkg in resolved_env.packages:
                if req_pkg.url in url_to_pkgs:
                    # We add ourself to the end of the list and update the
                    # first package with any additional information we have about
                    # cache files as it will be used to lazily fetch things (so should
                    # have all up-to-date info).
                    old_arch, all_pkgs = url_to_pkgs[req_pkg.url]
                    all_pkgs.append(req_pkg)
                    to_update_pkg = all_pkgs[0]
                    for pkg_fmt, pkg_hash in req_pkg.pkg_hashes:
                        to_update_pkg.add_pkg_hash(pkg_fmt, pkg_hash)
                    for pkg_fmt, cache_pkg in req_pkg.cached_versions:
                        to_update_pkg.add_cached_version(
                            pkg_fmt, CachePackage(cache_pkg.url)
                        )
                    # It is possible that arch is different (for noarch packages). In
                    # that case, we prefer to download it for our own arch to place the
                    # file in the right place for later. It doesn't matter for anything
                    # else
                    if env_id.arch == my_arch_id and old_arch != env_id.arch:
                        url_to_pkgs[req_pkg.url] = (my_arch_id, all_pkgs)
                else:
                    url_to_pkgs[req_pkg.url] = (env_id.arch, [req_pkg])

        # At this point, we can check if we are missing any cache information
        for base_url, (_, all_pkgs) in url_to_pkgs.items():
            pkg = all_pkgs[0]
            if not all([pkg.cached_version(f) != None for f in cache_formats]):
                # We are missing some cache information. We will check for the base
                # format and any links to other formats
                base_cache_url = CachePackage.make_cache_url(
                    base_url, cast(str, pkg.pkg_hash(pkg.url_format))
                )
                cache_paths_to_check.append((base_cache_url, base_url, pkg.url_format))
                debug.conda_exec(
                    "%s:%s -> check file @ %s"
                    % (pkg.filename, pkg.url_format, base_cache_url)
                )
                for f in cache_formats:
                    if f == pkg.url_format or pkg.cached_version(f) is not None:
                        continue
                    lnk_path = self._lnk_path_for_pkg(pkg, f)
                    cache_paths_to_check.append((lnk_path, base_url, f))
                    debug.conda_exec(
                        "%s:%s -> check link @ %s" % (pkg.filename, f, lnk_path)
                    )

        files_exist = self._storage.is_file(
            [x[0] for x in cache_paths_to_check]
        )  # type: List[bool]

        link_files_to_get = {}  # type: Dict[str, Tuple[str, str]]
        pkgs_to_fetch_per_arch = {}  # type: Dict[str, List[PackageSpecification]]
        # NOTE: saw_url allows us to fetch only *one* package. Note that some packages
        # are shared across arch (the noarch packages). This will ensure we download it
        # only once and basically for this arch if we are using this arch (so it will
        # be in the right place for later -- it will also be properly cached and
        # other archs will be updated)
        saw_url = set()  # type: Set[str]
        for exist, (req_url, base_url, pkg_fmt) in zip(
            files_exist, cache_paths_to_check
        ):
            arch, all_pkgs = url_to_pkgs[base_url]
            pkg = all_pkgs[0]
            if exist:
                if pkg_fmt != pkg.url_format:
                    # This means that we have a .lnk file, we need to actually fetch
                    # it and determine where it points to so we can update the cache URLs
                    # and hashes
                    debug.conda_exec(
                        "%s:%s -> Found link file" % (pkg.filename, pkg_fmt)
                    )
                    link_files_to_get[req_url] = (base_url, pkg_fmt)
                else:
                    debug.conda_exec(
                        "%s -> Found cache file %s" % (pkg.filename, req_url)
                    )
                    cache_pkg = CachePackage(req_url)
                    pkg.add_cached_version(pkg_fmt, cache_pkg)
            else:
                if base_url not in saw_url:
                    # If we don't have it in cache, we will need to actually fetch it
                    pkgs_to_fetch_per_arch.setdefault(arch, []).append(pkg)
                    saw_url.add(base_url)

        # Get the link files to properly update the cache packages and hashes
        if link_files_to_get:
            with self._storage.load_bytes(link_files_to_get.keys()) as loaded:
                for key, tmpfile, _ in loaded:
                    base_url, pkg_fmt = link_files_to_get[key]
                    _, all_pkgs = url_to_pkgs[base_url]
                    pkg = all_pkgs[0]
                    with open(tmpfile, mode="r", encoding="utf-8") as f:
                        cached_path = f.read().strip()
                        debug.conda_exec(
                            "%s:%s -> File at %s" % (pkg.filename, pkg_fmt, cached_path)
                        )
                        pkg.add_cached_version(pkg_fmt, CachePackage(cached_path))

        # Fetch what we need; we fetch all formats for all packages (this is a superset
        # but simplifies the code)
        upload_files = []  # type: List[Tuple[str, str]]

        def _cache_pkg(pkg: PackageSpecification, pkg_fmt: str, local_path: str) -> str:
            file_hash = cast(str, pkg.pkg_hash(pkg_fmt))
            cache_path = CachePackage.make_cache_url(
                pkg.url,
                file_hash,
                pkg_fmt,
                pkg.is_transmuted(pkg_fmt),
            )
            upload_files.append((cache_path, local_path))
            debug.conda_exec(
                "%s:%s -> will upload %s to %s"
                % (pkg.filename, pkg_fmt, local_path, cache_path)
            )
            pkg.add_cached_version(pkg_fmt, CachePackage(cache_path))
            return cache_path

        with tempfile.TemporaryDirectory() as download_dir:
            for arch, pkgs in pkgs_to_fetch_per_arch.items():
                arch_tmpdir = os.path.join(download_dir, arch)
                os.mkdir(arch_tmpdir)
                self.lazy_fetch_packages(
                    pkgs,
                    require_format=cache_formats,
                    requested_arch=arch,
                    tempdir=arch_tmpdir,
                )

                # Upload what we need to upload and update the representative package with
                # the new cache information. For all packages, we basically check if we have
                # a local file but not a cached version and upload that.
                for pkg in pkgs:
                    # We first need to check for url_format because the links are then
                    # created from there
                    if not pkg.cached_version(pkg.url_format):
                        _cache_pkg(
                            pkg,
                            pkg.url_format,
                            cast(str, pkg.local_file(pkg.url_format)),
                        )
                    for local_fmt, local_path in pkg.local_files:
                        if not pkg.cached_version(local_fmt):
                            # Since we just cached the url_format version (if needed),
                            # we know that here this is not it so we always create a link
                            cache_path = _cache_pkg(pkg, local_fmt, local_path)
                            lnk_path = self._lnk_path_for_pkg(pkg, local_fmt)
                            with tempfile.NamedTemporaryFile(
                                delete=False, mode="w", encoding="utf-8"
                            ) as lnk_file:
                                debug.conda_exec(
                                    "%s:%s -> will upload link @@%s@@ to %s"
                                    % (
                                        pkg.filename,
                                        local_fmt,
                                        cache_path,
                                        lnk_path,
                                    )
                                )
                                lnk_file.write(cache_path)
                                upload_files.append((lnk_path, lnk_file.name))

            # We now update all packages with the information from the first package which
            # we have been updating all this time. They are all the same but are referenced
            # from different environments.
            for _, all_pkgs in url_to_pkgs.values():
                cannonical_pkg = all_pkgs[0]
                for pkg in all_pkgs[1:]:
                    # We only really care about hashes and cached versions
                    for pkg_fmt, pkg_hash in cannonical_pkg.pkg_hashes:
                        pkg.add_pkg_hash(pkg_fmt, pkg_hash)
                    for pkg_fmt, pkg_cache in cannonical_pkg.cached_versions:
                        pkg.add_cached_version(pkg_fmt, CachePackage(pkg_cache.url))

            # At this point, the ResolvedEnvironments have been fully updated; we can
            # cache them
            for resolved_env in resolved_envs:
                env_id = resolved_env.env_id
                local_filepath = os.path.join(download_dir, "%s_%s_%s.env" % env_id)
                cache_path = self.get_datastore_path_to_env(env_id)
                with open(local_filepath, mode="w", encoding="utf-8") as f:
                    json.dump(resolved_env.to_dict(), f)
                upload_files.append((cache_path, local_filepath))
                debug.conda_exec("Will upload env %s to %s" % (str(env_id), cache_path))

            if upload_files:
                start = time.time()
                self._echo(
                    "    Caching %d items to %s ..."
                    % (len(upload_files), self._datastore_type),
                    nl=False,
                )
                self._upload_to_ds(upload_files)
                self._echo(" done in %d seconds." % int(time.time() - start))
            else:
                self._echo("    All items already cached in %s." % self._datastore_type)

    def write_out_environments(self) -> None:
        write_to_conda_manifest(self._local_root, self._cached_environment)

    def lazy_fetch_packages(
        self,
        packages: Iterable[PackageSpecification],
        require_format: Optional[Sequence[str]] = None,
        requested_arch: str = arch_id(),
        tempdir: Optional[str] = None,
    ):
        # Lazily fetch all packages specified by the PackageSpecification
        # for requested_arch.
        #
        # At the end of this function, all packages will either be:
        #  - present as a local directory (pkg.local_dir() is not None)
        #  - present as a local file (pkg.local_file(fmt) is not None for some fmt) and
        #    points to a tarball representing the package
        #
        # You can optionally request formats which will force the presence of a particular
        # local file (ie: even if a directory is present for the package, it will
        # still fetch the tarball)
        #
        # Tarballs are fetched from cache if available and the web if not. Package
        # transmutation also happens if the requested format is not found.

        if require_format is None:
            require_format = []

        use_package_dirs = True
        if requested_arch != arch_id():
            if tempdir is None:
                raise MetaflowInternalError(
                    "Cannot lazily fetch packages for another architecture "
                    "without a temporary directory"
                )
            use_package_dirs = False

        cache_downloads = []  # type: List[Tuple[PackageSpecification, str, str]]
        web_downloads = []  # type: List[Tuple[PackageSpecification, str]]
        transmutes = []  # type: List[Tuple[PackageSpecification, str]]
        url_adds = []  # type: List[str]
        known_urls = set()  # type: Set[str]

        # Helper functions
        def _add_to_fetch_lists(
            pkg_spec: PackageSpecification, pkg_format: str, mode: str, dst: str
        ):
            if mode == "cache":
                cache_downloads.append((pkg_spec, pkg_format, dst))
            elif mode == "web":
                web_downloads.append((pkg_spec, dst))

        def _download_web(
            session: requests.Session, entry: Tuple[PackageSpecification, str]
        ) -> Tuple[PackageSpecification, Optional[Exception]]:
            pkg_spec, local_path = entry
            md5_hash = md5()
            debug.conda_exec(
                "%s -> download %s to %s"
                % (pkg_spec.filename, pkg_spec.url, local_path)
            )
            try:
                with open(local_path, "wb") as f:
                    with session.get(pkg_spec.url, stream=True) as r:
                        for chunk in r.iter_content(chunk_size=None):
                            md5_hash.update(chunk)
                            f.write(chunk)
                pkg_spec.add_local_file(
                    pkg_spec.url_format,
                    local_path,
                    pkg_hash=md5_hash.hexdigest(),
                    downloaded=True,
                )
            except Exception as e:
                return (pkg_spec, e)
            return (pkg_spec, None)

        def _transmute(
            entry: Tuple[PackageSpecification, str]
        ) -> Tuple[PackageSpecification, Optional[str], Optional[Exception]]:
            pkg_spec, src_format = entry
            debug.conda_exec("%s -> transmute %s" % (pkg_spec.filename, src_format))

            def _cph_transmute(src_file: str, dst_file: str, dst_format: str):
                args = [
                    "t",
                    "--processes",
                    "1",
                    "--zstd-compression-level",
                    "3",
                    "--force",
                    "--out-folder",
                    os.path.dirname(dst_file),
                    src_file,
                    dst_format,
                ]
                self._call_conda(args, binary="cph")

            def _micromamba_transmute(src_file: str, dst_file: str, dst_format: str):
                args = ["package", "transmute", "-c", "3", src_file]
                self._call_conda(args, binary="micromamba")

            try:
                src_file = pkg_spec.local_file(src_format)
                if src_file is None:
                    raise ValueError(
                        "Need to transmute %s but %s does not have a local file in that format"
                        % (src_format, pkg_spec.filename)
                    )
                dst_format = [f for f in CONDA_FORMATS if f != src_format][0]
                dst_file = src_file[: -len(src_format)] + dst_format

                # For now, prefer cph due to issue in micromamba
                if "cph" in cast(Dict[str, str], self._bins):
                    _cph_transmute(src_file, dst_file, dst_format)
                elif "micromamba" in cast(Dict[str, str], self._bins):
                    _micromamba_transmute(src_file, dst_file, dst_format)
                else:
                    raise CondaException(
                        "Requesting to transmute package without cph or micromamba"
                    )

                pkg_spec.add_local_file(dst_format, dst_file, transmuted=True)
            except CondaException as e:
                return (pkg_spec, None, e)
            return (pkg_spec, dst_format, None)

        # Setup package_dirs which is where we look for existing packages.
        if use_package_dirs:
            package_dirs = self._package_dirs
        else:
            assert tempdir is not None  # Keep pyright happy
            package_dirs = [tempdir]

        # We are only adding to package_dirs[0] so we only read that one into known_urls
        with CondaLock(self._package_dir_lock_file(package_dirs[0])):
            url_file = os.path.join(package_dirs[0], "urls.txt")
            if os.path.isfile(url_file):
                with open(url_file, "rb") as f:
                    known_urls.update([l.strip().decode("utf-8") for l in f])

        # Iterate over all the filenames that we want to fetch.
        for pkg_spec in packages:
            found_dir = False
            # Look for it to exist in any of the package_dirs
            for p in [d for d in package_dirs if os.path.isdir(d)]:
                extract_path = os.path.join(p, pkg_spec.filename)
                if not require_format and os.path.isdir(extract_path):
                    debug.conda_exec(
                        "%s -> using existing directory %s"
                        % (pkg_spec.filename, extract_path)
                    )
                    pkg_spec.add_local_dir(extract_path)
                    # results.append(
                    #     LazyFetchResult(
                    #         filename=filename,
                    #         url=base_url,
                    #         is_dir=True,
                    #         per_format_desc={
                    #             ".local": {
                    #                 "cache_url": None,
                    #                 "local_path": extract_path,
                    #                 "fetched": False,
                    #                 "cached": False,
                    #                 "transmuted": False,
                    #             }
                    #         },
                    #     )
                    # )
                    found_dir = True
                    break
                # At this point, we don't have a directory or we need specific tarballs
                for f in CONDA_FORMATS:
                    # We may have found the file in another directory
                    if pkg_spec.local_file(f):
                        continue
                    tentative_path = os.path.join(p, "%s%s" % (pkg_spec.filename, f))
                    if os.path.isfile(tentative_path):
                        try:
                            pkg_spec.add_local_file(f, tentative_path)
                        except ValueError:
                            debug.conda_exec(
                                "%s -> rejecting %s due to hash mismatch (expected %s)"
                                % (
                                    pkg_spec.filename,
                                    tentative_path,
                                    pkg_spec.pkg_hash(f),
                                )
                            )
            if found_dir:
                # We didn't need specific tarballs and we found a directory -- happy clam
                continue

            # This code extracts the most preferred source of filename as well as a list
            # of where we can get a given format
            # The most_preferred_source will be:
            #  - if it exists, a local file (among local files, prefer CONDA_PREFERRED_FORMAT)
            #  - if no local files, if they exist, a cached version
            #    (among cached versions, prefer CONDA_PREFERRED_FORMAT)
            #  - if no cached version, the web download
            most_preferred_source = None
            most_preferred_format = None
            available_formats = {}  # type: Dict[str, Tuple[str, str]]
            dl_local_path = os.path.join(
                package_dirs[0], "%s{format}" % pkg_spec.filename
            )
            # Check for local files first
            for f in CONDA_FORMATS:
                local_path = pkg_spec.local_file(f)
                if local_path:
                    src = ("local", local_path)
                    if most_preferred_source is None:
                        most_preferred_source = src
                        most_preferred_format = f
                    available_formats[f] = src

            # Check for cache paths next
            for f in CONDA_FORMATS:
                cache_info = pkg_spec.cached_version(f)
                if cache_info and cache_info.url:
                    src = ("cache", dl_local_path.format(format=f))
                    if most_preferred_source is None:
                        most_preferred_source = src
                        most_preferred_format = f
                    if f not in available_formats:
                        available_formats[f] = src

            # And finally, fall back on the web
            web_src = (
                "web",
                dl_local_path.format(format=pkg_spec.url_format),
            )
            if most_preferred_source is None:
                most_preferred_source = web_src
                most_preferred_format = pkg_spec.url_format
            if pkg_spec.url_format not in available_formats:
                available_formats[pkg_spec.url_format] = web_src

            debug.conda_exec(
                "%s -> preferred %s @ %s"
                % (
                    pkg_spec.filename,
                    most_preferred_format,
                    most_preferred_source[0],
                )
            )

            assert most_preferred_format
            fetched_formats = [most_preferred_format]
            _add_to_fetch_lists(pkg_spec, most_preferred_format, *most_preferred_source)
            for f in [
                f
                for f in require_format
                if f in available_formats and f != most_preferred_format
            ]:
                _add_to_fetch_lists(pkg_spec, f, *available_formats[f])
                fetched_formats.append(f)

            # For anything that we need and we don't have an available source for,
            # we transmute from our most preferred source.
            for f in [f for f in require_format if f not in fetched_formats]:
                transmutes.append((pkg_spec, most_preferred_format))

        # Done going over all the files
        do_download = web_downloads or cache_downloads
        if do_download:
            start = time.time()
            self._echo(
                "    Downloading %d(web) + %d(cache) packages ..."
                % (len(web_downloads), len(cache_downloads)),
                nl=False,
            )

        # Ensure the packages directory exists at the very least
        if do_download and not os.path.isdir(package_dirs[0]):
            os.makedirs(package_dirs[0])

        pending_errors = []  # type: List[str]
        if web_downloads:
            with ThreadPoolExecutor() as executor:
                with requests.Session() as s:
                    a = requests.adapters.HTTPAdapter(
                        pool_connections=executor._max_workers,
                        pool_maxsize=executor._max_workers,
                        max_retries=3,
                    )
                    s.mount("https://", a)
                    download_results = [
                        executor.submit(_download_web, s, entry)
                        for entry in web_downloads
                    ]
                    for f in as_completed(download_results):
                        pkg_spec, error = f.result()
                        if error is None:
                            if pkg_spec.url not in known_urls:
                                url_adds.append(pkg_spec.url)
                        else:
                            pending_errors.append(
                                "Error downloading package for '%s': %s"
                                % (pkg_spec.filename, str(error))
                            )

        if cache_downloads:
            storage = DATASTORES[self._datastore_type](
                get_conda_root(self._datastore_type)
            )  # type: DataStoreStorage
            keys_to_info = (
                {}
            )  # type: Dict[str, Tuple[PackageSpecification, str, str, str]]
            for pkg_spec, pkg_format, local_path in cache_downloads:
                cache_info = pkg_spec.cached_version(pkg_format)
                if cache_info:
                    keys_to_info[cache_info.url] = (
                        pkg_spec,
                        pkg_format,
                        cache_info.hash,
                        local_path,
                    )
                else:
                    pending_errors.append(
                        "Internal error: trying to download a non-existent cache item for %s"
                        % pkg_spec.filename
                    )
            with storage.load_bytes(keys_to_info.keys()) as load_results:  # type: ignore
                for (key, tmpfile, _) in load_results:  # type: ignore
                    pkg_spec, pkg_format, pkg_hash, local_path = keys_to_info[key]  # type: ignore
                    if not tmpfile:
                        pending_errors.append(
                            "Error downloading package from cache for '%s': not found at %s"
                            % (pkg_spec.filename, key)
                        )
                    else:
                        url_to_add = self._make_urlstxt_from_cacheurl(key)  # type: ignore
                        if url_to_add not in known_urls:
                            url_adds.append(url_to_add)
                        shutil.move(tmpfile, local_path)  # type: ignore
                        # We consider stuff in the cache to be clean in terms of hash
                        # so we don't want to recompute it.
                        pkg_spec.add_local_file(
                            pkg_format, local_path, pkg_hash, downloaded=True
                        )

        if do_download:
            self._echo(
                " done in %d seconds." % int(time.time() - start), timestamp=False
            )
        if not pending_errors and transmutes:
            start = time.time()
            self._echo("    Transmuting %d packages ..." % len(transmutes), nl=False)
            with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
                transmut_results = [
                    executor.submit(_transmute, entry) for entry in transmutes
                ]
                for f in as_completed(transmut_results):
                    pkg_spec, dst_format, error = f.result()
                    if error:
                        pending_errors.append(
                            "Error transmuting '%s': %s" % (pkg_spec.filename, error)
                        )
                    else:
                        new_url = self._make_urlstxt_from_url(
                            pkg_spec.url, dst_format, is_transmuted=True
                        )

                        if new_url not in known_urls:
                            url_adds.append(new_url)
            self._echo(
                " done in %d seconds." % int(time.time() - start), timestamp=False
            )
        if url_adds:
            # Update the urls file in the packages directory so that Conda knows that the
            # files are there
            debug.conda_exec(
                "Adding the following URLs to %s: %s"
                % (os.path.join(package_dirs[0], "urls.txt"), str(url_adds))
            )
            with CondaLock(self._package_dir_lock_file(package_dirs[0])):
                with open(
                    os.path.join(package_dirs[0], "urls.txt"),
                    mode="a",
                    encoding="utf-8",
                ) as f:
                    f.writelines(["%s\n" % l for l in url_adds])
        if pending_errors:
            raise CondaException("\n".join(pending_errors))

    def get_datastore_path_to_env(self, env_id: EnvID) -> str:
        # Returns a path to where we store a resolved environment's information
        return os.path.join("envs", env_id.arch, env_id.req_id, env_id.full_id, "env")

    def _resolve_env_with_conda(
        self,
        deps: Sequence[str],
        channels: Sequence[str],
        architecture: str,
    ) -> List[PackageSpecification]:
        result = []
        with tempfile.TemporaryDirectory() as mamba_dir:
            args = [
                "create",
                "--prefix",
                os.path.join(mamba_dir, "prefix"),
                "--dry-run",
            ]
            for c in channels:
                args.extend(["-c", c])

            args.extend(deps)

            addl_env = {
                "CONDA_SUBDIR": architecture,
                "CONDA_PKGS_DIRS": mamba_dir,
                "CONDA_ROOT": self._info["root_prefix"],
                "CONDA_UNSATISFIABLE_HINTS_CHECK_DEPTH": "0",
            }
            conda_result = json.loads(self._call_conda(args, addl_env=addl_env))

        # This returns a JSON blob with:
        #  - actions:
        #    - FETCH: List of objects to fetch -- this is where we get hash and URL
        #    - LINK: Packages to actually install (in that order)
        if not conda_result["success"]:
            raise CondaException(
                "Could not resolve environment: %s" % str(conda_result)
            )

        def _pkg_key(
            name: str, platform: str, build_string: str, build_number: str
        ) -> str:
            return "%s_%s_%s_%s" % (name, platform, build_string, build_number)

        fetched_packages = {}  # type: Dict[str, Tuple[str, str]]
        result = []  # type: List[PackageSpecification]
        for pkg in conda_result["actions"]["FETCH"]:
            fetched_packages[
                _pkg_key(pkg["name"], pkg["subdir"], pkg["build"], pkg["build_number"])
            ] = (pkg["url"], pkg["md5"])
        for lnk in conda_result["actions"]["LINK"]:
            k = _pkg_key(
                lnk["name"],
                lnk["platform"],
                lnk["build_string"],
                lnk["build_number"],
            )
            url, md5_hash = fetched_packages[k]
            if not url.startswith(lnk["base_url"]):
                raise CondaException(
                    "Unexpected record for %s: %s" % (k, str(conda_result))
                )
            parse_result = parse_explicit_url("%s#%s" % (url, md5_hash))
            result.append(
                PackageSpecification(
                    filename=parse_result.filename,
                    url=parse_result.url,
                    url_format=parse_result.url_format,
                    hashes={parse_result.url_format: parse_result.hash},
                )
            )
        return result

    def _resolve_env_with_conda_lock(
        self,
        deps: Sequence[str],
        channels: Sequence[str],
        architecture: str,
    ) -> List[PackageSpecification]:
        outfile_name = None
        try:
            # We resolve the environment using conda-lock

            # Write out the requirement yml file. It's easy enough so don't use a YAML
            # library to avoid adding another dep

            # Add channels
            lines = ["channels:\n"]
            lines.extend(["  - %s" % c for c in channels])
            lines.append("  - conda-forge\n")

            # Add deps
            lines.append("dependencies:\n")
            lines.extend(["  - %s\n" % d for d in deps])

            assert self._bins

            with tempfile.NamedTemporaryFile(
                mode="w", encoding="ascii", delete=True
            ) as input_yml:
                input_yml.writelines(lines)
                input_yml.flush()
                outfile_name = "conda-lock-gen-%s" % os.path.basename(input_yml.name)
                args = [
                    "lock",
                    "-f",
                    input_yml.name,
                    "-p",
                    architecture,
                    "--filename-template",
                    outfile_name,
                    "-k",
                    "explicit",
                    "--conda",
                    self._bins["conda"],
                ]
                if self._dependency_solver == "mamba":
                    args.append("--mamba")

                # If arch_id() == architecture, we also use the same virtual packages
                # as the ones that exist on the machine to mimic the current behavior
                # of conda/mamba
                if arch_id() == architecture:
                    lines = ["subdirs:\n", "  %s:\n" % architecture, "    packages:\n"]
                    virtual_pkgs = self._info["virtual_pkgs"]
                    lines.extend(
                        [
                            "      %s: %s-%s\n" % (pkg_name, pkg_version, pkg_id)
                            for pkg_name, pkg_version, pkg_id in virtual_pkgs
                        ]
                    )
                    with tempfile.NamedTemporaryFile(
                        mode="w", encoding="ascii", delete=True
                    ) as virtual_yml:
                        virtual_yml.writelines(lines)
                        virtual_yml.flush()
                        args.extend(["--virtual-package-spec", virtual_yml.name])

                        self._call_conda(args, binary="conda-lock")
                else:
                    self._call_conda(args, binary="conda-lock")
            # At this point, we need to read the explicit dependencies in the file created
            emit = False
            result = []  # type: List[PackageSpecification]
            with open(outfile_name, "r", encoding="utf-8") as out:
                for l in out:
                    if emit:
                        parse_result = parse_explicit_url(l.strip())
                        result.append(
                            PackageSpecification(
                                parse_result.filename,
                                parse_result.url,
                                parse_result.url_format,
                                {parse_result.url_format: parse_result.hash},
                            )
                        )
                    if not emit and l.strip() == "@EXPLICIT":
                        emit = True
            return result
        finally:
            if outfile_name and os.path.isfile(outfile_name):
                os.unlink(outfile_name)

    def _find_conda_binary(self):
        if self._dependency_solver not in _CONDA_DEP_RESOLVERS:
            raise InvalidEnvironmentException(
                "Invalid Conda dependency resolver %s, valid candidates are %s."
                % (self._dependency_solver, _CONDA_DEP_RESOLVERS)
            )
        if self._mode == "local":
            self._ensure_local_conda()
        else:
            # Remote mode -- we install a conda environment or make sure we have
            # one already there
            self._ensure_remote_conda()

        err = self._validate_conda_installation()
        if err:
            raise err

    def _ensure_local_conda(self):
        if CONDA_LOCAL_PATH is not None:
            # We need to look in a specific place
            self._bins = {
                "conda": os.path.join(CONDA_LOCAL_PATH, "bin", self._dependency_solver),
                "conda-lock": os.path.join(CONDA_LOCAL_PATH, "bin", "conda-lock"),
                "micromamba": os.path.join(CONDA_LOCAL_PATH, "bin", "micromamba"),
                "cph": os.path.join(CONDA_LOCAL_PATH, "bin", "cph"),
            }
            if self._validate_conda_installation():
                # This means we have an exception so we are going to try to install
                with CondaLock(
                    os.path.abspath(
                        os.path.join(CONDA_LOCAL_PATH, "..", ".conda-install.lock")
                    )
                ):
                    if self._validate_conda_installation():
                        self._install_local_conda()
        else:
            self._bins = {
                "conda": which(self._dependency_solver),
                "conda-lock": which("conda-lock"),
                "micromamba": which("micromamba"),
                "cph": which("cph"),
            }

    def _install_local_conda(self):
        start = time.time()
        path = CONDA_LOCAL_PATH  # type: str
        self._echo("    Installing Conda environment at %s ..." % path, nl=False)
        shutil.rmtree(path, ignore_errors=True)

        try:
            os.makedirs(path)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

        path_to_fetch = os.path.join(
            CONDA_LOCAL_DIST_DIRNAME,
            CONDA_LOCAL_DIST.format(arch=arch_id()),
        )
        storage = DATASTORES[self._datastore_type](
            get_conda_root(self._datastore_type)
        )  # type: DataStoreStorage
        with tempfile.NamedTemporaryFile() as tmp:
            with storage.load_bytes([path_to_fetch]) as load_results:
                for _, tmpfile, _ in load_results:
                    if tmpfile is None:
                        raise InvalidEnvironmentException(
                            msg="Cannot find Conda installation tarball '%s'"
                            % os.path.join(
                                get_conda_root(self._datastore_type), path_to_fetch
                            )
                        )
                    shutil.move(tmpfile, tmp.name)
            try:
                tar = tarfile.open(tmp.name)
                tar.extractall(path)
                tar.close()
            except Exception as e:
                raise InvalidEnvironmentException(
                    msg="Could not extract environment: %s" % str(e)
                )
        self._echo(" done in %d seconds." % int(time.time() - start), timestamp=False)

    def _ensure_remote_conda(self):
        if CONDA_REMOTE_INSTALLER is not None:
            self._install_remote_conda()
        else:
            # If we don't have a REMOTE_INSTALLER, we check if we need to install one
            args = [
                "if ! type %s  >/dev/null 2>&1; \
                then wget --no-check-certificate "
                "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh -O Miniforge3.sh >/dev/null 2>&1; \
                bash ./Miniforge3.sh -b >/dev/null 2>&1; echo $HOME/miniforge3/bin/conda; "
                "else which %s; fi"
                % (self._dependency_solver, self._dependency_solver),
            ]
            self._bins = {"conda": subprocess.check_output(args).decode("utf-8")}

    def _install_remote_conda(self):
        # We download the installer and return a path to it
        final_path = os.path.join(os.getcwd(), "__conda_installer")
        from metaflow.datastore import DATASTORES

        path_to_fetch = os.path.join(
            CONDA_REMOTE_INSTALLER_DIRNAME,
            CONDA_REMOTE_INSTALLER.format(arch=arch_id()),
        )
        if self._datastore_type not in DATASTORES:
            raise MetaflowException(
                msg="Downloading conda remote installer from backend %s is unimplemented!"
                % self._datastore_type
            )
        storage = DATASTORES[self._datastore_type](
            get_conda_root(self._datastore_type)
        )  # type: DataStoreStorage
        with storage.load_bytes([path_to_fetch]) as load_results:
            for _, tmpfile, _ in load_results:
                if tmpfile is None:
                    raise MetaflowException(
                        msg="Cannot find Conda remote installer '%s'"
                        % os.path.join(
                            get_conda_root(self._datastore_type), path_to_fetch
                        )
                    )
                shutil.move(tmpfile, final_path)
        os.chmod(
            final_path,
            stat.S_IRUSR
            | stat.S_IXUSR
            | stat.S_IRGRP
            | stat.S_IXGRP
            | stat.S_IROTH
            | stat.S_IXOTH,
        )
        self._bins = {"conda": final_path}

    def _validate_conda_installation(self) -> Optional[Exception]:
        # Check if the dependency solver exists.
        to_remove = []  # type: List[str]
        if self._bins is None:
            return InvalidEnvironmentException("No binaries configured for Conda")
        for k, v in self._bins.items():
            if v is None or not os.path.isfile(v):
                if k == "conda":
                    return InvalidEnvironmentException(
                        "No %s installation found. Install %s first."
                        % (self._dependency_solver, self._dependency_solver)
                    )
                elif k in ("micromamba", "cph"):
                    # These are optional so we ignore.
                    to_remove.append(k)
                elif k in ("conda-lock",):
                    if self._use_conda_lock_to_resolve:
                        self._use_conda_lock_to_resolve = False
                        self._echo(
                            "Falling back to '%s' to resolve as conda-lock not installed"
                            % (self._dependency_solver)
                        )
                    to_remove.append(k)
                else:
                    return InvalidEnvironmentException(
                        "Required binary '%s' found. Install using `%s install -n base %s`"
                        % (k, self._dependency_solver, k)
                    )
        if to_remove:
            for k in to_remove:
                del self._bins[k]

        if "cph" in self._bins:
            cph_version = (
                self._call_conda(["--version"], "cph").decode("utf-8").split()[-1]
            )
            if LooseVersion(cph_version) < LooseVersion("1.9.0"):
                self._echo(
                    "cph is installed but not recent enough (1.9.0 or later is required) "
                    "-- ignoring"
                )
                del self._bins["cph"]

        if "micromamba version" in self._info:
            self._have_micromamba = True
            if LooseVersion(self._info["micromamba version"]) < LooseVersion("0.25.1"):
                msg = "Micromamba version 0.25.1 or newer is required."
                return InvalidEnvironmentException(msg)
        elif self._dependency_solver == "conda" or self._dependency_solver == "mamba":
            if LooseVersion(self._info["conda_version"]) < LooseVersion("4.14.0"):
                msg = "Conda version 4.14.0 or newer is required."
                if self._dependency_solver == "mamba":
                    msg += (
                        " Visit https://mamba.readthedocs.io/en/latest/installation.html "
                        "for installation instructions."
                    )
                else:
                    msg += (
                        " Visit https://docs.conda.io/en/latest/miniconda.html "
                        "for installation instructions."
                    )
                return InvalidEnvironmentException(msg)
        else:
            # Should never happen since we check for it but making it explicit
            raise InvalidEnvironmentException(
                "Unknown dependency solver: %s" % self._dependency_solver
            )

        if self._mode == "local":
            # Check if conda-forge is available as a channel to pick up Metaflow's
            # dependencies.
            if "conda-forge" not in "\t".join(self._info["channels"]):
                return InvalidEnvironmentException(
                    "Conda channel 'conda-forge' is required. "
                    "Specify it with CONDA_CHANNELS environment variable."
                )

        return None

    def _created_envs(
        self, prefix: str, full_match: bool = False
    ) -> Dict[EnvID, List[str]]:
        ret = {}  # type: Dict[EnvID, List[str]]

        debug.conda_exec(
            "Locating MF environment %s%s"
            % ("starting with " if not full_match else "", prefix)
        )

        def _check_match(dir_name: str) -> Optional[EnvID]:
            dir_lastcomponent = os.path.basename(dir_name)
            if (
                full_match and dir_lastcomponent == prefix
            ) or dir_lastcomponent.startswith(prefix):
                mf_env_file = os.path.join(dir_name, ".metaflowenv")
                if os.path.isfile(mf_env_file):
                    with open(mf_env_file, mode="r", encoding="utf-8") as f:
                        env_info = json.load(f)
                    debug.conda_exec(
                        "Found %s at dir_name %s" % (EnvID(*env_info), dir_name)
                    )
                    return EnvID(*env_info)
                else:
                    debug.conda_exec(
                        "Found directory at %s but no .metaflowenv" % dir_name
                    )
                    return EnvID(req_id="_invalid", full_id="_invalid", arch="_invalid")
            return None

        if self._have_micromamba:
            env_dir = os.path.join(self._info["base environment"], "envs")
            for entry in os.scandir(env_dir):
                if entry.is_dir():
                    possible_env_id = _check_match(entry.path)
                    if possible_env_id:
                        ret.setdefault(possible_env_id, []).append(entry.path)
        else:
            envs = self._info["envs"]  # type: List[str]
            for env in envs:
                # Named environments are always $CONDA_PREFIX/envs/
                if "/envs/" in env:
                    possible_env_id = _check_match(env)
                    if possible_env_id:
                        ret.setdefault(possible_env_id, []).append(env)
        return ret

    def _remote_env_fetch(self, env_ids: List[EnvID]) -> List[ResolvedEnvironment]:
        result = OrderedDict(
            {self.get_datastore_path_to_env(env_id): env_id for env_id in env_ids}
        )  # type: OrderedDict[str, Union[EnvID, ResolvedEnvironment]]
        s = [self.get_datastore_path_to_env(env_id) for env_id in env_ids]
        with self._storage.load_bytes(s) as loaded:
            for key, tmpfile, _ in loaded:
                env_id = cast(EnvID, result[cast(str, key)])
                if tmpfile:
                    with open(tmpfile, mode="r", encoding="utf-8") as f:
                        resolved_env = ResolvedEnvironment.from_dict(
                            env_id, json.load(f)
                        )
                        result[cast(str, key)] = resolved_env
                        self._cached_environment.add_resolved_env(resolved_env)
                debug.conda_exec(
                    "%s%sfound remotely" % (str(env_id), " " if tmpfile else " not ")
                )
        return [x for x in result.values() if isinstance(x, ResolvedEnvironment)]

    @property
    def _package_dirs(self) -> List[str]:
        info = self._info
        if self._have_micromamba:
            pkg_dir = os.path.join(info["base environment"], "pkgs")
            if not os.path.exists(pkg_dir):
                os.makedirs(pkg_dir)
            return [pkg_dir]
        return info["pkgs_dirs"]

    @property
    def _root_env_dir(self) -> str:
        info = self._info
        if self._have_micromamba:
            return os.path.join(info["base environment"], "envs")
        return info["envs_dirs"][0]

    @property
    def _info(self) -> Dict[str, Any]:
        if self._cached_info is None:
            self._cached_info = json.loads(self._call_conda(["info", "--json"]))
        return self._cached_info

    def _create(self, env: ResolvedEnvironment, env_name: str) -> None:

        # We first get all the packages needed
        self.lazy_fetch_packages(env.packages)

        # We build the list of explicit URLs to pass to conda to create the environment
        # We know here that we have all the packages present one way or another, we just
        # need to format the URLs appropriately.
        explicit_urls = []  # type: List[str]
        for p in env.packages:
            local_dir = p.local_dir
            if local_dir:
                # If this is a local directory, we make sure we use the URL for that
                # directory (we may have a url in `p` that is for the .tar.bz2 package
                # but actually contain an extraction of a .conda package)
                with open(
                    os.path.join(local_dir, "info", "repodata_record.json"),
                    mode="r",
                    encoding="utf-8",
                ) as f:
                    info = json.load(f)
                    explicit_urls.append("%s#%s\n" % (info["url"], info["md5"]))
            else:
                for f in CONDA_FORMATS:
                    cache_info = p.cached_version(f)
                    if cache_info:
                        explicit_urls.append(
                            "%s#%s\n"
                            % (
                                self._make_urlstxt_from_cacheurl(cache_info.url),
                                cache_info.hash,
                            )
                        )
                        break
                else:
                    # Here we don't have any cache format so we just use the base URL
                    explicit_urls.append("%s#%s\n" % (p.url, p.pkg_hash(p.url_format)))

        start = time.time()
        self._echo("    Extracting and linking Conda environment ...", nl=False)

        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", delete=False
        ) as explicit_list:
            # We create an explicit file
            lines = ["@EXPLICIT\n"] + explicit_urls
            explicit_list.writelines(lines)
            explicit_list.flush()
            self._call_conda(
                [
                    "create",
                    "--yes",
                    "--quiet",
                    "--offline",
                    "--no-deps",
                    "--name",
                    env_name,
                    "--file",
                    explicit_list.name,
                ],
                # Creating with micromamba is faster as it extracts in parallel. Prefer
                # it if it exists.
                binary="micromamba"
                if self._bins and "micromamba" in self._bins
                else "conda",
            )
        self._cached_info = None

        # We write a `.metaflowenv` file to be able to get back the env_id from it in
        # case the name doesn't contain it
        env_dir = os.path.join(self._root_env_dir, env_name)
        with open(
            os.path.join(env_dir, ".metaflowenv"), mode="w", encoding="utf-8"
        ) as f:
            json.dump(env.env_id, f)

        self._echo(" done in %s seconds." % int(time.time() - start), timestamp=False)

    def _remove(self, env_name: str, env_id: EnvID):
        # TODO: Verify that this is a proper metaflow environment to remove
        self._call_conda(["env", "remove", "--name", env_name, "--yes", "--quiet"])
        self._cached_info = None

    def _upload_to_ds(self, files: List[Tuple[str, str]]) -> None:
        def paths_and_handles():
            for cache_path, local_path in files:
                with open(local_path, mode="rb") as f:
                    yield cache_path, f

        self._storage.save_bytes(paths_and_handles(), len_hint=len(files))

    def _env_lock_file(self, env_directory: str):
        return os.path.join(self._root_env_dir, "mf_env-creation.lock")

    def _package_dir_lock_file(self, dir_path: str) -> str:
        return os.path.join(dir_path, "mf_pkgs-update.lock")

    def _make_urlstxt_from_url(
        self,
        base_url: str,
        file_format: Optional[str] = None,
        is_transmuted: bool = False,
    ):
        if not is_transmuted:
            return base_url
        url = urlparse(base_url)
        file_path, filename = convert_filepath(url.path, file_format)
        return os.path.join(
            get_conda_root(self._datastore_type),
            cast(str, CONDA_PACKAGE_DIRNAME),
            TRANSMUT_PATHCOMPONENT,
            url.netloc,
            file_path.lstrip("/"),
            filename,
        )

    def _make_urlstxt_from_cacheurl(self, cache_url: str) -> str:
        if TRANSMUT_PATHCOMPONENT in cache_url:
            return os.path.join(
                get_conda_root(self._datastore_type),
                cast(str, CONDA_PACKAGE_DIRNAME),
                os.path.split(os.path.split(cache_url)[0])[
                    0
                ],  # Strip off last two (hash and filename)
            )
        else:
            # Format is CONDA_PACKAGE_DIRNAME/url/hash/file so we strip first and 2 last
            components = cache_url.split("/")
            return "https://" + "/".join(components[1:-2])

    @staticmethod
    def _env_directory_from_envid(env_id: EnvID) -> str:
        return "metaflow_%s_%s" % (env_id.req_id, env_id.full_id)

    @staticmethod
    def _lnk_path_for_pkg(pkg: PackageSpecification, pkg_fmt: str) -> str:
        cached_base_version = pkg.cached_version(pkg.url_format)
        if cached_base_version:
            base_url = os.path.split(cached_base_version.url)[0]
        else:
            base_url = os.path.split(
                CachePackage.make_cache_url(
                    pkg.url, cast(str, pkg.pkg_hash(pkg.url_format))
                )
            )[0]
        return "/".join([base_url, "%s.lnk" % pkg_fmt])

    def _call_conda(
        self,
        args: List[str],
        binary: str = "conda",
        addl_env: Optional[Mapping[str, str]] = None,
    ) -> bytes:

        if self._bins is None or binary not in self._bins:
            raise CondaException("Binary '%s' is not known" % binary)
        try:
            env = {
                "CONDA_JSON": "True",
                "MAMBA_NO_BANNER": "1",
                "MAMBA_JSON": "True",
            }
            if addl_env:
                env.update(addl_env)

            if args and args[0] != "package":
                if binary == "micromamba":
                    # Add a few options to make sure it plays well with conda/mamba
                    # NOTE: This is only if we typically have conda/mamba and are using
                    # micromamba. When micromamba is used by itself, we don't do this
                    args.extend(["-r", os.path.dirname(self._package_dirs[0])])
                if binary == "micromamba" or self._have_micromamba:
                    # Both if we are using micromamba standalone or using micromamba instead of
                    # conda/mamba: no env-var like MAMBA_JSON.
                    args.append("--json")
            debug.conda_exec("Conda call: %s" % str([self._bins[binary]] + args))
            # if args and args[0] == "create":
            # print("SLEEPING")
            # time.sleep(3600)
            return subprocess.check_output(
                [self._bins[binary]] + args,
                stderr=subprocess.STDOUT,
                env=dict(os.environ, **env),
            ).strip()
        except subprocess.CalledProcessError as e:
            try:
                output = json.loads(e.output)
                if isinstance(output, dict) and "error" in output:
                    err = [output["err"]]
                else:
                    err = [output]
                for error in output.get("errors", []):
                    err.append(error["error"])
                raise CondaException(err)
            except (TypeError, ValueError):
                pass
            raise CondaException(
                "Conda command '{cmd}' returned error ({code}): {output}, stderr={stderr}".format(
                    cmd=e.cmd, code=e.returncode, output=e.output, stderr=e.stderr
                )
            )


TCondaLock = TypeVar("TCondaLock", bound="CondaLock")


class CondaLock(object):
    def __init__(self, lock: str, timeout: int = CONDA_LOCK_TIMEOUT, delay: int = 10):
        self.lock = lock
        self.locked = False
        self.timeout = timeout
        self.delay = delay

    def _acquire(self) -> None:
        start = time.time()
        try:
            os.makedirs(os.path.dirname(self.lock))
        except OSError as x:
            if x.errno != errno.EEXIST:
                raise
        while True:
            try:
                self.fd = os.open(self.lock, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                self.locked = True
                break
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise
                if self.timeout is None:
                    raise CondaException("Could not acquire lock {}".format(self.lock))
                if (time.time() - start) >= self.timeout:
                    raise CondaException(
                        "Timeout occurred while acquiring lock {}".format(self.lock)
                    )
                time.sleep(self.delay)

    def _release(self) -> None:
        if self.locked:
            os.close(self.fd)
            os.unlink(self.lock)
            self.locked = False

    def __enter__(self: TCondaLock) -> TCondaLock:
        if not self.locked:
            self._acquire()
        return self

    def __exit__(self, type: Any, value: Any, traceback: Any) -> None:
        self.__del__()

    def __del__(self) -> None:
        self._release()
