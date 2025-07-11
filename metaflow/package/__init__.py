import json
import os
import sys
import threading
import time

from io import BytesIO
from types import ModuleType
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING, Type, cast

from ..debug import debug
from ..packaging_sys import ContentType, MetaflowCodeContent
from ..packaging_sys.backend import PackagingBackend
from ..packaging_sys.tar_backend import TarPackagingBackend
from ..packaging_sys.v1 import MetaflowCodeContentV1
from ..packaging_sys.utils import suffix_filter, walk
from ..metaflow_config import DEFAULT_PACKAGE_SUFFIXES
from ..exception import MetaflowException
from ..user_configs.config_parameters import dump_config_values
from ..util import get_metaflow_root
from .. import R

DEFAULT_SUFFIXES_LIST = DEFAULT_PACKAGE_SUFFIXES.split(",")


if TYPE_CHECKING:
    import metaflow.datastore


class NonUniqueFileNameToFilePathMappingException(MetaflowException):
    headline = "Non-unique file path for a file name included in code package"

    def __init__(self, filename, file_paths, lineno=None):
        msg = (
            "Filename %s included in the code package includes multiple different "
            "paths for the same name : %s.\n"
            "The `filename` in the `add_to_package` decorator hook requires a unique "
            "`file_path` to `file_name` mapping" % (filename, ", ".join(file_paths))
        )
        super().__init__(msg=msg, lineno=lineno)


class MetaflowPackage(object):
    def __init__(
        self,
        flow,
        environment,
        echo,
        suffixes: Optional[List[str]] = DEFAULT_SUFFIXES_LIST,
        user_code_filter: Optional[Callable[[str], bool]] = None,
        flow_datastore: Optional["metaflow.datastore.FlowDataStore"] = None,
        mfcontent: Optional[MetaflowCodeContent] = None,
        exclude_tl_dirs=None,
        backend: Type[PackagingBackend] = TarPackagingBackend,
    ):
        self._environment = environment
        self._environment.init_environment(echo)

        self._echo = echo
        self._flow = flow
        self._flow_datastore = flow_datastore
        self._backend = backend

        # Info about the package
        self._name = None
        self._create_time = time.time()
        self._user_flow_dir = None

        # Content of the package (and settings on how to create it)
        if suffixes is not None:
            self._suffixes = list(set().union(suffixes, DEFAULT_SUFFIXES_LIST))
        else:
            self._suffixes = None

        def _module_selector(m) -> bool:
            from ..user_decorators.user_flow_decorator import FlowMutatorMeta
            from ..user_decorators.user_step_decorator import UserStepDecoratorMeta

            if (
                m.__name__ in FlowMutatorMeta._import_modules
                or m.__name__ in UserStepDecoratorMeta._import_modules
                or hasattr(m, "METAFLOW_PACKAGE")
            ):
                return True

        if mfcontent is None:
            self._mfcontent = MetaflowCodeContentV1(criteria=_module_selector)

        else:
            self._mfcontent = mfcontent
        # We exclude the environment when packaging as this will be packaged separately.
        # This comes into play primarily if packaging from a node already running packaged
        # code.
        # These directories are only excluded at the top-level (ie: not further down
        # in sub-directories)
        # "_escape_trampolines" is a special directory where trampoline escape hatch
        # files are stored (used by Netflix Extension's Conda implementation).
        self._exclude_tl_dirs = (
            self._mfcontent.get_excluded_tl_entries()
            + ["_escape_trampolines"]
            + (exclude_tl_dirs or [])
        )

        if self._suffixes is not None and user_code_filter is not None:
            self._user_code_filter = lambda x, f1=suffix_filter(
                self._suffixes
            ), f2=user_code_filter: f1(x) and f2(x)
            self._filter_type = "suffixes and user filter"
        elif self._suffixes is not None:
            self._user_code_filter = suffix_filter(self._suffixes)
            self._filter_type = "suffixes"
        elif user_code_filter is not None:
            self._user_code_filter = user_code_filter
            self._filter_type = "user filter"
        else:
            self._user_code_filter = lambda x: True
            self._filter_type = "no filter"

        # Info about the package creation (it happens async)
        self._is_package_available = None
        self._blob_sha = None
        self._blob_url = None
        self._blob = None

        # We launch a thread to create the package asynchronously and upload
        # it opportunistically
        self._create_thread = threading.Thread(
            target=self._package_and_upload,
            daemon=True,
        )
        self._create_thread.start()

    # HORRIBLE HACK SO THAT CURRENT COMPUTE IMPLEMENTATIONS CAN STILL
    # DO pkg.blob. Ideally, this goes away and blob_with_timeout becomes
    # the main method (called blob).
    @property
    def blob(self) -> BytesIO:
        return self.blob_with_timeout()

    def blob_with_timeout(self, timeout: Optional[float] = None) -> BytesIO:
        if self._blob is None:
            self._create_thread.join(timeout)
            if self._is_package_available is not None:
                # We have our result now
                if self._is_package_available:
                    return self._blob
                else:
                    raise self._packaging_exception
        return self._blob

    def package_sha(self, timeout: Optional[float] = None) -> Optional[str]:
        if self._blob_sha is None:
            self._create_thread.join(timeout)
            if self._is_package_available is not None:
                # We have our result now
                if self._is_package_available:
                    return self._blob_sha
                else:
                    raise self._packaging_exception
        return self._blob_sha

    def package_url(self, timeout: Optional[float] = None) -> Optional[str]:
        if self._blob_url is None:
            self._create_thread.join(timeout)
            if self._is_package_available is not None:
                # We have our result now
                if self._is_package_available:
                    return self._blob_url
                else:
                    raise self._packaging_exception
        return self._blob_url

    @property
    def package_metadata(self):
        return json.dumps(
            {
                "version": 0,
                "archive_format": self._backend.backend_type(),
                "mfcontent_version": self._mfcontent.get_package_version(),
            }
        )

    @classmethod
    def get_backend(cls, pkg_metadata: str) -> PackagingBackend:
        """
        Method to get the backend type from the package metadata.

        Parameters
        ----------
        pkg_metadata : str
            The metadata of the package to extract.

        Returns
        -------
        PackagingBackend
            The backend type that can be used to extract the package.
        """
        backend_type = json.loads(pkg_metadata).get("archive_format", "tgz")
        return PackagingBackend.get_backend(backend_type)

    @classmethod
    def get_extract_commands(
        cls, pkg_metadata: str, archive_path: str, dest_dir: str = "."
    ) -> List[str]:
        """
        Method to get the commands needed to extract the package into
        the directory dest_dir. Note that this will return a list of commands
        that can be passed to subprocess.run for example.

        Parameters
        ----------
        pkg_metadata : str
            The metadata of the package to extract.
        archive_path : str
            The path to the archive to extract.
        dest_dir : str, default "."
            The directory to extract the package into.

        Returns
        -------
        List[str]
            The commands needed to extract the package into the directory dest_dir.
        """
        backend_type = json.loads(pkg_metadata).get("archive_format", "tgz")
        # We now ask the backend type how to extract itself
        backend = PackagingBackend.get_backend(backend_type)
        cmds = backend.get_extract_commands(archive_path, dest_dir)
        debug.package_exec(f"Command to extract {archive_path} into {dest_dir}: {cmds}")
        return cmds

    @classmethod
    def get_post_extract_env_vars(
        cls, pkg_metadata: str, dest_dir: str = "."
    ) -> Dict[str, str]:
        """
        Method to get the environment variables needed to access the content
        that has been extracted into the directory dest_dir. This will
        typically involve setting PYTHONPATH

        Parameters
        ----------
        pkg_metadata : str
            The metadata of the package to extract.
        dest_dir : str, default "."
            The directory where the content has been extracted to.

        Returns
        -------
        Dict[str, str]
            The post-extract environment variables that are needed to access the content
            that has been extracted into dest_dir.
        """
        mfcontent_version = json.loads(pkg_metadata).get("mfcontent_version", 0)
        env_vars = MetaflowCodeContent.get_post_extract_env_vars(
            mfcontent_version, dest_dir
        )
        debug.package_exec(
            f"Environment variables to access content extracted into {dest_dir}: {env_vars}"
        )
        return env_vars

    @classmethod
    def cls_get_content(
        cls, pkg_metadata, archive: BytesIO, name: str
    ) -> Optional[bytes]:
        """
        Method to get the content of a member in the package archive.

        Parameters
        ----------
        pkg_metadata : str
            The metadata of the package to extract.
        archive : BytesIO
            The archive to extract the member from.
        name : str
            The name of the member to extract.

        Returns
        -------
        Optional[bytes]
            The content of the member if it exists, None otherwise.
        """
        backend = cls.get_backend(pkg_metadata)
        with backend.cls_open(archive) as opened_archive:
            return backend.cls_get_member(opened_archive, name)

    @classmethod
    def cls_get_info(cls, pkg_metadata, archive: BytesIO) -> Optional[Dict[str, str]]:
        """
        Method to get the info of the package from the archive.
        Parameters
        ----------
        pkg_metadata : str
            The metadata of the package to extract.
        archive : BytesIO
            The archive to extract the info from.
        Returns
        -------
        Optional[Dict[str, str]]
            The info of the package if it exists, None otherwise.
        """
        backend = cls.get_backend(pkg_metadata)
        with backend.cls_open(archive) as opened_archive:
            return MetaflowCodeContent.get_archive_info(opened_archive, backend)

    @classmethod
    def cls_get_config(
        cls, pkg_metadata: str, archive: BytesIO
    ) -> Optional[Dict[str, str]]:
        """
        Method to get the config of the package from the archive.

        Parameters
        ----------
        pkg_metadata : str
            The metadata of the package to extract.
        archive : BytesIO
            The archive to extract the config from.

        Returns
        -------
        Optional[Dict[str, str]]
            The config of the package if it exists, None otherwise.
        """
        backend = cls.get_backend(pkg_metadata)
        with backend.cls_open(archive) as opened_archive:
            return MetaflowCodeContent.get_archive_config(opened_archive, backend)

    @classmethod
    def cls_extract_into(
        cls,
        pkg_metadata: str,
        archive: BytesIO,
        dest_dir: str = ".",
        content_types: int = ContentType.ALL_CONTENT.value,
    ):
        """
        Method to extract the package archive into a directory.

        Parameters
        ----------
        pkg_metadata : str
            The metadata of the package to extract.
        archive : BytesIO
            The archive to extract.
        dest_dir : str, default "."
            The directory to extract the package into.
        content_types : int, default ALL_CONTENT
            The types of content to extract. This is a bitmask of ContentType values.
        """
        backend = cls.get_backend(pkg_metadata)
        with backend.cls_open(archive) as opened_archive:
            include_names = MetaflowCodeContent.get_archive_content_names(
                opened_archive, content_types, backend
            )
            backend.extract_members(include_names, dest_dir)

    def user_tuples(self, timeout: Optional[float] = None):
        # Wait for at least the blob to be formed
        _ = self.blob_with_timeout(timeout=timeout)
        for path, arcname in self._cached_user_members:
            yield path, arcname

    def path_tuples(self, timeout: Optional[float] = None):
        # Wait for at least the blob to be formed
        _ = self.blob_with_timeout(timeout=timeout)
        # Files included in the environment
        yield from self._mfcontent.content_names()

        # Files included in the user code
        yield from self.user_tuples()

    def show(self, timeout: Optional[float] = None) -> str:
        # Human-readable content of the package
        blob = self.blob_with_timeout(timeout=timeout)  # Ensure the package is created
        lines = [
            f"Package size: {self._format_size(len(blob))}",
            f"Number of files: {sum(1 for _ in self.path_tuples())}",
            self._mfcontent.show(),
        ]

        if self._flow:
            lines.append(f"\nUser code in flow {self._name}:")
            lines.append(f"  - Packaged from directory {self._user_flow_dir}")
            if self._filter_type != "no filter":
                if self._suffixes:
                    lines.append(
                        f"  - Filtered by suffixes: {', '.join(self._suffixes)}"
                    )
                else:
                    lines.append(f"  - Filtered by {self._filter_type}")
            else:
                lines.append("  - No user code filter applied")
            if self._exclude_tl_dirs:
                lines.append(
                    f"  - Excluded directories: {', '.join(self._exclude_tl_dirs)}"
                )
        return "\n".join(lines)

    def get_content(
        self, name: str, content_type: ContentType, timeout: Optional[float] = None
    ) -> Optional[bytes]:
        """
        Method to get the content of a file within the package. This method
        should be used for one-off access to small-ish files. If more files are
        needed, use extract_into to extract the package into a directory and
        then access the files from there.

        Parameters
        ----------
        name : str
            The name of the file to get the content of. Note that this
            is not necessarily the name in the archive but is the name
            that was passed in when creating the archive (in the archive,
            it may be prefixed by some directory structure).
        content_type : ContentType
            The type of file to get the content of.

        Returns
        -------
        Optional[bytes]
            The content of the file. If the file is not found, None is returned.
        """
        # Wait for at least the blob to be formed
        _ = self.blob_with_timeout(timeout=timeout)
        if content_type == ContentType.USER_CONTENT:
            for path, arcname in self.user_tuples():
                if name == arcname:
                    return open(path, "rb").read()
            return None
        elif content_type in (
            ContentType.CODE_CONTENT,
            ContentType.MODULE_CONTENT,
            ContentType.OTHER_CONTENT,
        ):
            mangled_name = self._mfcontent.get_archive_filename(name, content_type)
            for path_or_bytes, arcname in self._mfcontent.contents(content_type):
                if mangled_name == arcname:
                    if isinstance(path_or_bytes, bytes):
                        # In case this is generated content like an INFO file
                        return path_or_bytes
                    # Otherwise, it is a file path
                    return open(path_or_bytes, "rb").read()
            return None
        raise ValueError(f"Unknown content type: {content_type}")

    def extract_into(
        self,
        dest_dir: str = ".",
        content_types: int = ContentType.ALL_CONTENT.value,
        timeout: Optional[float] = None,
    ):
        """
        Method to extract the package (or some of the files) into a directory.

        Parameters
        ----------
        dest_dir : str, default "."
            The directory to extract the package into.
        content_types : int, default ALL_CONTENT
            The types of content to extract.
        """
        _ = self.blob_with_timeout(timeout=timeout)  # Ensure the package is created
        member_list = []
        if content_types & ContentType.USER_CONTENT.value:
            member_list.extend(
                [(m[0], os.path.join(dest_dir, m[1])) for m in self.user_tuples()]
            )
        if content_types & (
            ContentType.CODE_CONTENT.value | ContentType.MODULE_CONTENT.value
        ):
            # We need to get the name of the files in the content archive to extract
            member_list.extend(
                [
                    (m[0], os.path.join(dest_dir, m[1]))
                    for m in self._mfcontent.content_names(
                        content_types & ~ContentType.OTHER_CONTENT.value
                    )
                ]
            )
        for orig_path, new_path in member_list:
            os.makedirs(os.path.dirname(new_path), exist_ok=True)
            # TODO: In case there are duplicate files -- that should not be the case
            # but there is a bug currently with internal Netflix code.
            if not os.path.exists(new_path):
                os.symlink(orig_path, new_path)
            # Could copy files as well if we want to split them out.
            # shutil.copy(orig_path, new_path)
        # OTHER_CONTENT requires special handling because sometimes the file isn't a file
        # but generated content
        member_list = []
        if content_types & ContentType.OTHER_CONTENT.value:
            member_list.extend(
                [
                    (m[0], os.path.join(dest_dir, m[1]))
                    for m in self._mfcontent.contents(ContentType.OTHER_CONTENT)
                ]
            )
        for path_or_content, new_path in member_list:
            os.makedirs(os.path.dirname(new_path), exist_ok=True)
            if not os.path.exists(new_path):
                if isinstance(path_or_content, bytes):
                    with open(new_path, "wb") as f:
                        f.write(path_or_content)
                else:
                    os.symlink(path_or_content, new_path)

    @staticmethod
    def _format_size(size_in_bytes):
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size_in_bytes < 1024.0:
                return f"{size_in_bytes:.2f} {unit}"
            size_in_bytes /= 1024.0
        return f"{size_in_bytes:.2f} PB"

    def _package_and_upload(self):
        try:
            # Can be called without a flow (Function)
            if self._flow:
                for step in self._flow:
                    for deco in step.decorators:
                        deco.package_init(self._flow, step.__name__, self._environment)
                self._name = f"flow {self._flow.name}"
            else:
                self._name = "<generic code package>"

            # Add metacontent
            self._mfcontent.add_info(
                self._environment.get_environment_info(include_ext_info=True)
            )

            self._mfcontent.add_config(dump_config_values(self._flow))

            # Add user files (from decorators and environment)
            if self._flow:
                self._add_addl_files()
                self._cached_user_members = list(self._user_code_tuples())
                debug.package_exec(
                    f"User files to package: {self._cached_user_members}"
                )

            self._blob = self._make()
            if self._flow_datastore:
                if len(self._blob) > 100 * 1024 * 1024:
                    self._echo(
                        f"Warning: The code package for {self._flow.name} is larger than "
                        f"100MB (found it to be {self._format_size(len(self._blob))}) "
                        "This may lead to slower upload times for remote runs and no "
                        "uploads for local runs. Consider reducing the package size. "
                        "Use `<myflow.py> package info` or `<myflow.py> package list` "
                        "to get more information about what is included in the package."
                    )
                self._blob_url, self._blob_sha = self._flow_datastore.save_data(
                    [self._blob], len_hint=1
                )[0]
            else:
                self._blob_url = self._blob_sha = ""
            self._is_package_available = True
        except Exception as e:
            self._packaging_exception = e
            self._echo(f"Package creation/upload failed for {self._flow.name}: {e}")
            self._is_package_available = False

    def _add_addl_files(self):
        # Look at all decorators that provide additional files
        deco_module_paths = {}
        addl_modules = set()

        def _check_tuple(path_tuple):
            if len(path_tuple) == 2:
                path_tuple = (
                    path_tuple[0],
                    path_tuple[1],
                    ContentType.CODE_CONTENT,
                )
            file_path, file_name, file_type = path_tuple
            if file_type == ContentType.MODULE_CONTENT:
                if file_path in addl_modules:
                    return None  # Module was already added -- we don't add twice
                addl_modules.add(file_path)
            elif file_type in (
                ContentType.OTHER_CONTENT,
                ContentType.CODE_CONTENT,
            ):
                path_tuple = (os.path.realpath(path_tuple[0]), path_tuple[1], file_type)
                # These are files
                # Check if the path is not duplicated as
                # many steps can have the same packages being imported
                if file_name not in deco_module_paths:
                    deco_module_paths[file_name] = file_path
                elif deco_module_paths[file_name] != file_path:
                    raise NonUniqueFileNameToFilePathMappingException(
                        file_name, [deco_module_paths[file_name], file_path]
                    )
            else:
                raise ValueError(f"Unknown file type: {file_type}")
            return path_tuple

        def _add_tuple(path_tuple):
            file_path, file_name, file_type = path_tuple
            if file_type == ContentType.MODULE_CONTENT:
                # file_path is actually a module
                self._mfcontent.add_module(cast(ModuleType, file_path))
            elif file_type == ContentType.CODE_CONTENT:
                self._mfcontent.add_code_file(file_path, file_name)
            elif file_type == ContentType.OTHER_CONTENT:
                self._mfcontent.add_other_file(file_path, file_name)

        for step in self._flow:
            for deco in step.decorators:
                for path_tuple in deco.add_to_package():
                    path_tuple = _check_tuple(path_tuple)
                    if path_tuple is None:
                        continue
                    _add_tuple(path_tuple)

        # the package folders for environment
        for path_tuple in self._environment.add_to_package():
            path_tuple = _check_tuple(path_tuple)
            if path_tuple is None:
                continue
            _add_tuple(path_tuple)

    def _user_code_tuples(self):
        if R.use_r():
            # the R working directory
            self._user_flow_dir = R.working_dir()
            for path_tuple in walk(
                "%s/" % R.working_dir(), file_filter=self._user_code_filter
            ):
                yield path_tuple
            # the R package
            for path_tuple in R.package_paths():
                yield path_tuple
        else:
            # the user's working directory
            flowdir = os.path.dirname(os.path.abspath(sys.argv[0])) + "/"
            self._user_flow_dir = flowdir
            for path_tuple in walk(
                flowdir,
                file_filter=self._user_code_filter,
                exclude_tl_dirs=self._exclude_tl_dirs,
            ):
                # TODO: This is where we will check if the file is already included
                # in the mfcontent portion
                yield path_tuple

    def _make(self):
        backend = self._backend()
        with backend.create() as archive:
            # Package the environment
            for path_or_bytes, arcname in self._mfcontent.contents():
                if isinstance(path_or_bytes, str):
                    archive.add_file(path_or_bytes, arcname=arcname)
                else:
                    archive.add_data(BytesIO(path_or_bytes), arcname=arcname)

            # Package the user code
            for path, arcname in self._cached_user_members:
                archive.add_file(path, arcname=arcname)
        return backend.get_blob()

    def __str__(self):
        return f"<code package for {self._name} (created @ {self._create_time})>"
