""" 

"""

from hashlib import sha1
from io import BytesIO
import os
import shutil
from metaflow.datastore.local_storage import LocalStorage

from .exception import CardNotPresentException

CARD_DIRECTORY_NAME = "mf.cards"
TEMP_DIR_NAME = "metaflow_card_cache"
NUM_SHORT_HASH_CHARS = 5


def stepname_from_card_id(card_id, flow):
    from .card_decorator import CardDecorator

    for step in flow._steps:
        for deco in step.decorators:
            if isinstance(deco, CardDecorator):
                if deco.attributes["id"] == card_id:
                    return step.name
    return None


class CardPathBuilder(object):
    @classmethod
    def path_spec_resolver(cls, pathspec):
        run_id, step_name, task_id = None, None, None
        splits = pathspec.split("/")
        if len(splits) == 1:  # only flowname mentioned
            return splits[0], run_id, step_name, task_id
        elif len(splits) == 2:  # flowname , runid mentioned
            return splits[0], splits[1], step_name, task_id
        elif len(splits) == 3:  # flowname , runid , stepname
            return splits[0], splits[1], splits[2], task_id
        elif len(splits) == 4:  # flowname ,runid ,stepname , taskid
            return splits[0], splits[1], splits[2], splits[3]

    @classmethod
    def make_path(cls, sysroot, flow_name, run_id=None, task_id=None, pathspec=None):
        if sysroot is None:
            return None

        if pathspec is not None:
            flow_name, run_id, step_name, task_id = cls.path_spec_resolver(pathspec)

        if flow_name is None:
            return sysroot
        elif run_id is None:
            # todo :[DSC][FUTURE] find namespace here
            # todo :[DSC][FUTURE] what happens when user has namepsace set to none;
            #             What do we default to here ?
            from metaflow import get_namespace

            namespacename = get_namespace()
            if not namespacename:
                pth_arr = [sysroot, CARD_DIRECTORY_NAME, flow_name, "cards"]
            else:
                pth_arr = [
                    sysroot,
                    CARD_DIRECTORY_NAME,
                    flow_name,
                    "cards",
                    namespacename,
                    "cards",
                ]
        elif task_id is None:
            pth_arr = [sysroot, CARD_DIRECTORY_NAME, flow_name, "runs", run_id, "cards"]
        else:
            pth_arr = [
                sysroot,
                CARD_DIRECTORY_NAME,
                flow_name,
                "runs",
                run_id,
                "tasks",
                task_id,
                "cards",
            ]

        if sysroot == "":
            pth_arr.pop(0)
        return os.path.join(*pth_arr)


class CardDatastore(object):
    root = None
    # Todo :
    # should the datastore backend be a direct arguement or
    # should we use the flow datastore argument ?
    def __init__(self, flow_datastore, run_id, step_name, task_id, path_spec=None):
        self._backend = flow_datastore._storage_impl
        self._flow_name = flow_datastore.flow_name
        self._run_id = run_id
        self._step_name = step_name
        self._task_id = task_id
        self._path_spec = path_spec
        self._temp_card_save_path = self._get_card_path(base_pth=TEMP_DIR_NAME)
        LocalStorage._makedirs(self._temp_card_save_path)
        # TODO :
        # Figure if the path should follow the same pattern
        # for local and s3 datastore backend
        # todo: Check if main root is needed;

    @classmethod
    def get_card_location(cls, base_path, card_name, card_html):
        return os.path.join(
            base_path,
            "%s-%s.html" % (card_name, sha1(bytes(card_html, "utf-8")).hexdigest()),
        )

    def _get_card_path(self, base_pth=""):
        return CardPathBuilder.make_path(
            # Adding this to avoid errors with s3;
            # S3Backend has s3root set which requires a relative path
            # over an absolute path; Providing relative path for local datastore
            # also works similarly;
            base_pth,
            self._flow_name,
            run_id=self._run_id,
            task_id=self._task_id,
            pathspec=self._path_spec,
        )

    @staticmethod
    def _make_card_file_name(card_type, card_index, card_id=None):
        if card_id is not None:
            card_name = "%s-%s-%s" % (card_id, card_type, card_index)
        else:
            card_name = "%s-%s" % (card_type, card_index)
        return card_name

    @staticmethod
    def card_info_from_path(path):
        """
        Args:
            path (str): The path to the card

        Raises:
            Exception: When the card_path is invalid

        Returns:
            (card_id,card_type,card_index,card_hash)
        """
        card_file_name = path.split("/")[-1]
        file_split = card_file_name.split("-")
        has_id = False
        if len(file_split) == 4:
            has_id = True
        elif len(file_split) < 3 or len(file_split) > 4:
            raise Exception(
                "Invalid card file name %s. Card file names should be of form ID-TYPE-INDEX-HASH.html or TYPE-INDEX-HASH.html"
                % card_file_name
            )
        card_id, card_type, card_index, card_hash = None, None, None, None
        if has_id:
            card_id, card_type, card_index, card_hash = file_split
        else:
            card_type, card_index, card_hash = file_split
        card_hash = card_hash.split(".html")[0]
        return card_id, card_type, card_index, card_hash

    def save_card(self, card_type, card_id, card_index, card_html, overwrite=True):
        card_file_name = self._make_card_file_name(
            card_type, card_index, card_id=card_id
        )
        card_path = self.get_card_location(
            self._get_card_path(), card_file_name, card_html
        )
        self._backend.save_bytes(
            [(card_path, BytesIO(bytes(card_html, "utf-8")))], overwrite=overwrite
        )

    def _list_card_paths(
        self, card_type=None, card_id=None, card_index=None, card_hash=None
    ):
        card_path = self._get_card_path()
        card_paths = self._backend.list_content([card_path])
        if len(card_paths) == 0:
            # If there are no files found on the Path then raise an error of
            raise CardNotPresentException(
                self._flow_name,
                self._run_id,
                self._step_name,
                card_index=card_index,
                card_id=card_id,
                card_hash=card_hash,
                card_type=card_type,
            )
        cards_found = []
        for task_card_path in card_paths:
            card_path = task_card_path.path
            cid, ctype, cidx, chash = self.card_info_from_path(card_path)
            is_index_present = lambda x: True
            if card_index is not None:
                # if the index is not none then the lambda checks it
                is_index_present = lambda x: x == cidx

            if not is_index_present(str(card_index)):
                continue

            # if id,type or hash don't match then continue.
            if card_id is not None and card_id != cid:
                continue
            elif card_type is not None and ctype != card_type:
                continue
            elif card_hash is not None:
                if chash != card_hash and card_hash != chash[:NUM_SHORT_HASH_CHARS]:
                    continue

            if task_card_path.is_file:
                cards_found.append(card_path)

        return cards_found

    def get_card_names(self, card_paths):
        name_tuples = []
        for path in card_paths:
            name_tuples.append(self.card_info_from_path(path))
        return name_tuples

    def get_card_html(self, path):
        with self._backend.load_bytes([path]) as get_results:
            for _, path, _ in get_results:
                if path is not None:
                    with open(path, "r") as f:
                        return f.read()

    def cache_locally(self, path):
        with self._backend.load_bytes([path]) as get_results:
            for key, path, meta in get_results:
                if path is not None:
                    main_path = path
                    file_name = key.split("/")[-1]
                    main_path = os.path.join(self._temp_card_save_path, file_name)
                    shutil.copy(path, main_path)
                    return main_path

    def extract_card_paths(
        self, card_type=None, card_id=None, card_index=None, card_hash=None
    ):
        return self._list_card_paths(
            card_type=card_type,
            card_id=card_id,
            card_index=card_index,
            card_hash=card_hash,
        )
