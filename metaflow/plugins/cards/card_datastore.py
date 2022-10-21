""" 

"""

from collections import namedtuple
from hashlib import sha1
from io import BytesIO
import os
import shutil

from metaflow.plugins.datastores.local_storage import LocalStorage
from metaflow.metaflow_config import (
    CARD_S3ROOT,
    CARD_LOCALROOT,
    DATASTORE_LOCAL_DIR,
    CARD_SUFFIX,
    CARD_AZUREROOT,
    CARD_GSROOT,
    SKIP_CARD_DUALWRITE,
)
import metaflow.metaflow_config as metaflow_config

from .exception import CardNotPresentException

TEMP_DIR_NAME = "metaflow_card_cache"
NUM_SHORT_HASH_CHARS = 5

CardInfo = namedtuple("CardInfo", ["type", "hash", "id", "filename"])


def path_spec_resolver(pathspec):
    splits = pathspec.split("/")
    splits.extend([None] * (4 - len(splits)))
    return tuple(splits)


def is_file_present(path):
    try:
        os.stat(path)
        return True
    except FileNotFoundError:
        return False
    except:
        raise


class CardDatastore(object):
    @classmethod
    def get_storage_root(cls, storage_type):
        if storage_type == "s3":
            return CARD_S3ROOT
        elif storage_type == "azure":
            return CARD_AZUREROOT
        elif storage_type == "gs":
            return CARD_GSROOT
        elif storage_type == "local":
            # Borrowing some of the logic from LocalStorage.get_storage_root
            result = CARD_LOCALROOT
            if result is None:
                current_path = os.getcwd()
                check_dir = os.path.join(current_path, DATASTORE_LOCAL_DIR, CARD_SUFFIX)
                check_dir = os.path.realpath(check_dir)
                orig_path = check_dir
                while not os.path.isdir(check_dir):
                    new_path = os.path.dirname(current_path)
                    if new_path == current_path:
                        break  # We are no longer making upward progress
                    current_path = new_path
                    check_dir = os.path.join(
                        current_path, DATASTORE_LOCAL_DIR, CARD_SUFFIX
                    )
                result = orig_path

            return result
        else:
            # Let's make it obvious we need to update this block for each new datastore backend...
            raise NotImplementedError(
                "Card datastore does not support backend %s" % (storage_type,)
            )

    def __init__(self, flow_datastore, pathspec=None):
        self._backend = flow_datastore._storage_impl
        self._flow_name = flow_datastore.flow_name
        _, run_id, step_name, _ = pathspec.split("/")
        self._run_id = run_id
        self._step_name = step_name
        self._pathspec = pathspec
        self._temp_card_save_path = self._get_write_path(base_pth=TEMP_DIR_NAME)

    @classmethod
    def get_card_location(cls, base_path, card_name, card_html, card_id=None):
        chash = sha1(bytes(card_html, "utf-8")).hexdigest()
        if card_id is None:
            card_file_name = "%s-%s.html" % (card_name, chash)
        else:
            card_file_name = "%s-%s-%s.html" % (card_name, card_id, chash)
        return os.path.join(base_path, card_file_name)

    def _make_path(self, base_pth, pathspec=None, with_steps=False):
        sysroot = base_pth
        if pathspec is not None:
            # since most cards are at a task level there will always be 4 non-none values returned
            flow_name, run_id, step_name, task_id = path_spec_resolver(pathspec)

        # We have a condition that checks for `with_steps` because
        # when cards were introduced there was an assumption made
        # about task-ids being unique.
        # This assumption is incorrect since pathspec needs to be
        # unique but there is no such guarantees on task-ids.
        # This is why we have a `with_steps` flag that allows
        # constructing the path with and without steps so that
        # older-cards (cards with a path without `steps/<stepname>` in them)
        # can also be accessed by the card cli and the card client.
        if with_steps:
            pth_arr = [
                sysroot,
                flow_name,
                "runs",
                run_id,
                "steps",
                step_name,
                "tasks",
                task_id,
                "cards",
            ]
        else:
            pth_arr = [
                sysroot,
                flow_name,
                "runs",
                run_id,
                "tasks",
                task_id,
                "cards",
            ]
        if sysroot == "" or sysroot is None:
            pth_arr.pop(0)
        return os.path.join(*pth_arr)

    def _get_write_path(self, base_pth=""):
        return self._make_path(base_pth, pathspec=self._pathspec, with_steps=True)

    def _get_read_path(self, base_pth="", with_steps=False):
        return self._make_path(base_pth, pathspec=self._pathspec, with_steps=with_steps)

    @staticmethod
    def card_info_from_path(path):
        """
        Args:
            path (str): The path to the card

        Raises:
            Exception: When the card_path is invalid

        Returns:
            CardInfo
        """
        card_file_name = path.split("/")[-1]
        file_split = card_file_name.split("-")

        if len(file_split) not in [2, 3]:
            raise Exception(
                "Invalid card file name %s. Card file names should be of form TYPE-HASH.html or TYPE-ID-HASH.html"
                % card_file_name
            )
        card_type, card_hash, card_id = None, None, None

        if len(file_split) == 2:
            card_type, card_hash = file_split
        else:
            card_type, card_id, card_hash = file_split

        card_hash = card_hash.split(".html")[0]
        return CardInfo(card_type, card_hash, card_id, card_file_name)

    def save_card(self, card_type, card_html, card_id=None, overwrite=True):
        card_file_name = card_type
        # TEMPORARY_WORKAROUND: FIXME (LATER) : Fix the duplication of below block in a few months.
        # Check file blame to understand the age of this temporary workaround.

        # This function will end up saving cards at two locations.
        # Thereby doubling the number of cards. (Which is a temporary fix)
        # Why do this ? :
        # When cards were introduced there was an assumption made about task-ids being unique.
        # This assumption was incorrect.
        # Only the pathspec needs to be unique but there is no such guarantees about task-ids.
        # When task-ids are non-unique, card read would result in finding incorrect cards.
        # This happens because cards were stored based on task-ids.
        # If we immediately switch from storing based on task-ids to a step-name abstraction folder,
        # then card reading will crash for many users.
        # It would especially happen for users who are accessing cards created by a newer
        # MF client from an older version of MF client.
        # It will also easily end up breaking the metaflow-ui (which maybe using a client from an older version).
        # Hence, we are writing cards to both paths so that we can introduce breaking changes later in the future.
        card_path_with_steps = self.get_card_location(
            self._get_write_path(), card_file_name, card_html, card_id=card_id
        )
        if SKIP_CARD_DUALWRITE:
            self._backend.save_bytes(
                [(card_path_with_steps, BytesIO(bytes(card_html, "utf-8")))],
                overwrite=overwrite,
            )
        else:
            card_path_without_steps = self.get_card_location(
                self._get_read_path(with_steps=False),
                card_file_name,
                card_html,
                card_id=card_id,
            )
            for cp in [card_path_with_steps, card_path_without_steps]:
                self._backend.save_bytes(
                    [(cp, BytesIO(bytes(card_html, "utf-8")))], overwrite=overwrite
                )

        return self.card_info_from_path(card_path_with_steps)

    def _list_card_paths(self, card_type=None, card_hash=None, card_id=None):
        # Check for new cards first
        card_paths = []
        card_paths_with_steps = self._backend.list_content(
            [self._get_read_path(with_steps=True)]
        )

        if len(card_paths_with_steps) == 0:
            card_paths_without_steps = self._backend.list_content(
                [self._get_read_path(with_steps=False)]
            )
            if len(card_paths_without_steps) == 0:
                # If there are no files found on the Path then raise an error of
                raise CardNotPresentException(
                    self._pathspec,
                    card_hash=card_hash,
                    card_type=card_type,
                )
            else:
                card_paths = card_paths_without_steps
        else:
            card_paths = card_paths_with_steps

        cards_found = []
        for task_card_path in card_paths:
            card_path = task_card_path.path
            card_info = self.card_info_from_path(card_path)
            if card_type is not None and card_info.type != card_type:
                continue
            elif card_hash is not None:
                if not card_info.hash.startswith(card_hash):
                    continue
            elif card_id is not None and card_info.id != card_id:
                continue

            if task_card_path.is_file:
                cards_found.append(card_path)

        return cards_found

    def create_full_path(self, card_path):
        return os.path.join(self._backend.datastore_root, card_path)

    def get_card_names(self, card_paths):
        return [self.card_info_from_path(path) for path in card_paths]

    def get_card_html(self, path):
        with self._backend.load_bytes([path]) as get_results:
            for _, path, _ in get_results:
                if path is not None:
                    with open(path, "r") as f:
                        return f.read()

    def cache_locally(self, path, save_path=None):
        """
        Saves the data present in the `path` the `metaflow_card_cache` directory or to the `save_path`.
        """
        # todo : replace this function with the FileCache
        if save_path is None:
            if not is_file_present(self._temp_card_save_path):
                LocalStorage._makedirs(self._temp_card_save_path)
        else:
            save_dir = os.path.dirname(save_path)
            if save_dir != "" and not is_file_present(save_dir):
                LocalStorage._makedirs(os.path.dirname(save_path))

        with self._backend.load_bytes([path]) as get_results:
            for key, path, meta in get_results:
                if path is not None:
                    main_path = path
                    if save_path is None:
                        file_name = key.split("/")[-1]
                        main_path = os.path.join(self._temp_card_save_path, file_name)
                    else:
                        main_path = save_path
                    shutil.copy(path, main_path)
                    return main_path

    def extract_card_paths(self, card_type=None, card_hash=None, card_id=None):
        return self._list_card_paths(
            card_type=card_type, card_hash=card_hash, card_id=card_id
        )
