from typing import Optional, Union, TYPE_CHECKING
from metaflow.datastore import FlowDataStore
from metaflow.metaflow_config import CARD_SUFFIX
from .card_resolver import resolve_paths_from_task, resumed_info
from .card_datastore import CardDatastore
from .exception import (
    UnresolvableDatastoreException,
    IncorrectArguementException,
    IncorrectPathspecException,
)
import os
import tempfile
import uuid

if TYPE_CHECKING:
    from metaflow.client.core import Task

_TYPE = type
_ID_FUNC = id


class Card:
    """
    `Card` represents an individual Metaflow Card, a single HTML file, produced by
    the card `@card` decorator. `Card`s are contained by `CardContainer`, returned by
    `get_cards`.

    Note that the contents of the card, an HTML file, is retrieved lazily when you call
    `Card.get` for the first time or when the card is rendered in a notebook.
    """

    def __init__(
        self,
        card_ds,
        type,
        path,
        hash,
        id=None,
        html=None,
        created_on=None,
        from_resumed=False,
        origin_pathspec=None,
    ):
        # private attributes
        self._path = path
        self._html = html
        self._created_on = created_on
        self._card_ds = card_ds
        self._card_id = id

        # public attributes
        self.hash = hash
        self.type = type
        self.from_resumed = from_resumed
        self.origin_pathspec = origin_pathspec

        # Tempfile to open stuff in browser
        self._temp_file = None

    def get(self) -> str:
        """
        Retrieves the HTML contents of the card from the
        Metaflow datastore.

        Returns
        -------
        str
            HTML contents of the card.
        """
        if self._html is not None:
            return self._html
        self._html = self._card_ds.get_card_html(self.path)
        return self._html

    @property
    def path(self) -> str:
        """
        The path of the card in the datastore which uniquely
        identifies the card.

        Returns
        -------
        str
            Path to the card
        """
        return self._path

    @property
    def id(self) -> Optional[str]:
        """
        The ID of the card, if specified with `@card(id=ID)`.
        """
        return self._card_id

    def __str__(self):
        return "<Card at '%s'>" % self._path

    def view(self) -> None:
        """
        Opens the card in a local web browser.

        This call uses Python's built-in [`webbrowser`](https://docs.python.org/3/library/webbrowser.html)
        module to open the card.
        """
        import webbrowser

        self._temp_file = tempfile.NamedTemporaryFile(suffix=".html")
        html = self.get()
        self._temp_file.write(html.encode())
        self._temp_file.seek(0)
        url = "file://" + os.path.abspath(self._temp_file.name)
        webbrowser.open(url)

    def _repr_html_(self):
        main_html = []
        container_id = uuid.uuid4()
        main_html.append(
            "<script type='text/javascript'>var mfContainerId = '%s';</script>"
            % container_id
        )
        main_html.append(
            "<div class='embed' data-container='%s'>%s</div>"
            % (container_id, self.get())
        )
        return "\n".join(main_html)


class CardContainer:
    """
    `CardContainer` is an immutable list-like object, returned by `get_cards`,
    which contains individual `Card`s.

    Notably, `CardContainer` contains a special
    `_repr_html_` function which renders cards automatically in an output
    cell of a notebook.

    The following operations are supported:
    ```
    cards = get_cards(MyTask)

    # retrieve by index
    first_card = cards[0]

    # check length
    if len(cards) > 1:
        print('many cards present!')

    # iteration
    list_of_cards = list(cards)
    ```
    """

    def __init__(self, card_paths, card_ds, from_resumed=False, origin_pathspec=None):
        self._card_paths = card_paths
        self._card_ds = card_ds
        self._current = 0
        self._high = len(card_paths)
        self.from_resumed = from_resumed
        self.origin_pathspec = origin_pathspec

    def __len__(self):
        return self._high

    def __iter__(self):
        for idx in range(self._high):
            yield self._get_card(idx)

    def __getitem__(self, index):
        return self._get_card(index)

    def _get_card(self, index):
        if index >= self._high:
            raise IndexError
        path = self._card_paths[index]
        card_info = self._card_ds.card_info_from_path(path)
        # todo : find card creation date and put it in client.
        return Card(
            self._card_ds,
            card_info.type,
            path,
            card_info.hash,
            id=card_info.id,
            html=None,
            created_on=None,
        )

    def _make_heading(self, type):
        return "<h1>Displaying Card Of Type : %s</h1>" % type.title()

    def _repr_html_(self):
        main_html = []
        for idx, _ in enumerate(self._card_paths):
            card = self._get_card(idx)
            main_html.append(self._make_heading(card.type))
            container_id = uuid.uuid4()
            main_html.append(
                "<script type='text/javascript'>var mfContainerId = '%s';</script>"
                % container_id
            )
            main_html.append(
                "<div class='embed' data-container='%s'>%s</div>"
                % (container_id, card.get())
            )
        return "\n".join(main_html)


def get_cards(
    task: Union[str, "Task"],
    id: Optional[str] = None,
    type: Optional[str] = None,
    follow_resumed: bool = True,
) -> CardContainer:
    """
    Get cards related to a `Task`.

    Note that `get_cards` resolves the cards contained by the task, but it doesn't actually
    retrieve them from the datastore. Actual card contents are retrieved lazily either when
    the card is rendered in a notebook to when you call `Card.get`. This means that
    `get_cards` is a fast call even when individual cards contain a lot of data.

    Parameters
    ----------
    task : str or `Task`
        A `Task` object or pathspec `{flow_name}/{run_id}/{step_name}/{task_id}` that
        uniquely identifies a task.
    id : str, optional
        The ID of card to retrieve if multiple cards are present.
    type : str, optional
        The type of card to retrieve if multiple cards are present.
    follow_resumed : bool, default: True
        If the task has been resumed, then setting this flag will resolve the card for
        the origin task.

    Returns
    -------
    CardContainer
        A list-like object that holds `Card` objects.
    """
    from metaflow.client import Task
    from metaflow import namespace

    card_id = id
    if isinstance(task, str):
        task_str = task
        if len(task_str.split("/")) != 4:
            # Exception that pathspec is not of correct form
            raise IncorrectPathspecException(task_str)
        # set namespace as None so that we don't face namespace mismatch error.
        namespace(None)
        task = Task(task_str)
    elif not isinstance(task, Task):
        # Exception that the task argument should be of form `Task` or `str`
        raise IncorrectArguementException(_TYPE(task))

    if follow_resumed:
        origin_taskpathspec = resumed_info(task)
        if origin_taskpathspec:
            task = Task(origin_taskpathspec)

    card_paths, card_ds = resolve_paths_from_task(
        _get_flow_datastore(task), pathspec=task.pathspec, type=type, card_id=card_id
    )
    return CardContainer(
        card_paths,
        card_ds,
        from_resumed=origin_taskpathspec is not None,
        origin_pathspec=origin_taskpathspec,
    )


def _get_flow_datastore(task):
    flow_name = task.pathspec.split("/")[0]
    # Resolve datastore type
    ds_type = None
    # We need to set the correct datastore root here so that
    # we can ensure that the card client picks up the correct path to the cards

    meta_dict = task.metadata_dict
    ds_type = meta_dict.get("ds-type", None)

    if ds_type is None:
        raise UnresolvableDatastoreException(task)

    ds_root = meta_dict.get("ds-root", None)
    if ds_root:
        ds_root = os.path.join(ds_root, CARD_SUFFIX)
    else:
        ds_root = CardDatastore.get_storage_root(ds_type)

    # Delay load to prevent circular dep
    from metaflow.plugins import DATASTORES

    storage_impl = [d for d in DATASTORES if d.TYPE == ds_type][0]
    return FlowDataStore(
        flow_name=flow_name,
        environment=None,  # TODO: Add environment here
        storage_impl=storage_impl,
        # ! ds root cannot be none otherwise `list_content`
        # ! method fails in the datastore abstraction.
        ds_root=ds_root,
    )
