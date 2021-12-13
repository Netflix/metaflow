from metaflow.datastore import DATASTORES, FlowDataStore
from .card_resolver import resolve_paths_from_task, resumed_info
from .card_datastore import CardDatastore
from .exception import UnresolvableDatastoreException
import os
import tempfile


class Card:
    def __init__(
        self,
        card_ds,
        type,
        path,
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

        # public attributes
        self.type = type
        self.from_resumed = from_resumed
        self.origin_pathspec = origin_pathspec

        # Tempfile to open stuff in browser
        self._temp_file = None

    @property
    def html(self):
        if self._html is not None:
            return self._html
        self._html = self._card_ds.get_card_html(self.path)
        return self._html

    @property
    def path(self):
        return self._path

    def browser(self):
        import webbrowser

        self._temp_file = tempfile.NamedTemporaryFile(suffix=".html")
        html = self.html
        self._temp_file.write(html.encode())
        self._temp_file.seek(0)
        url = "file://" + os.path.abspath(self._temp_file.name)
        webbrowser.open(url)

    def nb(self):
        from IPython.core.display import HTML, display

        display(HTML(self.html))


class CardIterator:
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
            html=None,
            created_on=None,
        )

    def _make_heading(self, type):
        return "<h1>Displaying Card Of Type : %s</h1>" % type.title()

    def _wrap_html(self, html):
        return (
            """
        <html>
            <head></head>
            <body>
                %s
            </body>
        </html>
        """
            % html
        )

    def nb(self):
        from IPython.core.display import HTML
        from IPython.display import display_html

        main_html = []
        for idx, _ in enumerate(self._card_paths):
            card = self._get_card(idx)
            main_html.append(HTML(data=self._make_heading(card.type)))
            main_html.append(HTML(data=card.html))
        display_html(*main_html)

    def _repr_html_(self):
        from IPython.core.display import HTML
        from IPython.display import display_html

        main_html = []
        for idx, _ in enumerate(self._card_paths):
            card = self._get_card(idx)
            main_html.append(self._make_heading(card.type))
            main_html.append(card.html)
        # return self._wrap_html()
        return "\n".join(main_html)

    def __next__(self):
        self._current += 1
        if self._current >= self._high:
            raise StopIteration
        return self._get_card(self._current)


def get_cards(task, type=None, follow_resumed=True):
    """
    Get cards related to a Metaflow `Task`

    Args:
        task (`Task`): A metaflow `Task` object.
        type (str, optional): The type of card to retrieve. Defaults to None.
        follow_resumed (bool, optional): If a Task has been resumed and cloned, then setting this flag will resolve the card for the origin task. Defaults to True.

    Returns:
        `CardIterator` : A `list` like object that holds `Card` objects.
    """
    from metaflow.client import Task

    if follow_resumed:
        origin_taskpathspec = resumed_info(task)
        if origin_taskpathspec:
            task = Task(origin_taskpathspec)

    _, run_id, step_name, task_id = task.pathspec.split("/")

    card_paths, card_ds = resolve_paths_from_task(
        _get_flow_datastore(task),
        run_id,
        step_name,
        task_id,
        pathspec=task.pathspec,
        type=type,
    )
    return CardIterator(
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
    # we can ensure the the card client picks up the correct path to the cards

    for meta in task.metadata:
        if meta.name == "ds-type":
            ds_type = meta.value
            break

    ds_root = CardDatastore.get_storage_root(ds_type)

    if ds_root is None:
        for meta in task.metadata:
            if meta.name == "ds-root":
                ds_root = meta.value
                break

    if ds_type is None:
        raise UnresolvableDatastoreException(task)

    storage_impl = DATASTORES[ds_type]
    return FlowDataStore(
        flow_name=flow_name,
        environment=None,  # TODO: Add environment here
        storage_impl=storage_impl,
        # ! ds root cannot be none otherwise `list_content`
        # ! method fails in the datastore abstraction.
        ds_root=ds_root,
    )
