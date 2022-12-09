import base64
import json
import os
from .card import MetaflowCard, MetaflowCardComponent
from .convert_to_native_type import TaskToDict
import uuid

ABS_DIR_PATH = os.path.dirname(os.path.abspath(__file__))
RENDER_TEMPLATE_PATH = os.path.join(ABS_DIR_PATH, "base.html")
JS_PATH = os.path.join(ABS_DIR_PATH, "main.js")
CSS_PATH = os.path.join(ABS_DIR_PATH, "bundle.css")


def transform_flow_graph(step_info):
    def node_to_type(node_type):
        if node_type in ["linear", "start", "end", "join"]:
            return node_type
        elif node_type == "split":
            return "split"
        elif node_type == "split-parallel" or node_type == "split-foreach":
            return "foreach"
        return "unknown"  # Should never happen

    graph_dict = {}
    for stepname in step_info:
        graph_dict[stepname] = {
            "type": node_to_type(step_info[stepname]["type"]),
            "box_next": step_info[stepname]["type"] not in ("linear", "join"),
            "box_ends": None
            if "matching_join" not in step_info[stepname]
            else step_info[stepname]["matching_join"],
            "next": step_info[stepname]["next"],
            "doc": step_info[stepname]["doc"],
        }
    return graph_dict


def read_file(path):
    with open(path, "r") as f:
        return f.read()


class DefaultComponent(MetaflowCardComponent):
    """
    The `DefaultCard` and the `BlankCard` use a JS framework that build the HTML dynamically from JSON.
    The `DefaultComponent` is the base component that helps build the JSON when `render` is called.

    The underlying JS framework consists of various types of objects.
    These can be found in: "metaflow/plugins/cards/ui/types.ts".
    The `type` attribute in a `DefaultComponent` corresponds to the type of component in the Javascript framework.
    """

    type = None

    def __init__(self, title=None, subtitle=None):
        self._title = title
        self._subtitle = subtitle

    def render(self):
        datadict = dict(
            type=self.type,
        )
        if self._title is not None:
            datadict["title"] = self._title
        if self._subtitle is not None:
            datadict["subtitle"] = self._subtitle
        return datadict


class TitleComponent(MetaflowCardComponent):
    type = "title"

    def __init__(self, text=None):
        self._text = text

    def render(self):
        return dict(type=self.type, text=str(self._text))


class SubTitleComponent(MetaflowCardComponent):
    type = "subtitle"

    def __init__(self, text=None):
        self._text = text

    def render(self):
        return dict(type=self.type, text=str(self._text))


class SectionComponent(DefaultComponent):
    type = "section"

    def __init__(self, title=None, subtitle=None, columns=None, contents=[]):
        super().__init__(title=title, subtitle=subtitle)
        # Contents are expected to be list of dictionaries.
        self._contents = contents
        self._columns = columns

    @classmethod
    def render_subcomponents(
        cls, component_array, additional_allowed_types=[str, dict], allow_unknowns=False
    ):
        contents = []
        for content in component_array:
            # Render objects of `MetaflowCardComponent` type
            if issubclass(type(content), MetaflowCardComponent):
                rendered_content = content.render()
                if type(rendered_content) == str or type(rendered_content) == dict:
                    contents.append(rendered_content)
                else:
                    contents.append(
                        SerializationErrorComponent(
                            content.__class__.__name__,
                            "Component render didn't return a string or dict",
                        ).render()
                    )
            # Objects of allowed types should be present.
            elif type(content) in additional_allowed_types:
                contents.append(content)
            elif allow_unknowns:
                contents.append("<object>")

        return contents

    def render(self):
        datadict = super().render()
        contents = self.render_subcomponents(self._contents)
        datadict["contents"] = contents
        if self._columns is not None:
            datadict["columns"] = self._columns
        return datadict


class ImageComponent(DefaultComponent):
    type = "image"

    def __init__(self, src=None, label=None, title=None, subtitle=None):
        super().__init__(title=title, subtitle=subtitle)
        self._src = src
        self._label = label

    def render(self):
        datadict = super().render()
        img_dict = dict(
            src=self._src,
            label=self._label,
        )
        datadict.update(img_dict)
        return datadict


class TableComponent(DefaultComponent):
    type = "table"

    def __init__(
        self, title=None, subtitle=None, headers=[], data=[[]], vertical=False
    ):
        super().__init__(title=title, subtitle=subtitle)
        self._headers = []
        self._data = [[]]
        self._vertical = vertical

        if self._validate_header_type(headers):
            self._headers = headers
        if self._validate_row_type(data):
            self._data = data

    @classmethod
    def validate(cls, headers, data):
        return (cls._validate_header_type(headers), cls._validate_row_type(data))

    @staticmethod
    def _validate_header_type(data):
        if type(data) != list:
            return False
        return True

    @staticmethod
    def _validate_row_type(data):
        if type(data) != list:
            return False
        try:
            if type(data[0]) != list:
                return False
        except IndexError:
            return False
        except TypeError:
            return False

        return True

    def render(self):
        datadict = super().render()
        datadict["columns"] = self._headers
        datadict["data"] = self._data
        datadict["vertical"] = self._vertical
        return datadict


class DagComponent(DefaultComponent):
    type = "dag"

    def __init__(self, title=None, subtitle=None, data={}):
        super().__init__(title=title, subtitle=subtitle)
        self._data = data

    def render(self):
        datadict = super().render()
        datadict["data"] = self._data
        return datadict


class TextComponent(DefaultComponent):
    type = "text"

    def __init__(self, text=None):
        super().__init__(title=None, subtitle=None)
        self._text = text

    def render(self):
        datadict = super().render()
        datadict["text"] = self._text
        return datadict


class LogComponent(DefaultComponent):
    type = "log"

    def __init__(self, data=None):
        super().__init__(title=None, subtitle=None)
        self._data = data

    def render(self):
        datadict = super().render()
        datadict["data"] = self._data
        return datadict


class HTMLComponent(DefaultComponent):
    type = "html"

    def __init__(self, data=None):
        super().__init__(title=None, subtitle=None)
        self._data = data

    def render(self):
        datadict = super().render()
        datadict["data"] = self._data
        return datadict


class PageComponent(DefaultComponent):
    type = "page"

    def __init__(self, title=None, subtitle=None, contents=[]):
        super().__init__(title=title, subtitle=subtitle)
        self._contents = contents

    def render(self):
        datadict = super().render()
        contents = []
        for content in self._contents:
            if issubclass(type(content), MetaflowCardComponent):
                contents.append(content.render())
            else:
                contents.append(content)
        datadict["contents"] = contents
        return datadict


class ErrorComponent(MetaflowCardComponent):
    def __init__(self, headline, error_message):
        self._headline = headline
        self._error_message = error_message

    def render(self):
        return LogComponent(
            data="%s\n\n%s" % (self._headline, self._error_message)
        ).render()


class SerializationErrorComponent(ErrorComponent):
    def __init__(self, component_name, error_message):
        headline = "Render failed of component named `%s`" % component_name
        super().__init__(headline, error_message)


class ArtifactsComponent(DefaultComponent):
    type = "artifacts"

    def __init__(self, title=None, subtitle=None, data={}):
        super().__init__(title=title, subtitle=subtitle)
        self._data = data

    def render(self):
        datadict = super().render()
        datadict["data"] = self._data
        return datadict


class MarkdownComponent(DefaultComponent):
    type = "markdown"

    def __init__(self, text=None):
        super().__init__(title=None, subtitle=None)
        self._text = text

    def render(self):
        datadict = super().render()
        datadict["source"] = self._text
        return datadict


class TaskInfoComponent(MetaflowCardComponent):
    """
    Properties
        page_content : a list of MetaflowCardComponents going as task info
        final_component: the dictionary returned by the `render` function of this class.
    """

    def __init__(
        self, task, page_title="Task Info", only_repr=True, graph=None, components=[]
    ):
        self._task = task
        self._only_repr = only_repr
        self._graph = graph
        self._components = components
        self._page_title = page_title
        self.final_component = None
        self.page_component = None

    def render(self):
        """

        Returns:
            a dictionary of form:
                dict(metadata = {},components= [])
        """
        task_data_dict = TaskToDict(only_repr=self._only_repr)(
            self._task, graph=self._graph
        )
        # ignore the name as an artifact
        del task_data_dict["data"]["name"]

        _metadata = dict(version=1, template="defaultCardTemplate")
        # try to parse out metaflow version from tags, but let it go if unset
        # e.g. if a run came from a local, un-versioned metaflow codebase
        try:
            _metadata["metaflow_version"] = [
                t for t in self._task.parent.parent.tags if "metaflow_version" in t
            ][0].split("metaflow_version:")[1]
        except Exception:
            pass

        final_component_dict = dict(
            metadata=_metadata,
            components=[],
        )

        metadata = [
            "stderr",
            "stdout",
            "created_at",
            "finished_at",
            "pathspec",
        ]
        tags = self._task.parent.parent.tags
        user_info = [t for t in tags if t.startswith("user:")]
        task_metadata_dict = {
            "Task Created On": task_data_dict["created_at"],
            "Task Finished On": task_data_dict["finished_at"],
            # Remove Microseconds from timedelta
            "Task Duration": str(self._task.finished_at - self._task.created_at).split(
                "."
            )[0],
            "Tags": ", ".join(tags),
        }
        if len(user_info) > 0:
            task_metadata_dict["User"] = user_info[0].split("user:")[1]

        for m in metadata:
            final_component_dict["metadata"][m] = task_data_dict[m]

        metadata_table = SectionComponent(
            title="Task Metadata",
            contents=[
                TableComponent(
                    headers=list(task_metadata_dict.keys()),
                    data=[list(task_metadata_dict.values())],
                    vertical=True,
                )
            ],
        )

        img_components = []
        for img_name in task_data_dict["images"]:
            img_components.append(
                ImageComponent(
                    src=task_data_dict["images"][img_name], label=img_name
                ).render()
            )
        table_comps = []
        for tabname in task_data_dict["tables"]:
            tab_dict = task_data_dict["tables"][tabname]
            tab_title = "Artifact Name: %s" % tabname
            sec_tab_comp = [
                TableComponent(headers=tab_dict["headers"], data=tab_dict["data"])
            ]
            post_table_md = None

            if tab_dict["truncated"]:
                tab_title = "Artifact Name: %s (%d columns and %d rows)" % (
                    tabname,
                    tab_dict["full_size"][1],
                    tab_dict["full_size"][0],
                )
                post_table_md = MarkdownComponent(
                    "_Truncated - %d rows not shown_"
                    % ((tab_dict["full_size"][0] - len(tab_dict["data"])))
                )

            if post_table_md:
                sec_tab_comp.append(post_table_md)

            table_comps.append(
                SectionComponent(
                    title=tab_title,
                    contents=sec_tab_comp,
                )
            )

        # ignore the name as a parameter
        param_ids = [
            p.id for p in self._task.parent.parent["_parameters"].task if p.id != "name"
        ]
        if len(param_ids) > 0:
            param_component = ArtifactsComponent(
                data=[task_data_dict["data"][pid] for pid in param_ids]
            )
        else:
            param_component = TitleComponent(text="No Parameters")

        parameter_table = SectionComponent(
            title="Flow Parameters",
            contents=[param_component],
        ).render()

        # Don't include parameter ids + "name" in the task artifacts
        artifactlist = [
            task_data_dict["data"][k]
            for k in task_data_dict["data"]
            if k not in param_ids
        ]
        if len(artifactlist) > 0:
            artifact_component = ArtifactsComponent(data=artifactlist).render()
        else:
            artifact_component = TitleComponent(text="No Artifacts")

        artifact_section = SectionComponent(
            title="Artifacts", contents=[artifact_component]
        ).render()
        dag_component = SectionComponent(
            title="DAG", contents=[DagComponent(data=task_data_dict["graph"]).render()]
        ).render()

        page_contents = []
        if len(self._components) > 0:
            page_contents.extend(self._components)

        page_contents.extend(
            [
                metadata_table,
                parameter_table,
                artifact_section,
            ]
        )
        if len(table_comps) > 0:
            table_section = SectionComponent(
                title="Tabular Data", contents=table_comps
            ).render()
            page_contents.append(table_section)

        if len(img_components) > 0:
            img_section = SectionComponent(
                title="Image Data",
                columns=len(img_components),
                contents=img_components,
            ).render()
            page_contents.append(img_section)

        page_contents.append(dag_component)

        page_component = PageComponent(
            title=self._page_title,
            contents=page_contents,
        ).render()

        final_component_dict["components"].append(
            TitleComponent(text=task_data_dict["pathspec"]).render()
        )
        final_component_dict["components"].append(page_component)

        # These Properties will provide a way to access these components
        # once render is finished
        # this will Make this object reusable for run level cards.
        self.final_component = final_component_dict

        self.page_component = page_component

        return final_component_dict


class ErrorCard(MetaflowCard):

    type = "error"

    def __init__(self, options={}, components=[], graph=None):
        self._only_repr = True
        self._graph = None if graph is None else transform_flow_graph(graph)
        self._components = components

    def render(self, task, stack_trace=None):
        RENDER_TEMPLATE = read_file(RENDER_TEMPLATE_PATH)
        JS_DATA = read_file(JS_PATH)
        CSS_DATA = read_file(CSS_PATH)
        trace = "None"
        if stack_trace is not None:
            trace = stack_trace

        page = PageComponent(
            title="Error Card",
            contents=[
                SectionComponent(
                    title="Card Render Failed With Error",
                    contents=[LogComponent(data=trace)],
                )
            ],
        ).render()
        final_component_dict = dict(
            metadata={
                "pathspec": task.pathspec,
            },
            components=[page],
        )
        pt = self._get_mustache()
        data_dict = dict(
            task_data=base64.b64encode(
                json.dumps(final_component_dict).encode("utf-8")
            ).decode("utf-8"),
            javascript=JS_DATA,
            css=CSS_DATA,
            title=task.pathspec,
            card_data_id=uuid.uuid4(),
        )
        return pt.render(RENDER_TEMPLATE, data_dict)


class DefaultCardJSON(MetaflowCard):

    type = "default_json"

    def __init__(self, options=dict(only_repr=True), components=[], graph=None):
        self._only_repr = True
        self._graph = None if graph is None else transform_flow_graph(graph)
        if "only_repr" in options:
            self._only_repr = options["only_repr"]
        self._components = components

    def render(self, task):
        final_component_dict = TaskInfoComponent(
            task,
            only_repr=self._only_repr,
            graph=self._graph,
            components=self._components,
        ).render()
        return json.dumps(final_component_dict)


class DefaultCard(MetaflowCard):

    ALLOW_USER_COMPONENTS = True

    type = "default"

    def __init__(self, options=dict(only_repr=True), components=[], graph=None):
        self._only_repr = True
        self._graph = None if graph is None else transform_flow_graph(graph)
        if "only_repr" in options:
            self._only_repr = options["only_repr"]
        self._components = components

    def render(self, task):
        RENDER_TEMPLATE = read_file(RENDER_TEMPLATE_PATH)
        JS_DATA = read_file(JS_PATH)
        CSS_DATA = read_file(CSS_PATH)
        final_component_dict = TaskInfoComponent(
            task,
            only_repr=self._only_repr,
            graph=self._graph,
            components=self._components,
        ).render()
        pt = self._get_mustache()
        data_dict = dict(
            task_data=base64.b64encode(
                json.dumps(final_component_dict).encode("utf-8")
            ).decode("utf-8"),
            javascript=JS_DATA,
            title=task.pathspec,
            css=CSS_DATA,
            card_data_id=uuid.uuid4(),
        )
        return pt.render(RENDER_TEMPLATE, data_dict)


class BlankCard(MetaflowCard):

    ALLOW_USER_COMPONENTS = True

    type = "blank"

    def __init__(self, options=dict(title=""), components=[], graph=None):
        self._graph = None if graph is None else transform_flow_graph(graph)
        self._title = ""
        if "title" in options:
            self._title = options["title"]
        self._components = components

    def render(self, task, components=[]):
        RENDER_TEMPLATE = read_file(RENDER_TEMPLATE_PATH)
        JS_DATA = read_file(JS_PATH)
        CSS_DATA = read_file(CSS_PATH)
        if type(components) != list:
            components = []
        page_component = PageComponent(
            title=self._title,
            contents=components + self._components,
        ).render()
        final_component_dict = dict(
            metadata={
                "pathspec": task.pathspec,
            },
            components=[page_component],
        )
        pt = self._get_mustache()
        data_dict = dict(
            task_data=base64.b64encode(
                json.dumps(final_component_dict).encode("utf-8")
            ).decode("utf-8"),
            javascript=JS_DATA,
            title=task.pathspec,
            css=CSS_DATA,
            card_data_id=uuid.uuid4(),
        )
        return pt.render(RENDER_TEMPLATE, data_dict)


class TaskSpecCard(MetaflowCard):
    type = "taskspec_card"

    def render(self, task):
        return "%s" % task.pathspec
