import json
import os
from .card import MetaflowCard, MetaflowCardComponent
from .convert_to_native_type import TaskToDict

ABS_DIR_PATH = os.path.dirname(os.path.abspath(__file__))
RENDER_TEMPLATE_PATH = os.path.join(ABS_DIR_PATH, "base.html")
RENDER_TEMPLATE = None
with open(RENDER_TEMPLATE_PATH, "r") as f:
    RENDER_TEMPLATE = f.read()


class DefaultComponent(MetaflowCardComponent):

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

    def render(self):
        datadict = super().render()
        datadict["contents"] = self._contents
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


class ChartComponent(DefaultComponent):
    def __init__(
        self,
        chart_config=None,
        data=[[]],
        labels=[],
    ):
        super().__init__(title=None, subtitle=None)
        self._chart_config = chart_config
        self._data = data
        self._labels = labels
        # We either use data & labels OR chart_config
        # the chart_config object is a

    def render(self):
        render_dict = super().render()
        if self._chart_config is not None:
            render_dict["config"] = self._chart_config
            return render_dict
        # No `chart_config` is provided.
        # Since there is no `chart_config` we pass the `data` and `labels` object.
        render_dict.update(dict(data=self._data, labels=self._labels))
        return render_dict


class LineChartComponent(ChartComponent):
    type = "lineChart"

    def __init__(self, chart_config=None, data=[], labels=[]):
        super().__init__(chart_config=chart_config, data=data, labels=labels)


class BarChartComponent(ChartComponent):
    type = "barChart"

    def __init__(self, chart_config=None, data=[[]], labels=[]):
        super().__init__(chart_config=chart_config, data=data, labels=labels)


class TableComponent(DefaultComponent):
    type = "table"

    def __init__(self, title=None, subtitle=None, headers=[], data=[[]]):
        super().__init__(title=title, subtitle=subtitle)
        self._headers = []
        self._data = [[]]

        if self._validate_header_type(headers):
            self._headers = headers
        if self._validate_row_type(data):
            self._data = data

    def _validate_header_type(self, data):
        if type(data) != list:
            return False
        return True

    def _validate_row_type(self, data):
        if type(data) != list:
            return False
        if type(data[0]) != list:
            return False
        return True

    def render(self):
        datadict = super().render()
        datadict["columns"] = self._headers
        datadict["data"] = self._data
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


class PageComponent(DefaultComponent):
    type = "page"

    def __init__(self, title=None, subtitle=None, contents=[]):
        super().__init__(title=title, subtitle=subtitle)
        self._contents = contents

    def render(self):
        datadict = super().render()
        datadict["contents"] = self._contents
        return datadict


class ArtifactsComponent(DefaultComponent):
    type = "artifacts"

    def __init__(self, title=None, subtitle=None, data={}):
        super().__init__(title=title, subtitle=subtitle)
        self._data = data

    def render(self):
        datadict = super().render()
        datadict["data"] = self._data
        return datadict


class DefaultCard(MetaflowCard):

    type = "default"

    def __init__(self, options=dict(only_repr=True), components=[], graph=None):
        self._only_repr = True
        self._graph = graph
        if "only_repr" in options:
            self._only_repr = options["only_repr"]

    def render(self, task):
        task_data_dict = TaskToDict(only_repr=self._only_repr)(task, graph=self._graph)
        mf_version = [t for t in task.parent.parent.tags if "metaflow_version" in t][
            0
        ].split("metaflow_version:")[1]
        final_component_dict = dict(
            metadata=dict(
                metaflow_version=mf_version, version=1, template="defaultCardTemplate"
            ),
            components=[],
        )

        metadata = [
            "stderr",
            "stdout",
            "created_at",
            "finished_at",
            "pathspec",
        ]

        for m in metadata:
            final_component_dict["metadata"][m] = task_data_dict[m]

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
            table_comps.append(TableComponent(title=tabname, **tab_dict).render())

        param_ids = [p.id for p in task.parent.parent["_parameters"].task]
        # This makes a vertical table.
        parameter_table = SectionComponent(
            title="Flow Parameters",
            contents=[
                TableComponent(
                    headers=[],
                    data=[[pid, task[pid].data] for pid in param_ids],
                ).render()
            ],
        ).render()
        artrifact_component = ArtifactsComponent(data=task_data_dict["data"]).render()
        artifact_section = SectionComponent(
            title="Artifacts", contents=[artrifact_component]
        ).render()
        dag_component = SectionComponent(
            title="DAG", contents=[DagComponent(data=task_data_dict["graph"]).render()]
        ).render()

        page_contents = [parameter_table, dag_component, artifact_section]
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

        page_component = PageComponent(
            title="Task Info",
            contents=page_contents,
        ).render()

        final_component_dict["components"].append(
            TitleComponent(text=task_data_dict["pathspec"]).render()
        )
        final_component_dict["components"].append(page_component)
        pt = self._get_mustache()
        data_dict = dict(task_data=json.dumps(json.dumps(final_component_dict)))
        return pt.render(RENDER_TEMPLATE, data_dict)
