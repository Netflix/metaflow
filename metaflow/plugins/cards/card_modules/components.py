from .basic import (
    LogComponent,
    ErrorComponent,
    ArtifactsComponent,
    DagComponent,
    TableComponent,
    BarChartComponent,
    LineChartComponent,
    ImageComponent,
    SectionComponent,
    SubTitleComponent,
    TitleComponent,
    MarkdownComponent,
    DefaultComponent,
)
from .card import MetaflowCardComponent
from .convert_to_native_type import TaskToDict


class Artifact(MetaflowCardComponent):
    def __init__(self, artifact, name, compressed=True):
        self._artifact = artifact
        self._name = name
        self._task_to_dict = TaskToDict(only_repr=compressed)

    def render(self):
        artifact = {self._name: self._task_to_dict.infer_object(self._artifact)}
        return ArtifactsComponent(data=artifact).render()


class Table(MetaflowCardComponent):
    def __init__(self, data=[[]], headers=[]):
        header_bool, data_bool = TableComponent.validate(headers, data)
        self._headers = []
        self._data = [[]]
        if header_bool:
            self._headers = headers
        if data_bool:
            self._data = data

    @classmethod
    def from_dataframe(cls, dataframe=None, section_wrapped=True):
        task_to_dict = TaskToDict()
        object_type = task_to_dict.object_type(dataframe)
        if object_type == "pandas.core.frame.DataFrame":
            return cls(
                **task_to_dict._parse_pandas_dataframe(dataframe),
                section_wrapped=section_wrapped
            )
        else:
            return cls(
                headers=["Object type %s not supported" % object_type],
                section_wrapped=section_wrapped,
            )

    def _render_subcomponents(self):
        return [
            SectionComponent.render_subcomponents(
                row, additional_allowed_types=[str, bool, int, float, dict, list, tuple]
            )
            for row in self._data
        ]

    def render(self):
        return TableComponent(
            headers=self._headers, data=self._render_subcomponents()
        ).render()


class Image(MetaflowCardComponent):

    render_fail_headline = lambda msg: "[IMAGE_RENDER FAIL]: %s" % msg

    def __init__(self, src=None, label=None):
        self._src = src
        self._label = label

    @classmethod
    def from_bytes(cls, bytes_arr, label=None):
        try:
            import io

            task_to_dict = TaskToDict()
            if task_to_dict.object_type(bytes_arr) != "bytes":
                return ErrorComponent(
                    cls.render_fail_headline(
                        "first argument should be of type `bytes`"
                    ),
                    "Type of %s is invalid" % (task_to_dict.object_type(bytes_arr)),
                )
            parsed_image = task_to_dict.parse_image(bytes_arr)
            if parsed_image is not None:
                return cls(src=parsed_image, label=label)
            return ErrorComponent(cls.render_fail_headline(" Bytes not parsable"), "")
        except:
            import traceback

            return ErrorComponent(
                cls.render_fail_headline("Bytes not parsable"),
                "%s" % traceback.format_exc(),
            )

    @classmethod
    def from_pil_image(cls, pilimage, label=None):
        try:
            import io

            PIL_IMAGE_PATH = "PIL.Image.Image"
            task_to_dict = TaskToDict()
            if task_to_dict.object_type(pilimage) != PIL_IMAGE_PATH:
                return ErrorComponent(
                    cls.render_fail_headline(
                        "first argument for `Image` should be of type %s"
                        % PIL_IMAGE_PATH
                    ),
                    "Type of %s is invalid. Type of %s required"
                    % (task_to_dict.object_type(pilimage), PIL_IMAGE_PATH),
                )
            img_byte_arr = io.BytesIO()
            try:
                pilimage.save(img_byte_arr, format="PNG")
            except OSError as e:
                return ErrorComponent(
                    cls.render_fail_headline("PIL Image Not Parsable"), "%s" % repr(e)
                )
            img_byte_arr = img_byte_arr.getvalue()
            parsed_image = task_to_dict.parse_image(img_byte_arr)
            if parsed_image is not None:
                return cls(src=parsed_image, label=label)
            return ErrorComponent(
                cls.render_fail_headline("PIL Image Not Parsable"), ""
            )
        except:
            import traceback

            return ErrorComponent(
                cls.render_fail_headline("PIL Image Not Parsable"),
                "%s" % traceback.format_exc(),
            )

    @classmethod
    def from_matplotlib_plot(cls, plot, label=None):
        import io

        try:
            plt = getattr(plot, "get_figure", None)
            if plt is None:
                return ErrorComponent(
                    cls.render_fail_headline(
                        "Invalid Type. Object %s is not from `matlplotlib`" % type(plot)
                    ),
                    "",
                )
            task_to_dict = TaskToDict()
            figure = plot.get_figure()
            figure.show()
            img_bytes_arr = io.BytesIO()
            figure.savefig(img_bytes_arr, format="PNG")
            parsed_image = task_to_dict.parse_image(img_bytes_arr.getvalue())

            if parsed_image is not None:
                return cls(src=parsed_image, label=label)
            return ErrorComponent(
                cls.render_fail_headline("Matplotlib plot's image is not parsable"), ""
            )
        except:
            import traceback

            return ErrorComponent(
                cls.render_fail_headline("Matplotlib plot's image is not parsable"),
                "%s" % traceback.format_exc(),
            )

    def render(self):
        if self._src is not None:
            return ImageComponent(src=self._src, label=self._label).render()
        return ErrorComponent(
            self.render_fail_headline("`Image` Component `src` arguement is `None`"), ""
        ).render()


class Linechart(LineChartComponent):
    def __init__(self, data=[], labels=[], chart_config=None):
        super().__init__(chart_config=chart_config, data=data, labels=labels)

    def render(self):
        rendered_super = super().render()
        return rendered_super


class Barchart(BarChartComponent):
    def __init__(
        self,
        data=[],
        labels=[],
        chart_config=None,
    ):
        super().__init__(chart_config=chart_config, data=data, labels=labels)

    def render(self):
        rendered_super = super().render()
        return rendered_super


class Title(DefaultComponent):
    type = "heading"

    def __init__(self, title_text=None, subtitle_text=None):
        super().__init__(title_text, subtitle_text)

    def render(self):
        return super().render()


class Error(MetaflowCardComponent):
    def __init__(self, exception, title=None):
        self._exception = exception
        self._title = title

    def render(self):
        return SectionComponent(
            title=self._title, contents=[LogComponent(repr(self._exception))]
        ).render()


class Section(MetaflowCardComponent):
    def __init__(self, contents=[], title=None, subtitle=None, columns=None):
        self._title, self._subtitle, self._columns, self._contents = (
            title,
            subtitle,
            columns,
            contents,
        )

    def render(self):
        return SectionComponent(
            self._title, self._subtitle, self._columns, self._contents
        ).render()


class Markdown(MetaflowCardComponent):
    def __init__(self, text=None):
        self._text = text

    def render(self):
        return MarkdownComponent(self._text).render()
