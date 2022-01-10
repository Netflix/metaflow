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
    def from_dataframe(cls, dataframe=None, truncate=True):
        task_to_dict = TaskToDict()
        object_type = task_to_dict.object_type(dataframe)
        if object_type == "pandas.core.frame.DataFrame":
            table_data = task_to_dict._parse_pandas_dataframe(
                dataframe, truncate=truncate
            )
            return_val = cls(data=table_data["data"], headers=table_data["headers"])
            return return_val
        else:
            return cls(
                headers=["Object type %s not supported" % object_type],
            )

    def _render_subcomponents(self):
        return [
            SectionComponent.render_subcomponents(
                row,
                additional_allowed_types=[
                    str,
                    bool,
                    int,
                    float,
                    dict,
                    list,
                    tuple,
                    type(None),
                ],
            )
            for row in self._data
        ]

    def render(self):
        return TableComponent(
            headers=self._headers, data=self._render_subcomponents()
        ).render()


class Image(MetaflowCardComponent):
    @staticmethod
    def render_fail_headline(msg):
        return "[IMAGE_RENDER FAIL]: %s" % msg

    def __init__(self, src=None, label=None):
        self._error_comp = None
        self._label = label

        if type(src) is not str:
            try:
                self._src = self._bytes_to_base64(src)
            except TypeError:
                self._error_comp = ErrorComponent(
                    self.render_fail_headline(
                        "first argument should be of type `bytes` or vaild image base64 string"
                    ),
                    "Type of %s is invalid" % (str(type(src))),
                )
            except ValueError:
                self._error_comp = ErrorComponent(
                    self.render_fail_headline("Bytes not parsable as image"), ""
                )
            except Exception as e:
                import traceback

                self._error_comp = ErrorComponent(
                    self.render_fail_headline("Bytes not parsable as image"),
                    "%s\n\n%s" % (str(e), traceback.format_exc()),
                )
        else:
            if "data:image/" in src:
                self._src = src
            else:
                self._error_comp = ErrorComponent(
                    self.render_fail_headline(
                        "first argument should be of type `bytes` or vaild image base64 string"
                    ),
                    "String %s is invalid base64 string" % src,
                )

    @staticmethod
    def _bytes_to_base64(bytes_arr):
        task_to_dict = TaskToDict()
        if task_to_dict.object_type(bytes_arr) != "bytes":
            raise TypeError
        parsed_image = task_to_dict.parse_image(bytes_arr)
        if parsed_image is None:
            raise ValueError
        return parsed_image

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
        if self._error_comp is not None:
            return self._error_comp.render()

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
