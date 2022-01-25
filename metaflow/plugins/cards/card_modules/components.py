from .basic import (
    LogComponent,
    ErrorComponent,
    ArtifactsComponent,
    TableComponent,
    ImageComponent,
    SectionComponent,
    MarkdownComponent,
)
from .card import MetaflowCardComponent
from .convert_to_native_type import TaskToDict
from .renderer_tools import render_safely


class UserComponent(MetaflowCardComponent):
    pass


class Artifact(UserComponent):
    """
    This class helps visualize any variable on the `MetaflowCard`.
    The variable will be truncated using `reprlib.Repr.repr()`.

    ### Usage :
    ```python
    @card
    @step
    def my_step(self):
        from metaflow.cards import Artifact
        from metaflow import current
        x = dict(a=2,b=2..)
        current.card.append(Artifact(x)) # Adds a name to the artifact
        current.card.append(Artifact(x,'my artifact name'))
    ```
    """

    def __init__(self, artifact, name=None, compressed=True):
        self._artifact = artifact
        self._name = name
        self._task_to_dict = TaskToDict(only_repr=compressed)

    @render_safely
    def render(self):
        artifact = self._task_to_dict.infer_object(self._artifact)
        artifact["name"] = None
        if self._name is not None:
            artifact["name"] = str(self._name)
        return ArtifactsComponent(data=[artifact]).render()


class Table(UserComponent):
    """
    This class helps visualize information in the form of a table in a `MetaflowCard`.
    `Table` can take other `MetaflowCardComponent`s like `Artifact`, `Image`, `Markdown` and `Error` as sub elements.

    ### Parameters
    - `data` (List[List[Any]]) : The data to see in the table. Input is a 2d list that contains native types or `MetaflowCardComponent`s like `Artifact`, `Image`, `Markdown` and `Error`. Doesn't play friendly with `dict` as a sub-element. If passing a `dict`, pass it via `Artifact`. Example : `Table([[Artifact(my_dictionary)]])`. If a non serializable object is passed as a sub-element then the table cell on the `MetaflowCard` will show up as `<object>`. columns.  Defaults to [[]].
    - `headers` (List[str]) : The names of the columns.  Defaults to [].

    ### Usage with other components:
    ```python
    @card
    @step
    def my_step(self):
        from metaflow.cards import Table, Artifact
        from metaflow import current
        x = dict(a=2,b=2..)
        y = dict(b=3,c=2..)
        # Can take other components as arguments
        current.card.append(
            Table([[
                Artifact(x), # Adds a name to the artifact
                Artifact(y), # Adds a name to the artifact
            ]])
        current.card.append(Artifact(x,'my artifact name'))
    ```
    ### Usage with dataframes:
    ```python
    @card
    @step
    def my_step(self):
        from metaflow.cards import Table
        from metaflow import current
        # Can be created from a dataframe
        import pandas as pd
        import numpy as np
        current.card.append(
            Table.from_dataframe(
                pandas.DataFrame(
                    np.random.randint(0, 100, size=(15, 4)),
                    columns=list("ABCD"),
                )
            )
        )
    ```
    """

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
                allow_unknowns=True,
            )
            for row in self._data
        ]

    @render_safely
    def render(self):
        return TableComponent(
            headers=self._headers, data=self._render_subcomponents()
        ).render()


class Image(UserComponent):
    """
    This class helps visualize an image in a `MetaflowCard`. `Image`s can be created direcly from `bytes` or `PIL.Image`s or Matplotib figures.

    ### Parameters
    - `src` (bytes) : The image source in `bytes`.
    - `label` (str) : Label to the image show on the `MetaflowCard`.

    ### Usage
    ```python
    @card
    @step
    def my_step(self):
        from metaflow.cards import Image
        from metaflow import current
        import requests
        current.card.append(
            Image(
                requests.get("https://www.gif-vif.com/hacker-cat.gif").content,
                "Image From Bytes",
            ),
        )
    ```

    #### `Image.from_matplotlib` :
    ```python
    @card
    @step
    def my_step(self):
        from metaflow.cards import Image
        from metaflow import current
        import pandas as pd
        import numpy as np
        current.card.append(
            Image.from_matplotlib(
                pandas.DataFrame(
                    np.random.randint(0, 100, size=(15, 4)),
                    columns=list("ABCD"),
                ).plot()
            )
        )
    ```
    #### `Image.from_pil_image` :
    ```python
    @card
    @step
    def my_step(self):
        from metaflow.cards import Image
        from metaflow import current
        from PIL import Image as PILImage
        current.card.append(
            Image.from_pil_image(
                PILImage.fromarray(np.random.randn(1024, 768), "RGB"),
                "From PIL Image",
            ),
        )
    ```
    """

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
    def from_matplotlib(cls, plot, label=None):
        import io

        try:
            plt = getattr(plot, "get_figure", None)
            try:
                import matplotlib.pyplot as pyplt
            except ImportError:
                return ErrorComponent(
                    cls.render_fail_headline("Matplotlib cannot be imported"),
                    "%s" % traceback.format_exc(),
                )
            if plt is None:
                return ErrorComponent(
                    cls.render_fail_headline(
                        "Invalid Type. Object %s is not from `matlplotlib`" % type(plot)
                    ),
                    "",
                )
            task_to_dict = TaskToDict()
            figure = plot.get_figure()
            img_bytes_arr = io.BytesIO()
            figure.savefig(img_bytes_arr, format="PNG")
            parsed_image = task_to_dict.parse_image(img_bytes_arr.getvalue())
            pyplt.close(figure)
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

    @render_safely
    def render(self):
        if self._error_comp is not None:
            return self._error_comp.render()

        if self._src is not None:
            return ImageComponent(src=self._src, label=self._label).render()
        return ErrorComponent(
            self.render_fail_headline("`Image` Component `src` arguement is `None`"), ""
        ).render()


class Error(UserComponent):
    """
    This class helps visualize Error's on the `MetaflowCard`. It can help catch and print stack traces to errors that happen in `@step` code.

    ### Parameters
    - `exception` (Exception) : The `Exception` to visualize. This value will be `repr`'d before passed down to `MetaflowCard`
    - `title` (str) : The title that will appear over the visualized  `Exception`.

    ### Usage
    ```python
    @card
    @step
    def my_step(self):
        from metaflow.cards import Error
        from metaflow import current
        try:
            ...
            ...
        except Exception as e:
            current.card.append(
                Error(e,"Something misbehaved")
            )
        ...
    ```
    """

    def __init__(self, exception, title=None):
        self._exception = exception
        self._title = title

    @render_safely
    def render(self):
        return LogComponent("%s\n\n%s" % (self._title, repr(self._exception))).render()


class Markdown(UserComponent):
    """
    This class helps visualize Markdown on the `MetaflowCard`

    ### Parameters
    - `text` (str) : A markdown string

    ### Usage
    ```python
    @card
    @step
    def my_step(self):
        from metaflow.cards import Markdown
        from metaflow import current
        current.card.append(
            Markdown("# This is a header appended from @step code")
        )
        ...
    ```

    """

    def __init__(self, text=None):
        self._text = text

    @render_safely
    def render(self):
        return MarkdownComponent(self._text).render()
