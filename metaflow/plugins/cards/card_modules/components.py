from typing import Any, List, Optional, Union
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
from .convert_to_native_type import TaskToDict, _full_classname
from .renderer_tools import render_safely


class UserComponent(MetaflowCardComponent):
    pass


class Artifact(UserComponent):
    """
    A pretty-printed version of any Python object.

    Large objects are truncated using Python's built-in [`reprlib`](https://docs.python.org/3/library/reprlib.html).

    Example:
    ```
    from datetime import datetime
    current.card.append(Artifact({'now': datetime.utcnow()}))
    ```

    Parameters
    ----------
    artifact : object
        Any Python object.
    name : str, optional
        Optional label for the object.
    compressed : bool, default: True
        Use a truncated representation.
    """

    def __init__(
        self, artifact: Any, name: Optional[str] = None, compressed: bool = True
    ):
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
    A table.

    The contents of the table can be text or numerical data, a Pandas dataframe,
    or other card components: `Artifact`, `Image`, `Markdown` objects.

    Example: Text and artifacts
    ```
    from metaflow.cards import Table, Artifact
    current.card.append(
        Table([
            ['first row', Artifact({'a': 2})],
            ['second row', Artifact(3)]
        ])
    )
    ```

    Example: Table from a Pandas dataframe
    ```
    from metaflow.cards import Table
    import pandas as pd
    import numpy as np
    current.card.append(
        Table.from_dataframe(
            pd.DataFrame(
                np.random.randint(0, 100, size=(15, 4)),
                columns=list("ABCD")
            )
        )
    )
    ```

    Parameters
    ----------
    data : List[List[str or MetaflowCardComponent]], optional
        List (rows) of lists (columns). Each item can be a string or a `MetaflowCardComponent`.
    headers : List[str], optional
        Optional header row for the table.
    """

    def __init__(
        self,
        data: Optional[List[List[Union[str, MetaflowCardComponent]]]] = None,
        headers: Optional[List[str]] = None,
    ):
        data = data or [[]]
        headers = headers or []
        header_bool, data_bool = TableComponent.validate(headers, data)
        self._headers = []
        self._data = [[]]
        if header_bool:
            self._headers = headers
        if data_bool:
            self._data = data

    @classmethod
    def from_dataframe(cls, dataframe=None, truncate: bool = True):
        """
        Create a `Table` based on a Pandas dataframe.

        Parameters
        ----------
        dataframe : Optional[pandas.DataFrame]
            Pandas dataframe.
        truncate : bool, default: True
            Truncate large dataframe instead of showing all rows (default: True).
        """
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
    An image.

    `Images can be created directly from PNG/JPG/GIF `bytes`, `PIL.Image`s,
    or Matplotlib figures. Note that the image data is embedded in the card,
    so no external files are required to show the image.

    Example: Create an `Image` from bytes:
    ```
    current.card.append(
        Image(
            requests.get("https://www.gif-vif.com/hacker-cat.gif").content,
            "Image From Bytes"
        )
    )
    ```

    Example: Create an `Image` from a Matplotlib figure
    ```
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

    Example: Create an `Image` from a [PIL](https://pillow.readthedocs.io/) Image
    ```
    from PIL import Image as PILImage
    current.card.append(
        Image.from_pil_image(
            PILImage.fromarray(np.random.randn(1024, 768), "RGB"),
            "From PIL Image"
        )
    )
    ```

    Parameters
    ----------
    src : bytes
        The image data in `bytes`.
    label : str
        Optional label for the image.
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
                        "first argument should be of type `bytes` or valid image base64 string"
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
                        "first argument should be of type `bytes` or valid image base64 string"
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
    def from_pil_image(cls, pilimage, label: Optional[str] = None):
        """
        Create an `Image` from a PIL image.

        Parameters
        ----------
        pilimage : PIL.Image
            a PIL image object.
        label : str, optional
            Optional label for the image.
        """
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
    def from_matplotlib(cls, plot, label: Optional[str] = None):
        """
        Create an `Image` from a Matplotlib plot.

        Parameters
        ----------
        plot :  matplotlib.figure.Figure or matplotlib.axes.Axes or matplotlib.axes._subplots.AxesSubplot
            a PIL axes (plot) object.
        label : str, optional
            Optional label for the image.
        """
        import io

        try:
            try:
                import matplotlib.pyplot as pyplt
            except ImportError:
                return ErrorComponent(
                    cls.render_fail_headline("Matplotlib cannot be imported"),
                    "%s" % traceback.format_exc(),
                )
            # First check if it is a valid Matplotlib figure.
            figure = None
            if _full_classname(plot) == "matplotlib.figure.Figure":
                figure = plot

            # If it is not valid figure then check if it is matplotlib.axes.Axes or a matplotlib.axes._subplots.AxesSubplot
            # These contain the `get_figure` function to get the main figure object.
            if figure is None:
                if getattr(plot, "get_figure", None) is None:
                    return ErrorComponent(
                        cls.render_fail_headline(
                            "Invalid Type. Object %s is not from `matplotlib`"
                            % type(plot)
                        ),
                        "",
                    )
                else:
                    figure = plot.get_figure()

            task_to_dict = TaskToDict()
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
            self.render_fail_headline("`Image` Component `src` argument is `None`"), ""
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
    A block of text formatted in Markdown.

    Example:
    ```
    current.card.append(
        Markdown("# This is a header appended from `@step` code")
    )
    ```

    Parameters
    ----------
    text : str
        Text formatted in Markdown.
    """

    def __init__(self, text=None):
        self._text = text

    @render_safely
    def render(self):
        return MarkdownComponent(self._text).render()
