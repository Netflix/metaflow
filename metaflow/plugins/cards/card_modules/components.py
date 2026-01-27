from typing import Any, List, Optional, Union, Callable
from .basic import (
    LogComponent,
    ErrorComponent,
    ArtifactsComponent,
    TableComponent,
    ImageComponent,
    SectionComponent,
    MarkdownComponent,
    PythonCodeComponent,
)
from .card import MetaflowCardComponent, with_default_component_id
from .convert_to_native_type import TaskToDict, _full_classname
from .renderer_tools import render_safely
from .json_viewer import JSONViewer as _JSONViewer, YAMLViewer as _YAMLViewer
import uuid
import inspect
import textwrap


def _warning_with_component(component, msg):
    if component._logger is None:
        return None
    if component._warned_once:
        return None
    log_msg = "[@card-component WARNING] %s" % msg
    component._logger(log_msg, timestamp=False, bad=True)
    component._warned_once = True


class UserComponent(MetaflowCardComponent):

    _warned_once = False

    def update(self, *args, **kwargs):
        cls_name = self.__class__.__name__
        msg = (
            "MetaflowCardComponent doesn't have an `update` method implemented "
            "and is not compatible with realtime updates."
        ) % cls_name
        _warning_with_component(self, msg)


class StubComponent(UserComponent):
    def __init__(self, component_id):
        self._non_existing_comp_id = component_id

    def update(self, *args, **kwargs):
        msg = "Component with id %s doesn't exist. No updates will be made at anytime during runtime."
        _warning_with_component(self, msg % self._non_existing_comp_id)


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

    REALTIME_UPDATABLE = True

    def update(self, artifact):
        self._artifact = artifact

    def __init__(
        self, artifact: Any, name: Optional[str] = None, compressed: bool = True
    ):
        self._artifact = artifact
        self._name = name
        self._task_to_dict = TaskToDict(only_repr=compressed)

    @with_default_component_id
    @render_safely
    def render(self):
        artifact = self._task_to_dict.infer_object(self._artifact)
        artifact["name"] = None
        if self._name is not None:
            artifact["name"] = str(self._name)
        af_component = ArtifactsComponent(data=[artifact])
        af_component.component_id = self.component_id
        return af_component.render()


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

    REALTIME_UPDATABLE = True

    def update(self, *args, **kwargs):
        msg = (
            "`Table` doesn't have an `update` method implemented. "
            "Components within a table can be updated individually "
            "but the table itself cannot be updated."
        )
        _warning_with_component(self, msg)

    def __init__(
        self,
        data: Optional[List[List[Union[str, MetaflowCardComponent]]]] = None,
        headers: Optional[List[str]] = None,
        disable_updates: bool = False,
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

        if disable_updates:
            self.REALTIME_UPDATABLE = False

    @classmethod
    def from_dataframe(
        cls,
        dataframe=None,
        truncate: bool = True,
    ):
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
            return_val = cls(
                data=table_data["data"],
                headers=table_data["headers"],
                disable_updates=True,
            )
            return return_val
        else:
            return cls(
                headers=["Object type %s not supported" % object_type],
                disable_updates=True,
            )

    def _render_subcomponents(self):
        for row in self._data:
            for col in row:
                if isinstance(col, VegaChart):
                    col._chart_inside_table = True

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

    @with_default_component_id
    @render_safely
    def render(self):
        table_component = TableComponent(
            headers=self._headers, data=self._render_subcomponents()
        )
        table_component.component_id = self.component_id
        return table_component.render()


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

    REALTIME_UPDATABLE = True

    _PIL_IMAGE_MODULE_PATH = "PIL.Image.Image"

    _MATPLOTLIB_FIGURE_MODULE_PATH = "matplotlib.figure.Figure"

    _PLT_MODULE = None

    _PIL_MODULE = None

    @classmethod
    def _get_pil_module(cls):
        if cls._PIL_MODULE == "NOT_PRESENT":
            return None
        if cls._PIL_MODULE is None:
            try:
                import PIL
            except ImportError:
                cls._PIL_MODULE = "NOT_PRESENT"
                return None
            cls._PIL_MODULE = PIL
        return cls._PIL_MODULE

    @classmethod
    def _get_plt_module(cls):
        if cls._PLT_MODULE == "NOT_PRESENT":
            return None
        if cls._PLT_MODULE is None:
            try:
                import matplotlib.pyplot as pyplt
            except ImportError:
                cls._PLT_MODULE = "NOT_PRESENT"
                return None
            cls._PLT_MODULE = pyplt
        return cls._PLT_MODULE

    @staticmethod
    def render_fail_headline(msg):
        return "[IMAGE_RENDER FAIL]: %s" % msg

    def _set_image_src(self, src, label=None):
        self._label = label
        self._src = None
        self._error_comp = None
        if src is None:
            self._error_comp = ErrorComponent(
                self.render_fail_headline("`Image` Component `src` cannot be `None`"),
                "",
            )
        elif type(src) is not str:
            try:
                self._src = self._bytes_to_base64(src)
            except TypeError:
                self._error_comp = ErrorComponent(
                    self.render_fail_headline(
                        "The `Image` `src` argument should be of type `bytes` or valid image base64 string"
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
                        "The `Image` `src` argument should be of type `bytes` or valid image base64 string"
                    ),
                    "String %s is invalid base64 string" % src,
                )

    def __init__(self, src=None, label=None, disable_updates: bool = True):
        if disable_updates:
            self.REALTIME_UPDATABLE = False
        self._set_image_src(src, label=label)

    def _update_image(self, img_obj, label=None):
        task_to_dict = TaskToDict()
        parsed_image, err_comp = None, None

        # First set image for bytes/string type
        if task_to_dict.object_type(img_obj) in ["bytes", "str"]:
            self._set_image_src(img_obj, label=label)
            return

        if task_to_dict.object_type(img_obj).startswith("PIL"):
            parsed_image, err_comp = self._parse_pil_image(img_obj)
        elif _full_classname(img_obj) == self._MATPLOTLIB_FIGURE_MODULE_PATH:
            parsed_image, err_comp = self._parse_matplotlib(img_obj)
        else:
            parsed_image, err_comp = None, ErrorComponent(
                self.render_fail_headline(
                    "Invalid Type. Object %s is not supported. Supported types: %s"
                    % (
                        type(img_obj),
                        ", ".join(
                            [
                                "str",
                                "bytes",
                                self._PIL_IMAGE_MODULE_PATH,
                                self._MATPLOTLIB_FIGURE_MODULE_PATH,
                            ]
                        ),
                    )
                ),
                "",
            )

        if parsed_image is not None:
            self._set_image_src(parsed_image, label=label)
        else:
            self._set_image_src(None, label=label)
            self._error_comp = err_comp

    @classmethod
    def _pil_parsing_error(cls, error_type):
        return None, ErrorComponent(
            cls.render_fail_headline(
                "first argument for `Image` should be of type %s"
                % cls._PIL_IMAGE_MODULE_PATH
            ),
            "Type of %s is invalid. Type of %s required"
            % (error_type, cls._PIL_IMAGE_MODULE_PATH),
        )

    @classmethod
    def _parse_pil_image(cls, pilimage):
        parsed_value = None
        error_component = None
        import io

        task_to_dict = TaskToDict()
        _img_type = task_to_dict.object_type(pilimage)

        if not _img_type.startswith("PIL"):
            return cls._pil_parsing_error(_img_type)

        # Set the module as a part of the class so that
        # we don't keep reloading the module everytime
        pil_module = cls._get_pil_module()

        if pil_module is None:
            return parsed_value, ErrorComponent(
                cls.render_fail_headline("PIL cannot be imported"), ""
            )
        if not isinstance(pilimage, pil_module.Image.Image):
            return cls._pil_parsing_error(_img_type)

        img_byte_arr = io.BytesIO()
        try:
            pilimage.save(img_byte_arr, format="PNG")
        except OSError as e:
            return parsed_value, ErrorComponent(
                cls.render_fail_headline("PIL Image Not Parsable"), "%s" % repr(e)
            )
        img_byte_arr = img_byte_arr.getvalue()
        parsed_value = task_to_dict.parse_image(img_byte_arr)
        return parsed_value, error_component

    @classmethod
    def _parse_matplotlib(cls, plot):
        import io
        import traceback

        parsed_value = None
        error_component = None
        pyplt = cls._get_plt_module()
        if pyplt is None:
            return parsed_value, ErrorComponent(
                cls.render_fail_headline("Matplotlib cannot be imported"),
                "%s" % traceback.format_exc(),
            )
        # First check if it is a valid Matplotlib figure.
        figure = None
        if _full_classname(plot) == cls._MATPLOTLIB_FIGURE_MODULE_PATH:
            figure = plot

        # If it is not valid figure then check if it is matplotlib.axes.Axes or a matplotlib.axes._subplots.AxesSubplot
        # These contain the `get_figure` function to get the main figure object.
        if figure is None:
            if getattr(plot, "get_figure", None) is None:
                return parsed_value, ErrorComponent(
                    cls.render_fail_headline(
                        "Invalid Type. Object %s is not from `matplotlib`" % type(plot)
                    ),
                    "",
                )
            else:
                figure = plot.get_figure()

        task_to_dict = TaskToDict()
        img_bytes_arr = io.BytesIO()
        figure.savefig(img_bytes_arr, format="PNG")
        parsed_value = task_to_dict.parse_image(img_bytes_arr.getvalue())
        pyplt.close(figure)
        if parsed_value is not None:
            return parsed_value, error_component
        return parsed_value, ErrorComponent(
            cls.render_fail_headline("Matplotlib plot's image is not parsable"), ""
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
    def from_pil_image(
        cls, pilimage, label: Optional[str] = None, disable_updates: bool = False
    ):
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
            parsed_image, error_comp = cls._parse_pil_image(pilimage)
            if parsed_image is not None:
                img = cls(
                    src=parsed_image, label=label, disable_updates=disable_updates
                )
            else:
                img = cls(src=None, label=label, disable_updates=disable_updates)
                img._error_comp = error_comp
            return img
        except:
            import traceback

            img = cls(src=None, label=label, disable_updates=disable_updates)
            img._error_comp = ErrorComponent(
                cls.render_fail_headline("PIL Image Not Parsable"),
                "%s" % traceback.format_exc(),
            )
            return img

    @classmethod
    def from_matplotlib(
        cls, plot, label: Optional[str] = None, disable_updates: bool = False
    ):
        """
        Create an `Image` from a Matplotlib plot.

        Parameters
        ----------
        plot :  matplotlib.figure.Figure or matplotlib.axes.Axes or matplotlib.axes._subplots.AxesSubplot
            a PIL axes (plot) object.
        label : str, optional
            Optional label for the image.
        """
        try:
            parsed_image, error_comp = cls._parse_matplotlib(plot)
            if parsed_image is not None:
                img = cls(
                    src=parsed_image, label=label, disable_updates=disable_updates
                )
            else:
                img = cls(src=None, label=label, disable_updates=disable_updates)
                img._error_comp = error_comp
            return img
        except:
            import traceback

            img = cls(src=None, label=label, disable_updates=disable_updates)
            img._error_comp = ErrorComponent(
                cls.render_fail_headline("Matplotlib plot's image is not parsable"),
                "%s" % traceback.format_exc(),
            )
            return img

    @with_default_component_id
    @render_safely
    def render(self):
        if self._error_comp is not None:
            return self._error_comp.render()

        if self._src is not None:
            img_comp = ImageComponent(src=self._src, label=self._label)
            img_comp.component_id = self.component_id
            return img_comp.render()
        return ErrorComponent(
            self.render_fail_headline("`Image` Component `src` argument is `None`"), ""
        ).render()

    def update(self, image, label=None):
        """
        Update the image.

        Parameters
        ----------
        image : PIL.Image or matplotlib.figure.Figure or matplotlib.axes.Axes or matplotlib.axes._subplots.AxesSubplot or bytes or str
            The updated image object
        label : str, optional
            Optional label for the image.
        """
        if not self.REALTIME_UPDATABLE:
            msg = (
                "The `Image` component is disabled for realtime updates. "
                "Please set `disable_updates` to `False` while creating the `Image` object."
            )
            _warning_with_component(self, msg)
            return

        _label = label if label is not None else self._label
        self._update_image(image, label=_label)


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

    Multi-line strings with indentation are automatically dedented:
    ```
    current.card.append(
        Markdown(f'''
            # Header
            - Item 1
            - Item 2
        ''')
    )
    ```

    Parameters
    ----------
    text : str
        Text formatted in Markdown. Leading whitespace common to all lines
        is automatically removed to support indented multi-line strings.
    """

    REALTIME_UPDATABLE = True

    @staticmethod
    def _dedent_text(text):
        """Remove common leading whitespace from all lines."""
        if text is None:
            return None
        return textwrap.dedent(text)

    def update(self, text=None):
        self._text = self._dedent_text(text)

    def __init__(self, text=None):
        self._text = self._dedent_text(text)

    @with_default_component_id
    @render_safely
    def render(self):
        comp = MarkdownComponent(self._text)
        comp.component_id = self.component_id
        return comp.render()


class ProgressBar(UserComponent):
    """
    A Progress bar for tracking progress of any task.

    Example:
    ```
    progress_bar = ProgressBar(
        max=100,
        label="Progress Bar",
        value=0,
        unit="%",
        metadata="0.1 items/s"
    )
    current.card.append(
        progress_bar
    )
    for i in range(100):
        progress_bar.update(i, metadata="%s items/s" % i)

    ```

    Parameters
    ----------
    max : int, default 100
        The maximum value of the progress bar.
    label : str, optional, default None
        Optional label for the progress bar.
    value : int, default 0
        Optional initial value of the progress bar.
    unit : str, optional, default None
        Optional unit for the progress bar.
    metadata : str, optional, default None
        Optional additional information to show on the progress bar.
    """

    type = "progressBar"

    REALTIME_UPDATABLE = True

    def __init__(
        self,
        max: int = 100,
        label: Optional[str] = None,
        value: int = 0,
        unit: Optional[str] = None,
        metadata: Optional[str] = None,
    ):
        self._label = label
        self._max = max
        self._value = value
        self._unit = unit
        self._metadata = metadata

    def update(self, new_value: int, metadata: Optional[str] = None):
        self._value = new_value
        if metadata is not None:
            self._metadata = metadata

    @with_default_component_id
    @render_safely
    def render(self):
        data = {
            "type": self.type,
            "id": self.component_id,
            "max": self._max,
            "value": self._value,
        }
        if self._label:
            data["label"] = self._label
        if self._unit:
            data["unit"] = self._unit
        if self._metadata:
            data["details"] = self._metadata
        return data


class ValueBox(UserComponent):
    """
    A Value Box component for displaying key metrics with styling and change indicators.

    Inspired by Shiny's value box component, this displays a primary value with optional
    title, subtitle, theme, and change indicators.

    Example:
    ```
    # Basic value box
    value_box = ValueBox(
        title="Revenue",
        value="$1.2M",
        subtitle="Monthly Revenue",
        change_indicator="Up 15% from last month"
    )
    current.card.append(value_box)

    # Themed value box
    value_box = ValueBox(
        title="Total Savings",
        value=50000,
        theme="success",
        change_indicator="Up 30% from last month"
    )
    current.card.append(value_box)

    # Updatable value box for real-time metrics
    metrics_box = ValueBox(
        title="Processing Progress",
        value=0,
        subtitle="Items processed"
    )
    current.card.append(metrics_box)

    for i in range(1000):
        metrics_box.update(value=i, change_indicator=f"Rate: {i*2}/sec")
    ```

    Parameters
    ----------
    title : str, optional
        The title/label for the value box (usually displayed above the value).
        Must be 200 characters or less.
    value : Union[str, int, float]
        The main value to display prominently. Required parameter.
    subtitle : str, optional
        Additional descriptive text displayed below the title.
        Must be 300 characters or less.
    theme : str, optional
        CSS class name for styling the value box. Supported themes: 'default', 'success',
        'warning', 'danger', 'bg-gradient-indigo-purple'. Custom themes must be valid CSS class names.
    change_indicator : str, optional
        Text indicating change or additional context (e.g., "Up 30% VS PREVIOUS 30 DAYS").
        Must be 200 characters or less.
    """

    type = "valueBox"

    REALTIME_UPDATABLE = True

    # Valid built-in themes
    VALID_THEMES = {
        "default",
        "success",
        "warning",
        "danger",
        "bg-gradient-indigo-purple",
    }

    def __init__(
        self,
        title: Optional[str] = None,
        value: Union[str, int, float] = "",
        subtitle: Optional[str] = None,
        theme: Optional[str] = None,
        change_indicator: Optional[str] = None,
    ):
        # Validate inputs
        self._validate_title(title)
        self._validate_value(value)
        self._validate_subtitle(subtitle)
        self._validate_theme(theme)
        self._validate_change_indicator(change_indicator)

        self._title = title
        self._value = value
        self._subtitle = subtitle
        self._theme = theme
        self._change_indicator = change_indicator

    def update(
        self,
        title: Optional[str] = None,
        value: Optional[Union[str, int, float]] = None,
        subtitle: Optional[str] = None,
        theme: Optional[str] = None,
        change_indicator: Optional[str] = None,
    ):
        """
        Update the value box with new data.

        Parameters
        ----------
        title : str, optional
            New title for the value box.
        value : Union[str, int, float], optional
            New value to display.
        subtitle : str, optional
            New subtitle text.
        theme : str, optional
            New theme/styling class.
        change_indicator : str, optional
            New change indicator text.
        """
        if title is not None:
            self._validate_title(title)
            self._title = title
        if value is not None:
            self._validate_value(value)
            self._value = value
        if subtitle is not None:
            self._validate_subtitle(subtitle)
            self._subtitle = subtitle
        if theme is not None:
            self._validate_theme(theme)
            self._theme = theme
        if change_indicator is not None:
            self._validate_change_indicator(change_indicator)
            self._change_indicator = change_indicator

    def _validate_title(self, title: Optional[str]) -> None:
        """Validate title parameter."""
        if title is not None:
            if not isinstance(title, str):
                raise TypeError(f"Title must be a string, got {type(title).__name__}")
            if len(title) > 200:
                raise ValueError(
                    f"Title must be 200 characters or less, got {len(title)} characters"
                )
            if not title.strip():
                raise ValueError("Title cannot be empty or whitespace only")

    def _validate_value(self, value: Union[str, int, float]) -> None:
        """Validate value parameter."""
        if value is None:
            raise ValueError("Value is required and cannot be None")
        if not isinstance(value, (str, int, float)):
            raise TypeError(
                f"Value must be str, int, or float, got {type(value).__name__}"
            )
        if isinstance(value, str):
            if len(value) > 100:
                raise ValueError(
                    f"String value must be 100 characters or less, got {len(value)} characters"
                )
            if not value.strip():
                raise ValueError("String value cannot be empty or whitespace only")
        if isinstance(value, (int, float)):
            if not (-1e15 <= value <= 1e15):
                raise ValueError(
                    f"Numeric value must be between -1e15 and 1e15, got {value}"
                )

    def _validate_subtitle(self, subtitle: Optional[str]) -> None:
        """Validate subtitle parameter."""
        if subtitle is not None:
            if not isinstance(subtitle, str):
                raise TypeError(
                    f"Subtitle must be a string, got {type(subtitle).__name__}"
                )
            if len(subtitle) > 300:
                raise ValueError(
                    f"Subtitle must be 300 characters or less, got {len(subtitle)} characters"
                )
            if not subtitle.strip():
                raise ValueError("Subtitle cannot be empty or whitespace only")

    def _validate_theme(self, theme: Optional[str]) -> None:
        """Validate theme parameter."""
        if theme is not None:
            if not isinstance(theme, str):
                raise TypeError(f"Theme must be a string, got {type(theme).__name__}")
            if not theme.strip():
                raise ValueError("Theme cannot be empty or whitespace only")
            # Allow custom themes but warn if not in valid set
            if theme not in self.VALID_THEMES:
                import re

                # Basic CSS class name validation
                if not re.match(r"^[a-zA-Z][a-zA-Z0-9_-]*$", theme):
                    raise ValueError(
                        f"Theme must be a valid CSS class name, got '{theme}'"
                    )

    def _validate_change_indicator(self, change_indicator: Optional[str]) -> None:
        """Validate change_indicator parameter."""
        if change_indicator is not None:
            if not isinstance(change_indicator, str):
                raise TypeError(
                    f"Change indicator must be a string, got {type(change_indicator).__name__}"
                )
            if len(change_indicator) > 200:
                raise ValueError(
                    f"Change indicator must be 200 characters or less, got {len(change_indicator)} characters"
                )
            if not change_indicator.strip():
                raise ValueError("Change indicator cannot be empty or whitespace only")

    @with_default_component_id
    @render_safely
    def render(self):
        data = {
            "type": self.type,
            "id": self.component_id,
            "value": self._value,
        }
        if self._title is not None:
            data["title"] = self._title
        if self._subtitle is not None:
            data["subtitle"] = self._subtitle
        if self._theme is not None:
            data["theme"] = self._theme
        if self._change_indicator is not None:
            data["change_indicator"] = self._change_indicator
        return data


class VegaChart(UserComponent):
    type = "vegaChart"

    REALTIME_UPDATABLE = True

    def __init__(self, spec: dict, show_controls: bool = False):
        self._spec = spec
        self._show_controls = show_controls
        self._chart_inside_table = False

    def update(self, spec=None):
        """
        Update the chart.

        Parameters
        ----------
        spec : dict or altair.Chart
            The updated chart spec or an altair Chart Object.
        """
        _spec = spec
        if self._object_is_altair_chart(spec):
            _spec = spec.to_dict()
        if _spec is not None:
            self._spec = _spec

    @staticmethod
    def _object_is_altair_chart(altair_chart):
        # This will feel slightly hacky but I am unable to find a natural way of determining the class
        # name of the Altair chart. The only way simple way is to extract the full class name and then
        # match with heuristics
        fulclsname = _full_classname(altair_chart)
        if not all([x in fulclsname for x in ["altair", "vegalite", "Chart"]]):
            return False
        return True

    @classmethod
    def from_altair_chart(cls, altair_chart):
        if not cls._object_is_altair_chart(altair_chart):
            raise ValueError(_full_classname(altair_chart) + " is not an altair chart")
        altair_chart_dict = altair_chart.to_dict()
        cht = cls(spec=altair_chart_dict)
        return cht

    @with_default_component_id
    @render_safely
    def render(self):
        data = {
            "type": self.type,
            "id": self.component_id,
            "spec": self._spec,
        }
        if not self._show_controls:
            data["options"] = {"actions": False}
        if "width" not in self._spec and not self._chart_inside_table:
            data["spec"]["width"] = "container"
        if self._chart_inside_table and "autosize" not in self._spec:
            data["spec"]["autosize"] = "fit-x"
        return data


class PythonCode(UserComponent):
    """
    A component to display Python code with syntax highlighting.

    Example:
    ```python
    @card
    @step
    def my_step(self):
        # Using code_func
        def my_function():
            x = 1
            y = 2
            return x + y
        current.card.append(
            PythonCode(my_function)
        )

        # Using code_string
        code = '''
        def another_function():
            return "Hello World"
        '''
        current.card.append(
            PythonCode(code_string=code)
        )
    ```

    Parameters
    ----------
    code_func : Callable[..., Any], optional, default None
        The function whose source code should be displayed.
    code_string : str, optional, default None
        A string containing Python code to display.
        Either code_func or code_string must be provided.
    """

    def __init__(
        self,
        code_func: Optional[Callable[..., Any]] = None,
        code_string: Optional[str] = None,
    ):
        if code_func is not None:
            self._code_string = inspect.getsource(code_func)
        else:
            self._code_string = code_string

    @with_default_component_id
    @render_safely
    def render(self):
        if self._code_string is None:
            return ErrorComponent(
                "`PythonCode` component requires a `code_func` or `code_string` argument. ",
                "None provided for both",
            ).render()
        _code_component = PythonCodeComponent(self._code_string)
        _code_component.component_id = self.component_id
        return _code_component.render()


class EventsTimeline(UserComponent):
    """
    An events timeline component for displaying structured log messages in real-time.

    This component displays events in a timeline format with the latest events at the top.
    Each event can contain structured data including other UserComponents for rich display.

    Example: Basic usage
    ```python
    @card
    @step
    def my_step(self):
        from metaflow.cards import EventsTimeline
        from metaflow import current

        # Create an events component
        events = EventsTimeline(title="Processing Events")
        current.card.append(events)

        # Add events during processing
        for i in range(10):
            events.update(
                event_data={
                    "timestamp": datetime.now().isoformat(),
                    "event_type": "processing",
                    "item_id": i,
                    "status": "completed",
                    "duration_ms": random.randint(100, 500)
                }
            )
            time.sleep(1)
    ```

    Example: With styling and rich components
    ```python
    from metaflow.cards import EventsTimeline, Markdown, PythonCode

    events = EventsTimeline(title="Agent Actions")
    current.card.append(events)

    # Event with styling
    events.update(
        event_data={
            "action": "tool_call",
            "function": "get_weather",
            "result": "Success"
        },
        style_theme="success"
    )

    # Event with rich components
    events.update(
        event_data={
            "action": "code_execution",
            "status": "completed"
        },
        payloads={
            "code": PythonCode(code_string="print('Hello World')"),
            "notes": Markdown("**Important**: This ran successfully")
        },
        style_theme="info"
    )
    ```

    Parameters
    ----------
    title : str, optional
        Title for the events timeline.
    max_events : int, default 100
        Maximum number of events to display. Older events are removed from display
        but total count is still tracked. Stats and relative time display are always enabled.
    """

    type = "eventsTimeline"

    REALTIME_UPDATABLE = True

    # Valid style themes
    VALID_THEMES = {
        "default",
        "success",
        "warning",
        "error",
        "info",
        "primary",
        "secondary",
        "tool_call",
        "ai_response",
    }

    def __init__(
        self,
        title: Optional[str] = None,
        max_events: int = 100,
    ):
        self._title = title
        self._max_events = max_events
        self._events = []

        # Metadata tracking
        self._total_events_count = 0
        self._first_event_time = None
        self._last_update_time = None
        self._finished = False

    def update(
        self,
        event_data: dict,
        style_theme: Optional[str] = None,
        priority: Optional[str] = None,
        payloads: Optional[dict] = None,
        finished: Optional[bool] = None,
    ):
        """
        Add a new event to the timeline.

        Parameters
        ----------
        event_data : dict
            Basic event metadata (strings, numbers, simple values only).
            This appears in the main event display area.
        style_theme : str, optional
            Visual theme for this event. Valid values: 'default', 'success', 'warning',
            'error', 'info', 'primary', 'secondary', 'tool_call', 'ai_response'.
        priority : str, optional
            Priority level for the event ('low', 'normal', 'high', 'critical').
            Affects visual prominence.
        payloads : dict, optional
            Rich payload components that will be displayed in collapsible sections.
            Values must be UserComponent instances: ValueBox, Image, Markdown,
            Artifact, JSONViewer, YAMLViewer. VegaChart is not supported inside EventsTimeline.
        finished : bool, optional
            Mark the timeline as finished. When True, the status indicator will show
            "Finished" in the header.
        """
        import time

        # Validate style_theme
        if style_theme is not None and style_theme not in self.VALID_THEMES:
            import re

            if not re.match(r"^[a-zA-Z][a-zA-Z0-9_-]*$", style_theme):
                raise ValueError(
                    f"Invalid style_theme '{style_theme}'. Must be a valid CSS class name."
                )

        # Validate payloads contain only allowed UserComponents
        if payloads is not None:
            allowed_components = (
                ValueBox,
                Image,
                Markdown,
                Artifact,
                PythonCode,
                _JSONViewer,
                _YAMLViewer,
            )
            for key, payload in payloads.items():
                if not isinstance(payload, allowed_components):
                    raise TypeError(
                        f"Payload '{key}' must be one of: ValueBox, Image, Markdown, "
                        f"Artifact, JSONViewer, YAMLViewer. Got {type(payload).__name__}"
                    )

        # Add timestamp if not provided
        if "timestamp" not in event_data:
            event_data["timestamp"] = time.time()

        # Create event object with metadata and payloads
        event = {
            "metadata": event_data,
            "payloads": payloads or {},
            "event_id": f"event_{self._total_events_count}",
            "received_at": time.time(),
        }

        # Add styling metadata if provided
        if style_theme is not None:
            event["style_theme"] = style_theme
        if priority is not None:
            event["priority"] = priority

        # Update metadata
        self._total_events_count += 1
        self._last_update_time = time.time()
        if self._first_event_time is None:
            self._first_event_time = time.time()

        # Update finished status if provided
        if finished is not None:
            self._finished = finished

        # Add the event to the beginning of the list (latest first)
        self._events.insert(0, event)

        # Trim displayed events if we exceed max_events
        if len(self._events) > self._max_events:
            self._events = self._events[: self._max_events]

    def get_stats(self) -> dict:
        """
        Get timeline statistics.

        Returns
        -------
        dict
            Statistics including total events, display count, timing info, etc.
        """
        import time

        current_time = time.time()

        stats = {
            "total_events": self._total_events_count,
            "displayed_events": len(self._events),
            "last_update": self._last_update_time,
            "first_event": self._first_event_time,
        }

        # seconds_since_last_update removed; UI derives recency from last event timestamp

        # Add finished status
        stats["finished"] = self._finished

        if self._first_event_time and self._total_events_count > 1:
            runtime = self._last_update_time - self._first_event_time
            if runtime > 0:
                stats["events_per_minute"] = round(
                    (self._total_events_count / runtime) * 60, 1
                )
                stats["total_runtime_seconds"] = round(runtime, 1)

        return stats

    def _render_subcomponents(self):
        """
        Render any UserComponents within event payloads.
        """
        rendered_events = []

        for event in self._events:
            rendered_event = dict(event)  # Copy event metadata

            # Event metadata should only contain simple values (no components)
            rendered_event["metadata"] = event["metadata"]

            # Render payload components
            rendered_payloads = {}
            for key, payload in event["payloads"].items():
                if isinstance(payload, MetaflowCardComponent):
                    # Render the component
                    rendered_payloads[key] = payload.render()
                else:
                    # This shouldn't happen due to validation, but handle gracefully
                    rendered_payloads[key] = str(payload)

            rendered_event["payloads"] = rendered_payloads
            rendered_events.append(rendered_event)

        return rendered_events

    @with_default_component_id
    @render_safely
    def render(self):
        data = {
            "type": self.type,
            "id": self.component_id,
            "events": self._render_subcomponents(),
            "config": {
                "show_stats": True,
                "show_relative_time": True,
                "max_events": self._max_events,
            },
        }

        if self._title is not None:
            data["title"] = self._title

        # Always include stats
        data["stats"] = self.get_stats()

        return data


# Rich viewer components
class JSONViewer(_JSONViewer, UserComponent):
    """
    A component for displaying JSON data with syntax highlighting and collapsible sections.

    This component provides a rich view of JSON data with proper formatting, syntax highlighting,
    and the ability to collapse/expand sections for better readability.

    Example:
    ```python
    from metaflow.cards import JSONViewer, EventsTimeline
    from metaflow import current

    # Use in events timeline
    events = EventsTimeline(title="API Calls")
    events.update({
        "action": "api_request",
        "endpoint": "/users",
        "payload": JSONViewer({"user_id": 123, "fields": ["name", "email"]})
    })

    # Use standalone
    data = {"config": {"debug": True, "timeout": 30}}
    current.card.append(JSONViewer(data, collapsible=True))
    ```
    """

    pass


class YAMLViewer(_YAMLViewer, UserComponent):
    """
    A component for displaying YAML data with syntax highlighting and collapsible sections.

    This component provides a rich view of YAML data with proper formatting and syntax highlighting.

    Example:
    ```python
    from metaflow.cards import YAMLViewer, EventsTimeline
    from metaflow import current

    # Use in events timeline
    events = EventsTimeline(title="Configuration Changes")
    events.update({
        "action": "config_update",
        "config": YAMLViewer({
            "database": {"host": "localhost", "port": 5432},
            "features": ["auth", "logging"]
        })
    })
    ```
    """

    pass
