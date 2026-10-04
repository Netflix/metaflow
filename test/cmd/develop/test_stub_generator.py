import inspect
import sys
import typing
from typing import TypeVar, Optional

import pytest

from metaflow.cmd.develop.stub_generator import StubGenerator

T = TypeVar("T")
U = TypeVar("U")


class TestClass:
    """Test class for stub generation"""

    pass


class GenericClass(typing.Generic[T]):
    """Generic test class"""

    pass


class ComplexGenericClass(typing.Generic[T, U]):
    """Complex generic test class"""

    pass


@pytest.fixture
def stub_generator(tmp_path):
    """Provides a fresh StubGenerator instance for each test."""
    generator = StubGenerator(str(tmp_path), include_generated_for=False)
    generator._reset()
    generator._current_module_name = "test_module"
    generator._current_name = None  # Initialize to avoid AttributeError
    return generator


@pytest.fixture
def mock_getmodule(mocker):
    """Helper fixture to easily mock inspect.getmodule to return a specific module name."""

    def _mocker(module_name="test.module"):
        mock_module = mocker.Mock()
        mock_module.__name__ = module_name
        return mocker.patch("inspect.getmodule", return_value=mock_module)

    return _mocker


@pytest.mark.parametrize(
    "type_obj, expected",
    [
        (int, "int"),
        (str, "str"),
        (type(None), "None"),
    ],
    ids=["int", "str", "NoneType"],
)
def test_get_element_name_resolves_basic_builtin_types(
    stub_generator, type_obj, expected
):
    """Test basic builtin type handling."""
    assert stub_generator._get_element_name_with_module(type_obj) == expected


def test_get_element_name_registers_and_resolves_typevars(stub_generator):
    """Test TypeVar parsing and registration."""
    type_var = TypeVar("TestTypeVar")
    result = stub_generator._get_element_name_with_module(type_var)

    assert result == "TestTypeVar"
    assert "TestTypeVar" in stub_generator._typevars


def test_get_element_name_resolves_class_objects_with_module(
    stub_generator, mock_getmodule
):
    """Test handling of class objects - ensuring module paths are prepended."""
    mock_getmodule("test.module")

    result = stub_generator._get_element_name_with_module(TestClass)

    assert result == "test.module.TestClass"
    assert "test.module" in stub_generator._typing_imports


def test_get_element_name_strips_class_repr_from_generic_alias(
    stub_generator, mock_getmodule
):
    """Test the specific case that was failing - class objects in generic type arguments."""
    callable_type = typing.Callable[[TestClass, Optional[int]], TestClass]
    mock_getmodule("test.module")

    result = stub_generator._get_element_name_with_module(callable_type)

    assert "<class '" not in result
    assert "test.module.TestClass" in result
    assert "typing.Callable" in result


def test_get_element_name_resolves_nested_generics(stub_generator, mock_getmodule):
    """Test deeply nested generic types."""
    nested_type = typing.Dict[str, typing.List[typing.Optional[TestClass]]]
    mock_getmodule("test.module")

    result = stub_generator._get_element_name_with_module(nested_type)

    assert "<class '" not in result
    assert "typing.Dict" in result
    assert "typing.List" in result
    # Python 3.9 expands Optional[X] to Union[X, None]
    assert "typing.Optional" in result or "typing.Union" in result
    assert "test.module.TestClass" in result


def test_get_element_name_aliases_callable_with_class_args(
    stub_generator, mock_getmodule
):
    """Test Callable types with class objects as arguments and correct module aliasing."""
    callable_type = typing.Callable[
        [TestClass, typing.Optional[GenericClass[T]]], TestClass
    ]
    mock_getmodule("metaflow_extensions.nflx.plugins.datatools.dataframe")

    result = stub_generator._get_element_name_with_module(callable_type)

    assert "<class '" not in result
    assert "typing.Callable" in result
    expected_module = "metaflow.mf_extensions.nflx.plugins.datatools.dataframe"
    assert expected_module in result


def test_get_element_name_wraps_forward_references(stub_generator):
    """Test forward reference handling."""
    forward_ref = typing.ForwardRef("SomeClass")
    assert stub_generator._get_element_name_with_module(forward_ref) == '"SomeClass"'


def test_get_element_name_wraps_string_annotations(stub_generator):
    """Test string type annotations."""
    assert stub_generator._get_element_name_with_module("SomeClass") == '"SomeClass"'


def test_get_element_name_wraps_self_references(stub_generator):
    """Test self-referential types in classes."""
    stub_generator._current_name = "TestClass"
    assert stub_generator._get_element_name_with_module("TestClass") == '"TestClass"'


def test_generate_function_stub_strips_class_reprs(stub_generator, mock_getmodule):
    """Test function stub generation with complex types."""

    def test_func(x: typing.Callable[[TestClass], TestClass]) -> TestClass:
        pass

    mock_getmodule("test.module")
    stub = stub_generator._generate_function_stub("test_func", test_func)

    assert "<class '" not in stub
    assert "def test_func(" in stub
    assert "test.module.TestClass" in stub


def test_generate_class_stub_processes_methods(stub_generator, mock_getmodule):
    """Test class stub generation."""

    class TestClassWithMethods:
        def method_with_types(self, x: TestClass) -> Optional[TestClass]:
            pass

    mock_getmodule("test.module")
    stub = stub_generator._generate_class_stub(
        "TestClassWithMethods", TestClassWithMethods
    )

    assert "<class '" not in stub
    assert "class TestClassWithMethods" in stub
    assert "def method_with_types" in stub


def test_exploit_annotation_handles_various_inputs(stub_generator, mock_getmodule):
    """Test the _exploit_annotation helper method against classes, None, and empty."""
    mock_getmodule("test.module")

    # Test with class annotation
    result = stub_generator._exploit_annotation(TestClass)
    assert result.startswith(": ")
    assert "test.module.TestClass" in result
    assert "<class '" not in result

    # Test with None annotation
    assert stub_generator._exploit_annotation(None) == ""

    # Test with empty annotation
    assert stub_generator._exploit_annotation(inspect.Parameter.empty) == ""


def test_get_element_name_tracks_typing_imports(stub_generator, mock_getmodule):
    """Test that imports are properly tracked."""
    mock_getmodule("some.external.module")
    stub_generator._get_element_name_with_module(TestClass)

    assert "some.external.module" in stub_generator._typing_imports


def test_get_module_name_alias_maps_extensions(stub_generator):
    """Test that safe modules are properly aliased."""
    aliased_ext = stub_generator._get_module_name_alias(
        "metaflow_extensions.some.module"
    )
    assert aliased_ext == "metaflow.mf_extensions.some.module"

    aliased_reg = stub_generator._get_module_name_alias("metaflow.some.module")
    assert aliased_reg == "metaflow.some.module"


def test_get_element_name_resolves_union_types(stub_generator, mock_getmodule):
    """Test Union types with class objects."""
    union_type = typing.Union[TestClass, str, int]
    mock_getmodule("test.module")

    result = stub_generator._get_element_name_with_module(union_type)

    assert "<class '" not in result
    assert "typing.Union" in result
    assert "test.module.TestClass" in result
    assert "str" in result
    assert "int" in result


def test_get_element_name_resolves_tuple_types(stub_generator, mock_getmodule):
    """Test Tuple types with class objects."""
    tuple_type = typing.Tuple[TestClass, str, GenericClass[T]]
    mock_getmodule("test.module")

    result = stub_generator._get_element_name_with_module(tuple_type)

    assert "<class '" not in result
    assert "typing.Tuple" in result
    assert "test.module.TestClass" in result
    assert "test.module.GenericClass" in result


def test_get_element_name_retains_ellipsis_in_tuples(stub_generator, mock_getmodule):
    """Test Tuple with ellipsis."""
    tuple_type = typing.Tuple[TestClass, ...]
    mock_getmodule("test.module")

    result = stub_generator._get_element_name_with_module(tuple_type)

    assert "<class '" not in result
    assert "typing.Tuple" in result
    assert "test.module.TestClass" in result
    assert "..." in result


def test_get_element_name_retains_ellipsis_in_callables(stub_generator, mock_getmodule):
    """Test Callable with ellipsis."""
    callable_type = typing.Callable[..., TestClass]
    mock_getmodule("test.module")

    result = stub_generator._get_element_name_with_module(callable_type)

    assert "<class '" not in result
    assert "typing.Callable" in result
    assert "test.module.TestClass" in result
    assert "..." in result


def test_get_element_name_registers_newtype(stub_generator):
    """Test NewType handling."""
    from typing import NewType

    UserId = NewType("UserId", int)

    result = stub_generator._get_element_name_with_module(UserId)

    assert result == "UserId"
    assert "UserId" in stub_generator._typevars


def test_get_element_name_resolves_classvars(stub_generator, mock_getmodule):
    """Test ClassVar types."""
    classvar_type = typing.ClassVar[TestClass]
    mock_getmodule("test.module")

    result = stub_generator._get_element_name_with_module(classvar_type)

    assert "<class '" not in result
    assert "typing.ClassVar" in result
    assert "test.module.TestClass" in result


@pytest.mark.skipif(
    sys.version_info < (3, 8), reason="typing.Final not available in Python 3.7"
)
def test_get_element_name_resolves_final(stub_generator, mock_getmodule):
    """Test Final types."""
    final_type = typing.Final[TestClass]
    mock_getmodule("test.module")

    result = stub_generator._get_element_name_with_module(final_type)

    assert "<class '" not in result
    assert "typing.Final" in result
    assert "test.module.TestClass" in result


def test_get_element_name_resolves_literal(stub_generator):
    """Test Literal types."""
    try:
        from typing import Literal
    except ImportError:
        from typing_extensions import Literal

    literal_type = Literal["test", "value"]
    result = stub_generator._get_element_name_with_module(literal_type)

    assert "<class '" not in result


def test_get_element_name_resolves_deeply_nested_mix(stub_generator, mock_getmodule):
    """Test very deeply nested types with Unions and Callables."""
    nested_type = typing.Dict[
        str,
        typing.List[
            typing.Optional[
                typing.Union[
                    TestClass,
                    typing.Callable[[GenericClass[T]], ComplexGenericClass[T, str]],
                ]
            ]
        ],
    ]
    mock_getmodule("test.module")

    result = stub_generator._get_element_name_with_module(nested_type)

    assert "<class '" not in result
    assert "typing.Dict" in result
    assert "typing.List" in result
    assert "typing.Union" in result
    assert "typing.Callable" in result
    assert "test.module.TestClass" in result


def test_get_element_name_falls_back_to_name_when_module_none(stub_generator, mocker):
    """Test error handling when inspect.getmodule returns None."""
    mocker.patch("inspect.getmodule", return_value=None)

    result = stub_generator._get_element_name_with_module(TestClass)
    assert result == "TestClass"


def test_get_element_name_returns_string_literal_on_eval_failure(stub_generator):
    """Test error handling when eval fails on string annotations."""
    result = stub_generator._get_element_name_with_module(
        "NonExistentClass.InvalidSyntax["
    )
    assert result == '"NonExistentClass.InvalidSyntax["'


def test_get_element_name_handles_generic_origin_without_name(
    stub_generator, mock_getmodule
):
    """Test generic types that don't have _name attribute."""
    list_type = typing.List[TestClass]
    mock_getmodule("test.module")

    result = stub_generator._get_element_name_with_module(list_type)

    assert "<class '" not in result
    assert "typing.List" in result
    assert "test.module.TestClass" in result


@pytest.mark.parametrize(
    "collection_type, expected_name",
    [
        (typing.List[TestClass], "typing.List"),
        (typing.Dict[str, TestClass], "typing.Dict"),
        (typing.Set[TestClass], "typing.Set"),
    ],
    ids=["List", "Dict", "Set"],
)
def test_get_element_name_formats_builtin_collections(
    stub_generator, mock_getmodule, collection_type, expected_name
):
    """Test builtin collection types are correctly mapped."""
    mock_getmodule("test.module")

    result = stub_generator._get_element_name_with_module(collection_type)

    assert "<class '" not in result
    assert expected_name in result
    assert "test.module.TestClass" in result


def test_get_element_name_retains_special_chars(stub_generator, mock_getmodule):
    """Test handling of types with special characters in names."""
    special_class = type("Class_With_Special-Chars", (), {})
    mock_getmodule("test.module")

    result = stub_generator._get_element_name_with_module(special_class)

    assert result == "test.module.Class_With_Special-Chars"
    assert "<class '" not in result


def test_get_element_name_resolves_nested_classes(stub_generator, mock_getmodule):
    """Test nested/inner classes resolve correctly."""
    nested_class = type("OuterClass.InnerClass", (), {})
    nested_class.__name__ = "InnerClass"
    nested_class.__qualname__ = "OuterClass.InnerClass"

    mock_getmodule("test.module")

    result = stub_generator._get_element_name_with_module(nested_class)

    assert result == "test.module.InnerClass"
    assert "<class '" not in result


def test_generator_state_isolates_typing_imports(stub_generator, mock_getmodule):
    """Test that generator state is properly isolated between calls."""
    initial_typing_imports = len(stub_generator._typing_imports)
    mock_getmodule("test.isolated.module")

    stub_generator._get_element_name_with_module(TestClass)

    assert len(stub_generator._typing_imports) > initial_typing_imports
    assert "test.isolated.module" in stub_generator._typing_imports


def test_get_element_name_converts_nonetype(stub_generator, mock_getmodule):
    """Test that NoneType is properly converted to None in generic type annotations."""
    callable_type = typing.Callable[[TestClass], type(None)]
    mock_getmodule("test.module")

    result = stub_generator._get_element_name_with_module(callable_type)

    assert "NoneType" not in result
    assert "None" in result
    assert "typing.Callable" in result
    assert "test.module.TestClass" in result


def test_class_objects_in_generic_types_do_not_leak_class_repr(tmp_path, mocker):
    """Regression test ensuring class objects don't leak as '<class '...'>' in type annotations."""
    generator = StubGenerator(str(tmp_path), include_generated_for=False)
    generator._reset()
    generator._current_module_name = "test_module"

    # Create the exact problematic type from the original issue
    MetaflowDataFrame = type("MetaflowDataFrame", (), {})
    FunctionParameters = type("FunctionParameters", (), {})

    # Mock modules
    mock_df_module = mocker.Mock()
    mock_df_module.__name__ = "metaflow_extensions.nflx.plugins.datatools.dataframe"
    mock_fp_module = mocker.Mock()
    mock_fp_module.__name__ = (
        "metaflow_extensions.nflx.plugins.functions.core.function_parameters"
    )

    def custom_getmodule(obj):
        if obj is MetaflowDataFrame:
            return mock_df_module
        elif obj is FunctionParameters:
            return mock_fp_module
        return None

    mocker.patch("inspect.getmodule", side_effect=custom_getmodule)

    # The problematic type annotation
    problematic_type = typing.Callable[
        [MetaflowDataFrame, typing.Optional[FunctionParameters]], MetaflowDataFrame
    ]

    result = generator._get_element_name_with_module(problematic_type)

    assert "<class '" not in result
    assert "typing.Callable" in result
    assert (
        "metaflow.mf_extensions.nflx.plugins.datatools.dataframe.MetaflowDataFrame"
        in result
    )
    assert (
        "metaflow.mf_extensions.nflx.plugins.functions.core.function_parameters.FunctionParameters"
        in result
    )
