import tempfile
import typing
import inspect
from typing import TypeVar, Optional
from unittest.mock import Mock, patch

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


class TestStubGenerator:
    """Test suite for StubGenerator functionality"""

    def setup_method(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.generator = StubGenerator(self.temp_dir, include_generated_for=False)
        # Reset internal state
        self.generator._reset()
        self.generator._current_module_name = "test_module"
        self.generator._current_name = None  # Initialize to avoid AttributeError

    def test_get_element_name_basic_types(self):
        """Test basic type handling"""
        # Test builtin types
        assert self.generator._get_element_name_with_module(int) == "int"
        assert self.generator._get_element_name_with_module(str) == "str"
        assert self.generator._get_element_name_with_module(type(None)) == "None"

        # Test TypeVar
        type_var = TypeVar("TestTypeVar")
        result = self.generator._get_element_name_with_module(type_var)
        assert result == "TestTypeVar"
        assert "TestTypeVar" in self.generator._typevars

    def test_get_element_name_class_objects(self):
        """Test handling of class objects - the main issue we're fixing"""
        # Mock the module to avoid import issues
        mock_module = Mock()
        mock_module.__name__ = "test.module"

        with patch("inspect.getmodule", return_value=mock_module):
            result = self.generator._get_element_name_with_module(TestClass)
            assert result == "test.module.TestClass"
            assert "test.module" in self.generator._typing_imports

    def test_get_element_name_generic_alias_with_class_objects(self):
        """Test the specific case that was failing - class objects in generic type arguments"""
        # Create a generic alias with class objects as arguments
        callable_type = typing.Callable[[TestClass, Optional[int]], TestClass]

        # Mock the module for TestClass
        mock_module = Mock()
        mock_module.__name__ = "test.module"

        with patch("inspect.getmodule", return_value=mock_module):
            result = self.generator._get_element_name_with_module(callable_type)

            # Should not contain <class '...'>
            assert "<class '" not in result
            assert "test.module.TestClass" in result
            assert "typing.Callable" in result

    def test_get_element_name_nested_generics(self):
        """Test deeply nested generic types"""
        # Create a complex nested type: Dict[str, List[Optional[TestClass]]]
        nested_type = typing.Dict[str, typing.List[typing.Optional[TestClass]]]

        mock_module = Mock()
        mock_module.__name__ = "test.module"

        with patch("inspect.getmodule", return_value=mock_module):
            result = self.generator._get_element_name_with_module(nested_type)

            assert "<class '" not in result
            assert "typing.Dict" in result
            assert "typing.List" in result
            assert "typing.Optional" in result
            assert "test.module.TestClass" in result

    def test_get_element_name_callable_with_class_args(self):
        """Test Callable types with class objects as arguments"""
        # This is the exact scenario from the original issue
        callable_type = typing.Callable[
            [TestClass, typing.Optional[GenericClass[T]]], TestClass
        ]

        mock_module = Mock()
        mock_module.__name__ = "metaflow_extensions.nflx.plugins.datatools.dataframe"

        with patch("inspect.getmodule", return_value=mock_module):
            result = self.generator._get_element_name_with_module(callable_type)

            # Verify no class objects leak through
            assert "<class '" not in result
            assert "typing.Callable" in result
            # Should contain proper module references (aliased version)
            expected_module = "metaflow.mf_extensions.nflx.plugins.datatools.dataframe"
            assert expected_module in result

    def test_get_element_name_forward_references(self):
        """Test forward reference handling"""
        forward_ref = typing.ForwardRef("SomeClass")
        result = self.generator._get_element_name_with_module(forward_ref)
        assert result == '"SomeClass"'

    def test_get_element_name_string_annotations(self):
        """Test string type annotations"""
        result = self.generator._get_element_name_with_module("SomeClass")
        assert result == '"SomeClass"'

    def test_get_element_name_self_reference(self):
        """Test self-referential types in classes"""
        self.generator._current_name = "TestClass"
        result = self.generator._get_element_name_with_module("TestClass")
        assert result == '"TestClass"'

    def test_function_stub_generation(self):
        """Test function stub generation with complex types"""

        def test_func(x: typing.Callable[[TestClass], TestClass]) -> TestClass:
            pass

        mock_module = Mock()
        mock_module.__name__ = "test.module"

        with patch("inspect.getmodule", return_value=mock_module):
            stub = self.generator._generate_function_stub("test_func", test_func)

            # Should not contain class objects
            assert "<class '" not in stub
            assert "def test_func(" in stub
            assert "test.module.TestClass" in stub

    def test_class_stub_generation(self):
        """Test class stub generation"""

        class TestClassWithMethods:
            def method_with_types(self, x: TestClass) -> Optional[TestClass]:
                pass

        mock_module = Mock()
        mock_module.__name__ = "test.module"

        with patch("inspect.getmodule", return_value=mock_module):
            stub = self.generator._generate_class_stub(
                "TestClassWithMethods", TestClassWithMethods
            )

            assert "<class '" not in stub
            assert "class TestClassWithMethods" in stub
            assert "def method_with_types" in stub

    def test_exploit_annotation_method(self):
        """Test the _exploit_annotation helper method"""
        mock_module = Mock()
        mock_module.__name__ = "test.module"

        with patch("inspect.getmodule", return_value=mock_module):
            # Test with class annotation
            result = self.generator._exploit_annotation(TestClass)
            assert result.startswith(": ")
            assert "test.module.TestClass" in result
            assert "<class '" not in result

            # Test with None annotation
            result = self.generator._exploit_annotation(None)
            assert result == ""

            # Test with empty annotation
            result = self.generator._exploit_annotation(inspect.Parameter.empty)
            assert result == ""

    def test_imports_and_typing_imports(self):
        """Test that imports are properly tracked"""
        mock_module = Mock()
        mock_module.__name__ = "some.external.module"

        with patch("inspect.getmodule", return_value=mock_module):
            self.generator._get_element_name_with_module(TestClass)

            assert "some.external.module" in self.generator._typing_imports

    def test_safe_module_aliasing(self):
        """Test that safe modules are properly aliased"""
        # Test metaflow_extensions aliasing
        test_module_name = "metaflow_extensions.some.module"
        aliased = self.generator._get_module_name_alias(test_module_name)
        assert aliased == "metaflow.mf_extensions.some.module"

        # Test regular metaflow modules
        test_module_name = "metaflow.some.module"
        aliased = self.generator._get_module_name_alias(test_module_name)
        assert aliased == "metaflow.some.module"

    def teardown_method(self):
        """Clean up test environment"""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    # Additional test cases for better coverage

    def test_get_element_name_union_types(self):
        """Test Union types with class objects"""
        union_type = typing.Union[TestClass, str, int]

        mock_module = Mock()
        mock_module.__name__ = "test.module"

        with patch("inspect.getmodule", return_value=mock_module):
            result = self.generator._get_element_name_with_module(union_type)

            assert "<class '" not in result
            assert "typing.Union" in result
            assert "test.module.TestClass" in result
            assert "str" in result
            assert "int" in result

    def test_get_element_name_tuple_types(self):
        """Test Tuple types with class objects"""
        tuple_type = typing.Tuple[TestClass, str, GenericClass[T]]

        mock_module = Mock()
        mock_module.__name__ = "test.module"

        with patch("inspect.getmodule", return_value=mock_module):
            result = self.generator._get_element_name_with_module(tuple_type)

            assert "<class '" not in result
            assert "typing.Tuple" in result
            assert "test.module.TestClass" in result
            assert "test.module.GenericClass" in result

    def test_get_element_name_tuple_with_ellipsis(self):
        """Test Tuple with ellipsis"""
        tuple_type = typing.Tuple[TestClass, ...]

        mock_module = Mock()
        mock_module.__name__ = "test.module"

        with patch("inspect.getmodule", return_value=mock_module):
            result = self.generator._get_element_name_with_module(tuple_type)

            assert "<class '" not in result
            assert "typing.Tuple" in result
            assert "test.module.TestClass" in result
            assert "..." in result

    def test_get_element_name_callable_with_ellipsis(self):
        """Test Callable with ellipsis"""
        callable_type = typing.Callable[..., TestClass]

        mock_module = Mock()
        mock_module.__name__ = "test.module"

        with patch("inspect.getmodule", return_value=mock_module):
            result = self.generator._get_element_name_with_module(callable_type)

            assert "<class '" not in result
            assert "typing.Callable" in result
            assert "test.module.TestClass" in result
            assert "..." in result

    def test_get_element_name_newtype(self):
        """Test NewType handling"""
        from typing import NewType

        UserId = NewType("UserId", int)

        result = self.generator._get_element_name_with_module(UserId)
        assert result == "UserId"
        assert "UserId" in self.generator._typevars

    def test_get_element_name_classvar(self):
        """Test ClassVar types"""
        classvar_type = typing.ClassVar[TestClass]

        mock_module = Mock()
        mock_module.__name__ = "test.module"

        with patch("inspect.getmodule", return_value=mock_module):
            result = self.generator._get_element_name_with_module(classvar_type)

            assert "<class '" not in result
            assert "typing.ClassVar" in result
            assert "test.module.TestClass" in result

    def test_get_element_name_final(self):
        """Test Final types"""
        final_type = typing.Final[TestClass]

        mock_module = Mock()
        mock_module.__name__ = "test.module"

        with patch("inspect.getmodule", return_value=mock_module):
            result = self.generator._get_element_name_with_module(final_type)

            assert "<class '" not in result
            assert "typing.Final" in result
            assert "test.module.TestClass" in result

    def test_get_element_name_literal(self):
        """Test Literal types"""
        try:
            from typing import Literal
        except ImportError:
            from typing_extensions import Literal

        literal_type = Literal["test", "value"]
        result = self.generator._get_element_name_with_module(literal_type)

        assert "<class '" not in result
        # Literal behavior may vary by Python version, just ensure no class objects leak

    def test_get_element_name_deeply_nested(self):
        """Test very deeply nested types"""
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

        mock_module = Mock()
        mock_module.__name__ = "test.module"

        with patch("inspect.getmodule", return_value=mock_module):
            result = self.generator._get_element_name_with_module(nested_type)

            assert "<class '" not in result
            assert "typing.Dict" in result
            assert "typing.List" in result
            # Optional[Union[...]] gets flattened to Union[..., NoneType]
            assert "typing.Union" in result
            assert "typing.Callable" in result
            assert "test.module.TestClass" in result

    def test_get_element_name_error_handling_none_module(self):
        """Test error handling when inspect.getmodule returns None"""
        with patch("inspect.getmodule", return_value=None):
            result = self.generator._get_element_name_with_module(TestClass)
            # Should fall back to just the class name
            assert result == "TestClass"

    def test_get_element_name_error_handling_eval_failure(self):
        """Test error handling when eval fails on string annotations"""
        # Test with a string that can't be evaluated
        result = self.generator._get_element_name_with_module(
            "NonExistentClass.InvalidSyntax["
        )
        assert result == '"NonExistentClass.InvalidSyntax["'

    def test_get_element_name_generic_origin_without_name(self):
        """Test generic types that don't have _name attribute"""
        # Simplified test - just test that we can handle a generic type without _name
        # This is an edge case that's hard to mock properly, so we'll test it indirectly
        list_type = typing.List[TestClass]

        mock_module = Mock()
        mock_module.__name__ = "test.module"

        with patch("inspect.getmodule", return_value=mock_module):
            result = self.generator._get_element_name_with_module(list_type)

            # Verify it works and doesn't contain class objects
            assert "<class '" not in result
            assert "typing.List" in result
            assert "test.module.TestClass" in result

    def test_get_element_name_builtin_collections(self):
        """Test builtin collection types"""
        list_type = typing.List[TestClass]
        dict_type = typing.Dict[str, TestClass]
        set_type = typing.Set[TestClass]

        mock_module = Mock()
        mock_module.__name__ = "test.module"

        with patch("inspect.getmodule", return_value=mock_module):
            for type_obj, expected_name in [
                (list_type, "typing.List"),
                (dict_type, "typing.Dict"),
                (set_type, "typing.Set"),
            ]:
                result = self.generator._get_element_name_with_module(type_obj)
                assert "<class '" not in result
                assert expected_name in result
                assert "test.module.TestClass" in result

    def test_get_element_name_type_with_special_chars(self):
        """Test handling of types with special characters in names"""
        # Create a class with special characters
        special_class = type("Class_With_Special-Chars", (), {})

        mock_module = Mock()
        mock_module.__name__ = "test.module"

        with patch("inspect.getmodule", return_value=mock_module):
            result = self.generator._get_element_name_with_module(special_class)
            assert result == "test.module.Class_With_Special-Chars"
            assert "<class '" not in result

    def test_get_element_name_nested_classes(self):
        """Test nested/inner classes"""
        # Simulate a nested class
        nested_class = type("OuterClass.InnerClass", (), {})
        nested_class.__name__ = "InnerClass"
        nested_class.__qualname__ = "OuterClass.InnerClass"

        mock_module = Mock()
        mock_module.__name__ = "test.module"

        with patch("inspect.getmodule", return_value=mock_module):
            result = self.generator._get_element_name_with_module(nested_class)
            assert result == "test.module.InnerClass"
            assert "<class '" not in result

    def test_generator_state_isolation(self):
        """Test that generator state is properly isolated between calls"""
        # Test that imports and typing_imports are properly managed
        initial_typing_imports = len(self.generator._typing_imports)
        
        mock_module = Mock()
        mock_module.__name__ = "test.isolated.module"
        
        with patch("inspect.getmodule", return_value=mock_module):
            self.generator._get_element_name_with_module(TestClass)
            
            # Should have added to typing imports
            assert len(self.generator._typing_imports) > initial_typing_imports
            assert "test.isolated.module" in self.generator._typing_imports

    def test_get_element_name_nonetype_handling(self):
        """Test that NoneType is properly converted to None in type annotations"""
        # Test direct NoneType
        result = self.generator._get_element_name_with_module(type(None))
        assert result == "None"
        
        # Test NoneType in generic type (like Callable[..., None])
        callable_type = typing.Callable[[TestClass], type(None)]
        
        mock_module = Mock()
        mock_module.__name__ = "test.module"
        
        with patch("inspect.getmodule", return_value=mock_module):
            result = self.generator._get_element_name_with_module(callable_type)
            
            # Should not contain NoneType, should contain None
            assert "NoneType" not in result
            assert "None" in result
            assert "typing.Callable" in result
            assert "test.module.TestClass" in result


# Integration test to verify class objects in generic types are properly handled
def test_class_objects_in_generic_types_no_leakage():
    """Regression test ensuring class objects don't leak as '<class '...'>' in type annotations"""
    generator = StubGenerator("/tmp/test_stubs", include_generated_for=False)
    generator._reset()
    generator._current_module_name = "test_module"

    # Create the exact problematic type from the original issue
    MetaflowDataFrame = type("MetaflowDataFrame", (), {})
    FunctionParameters = type("FunctionParameters", (), {})

    # Mock modules
    mock_df_module = Mock()
    mock_df_module.__name__ = "metaflow_extensions.nflx.plugins.datatools.dataframe"
    mock_fp_module = Mock()
    mock_fp_module.__name__ = (
        "metaflow_extensions.nflx.plugins.functions.core.function_parameters"
    )

    def mock_getmodule(obj):
        if obj == MetaflowDataFrame:
            return mock_df_module
        elif obj == FunctionParameters:
            return mock_fp_module
        return None

    # The problematic type annotation
    problematic_type = typing.Callable[
        [MetaflowDataFrame, typing.Optional[FunctionParameters]], MetaflowDataFrame
    ]

    with patch("inspect.getmodule", side_effect=mock_getmodule):
        result = generator._get_element_name_with_module(problematic_type)

        # The key assertion - no class objects should appear
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
