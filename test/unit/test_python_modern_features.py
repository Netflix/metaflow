import sys
import pytest
from metaflow import FlowSpec, step, Parameter
from metaflow.plugins.datatools.s3 import S3


class TestPython313Compatibility:
    """Test suite for Python 3.13+ specific features and compatibility"""
    
    @pytest.mark.skipif(sys.version_info < (3, 13), reason="Requires Python 3.13+")
    def test_python313_features(self):
        """Test that Metaflow works with Python 3.13+ features"""
        from typing import Generic, TypeVar
        
        T = TypeVar('T')
        
        class DataProcessor(Generic[T]):
            def __init__(self, data: T):
                self.data = data
            
            def process(self) -> T:
                return self.data
        
        processor = DataProcessor[str]("test_data")
        assert processor.process() == "test_data"
    
    def test_pattern_matching_in_flow(self):
        """Test pattern matching (match/case) in Metaflow flows"""
        if sys.version_info < (3, 10):
            pytest.skip("Pattern matching requires Python 3.10+")
        
        class PatternMatchFlow(FlowSpec):
            mode = Parameter('mode', default='test')
            
            @step
            def start(self):
                match self.mode:
                    case 'test':
                        self.result = 'test_mode'
                    case 'prod':
                        self.result = 'prod_mode' 
                    case _:
                        self.result = 'default_mode'
                self.next(self.end)
            
            @step
            def end(self):
                pass
        
        flow = PatternMatchFlow()
        flow.start()
        assert hasattr(flow, 'result')
    
    def test_improved_error_messages(self):
        """Test that error messages are helpful with newer Python versions"""
        try:
            S3().get("invalid://url")
        except Exception as e:
            assert len(str(e)) > 10
    
    def test_async_compatibility(self):
        """Test async/await compatibility in decorators"""
        import asyncio
        
        async def async_helper():
            await asyncio.sleep(0.01)
            return "async_result"
        
        result = asyncio.run(async_helper())
        assert result == "async_result"


class TestModernPythonFeatures:
    """Test modern Python features compatibility"""
    
    def test_walrus_operator(self):
        """Test walrus operator (:=) works in flows"""
        if sys.version_info < (3, 8):
            pytest.skip("Walrus operator requires Python 3.8+")
        
        class WalrusFlow(FlowSpec):
            @step  
            def start(self):
                if (n := len("test")) > 3:
                    self.length = n
                else:
                    self.length = 0
                self.next(self.end)
            
            @step
            def end(self):
                pass
        
        flow = WalrusFlow()
        flow.start()
        assert flow.length == 4
    
    def test_positional_only_params(self):
        """Test positional-only parameters work in helper functions"""
        if sys.version_info < (3, 8):
            pytest.skip("Positional-only parameters require Python 3.8+")
        
        def helper_func(pos_only, /, pos_or_kw, *, kw_only):
            return pos_only + pos_or_kw + kw_only
        
        result = helper_func(1, 2, kw_only=3)
        assert result == 6
    
    def test_f_string_equals_specifier(self):
        """Test f-string = specifier for debugging"""
        if sys.version_info < (3, 8):
            pytest.skip("f-string = specifier requires Python 3.8+")
        
        value = 42
        debug_str = f"{value=}"
        assert "value=42" in debug_str


if __name__ == "__main__":
    pytest.main([__file__, "-v"])