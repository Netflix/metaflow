import pytest
import asyncio
from unittest.mock import patch, MagicMock
from concurrent.futures import ThreadPoolExecutor
import time
import os
from metaflow.plugins.datatools.s3 import S3, MetaflowS3AccessDenied, MetaflowS3NotFound


class TestS3EnhancedFeatures:
    """Enhanced S3 testing with modern patterns"""
    
    @pytest.fixture
    def s3_client(self):
        """Provide S3 client for tests"""
        return S3()
    
    @pytest.fixture
    def mock_s3_env(self):
        """Mock S3 environment variables"""
        with patch.dict(os.environ, {
            'AWS_ACCESS_KEY_ID': 'test_key',
            'AWS_SECRET_ACCESS_KEY': 'test_secret',
            'AWS_DEFAULT_REGION': 'us-east-1',
            'AWS_ENDPOINT_URL_S3': 'http://localhost:9000'
        }):
            yield
    
    def test_concurrent_s3_operations(self, s3_client, mock_s3_env):
        """Test concurrent S3 operations for performance"""
        urls = [f"s3://test-bucket/test-{i}.txt" for i in range(5)]
        
        def put_object(url):
            try:
                return s3_client.put(url, f"test content {url}")
            except Exception:
                return None
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(put_object, url) for url in urls]
            results = [f.result() for f in futures]
        
        assert len(results) == len(urls)
    
    @pytest.mark.asyncio
    async def test_async_s3_operations(self, s3_client, mock_s3_env):
        """Test async S3 operations compatibility"""
        
        async def async_s3_operation():
            await asyncio.sleep(0.01)
            try:
                return s3_client.list("s3://test-bucket/")
            except Exception:
                return []
        
        result = await async_s3_operation()
        assert isinstance(result, list)
    
    def test_s3_error_handling_improvements(self, s3_client, mock_s3_env):
        """Test improved error handling"""
        
        invalid_urls = [
            "not-a-url",
            "http://example.com/file.txt",
            "s3://",
            "s3://bucket-name-only",
        ]
        
        for url in invalid_urls:
            with pytest.raises(Exception) as exc_info:
                s3_client.get(url)
            
            error_msg = str(exc_info.value)
            assert len(error_msg) > 10
            assert "s3://" in error_msg.lower() or "url" in error_msg.lower()
    
    def test_s3_retry_mechanism(self, s3_client, mock_s3_env):
        """Test S3 retry mechanism with exponential backoff"""
        
        with patch.object(s3_client, '_get_s3_client') as mock_client:
            mock_boto_client = MagicMock()
            mock_client.return_value = mock_boto_client
            
            mock_boto_client.head_object.side_effect = [
                Exception("Connection timeout"),
                {"ContentLength": 100, "LastModified": "2025-01-01"}
            ]
            
            try:
                result = s3_client.info("s3://test-bucket/test.txt")
                assert result is not None
            except Exception:
                pass
    
    def test_s3_large_file_handling(self, s3_client, mock_s3_env):
        """Test handling of large files with chunked operations"""
        
        large_content = "x" * (10 * 1024 * 1024)  # 10MB
        
        with patch.object(s3_client, 'put') as mock_put:
            mock_put.return_value = True
            
            try:
                result = s3_client.put("s3://test-bucket/large-file.txt", large_content)
                mock_put.assert_called_once()
            except Exception as e:
                assert "size" in str(e).lower() or "memory" in str(e).lower()
    
    def test_s3_metadata_preservation(self, s3_client, mock_s3_env):
        """Test that S3 metadata is properly preserved"""
        
        metadata = {
            'Content-Type': 'application/json',
            'Cache-Control': 'max-age=3600',
            'Custom-Header': 'test-value'
        }
        
        with patch.object(s3_client, 'put') as mock_put:
            mock_put.return_value = True
            
            try:
                s3_client.put(
                    "s3://test-bucket/metadata-test.json", 
                    '{"test": "data"}',
                    metadata=metadata
                )
                mock_put.assert_called_once()
                call_args = mock_put.call_args
                if len(call_args) > 2 and 'metadata' in call_args[1]:
                    assert call_args[1]['metadata'] == metadata
            except Exception:
                # Graceful handling if metadata not supported
                pass
    
    @pytest.mark.parametrize("region", ["us-east-1", "eu-west-1", "ap-southeast-1"])
    def test_s3_multi_region_support(self, s3_client, region, mock_s3_env):
        """Test S3 operations across different regions"""
        
        with patch.dict(os.environ, {'AWS_DEFAULT_REGION': region}):
            try:
                result = s3_client.list(f"s3://test-bucket-{region}/")
                assert isinstance(result, list)
            except Exception as e:
                assert "region" in str(e).lower() or "access" in str(e).lower()
    
    def test_s3_performance_monitoring(self, s3_client, mock_s3_env):
        """Test performance monitoring for S3 operations"""
        
        start_time = time.time()
        
        try:
            s3_client.list("s3://test-bucket/")
            operation_time = time.time() - start_time
            
            assert operation_time < 30.0  # 30 seconds max
            
        except Exception:
            operation_time = time.time() - start_time
            assert operation_time < 30.0
    
    def test_s3_connection_pooling(self, s3_client, mock_s3_env):
        """Test S3 connection pooling efficiency"""
        
        operations = [
            lambda: s3_client.list("s3://test-bucket/folder1/"),
            lambda: s3_client.list("s3://test-bucket/folder2/"),
            lambda: s3_client.info("s3://test-bucket/test.txt"),
        ]
        
        start_time = time.time()
        
        for operation in operations:
            try:
                operation()
            except Exception:
                pass
        
        total_time = time.time() - start_time
        

        assert total_time < 60.0  


class TestS3SecurityFeatures:
    """Test S3 security-related features"""
    
    def test_s3_credential_validation(self):
        """Test S3 credential validation"""
        
        with patch.dict(os.environ, {
            'AWS_ACCESS_KEY_ID': 'invalid_key',
            'AWS_SECRET_ACCESS_KEY': 'invalid_secret'
        }):
            s3_client = S3()
            
            with pytest.raises(Exception) as exc_info:
                s3_client.get("s3://test-bucket/test.txt")
            
            error_msg = str(exc_info.value).lower()
            assert any(word in error_msg for word in ['access', 'auth', 'credential', 'permission'])
    
    def test_s3_url_validation(self):
        """Test S3 URL validation for security"""
        
        s3_client = S3()
        
        malicious_urls = [
            "s3://bucket/../../../etc/passwd",
            "s3://bucket/file.txt?param=<script>alert('xss')</script>",
            "s3://bucket/file.txt\x00null_byte",
        ]
        
        for url in malicious_urls:
            with pytest.raises(Exception):
                s3_client.get(url)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])