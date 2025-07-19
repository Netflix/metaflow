import os
import sys
import subprocess
import argparse
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
VERBOSE = False


def log(message):
    """Log message if verbose mode is enabled"""
    if VERBOSE:
        print(f"[{time.strftime('%H:%M:%S')}] {message}")


def run_command(cmd, cwd=None, timeout=300):
    """Run a command and return success status"""
    log(f"Running: {' '.join(cmd)}")
    try:
        result = subprocess.run(
            cmd,
            cwd=cwd or PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return False, "", f"Command timed out after {timeout} seconds"
    except Exception as e:
        return False, "", str(e)

def test_basic_imports():
    """Test that basic imports work"""
    log("Testing basic imports...")
    
    import_tests = [
        "import metaflow",
        "from metaflow import FlowSpec, step",
        "from metaflow.plugins.datatools.s3 import S3",
        "from metaflow import current, Parameter",
    ]
    
    for test in import_tests:
        success, stdout, stderr = run_command([
            sys.executable, "-c", test
        ])
        
        if not success:
            log(f"Import test failed: {test}")
            log(f"Error: {stderr}")
            return False
    
    log("✓ All import tests passed")
    return True


def test_python_compatibility():
    """Test Python version compatibility"""
    log(f"Testing Python {sys.version_info.major}.{sys.version_info.minor} compatibility...")
    
    test_code = '''
import sys
from metaflow import FlowSpec, step

class TestFlow(FlowSpec):
    @step
    def start(self):
        version = f"Python {sys.version_info.major}.{sys.version_info.minor}"
        print(f"Running on {version}")
        self.next(self.end)
    
    @step
    def end(self):
        print("Flow completed successfully")

if __name__ == '__main__':
    TestFlow()
'''
    
    success, stdout, stderr = run_command([
        sys.executable, "-c", test_code
    ])
    
    if success:
        log("✓ Python compatibility test passed")
        return True
    else:
        log(f"Python compatibility test failed: {stderr}")
        return False


def test_s3_functionality():
    """Test S3 functionality with mock environment"""
    log("Testing S3 functionality...")
    
    test_code = '''
import os
from metaflow.plugins.datatools.s3 import S3

# Set mock environment
os.environ.update({
    "AWS_ACCESS_KEY_ID": "test_key",
    "AWS_SECRET_ACCESS_KEY": "test_secret", 
    "AWS_DEFAULT_REGION": "us-east-1",
    "AWS_ENDPOINT_URL_S3": "http://localhost:9000"
})

try:
    s3 = S3()
    try:
        s3.list("s3://test-bucket/")
    except Exception as e:
        print(f"Expected S3 error: {type(e).__name__}")
    
    print("S3 client initialization successful")
except Exception as e:
    print(f"S3 test failed: {e}")
    raise
'''
    
    success, stdout, stderr = run_command([
        sys.executable, "-c", test_code
    ])
    
    if success:
        log("✓ S3 functionality test passed")
        return True
    else:
        log(f"S3 functionality test failed: {stderr}")
        return False


def run_unit_tests():
    """Run unit tests"""
    log("Running unit tests...")
    
    test_dirs = [
        PROJECT_ROOT / "test" / "unit",
        PROJECT_ROOT / "test" / "data" / "s3",
    ]
    
    for test_dir in test_dirs:
        if test_dir.exists():
            success, stdout, stderr = run_command([
                sys.executable, "-m", "pytest", 
                str(test_dir), "-v", "--tb=short"
            ], timeout=600)
            
            if not success:
                log(f"Unit tests failed in {test_dir}")
                log(f"Error: {stderr}")
                return False
    
    log("✓ Unit tests passed")
    return True


def run_performance_tests():
    """Run performance benchmarks"""
    log("Running performance tests...")
    
    benchmark_dir = PROJECT_ROOT / "test" / "data"
    if not benchmark_dir.exists():
        log("⚠ No benchmark tests found")
        return True
    
    success, stdout, stderr = run_command([
        sys.executable, "-m", "pytest",
        str(benchmark_dir), "--benchmark-only", "-v"
    ], timeout=900)
    
    if success:
        log("✓ Performance tests passed")
        return True
    else:
        log(f"Performance tests failed: {stderr}")
        return False


def run_code_quality_checks():
    """Run code quality checks"""
    log("Running code quality checks...")
    
    checks = [
        (["python", "-m", "black", "--check", "metaflow/"], "Black formatting"),
        (["python", "-m", "isort", "--check-only", "metaflow/"], "Import sorting"),
        (["python", "-m", "flake8", "metaflow/", "--max-line-length=88"], "Flake8 linting"),
    ]
    
    for cmd, description in checks:
        try:
            success, stdout, stderr = run_command(cmd)
            if success:
                log(f"✓ {description} passed")
            else:
                log(f"⚠ {description} failed (non-blocking)")
        except Exception:
            log(f"⚠ {description} check not available")
    
    return True


def run_all_tests(include_performance=False, include_quality=False):
    """Run all tests"""
    start_time = time.time()
    
    tests = [
        ("Basic Imports", test_basic_imports),
        ("Python Compatibility", test_python_compatibility),
        ("S3 Functionality", test_s3_functionality),
        ("Unit Tests", run_unit_tests),
    ]
    
    if include_performance:
        tests.append(("Performance Tests", run_performance_tests))
    
    if include_quality:
        tests.append(("Code Quality", run_code_quality_checks))
    
    results = {}
    
    for test_name, test_func in tests:
        print(f"\n{'='*50}")
        print(f"Running {test_name}")
        print('='*50)
        
        test_start = time.time()
        try:
            success = test_func()
            test_time = time.time() - test_start
            results[test_name] = {
                'success': success,
                'time': test_time
            }
            
            status = "✓ PASSED" if success else "✗ FAILED"
            print(f"{test_name}: {status} ({test_time:.2f}s)")
            
        except Exception as e:
            test_time = time.time() - test_start
            results[test_name] = {
                'success': False,
                'time': test_time,
                'error': str(e)
            }
            print(f"{test_name}: ✗ ERROR - {e}")
    
    # Print summary
    total_time = time.time() - start_time
    print(f"\n{'='*50}")
    print("TEST SUMMARY")
    print('='*50)
    
    passed = sum(1 for r in results.values() if r['success'])
    total = len(results)
    
    for test_name, result in results.items():
        status = "✓" if result['success'] else "✗"
        time_str = f"{result['time']:.2f}s"
        print(f"{status} {test_name:<30} {time_str}")
    
    print(f"\nResults: {passed}/{total} tests passed")
    print(f"Total time: {total_time:.2f}s")
    
    return passed == total


def main():
    global VERBOSE
    
    parser = argparse.ArgumentParser(description="Simple Metaflow test runner")
    parser.add_argument("-v", "--verbose", action="store_true", 
                       help="Enable verbose output")
    parser.add_argument("-p", "--performance", action="store_true",
                       help="Include performance tests")
    parser.add_argument("-q", "--quality", action="store_true",
                       help="Include code quality checks")
    parser.add_argument("--quick", action="store_true",
                       help="Run only quick tests")
    
    args = parser.parse_args()
    VERBOSE = args.verbose
    
    if args.quick:
        # Run only basic tests for quick feedback
        success = test_basic_imports() and test_python_compatibility()
    else:
        success = run_all_tests(
            include_performance=args.performance,
            include_quality=args.quality
        )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
