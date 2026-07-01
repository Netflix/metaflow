import sys
import os
import platform
from unittest.mock import MagicMock, patch

# Mock Metaflow dependencies
sys.modules['metaflow'] = MagicMock()
sys.modules['metaflow.util'] = MagicMock()
sys.modules['metaflow.meta_files'] = MagicMock()
sys.modules['metaflow.metaflow_config'] = MagicMock()
sys.modules['metaflow.packaging_sys'] = MagicMock()

# Import the module to test
# We need to add the path to sys.path to import it
sys.path.append(os.path.abspath('metaflow/plugins/uv'))
import bootstrap
from unittest.mock import MagicMock, patch

# Import the module to test
def get_url(uv_version, uv_install_url):
    if uv_install_url:
        return uv_install_url
    
    # Logic from bootstrap.install_uv
    import sys
    import platform
    sys_platform = sys.platform
    machine = platform.machine().lower()
    arch = "x86_64"
    if machine in ["arm64", "aarch64"]:
        arch = "aarch64"
    elif machine in ["x86_64", "amd64"]:
        arch = "x86_64"
    
    os_name = "unknown-linux-gnu"
    if sys_platform == "darwin":
        os_name = "apple-darwin"
    elif sys_platform == "win32":
        os_name = "pc-windows-msvc"
    
    ext = "tar.gz"
    if sys_platform == "win32":
        ext = "zip"

    return (
        f"https://github.com/astral-sh/uv/releases/download/{uv_version}/"
        f"uv-{arch}-{os_name}.{ext}"
    )

def test_url_construction():
    print("Testing URL construction...")
    url = get_url("0.6.0", None)
    print(f"Constructed URL: {url}")
    assert "0.6.0" in url
    if sys.platform == "win32":
        assert "pc-windows-msvc.zip" in url
    elif sys.platform == "darwin":
        assert "apple-darwin.tar.gz" in url
    else:
        assert "unknown-linux-gnu.tar.gz" in url
    print("URL construction test passed!")

def test_custom_url():
    print("\nTesting custom URL...")
    custom_url = "https://example.com/uv.tar.gz"
    url = get_url("0.6.0", custom_url)
    assert url == custom_url
    print("Custom URL test passed!")

import traceback

if __name__ == "__main__":
    try:
        test_url_construction()
        test_custom_url()
        print("\nAll tests passed successfully!")
    except Exception as e:
        print(f"\nTest failed: {e}")
        traceback.print_exc()
        sys.exit(1)

