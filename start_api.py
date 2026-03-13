"""
Start the Macro Strategy Engine API.
Uses --reload-dir to watch only source code, ignoring outputs/ and data/
which would otherwise trigger constant server restarts during analysis runs.
"""
import subprocess
import sys

subprocess.run([
    sys.executable, "-m", "uvicorn", "api.main:app",
    "--reload",
    "--reload-dir", "api",
    "--reload-dir", "core",
    "--reload-dir", "connectors",
    "--reload-dir", "ai",
    "--reload-dir", "config",
    "--host", "0.0.0.0",
    "--port", "8000",
])
