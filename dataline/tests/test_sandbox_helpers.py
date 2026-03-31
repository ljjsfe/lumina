"""Test that sandbox installs helpers correctly."""

import os
import tempfile

from dataline.core.sandbox import Sandbox


class TestSandboxHelperInstall:
    def test_helpers_copied_to_temp_dir(self):
        with tempfile.TemporaryDirectory() as td:
            sandbox = Sandbox(task_dir=td, timeout=10)
            try:
                helpers_path = os.path.join(sandbox.temp_dir, "data_helpers.py")
                assert os.path.exists(helpers_path), "data_helpers.py should be in TEMP_DIR"

                with open(helpers_path) as f:
                    content = f.read()
                assert "safe_read_csv" in content
                assert "describe_df" in content
                assert "find_join_keys" in content
            finally:
                sandbox.cleanup()

    def test_helpers_importable_in_sandbox(self):
        with tempfile.TemporaryDirectory() as td:
            # Create a test CSV
            import pandas as pd
            pd.DataFrame({"x": [1, 2, 3]}).to_csv(os.path.join(td, "test.csv"), index=False)

            sandbox = Sandbox(task_dir=td, timeout=10)
            try:
                result = sandbox.execute(
                    "from data_helpers import safe_read_csv, describe_df\n"
                    "df = safe_read_csv('test.csv')\n"
                    "print(describe_df(df, 'test'))\n"
                )
                assert result.return_code == 0, f"stderr: {result.stderr}"
                assert "3 rows" in result.stdout
                assert "x" in result.stdout
            finally:
                sandbox.cleanup()
