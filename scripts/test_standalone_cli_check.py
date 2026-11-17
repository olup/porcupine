"""Keep artifact reuse explicit while the CLI journey uses an isolated server."""
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from standalone_cli_check import local_server


class ExistingServerTests(unittest.TestCase):
    def test_existing_binary_skips_build_and_keeps_credentials_isolated(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            binary = directory / "published server"
            binary.touch()
            with mock.patch("standalone_cli_check.subprocess.run") as build, \
                 mock.patch("standalone_cli_check.subprocess.Popen") as launch, \
                 mock.patch("standalone_cli_check.urllib.request.urlopen"):
                process = launch.return_value.__enter__.return_value
                process.poll.return_value = None
                with mock.patch.dict("os.environ", {"PORCUPINE_API_KEY": "do-not-inherit"}):
                    with local_server(directory, "test-bootstrap", binary) as origin:
                        self.assertTrue(origin.startswith("http://127.0.0.1:"))
                build.assert_not_called()
                self.assertEqual(launch.call_args.args[0], [binary.resolve()])
                env = launch.call_args.kwargs["env"]
                self.assertNotIn("PORCUPINE_API_KEY", env)
                self.assertEqual(env["PORCUPINE_DB"], str(directory / "test.db"))
                process.terminate.assert_called_once()
                process.wait.assert_called_once()

    def test_missing_binary_fails_instead_of_building_another(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            with mock.patch("standalone_cli_check.subprocess.run") as build:
                with self.assertRaises(FileNotFoundError):
                    with local_server(directory, "test-bootstrap", directory / "missing"):
                        self.fail("missing artifact was accepted")
                build.assert_not_called()


if __name__ == "__main__":
    unittest.main()
