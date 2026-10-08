"""Secret setup should be repeatable without invalidating persisted Auth data."""
from pathlib import Path
import tempfile
import unittest
from setup_auth_env import ROOT, SECRET_FACTORIES, configure


class SetupAuthEnvTests(unittest.TestCase):
    def test_generation_and_rerun_preserve_secrets(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".env.auth.example").write_text((ROOT / ".env.auth.example").read_text(), encoding="utf-8")
            path = configure(root)
            first = path.read_text()
            self.assertNotIn("=REPLACE_", first)
            self.assertEqual(first, configure(root).read_text())
            values = dict(line.split("=", 1) for line in first.splitlines() if line and not line.startswith("#"))
            self.assertEqual(len(values["ZITADEL_MASTERKEY"]), 32)
            self.assertEqual(len(values["AUTH_SESSION_KEY"]), 64)
            self.assertEqual(len({values[name] for name in SECRET_FACTORIES}), len(SECRET_FACTORIES))

    def test_only_missing_or_placeholder_values_are_filled(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / ".env.auth"
            path.write_text("ZITADEL_MASTERKEY=" + "a" * 32 + "\nAUTH_SESSION_KEY=" + "b" * 64 + "\nAUTH_MEDIA_PASSWORD=KeepMe1!\nAUTH_BNPB_PASSWORD=REPLACE_THIS\n", encoding="utf-8")
            configure(root)
            self.assertIn("AUTH_MEDIA_PASSWORD=KeepMe1!", path.read_text())
            self.assertNotIn("REPLACE_THIS", path.read_text())

    def test_invalid_existing_key_is_not_replaced(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / ".env.auth"
            path.write_text("ZITADEL_MASTERKEY=too-short\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                configure(root)
            self.assertEqual(path.read_text(), "ZITADEL_MASTERKEY=too-short\n")


if __name__ == "__main__":
    unittest.main()
