import builtins
import importlib.util
import sys
import tempfile
import types
import unittest
import warnings
from pathlib import Path
from unittest import mock


MODULE_PATH = Path(__file__).resolve().parents[1] / "ann_benchmark.py"


def _load_ann_benchmark():
    for name in ("faiss", "usearch", "numpy"):
        sys.modules.setdefault(name, types.ModuleType(name))
    spec = importlib.util.spec_from_file_location("ann_benchmark", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


ann_benchmark = _load_ann_benchmark()


class WriteOutputsTests(unittest.TestCase):
    def test_write_outputs_survives_missing_matplotlib(self) -> None:
        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name.startswith("matplotlib"):
                raise ImportError("cannot import name '_c_internal_utils'")
            return real_import(name, *args, **kwargs)

        results = {"metrics": {"usearch": {"search_sec": 0.1}}}

        with tempfile.TemporaryDirectory() as tmp:
            json_output = Path(tmp) / "results.json"
            png_output = Path(tmp) / "results.png"

            with mock.patch.object(builtins, "__import__", side_effect=fake_import):
                with warnings.catch_warnings(record=True) as caught:
                    warnings.simplefilter("always")
                    ann_benchmark.write_outputs(results, json_output, png_output)

            self.assertTrue(json_output.exists())
            self.assertFalse(png_output.exists())
            self.assertTrue(
                any("matplotlib" in str(w.message) for w in caught),
                "expected a warning about missing matplotlib",
            )


if __name__ == "__main__":
    unittest.main()
