import builtins
import importlib.util
import json
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
            with json_output.open(encoding="utf-8") as f:
                self.assertEqual(json.load(f), results)
            self.assertTrue(
                any("Skipping plot generation" in str(w.message) for w in caught),
                "expected a warning about skipped plot generation",
            )

    def test_write_outputs_survives_non_import_error_during_plotting(self) -> None:
        stub_matplotlib = types.ModuleType("matplotlib")
        stub_pyplot = types.ModuleType("matplotlib.pyplot")
        stub_pyplot.bar = mock.Mock()
        stub_pyplot.title = mock.Mock()
        stub_pyplot.ylabel = mock.Mock()
        stub_pyplot.savefig = mock.Mock(side_effect=OSError("cannot write font cache"))
        stub_pyplot.close = mock.Mock()
        stub_matplotlib.pyplot = stub_pyplot

        results = {"metrics": {"usearch": {"search_sec": 0.1}}}

        with tempfile.TemporaryDirectory() as tmp:
            json_output = Path(tmp) / "results.json"
            png_output = Path(tmp) / "results.png"

            with mock.patch.dict(
                sys.modules,
                {"matplotlib": stub_matplotlib, "matplotlib.pyplot": stub_pyplot},
            ):
                with warnings.catch_warnings(record=True) as caught:
                    warnings.simplefilter("always")
                    ann_benchmark.write_outputs(results, json_output, png_output)

            self.assertTrue(json_output.exists())
            self.assertFalse(png_output.exists())
            with json_output.open(encoding="utf-8") as f:
                self.assertEqual(json.load(f), results)
            self.assertTrue(
                any("Skipping plot generation" in str(w.message) for w in caught),
                "expected a warning about skipped plot generation",
            )

    def test_write_outputs_saves_plot_when_matplotlib_available(self) -> None:
        stub_matplotlib = types.ModuleType("matplotlib")
        stub_pyplot = types.ModuleType("matplotlib.pyplot")
        stub_pyplot.bar = mock.Mock()
        stub_pyplot.title = mock.Mock()
        stub_pyplot.ylabel = mock.Mock()
        stub_pyplot.savefig = mock.Mock()
        stub_pyplot.close = mock.Mock()
        stub_matplotlib.pyplot = stub_pyplot

        results = {"metrics": {"usearch": {"search_sec": 0.1}}}

        with tempfile.TemporaryDirectory() as tmp:
            json_output = Path(tmp) / "results.json"
            png_output = Path(tmp) / "results.png"

            with mock.patch.dict(
                sys.modules,
                {"matplotlib": stub_matplotlib, "matplotlib.pyplot": stub_pyplot},
            ):
                ann_benchmark.write_outputs(results, json_output, png_output)

            self.assertTrue(json_output.exists())
            with json_output.open(encoding="utf-8") as f:
                self.assertEqual(json.load(f), results)
            stub_pyplot.savefig.assert_called_once_with(png_output)
            stub_pyplot.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
