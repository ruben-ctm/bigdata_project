import re
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class RepositoryContractTests(unittest.TestCase):
    def test_readme_reproduction_lists_six_steps(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        for step in ("01_ingest", "02_silver", "03_gold", "04_benchmark", "05_verify", "06_scale"):
            self.assertIn(step, readme)
        self.assertIn("21/11/2026", readme)
        self.assertIn("data/raw/*.dat", readme)
        self.assertIn("## Prérequis", readme)
        self.assertIn("## Chemins S3", readme)
        self.assertIn("SPARK_MAX_FACTOR=100", readme)

    def test_bash_scripts_have_valid_syntax(self):
        for script in ("src/run_all.sh", "src/onyxia_init.sh", "src/onyxia_run.sh"):
            subprocess.run(["bash", "-n", str(ROOT / script)], check=True)

    def test_no_undefined_control_or_remote_results_branch(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        onyxia_run = (ROOT / "src/onyxia_run.sh").read_text(encoding="utf-8")
        self.assertNotIn("control.txt", readme + onyxia_run)
        self.assertNotIn("onyxia-results", readme + onyxia_run)

    def test_k8s_directory_is_not_falsely_empty(self):
        self.assertTrue((ROOT / "k8s/README.md").is_file())

    def test_benchmark_has_warmup_and_five_default_runs(self):
        benchmark = (ROOT / "src/benchmark.py").read_text(encoding="utf-8")
        self.assertIn("BENCH_RUNS", benchmark)
        self.assertIn('"5"', benchmark)
        self.assertIn("warm-up", benchmark)
        self.assertIn("random.shuffle", benchmark)

    def test_manual_verification_and_hub_alternatives_are_documented(self):
        verify = (ROOT / "src/verify.py").read_text(encoding="utf-8")
        gold = (ROOT / "src/gold.py").read_text(encoding="utf-8")
        self.assertIn("ATL", verify)
        self.assertIn("gold : routes, destinations distinctes, sans codeshare", verify)
        self.assertIn("ATL (routes, destinations distinctes, sans codeshare)", verify)
        self.assertIn("routes_out_sans_codeshare", gold)
        self.assertIn("destinations_distinctes", gold)


if __name__ == "__main__":
    unittest.main()
