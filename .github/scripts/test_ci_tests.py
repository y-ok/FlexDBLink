"""Exercise CI failures that could otherwise hide missing tests or coverage."""

import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest

import ci_tests


class CiTestsTest(unittest.TestCase):
    def setUp(self):
        temporary_root = ci_tests.ROOT / "target/ci-script-tests"
        temporary_root.mkdir(parents=True, exist_ok=True)
        self.directory = tempfile.TemporaryDirectory(dir=temporary_root)
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / ".github").mkdir()
        self.shards = {"1": ["example.FirstTest"], "2": ["example.SecondIT"]}
        self.write_manifest()
        sources = self.root / "flexdblink/src/test/java/example"
        sources.mkdir(parents=True)
        (sources / "FirstTest.java").touch()
        (sources / "SecondIT.java").touch()

    def write_manifest(self):
        (self.root / ".github/test-shards.json").write_text(json.dumps(self.shards))

    def write_report(self, folder, name, status=""):
        folder.mkdir(parents=True, exist_ok=True)
        skipped = int(status == "skipped")
        failures = int(status == "failure")
        errors = int(status == "error")
        detail = ""
        if status:
            detail = f"<{status}/>"
        (folder / f"TEST-{name}.xml").write_text(
            f'<testsuite name="{name}" tests="1" skipped="{skipped}" '
            f'failures="{failures}" errors="{errors}">'
            f'<testcase name="example">{detail}</testcase></testsuite>'
        )

    def create_artifacts(self):
        target = self.root / "flexdblink/target"
        artifacts = []
        for shard, names in self.shards.items():
            artifact = target / "ci-shards" / f"core-tests-java-21-shard-{shard}"
            self.write_report(artifact / "surefire-reports", names[0])
            (artifact / "jacoco.exec").write_bytes(b"execution data")
            (artifact / "classes").mkdir()
            (artifact / "classes/Example.class").write_bytes(b"same bytecode")
            artifacts.append(artifact)
        return artifacts

    def test_manifest_covers_both_test_suffixes(self):
        self.assertEqual(self.shards, ci_tests.load_shards(self.root))

    def test_manifest_rejects_missing_duplicate_and_stale_classes(self):
        for replacement in ("example.UnknownTest", "example.FirstTest"):
            with self.subTest(replacement=replacement):
                self.shards["2"] = [replacement]
                self.write_manifest()
                with self.assertRaisesRegex(ValueError, "Invalid test groups"):
                    ci_tests.load_shards(self.root)

    def test_manifest_rejects_an_empty_group(self):
        self.shards["2"] = []
        self.write_manifest()
        with self.assertRaisesRegex(ValueError, "two nonempty groups"):
            ci_tests.load_shards(self.root)

    def test_reports_require_every_selected_class(self):
        reports = self.root / "reports"
        self.write_report(reports, "example.FirstTest")
        self.assertEqual(1, ci_tests.verify_reports(self.shards["1"], reports))
        with self.assertRaisesRegex(ValueError, "do not match"):
            ci_tests.verify_reports(self.shards["1"] + self.shards["2"], reports)
        self.write_report(reports, "example.UnexpectedTest")
        with self.assertRaisesRegex(ValueError, "do not match"):
            ci_tests.verify_reports(self.shards["1"], reports)

    def test_reports_reject_failures_errors_and_skips(self):
        reports = self.root / "reports"
        for status in ("failure", "error", "skipped"):
            with self.subTest(status=status):
                self.write_report(reports, "example.FirstTest", status)
                with self.assertRaisesRegex(ValueError, "Failed or skipped"):
                    ci_tests.verify_reports(self.shards["1"], reports)

    def test_reports_reject_empty_or_duplicate_suites(self):
        reports = self.root / "reports"
        self.write_report(reports, "example.FirstTest")
        report = reports / "TEST-example.FirstTest.xml"
        duplicate = reports / "TEST-duplicate.xml"
        duplicate.write_bytes(report.read_bytes())
        with self.assertRaisesRegex(ValueError, "duplicates"):
            ci_tests.verify_reports(self.shards["1"], reports)
        report.write_text('<testsuite name="example.FirstTest" tests="0"/>')
        with self.assertRaisesRegex(ValueError, "Missing test cases"):
            ci_tests.verify_reports(self.shards["1"], reports)

    def test_prepare_restores_verified_original_bytecode(self):
        self.create_artifacts()
        with contextlib.redirect_stdout(io.StringIO()):
            ci_tests.prepare_coverage(self.root, 21)
        self.assertEqual(b"same bytecode",
                         (self.root / "flexdblink/target/classes/Example.class").read_bytes())

    def test_prepare_rejects_different_bytecode(self):
        artifacts = self.create_artifacts()
        (artifacts[1] / "classes/Example.class").write_bytes(b"different bytecode")
        with self.assertRaisesRegex(ValueError, "bytecode differs"):
            ci_tests.prepare_coverage(self.root, 21)

    def test_prepare_requires_both_execution_data_files(self):
        artifacts = self.create_artifacts()
        (artifacts[1] / "jacoco.exec").rename(artifacts[1] / "missing.exec")
        with self.assertRaises(FileNotFoundError):
            ci_tests.prepare_coverage(self.root, 21)
        (artifacts[1] / "jacoco.exec").touch()
        with self.assertRaisesRegex(ValueError, "Empty coverage"):
            ci_tests.prepare_coverage(self.root, 21)

    def test_coverage_requires_full_instruction_and_branch_coverage(self):
        report = self.root / "jacoco.xml"
        for instruction_misses, branch_misses in ((0, 0), (1, 0), (0, 1)):
            with self.subTest(instruction=instruction_misses, branch=branch_misses):
                report.write_text(
                    '<report><counter type="INSTRUCTION" covered="10" '
                    f'missed="{instruction_misses}"/>'
                    f'<counter type="BRANCH" covered="2" missed="{branch_misses}"/></report>'
                )
                if instruction_misses or branch_misses:
                    with self.assertRaisesRegex(ValueError, "below 100%"):
                        with contextlib.redirect_stdout(io.StringIO()):
                            ci_tests.verify_coverage(report)
                else:
                    with contextlib.redirect_stdout(io.StringIO()) as output:
                        ci_tests.verify_coverage(report)
                    self.assertIn("INSTRUCTION: 100% (10/10)", output.getvalue())
                    self.assertIn("BRANCH: 100% (2/2)", output.getvalue())


if __name__ == "__main__":
    unittest.main()
