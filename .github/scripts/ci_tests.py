"""Select isolated CI test groups and validate their combined results."""

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import shutil
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[2]


def load_shards(root):
    """Require every Surefire test class to belong to exactly one group."""
    shards = json.loads((root / ".github/test-shards.json").read_text())
    if set(shards) != {"1", "2"} or not all(shards.values()):
        raise ValueError("The manifest must contain two nonempty groups: 1 and 2")
    assigned = Counter(name for names in shards.values() for name in names)
    source_root = root / "flexdblink/src/test/java"
    discovered = {
        ".".join(path.relative_to(source_root).with_suffix("").parts)
        for path in source_root.rglob("*.java")
        if path.name.endswith(("Test.java", "IT.java"))
    }
    missing = sorted(discovered - assigned.keys())
    extra = sorted(assigned.keys() - discovered)
    duplicates = sorted(name for name, count in assigned.items() if count != 1)
    if missing or extra or duplicates:
        raise ValueError(
            f"Invalid test groups: missing={missing}, extra={extra}, duplicates={duplicates}"
        )
    return shards


def verify_reports(expected, reports):
    """Require one successful, nonempty report for each selected test class."""
    actual = []
    total = 0
    for path in sorted(reports.glob("TEST-*.xml")):
        suite = ET.parse(path).getroot()
        actual.append(suite.attrib["name"])
        cases = suite.findall("testcase")
        if not cases or len(cases) != int(suite.attrib["tests"]):
            raise ValueError(f"Missing test cases in {path}")
        if any(int(suite.attrib[key]) for key in ("failures", "errors", "skipped")):
            raise ValueError(f"Failed or skipped tests in {path}")
        if any(case.find(tag) is not None for case in cases
               for tag in ("failure", "error", "skipped")):
            raise ValueError(f"Failed or skipped test case in {path}")
        total += len(cases)
    if Counter(actual) != Counter(expected):
        raise ValueError(
            f"Test reports do not match the selected classes: "
            f"missing={sorted(set(expected) - set(actual))}, "
            f"extra={sorted(set(actual) - set(expected))}, "
            f"duplicates={sorted(name for name, count in Counter(actual).items() if count > 1)}"
        )
    return total


def class_hashes(directory):
    """Identify the exact production bytecode used to collect coverage."""
    hashes = {
        str(path.relative_to(directory)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in directory.rglob("*.class")
    }
    if not hashes:
        raise ValueError(f"No production class files in {directory}")
    return hashes


def prepare_coverage(root, java):
    """Validate both artifacts and restore their original classes without compiling."""
    shards = load_shards(root)
    target = root / "flexdblink/target"
    artifacts = [target / "ci-shards" / f"core-tests-java-{java}-shard-{shard}"
                 for shard in shards]
    total = 0
    for shard, artifact in zip(shards, artifacts):
        total += verify_reports(shards[shard], artifact / "surefire-reports")
        if (artifact / "jacoco.exec").stat().st_size == 0:
            raise ValueError(f"Empty coverage execution data in {artifact}")
    if class_hashes(artifacts[0] / "classes") != class_hashes(artifacts[1] / "classes"):
        raise ValueError("Production bytecode differs between the two test groups")
    shutil.copytree(artifacts[0] / "classes", target / "classes", dirs_exist_ok=True)
    print(f"Java {java}: {sum(map(len, shards.values()))} classes, {total} tests; "
          "both groups passed and production bytecode matches")


def verify_coverage(path):
    """Require measured instruction and branch coverage to be exactly 100 percent."""
    counters = {counter.attrib["type"]: counter.attrib
                for counter in ET.parse(path).getroot().findall("counter")}
    for name in ("INSTRUCTION", "BRANCH"):
        covered = int(counters[name]["covered"])
        missed = int(counters[name]["missed"])
        if covered == 0 or missed != 0:
            raise ValueError(f"{name} coverage is below 100%: {covered}/{covered + missed}")
        print(f"{name}: 100% ({covered}/{covered + missed})")


def main():
    """Run the CI selection, report validation, or coverage validation command."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    select = commands.add_parser("select")
    select.add_argument("shard", choices=("1", "2"))
    verify = commands.add_parser("verify")
    verify.add_argument("shard", choices=("1", "2"))
    verify.add_argument("reports", type=Path)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("java", type=int)
    coverage = commands.add_parser("coverage")
    coverage.add_argument("report", type=Path)
    args = parser.parse_args()
    if args.command == "select":
        print(",".join(load_shards(ROOT)[args.shard]))
    elif args.command == "verify":
        expected = load_shards(ROOT)[args.shard]
        total = verify_reports(expected, args.reports)
        print(f"Group {args.shard}: {len(expected)} classes, {total} tests passed")
    elif args.command == "prepare":
        prepare_coverage(ROOT, args.java)
    elif args.command == "coverage":
        verify_coverage(args.report)


if __name__ == "__main__":
    main()
