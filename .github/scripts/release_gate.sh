#!/usr/bin/env bash

set -euo pipefail

EXIT_VERSION_MISMATCH=11
EXIT_CI_NOT_GREEN=12
EXIT_TEST_FAILED=13
EXIT_COVERAGE_FAIL=14
EXIT_UNUSED_WARNINGS=15
EXIT_TAG_REQUIRED=16
EXIT_TAG_NOT_FOUND=17

TAG_NAME="${TAG_NAME:-}"
TARGET_SHA="${TARGET_SHA:-}"
REPO_NAME="${GITHUB_REPOSITORY:-}"
API_URL="${GITHUB_API_URL:-https://api.github.com}"
WORKFLOW_FILE="${CI_WORKFLOW_FILE:-ci.yml}"
WORKFLOW_RUN_ID=""
WORKFLOW_STATUS=""
WORKFLOW_CONCLUSION=""
POM_VERSION=""
INST_COVERED=0
INST_TOTAL=0
INST_PCT=0
BR_COVERED=0
BR_TOTAL=0
BR_PCT=0
UNUSED_COUNT=0
LOG_FILE="${RUNNER_TEMP:-/tmp}/mvn-clean-test.log"

print_error() {
  echo "::error::$1"
}

append_summary() {
  if [ -z "${GITHUB_STEP_SUMMARY:-}" ]; then
    return
  fi
  {
    echo "## Release Gate Result"
    echo ""
    echo "| Item | Value |"
    echo "| --- | --- |"
    echo "| tag | ${TAG_NAME} |"
    echo "| pom version | ${POM_VERSION} |"
    echo "| target sha | ${TARGET_SHA} |"
    echo "| ci workflow | ${WORKFLOW_FILE} |"
    echo "| ci run id | ${WORKFLOW_RUN_ID} |"
    echo "| ci status | ${WORKFLOW_STATUS} |"
    echo "| ci conclusion | ${WORKFLOW_CONCLUSION} |"
    echo "| instruction coverage | ${INST_COVERED}/${INST_TOTAL} (${INST_PCT}%) |"
    echo "| branch coverage | ${BR_COVERED}/${BR_TOTAL} (${BR_PCT}%) |"
    echo "| unused field warnings | ${UNUSED_COUNT} |"
  } >> "${GITHUB_STEP_SUMMARY}"
}

ensure_tag() {
  if [ -n "${TAG_NAME}" ]; then
    return
  fi

  if [ -n "${GITHUB_REF_NAME:-}" ]; then
    TAG_NAME="${GITHUB_REF_NAME}"
  fi

  if [ -z "${TAG_NAME}" ]; then
    print_error "GATE_TAG_REQUIRED: tag is required."
    exit "${EXIT_TAG_REQUIRED}"
  fi
}

resolve_sha() {
  if [ -n "${TARGET_SHA}" ]; then
    return
  fi

  TARGET_SHA="$(git rev-parse HEAD)"
}

check_pom_version() {
  POM_VERSION="$(
    python - <<'PY'
import xml.etree.ElementTree as ET
root = ET.parse("pom.xml").getroot()
ns = {"m": "http://maven.apache.org/POM/4.0.0"}
version = root.find("m:version", ns)
print("" if version is None else version.text.strip())
PY
  )"

  if [ -z "${POM_VERSION}" ]; then
    print_error "GATE_VERSION_MISMATCH: failed to read pom.xml version."
    exit "${EXIT_VERSION_MISMATCH}"
  fi

  if [ "${POM_VERSION}" != "${TAG_NAME}" ]; then
    print_error "GATE_VERSION_MISMATCH: pom.xml version '${POM_VERSION}' does not match tag '${TAG_NAME}'."
    exit "${EXIT_VERSION_MISMATCH}"
  fi
}

check_ci_green() {
  if [ -z "${GH_TOKEN:-}" ] && [ -z "${GITHUB_TOKEN:-}" ]; then
    print_error "GATE_CI_NOT_GREEN: GH_TOKEN or GITHUB_TOKEN is required."
    exit "${EXIT_CI_NOT_GREEN}"
  fi

  local token
  token="${GH_TOKEN:-${GITHUB_TOKEN:-}}"

  local url
  url="${API_URL}/repos/${REPO_NAME}/actions/workflows/${WORKFLOW_FILE}/runs?head_sha=${TARGET_SHA}&per_page=20"

  local response
  response="$(
    curl -fsSL \
      -H "Accept: application/vnd.github+json" \
      -H "Authorization: Bearer ${token}" \
      "${url}"
  )"

  WORKFLOW_RUN_ID="$(
    echo "${response}" | jq -r '.workflow_runs[0].id // empty'
  )"
  WORKFLOW_STATUS="$(
    echo "${response}" | jq -r '.workflow_runs[0].status // empty'
  )"
  WORKFLOW_CONCLUSION="$(
    echo "${response}" | jq -r '.workflow_runs[0].conclusion // empty'
  )"

  if [ -z "${WORKFLOW_RUN_ID}" ]; then
    print_error "GATE_CI_NOT_GREEN: no ${WORKFLOW_FILE} run found for sha ${TARGET_SHA}."
    exit "${EXIT_CI_NOT_GREEN}"
  fi

  if [ "${WORKFLOW_STATUS}" != "completed" ] || [ "${WORKFLOW_CONCLUSION}" != "success" ]; then
    print_error "GATE_CI_NOT_GREEN: ${WORKFLOW_FILE} run ${WORKFLOW_RUN_ID} is ${WORKFLOW_STATUS}/${WORKFLOW_CONCLUSION}."
    exit "${EXIT_CI_NOT_GREEN}"
  fi
}

run_tests() {
  rm -f "${LOG_FILE}"
  mvn -B clean test | tee "${LOG_FILE}"
}

check_coverage() {
  if [ ! -f target/site/jacoco/jacoco.xml ]; then
    print_error "GATE_COVERAGE_FAIL: jacoco.xml not found."
    exit "${EXIT_COVERAGE_FAIL}"
  fi

  local coverage_line
  coverage_line="$(
    python - <<'PY'
import xml.etree.ElementTree as ET
root = ET.parse("target/site/jacoco/jacoco.xml").getroot()
data = {}
for counter in root.findall("counter"):
    ctype = counter.attrib.get("type")
    if ctype in ("INSTRUCTION", "BRANCH"):
        missed = int(counter.attrib.get("missed", "0"))
        covered = int(counter.attrib.get("covered", "0"))
        total = missed + covered
        pct = 100.0 if total == 0 else (covered * 100.0 / total)
        data[ctype] = (missed, covered, total, pct)
inst = data.get("INSTRUCTION", (1, 0, 1, 0.0))
branch = data.get("BRANCH", (1, 0, 1, 0.0))
print(
    f"{inst[0]} {inst[1]} {inst[2]} {inst[3]:.2f} "
    f"{branch[0]} {branch[1]} {branch[2]} {branch[3]:.2f}"
)
PY
  )"

  local inst_missed
  local branch_missed
  read -r inst_missed INST_COVERED INST_TOTAL INST_PCT branch_missed BR_COVERED BR_TOTAL BR_PCT <<< "${coverage_line}"

  if [ "${inst_missed}" -ne 0 ] || [ "${branch_missed}" -ne 0 ]; then
    print_error "GATE_COVERAGE_FAIL: coverage must be 100% (INSTRUCTION and BRANCH)."
    exit "${EXIT_COVERAGE_FAIL}"
  fi
}

check_unused_warnings() {
  UNUSED_COUNT="$(
    grep -E -c "The value of the field .* is not used" "${LOG_FILE}" || true
  )"

  if [ "${UNUSED_COUNT}" -gt 0 ]; then
    print_error "GATE_UNUSED_WARNINGS: found ${UNUSED_COUNT} unused field warning(s)."
    exit "${EXIT_UNUSED_WARNINGS}"
  fi
}

main() {
  ensure_tag
  resolve_sha
  check_pom_version
  check_ci_green

  if ! run_tests; then
    print_error "GATE_TEST_FAILED: mvn clean test failed."
    append_summary
    exit "${EXIT_TEST_FAILED}"
  fi

  check_coverage
  check_unused_warnings
  append_summary
  echo "Release gate passed."
}

main "$@"
