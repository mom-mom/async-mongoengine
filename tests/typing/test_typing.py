"""Consumer type regression check.

Runs Pyright in plain ``basic`` mode (``tests/typing/pyrightconfig.json``, no
severity overrides) over the DB-free case files in ``tests/typing/cases`` and
asserts that the library's public typing surface behaves as documented:

1. every line carrying ``# expect-error: <rule>`` (optionally followed by a
   quoted message substring, e.g. ``# expect-error: reportAttributeAccessIssue "label"``)
   produces at least one error diagnostic with that rule (and substring);
2. no other error-severity diagnostic is reported in ``cases/`` (this is what
   catches ``assert_type`` failures, reported as ``reportAssertTypeFailure``);
3. the Pyright process did not crash (exit code 0 or 1 only).

The case files are named ``check_*.py`` so pytest never imports them; they are
only ever read by Pyright.  ``mongoengine`` resolves through the editable
install, i.e. exactly like a consumer package would see it.
"""

import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

TYPING_DIR = Path(__file__).resolve().parent
REPO_ROOT = TYPING_DIR.parent.parent
CASES_DIR = TYPING_DIR / "cases"
PYRIGHT_CONFIG = TYPING_DIR / "pyrightconfig.json"

_EXPECT_ERROR = re.compile(r"#\s*expect-error:\s*(?P<rule>[A-Za-z][A-Za-z0-9]*)(?:\s+\"(?P<message>[^\"]*)\")?")

# (file, 1-based line) -> list of (rule, message substring or None)
Expectations = dict[tuple[Path, int], list[tuple[str, str | None]]]


def _collect_expectations() -> Expectations:
    expectations: Expectations = {}
    for case_file in sorted(CASES_DIR.glob("check_*.py")):
        for line_no, line in enumerate(case_file.read_text().splitlines(), start=1):
            match = _EXPECT_ERROR.search(line)
            if match:
                expectations.setdefault((case_file, line_no), []).append((match["rule"], match["message"]))
    return expectations


def _run_pyright() -> dict[str, Any]:
    command = [sys.executable, "-m", "pyright", "--outputjson", "-p", str(PYRIGHT_CONFIG)]
    proc = subprocess.run(command, cwd=REPO_ROOT, capture_output=True, text=True)
    # 0: no errors, 1: errors reported.  Anything else means Pyright itself failed.
    assert proc.returncode in (0, 1), (
        f"pyright exited with {proc.returncode}\ncommand: {' '.join(command)}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as exc:  # pragma: no cover - only on a broken toolchain
        raise AssertionError(
            f"pyright did not produce JSON output: {exc}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )


def _format(file: Path, line: int, rule: str | None, message: str) -> str:
    first_line = message.strip().splitlines()[0] if message.strip() else ""
    return f"{file.relative_to(REPO_ROOT)}:{line} [{rule or 'no-rule'}] {first_line}"


def test_consumer_typing_cases() -> None:
    case_files = sorted(CASES_DIR.glob("check_*.py"))
    assert case_files, f"no check_*.py case files found in {CASES_DIR}"

    expectations = _collect_expectations()
    output = _run_pyright()

    matched: set[tuple[Path, int, int]] = set()  # (file, line, index into expectation list)
    unexpected: list[str] = []
    for diagnostic in output.get("generalDiagnostics", []):
        if diagnostic.get("severity") != "error":
            continue
        file = Path(diagnostic["file"]).resolve()
        line = diagnostic["range"]["start"]["line"] + 1
        rule = diagnostic.get("rule")
        message = diagnostic.get("message", "")

        expected_here = expectations.get((file, line), [])
        for index, (expected_rule, expected_message) in enumerate(expected_here):
            if rule == expected_rule and (expected_message is None or expected_message in message):
                matched.add((file, line, index))
                break
        else:
            unexpected.append(_format(file, line, rule, message))

    missing = [
        f"{file.relative_to(REPO_ROOT)}:{line} expected [{rule}]" + (f' containing "{message}"' if message else "")
        for (file, line), expected in sorted(expectations.items())
        for index, (rule, message) in enumerate(expected)
        if (file, line, index) not in matched
    ]

    problems: list[str] = []
    if unexpected:
        problems.append("Unexpected Pyright errors:\n  " + "\n  ".join(unexpected))
    if missing:
        problems.append("Expected errors that were not reported:\n  " + "\n  ".join(missing))
    assert not problems, "\n\n".join(problems)
