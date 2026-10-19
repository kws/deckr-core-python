"""Enforce the authority lifecycle branch-coverage acceptance gate."""

from __future__ import annotations

import json
import sys
from pathlib import Path

MINIMUM_BRANCH_COVERAGE = 95.0
# Preserve the 95% gate for each logical owner across its responsibility splits.
LIFECYCLE_GROUPS = {
    "beacon": [
        "src/deckr/_beacon/_support.py",
        "src/deckr/_beacon/_records.py",
        "src/deckr/_beacon/_api.py",
        "src/deckr/_beacon/_tasks.py",
        "src/deckr/_beacon/_directory.py",
        "src/deckr/_beacon/_lease.py",
        "src/deckr/_beacon/_models.py",
    ],
    "agreement": [
        "src/deckr/_concord/_agreement_context.py",
        "src/deckr/_concord/_agreement_models.py",
    ],
    "participant": [
        "src/deckr/_concord/_participant_context.py",
        "src/deckr/_concord/_participant_models.py",
        "src/deckr/_concord/_participant_attachments.py",
        "src/deckr/_concord/_participant_resources.py",
    ],
    "lease": ["src/deckr/_concord/_lease.py"],
    "owner": ["src/deckr/_concord/_owner.py"],
    "owned tasks": ["src/deckr/core/util/owned_tasks.py"],
}


def main() -> int:
    report_path = Path(sys.argv[1] if len(sys.argv) > 1 else "coverage.json")
    report = json.loads(report_path.read_text())
    failures: list[str] = []
    for name, modules in LIFECYCLE_GROUPS.items():
        summaries = [report["files"][module]["summary"] for module in modules]
        covered = sum(item["covered_branches"] for item in summaries)
        total = sum(item["num_branches"] for item in summaries)
        percentage = 100 * covered / total if total else 100.0
        print(f"{name}: {percentage:.2f}% ({covered}/{total} branches)")
        if percentage < MINIMUM_BRANCH_COVERAGE:
            failures.append(f"{name}: {percentage:.2f}%")
    if failures:
        print(
            f"Lifecycle branch coverage must be at least "
            f"{MINIMUM_BRANCH_COVERAGE:.0f}%: {', '.join(failures)}",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
