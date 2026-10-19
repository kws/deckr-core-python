"""Report lifecycle branch coverage to guide behavioral test review."""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Group logical owners across their implementation modules. Coverage is a
# diagnostic: a percentage cannot establish authority or lifecycle conformance.
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
    for name, modules in LIFECYCLE_GROUPS.items():
        summaries = [report["files"][module]["summary"] for module in modules]
        covered = sum(item["covered_branches"] for item in summaries)
        total = sum(item["num_branches"] for item in summaries)
        percentage = 100 * covered / total if total else 100.0
        print(f"{name}: {percentage:.2f}% ({covered}/{total} branches)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
