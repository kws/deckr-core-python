"""Keep implementation responsibilities bounded and public protocol APIs generic."""

import subprocess
import sys
from pathlib import Path


def test_production_modules_remain_reviewable():
    root = Path(__file__).resolve().parents[1]
    oversized = [
        (str(p.relative_to(root)), len(p.read_text().splitlines()))
        for p in (root / "src").rglob("*.py")
        if len(p.read_text().splitlines()) > 650
    ]
    assert not oversized, (
        f"Split production modules by responsibility (650-line ceiling): {oversized}"
    )


def test_public_generic_protocol_imports_do_not_load_deckr_bindings():
    subprocess.run(
        [
            sys.executable,
            "-c",
            """
import sys
from deckr.protocols.beacon import Beacon
from deckr.protocols.concord import Concord
from deckr.protocols.concord_maintenance import ConcordMaintenance
from deckr.substrates.nats_kv import NatsJsonKvBucket
for prefix in ('deckr.bindings', 'deckr.contracts', 'deckr.hardware', 'deckr.actions', 'deckr.services'):
    assert not any(name == prefix or name.startswith(prefix + '.') for name in sys.modules), prefix
""",
        ],
        check=True,
    )
