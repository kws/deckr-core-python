from __future__ import annotations

import base64
import re

_SAFE_TOKEN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")


def encode_key_token(raw: str) -> str:
    """Encode arbitrary text into a store-safe key token."""

    if _SAFE_TOKEN_RE.fullmatch(raw) and not raw.startswith("b64_"):
        return raw
    encoded = base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii")
    return "b64_" + encoded.rstrip("=")


def decode_key_token(token: str) -> str:
    """Decode a token produced by :func:`encode_key_token`."""

    if not _SAFE_TOKEN_RE.fullmatch(token):
        raise ValueError("Invalid key token")
    if token.startswith("b64_"):
        encoded = token[4:]
        padded = encoded + "=" * (-len(encoded) % 4)
        raw = base64.b64decode(padded, altchars=b"-_", validate=True).decode("utf-8")
    else:
        raw = token
    if encode_key_token(raw) != token:
        raise ValueError("Noncanonical key token")
    return raw


__all__ = ["decode_key_token", "encode_key_token"]
