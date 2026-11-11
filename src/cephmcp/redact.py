"""Keep cluster secrets out of tool results.

Anything a tool returns ends up in a model's context, in transcripts and
possibly at a third party. Authentication keys and passwords have no business
there unless the operator opts in (--allow-secrets).
"""

from __future__ import annotations

import re
from typing import Any

REDACTED = "<redacted>"

# cephx secrets are base64 of a small header plus key material and start with
# "AQ" (AES-128, 40 chars) or "Ag" (AES-256, 60 chars).
_CEPHX_KEY = re.compile(r"(?<![A-Za-z0-9+/])A[Qg][A-Za-z0-9+/]{30,70}={1,2}(?![A-Za-z0-9+/=])")
_PEM_KEY = re.compile(
    r"-----BEGIN ([A-Z0-9 ]*)PRIVATE KEY-----.*?-----END \1PRIVATE KEY-----", re.DOTALL
)
# "secret_key": "..." style fields in JSON or key = value text.
_SENSITIVE_NAME = re.compile(
    r"(?i)(passw(or)?d|secret|passphrase|token|private_?key|_key$|encryption_keys|credential)"
)
_JSON_FIELD = re.compile(
    r'("(?:[A-Za-z0-9_./-]*(?:passw(?:or)?d|secret|passphrase|token|private_?key|credential)'
    r'[A-Za-z0-9_./-]*|[A-Za-z0-9_./-]*_key)"\s*:\s*)"(?:[^"\\]|\\.)*"',
    re.IGNORECASE,
)


# An option name followed by its value, as in the `ceph config dump` table or
# in "name = value" lines. Only names with a '_' or '/' count, so that prose
# mentioning a password is left alone.
_OPTION_VALUE = re.compile(
    r"(?P<name>\b[A-Za-z0-9.:-]*[_/][A-Za-z0-9_/.:-]*)(?P<gap>[ \t]+(?:=[ \t]*)?)(?P<value>\S+)"
)


def is_sensitive_name(name: str) -> bool:
    return bool(_SENSITIVE_NAME.search(name))


def _mask_option(match: re.Match[str]) -> str:
    if is_sensitive_name(match["name"]):
        return f"{match['name']}{match['gap']}{REDACTED}"
    return match[0]


def redact_keys(text: str) -> str:
    """Mask what is recognisable as a key by its shape alone."""
    return _CEPHX_KEY.sub(REDACTED, _PEM_KEY.sub(REDACTED, text))


def redact_text(text: str) -> str:
    """Mask secrets in free-form command output."""
    text = _PEM_KEY.sub(REDACTED, text)
    text = _CEPHX_KEY.sub(REDACTED, text)
    text = _JSON_FIELD.sub(lambda m: f'{m.group(1)}"{REDACTED}"', text)
    return _OPTION_VALUE.sub(_mask_option, text)


def redact_obj(obj: Any) -> Any:
    """Mask secrets in decoded JSON, by field name and by value shape."""
    if isinstance(obj, dict):
        columns, rows = obj.get("columns"), obj.get("rows")
        if isinstance(columns, list) and isinstance(rows, list):
            # A rendered table: judge each row as the record it stands for.
            masked = [
                list(redact_obj(dict(zip(columns, row, strict=False))).values())
                if isinstance(row, list) and len(row) == len(columns)
                else redact_obj(row)
                for row in rows
            ]
            return {k: masked if k == "rows" else redact_obj(v) for k, v in obj.items()}
        # {"name": "<option>", "value": "..."} rows, as in `ceph config dump`.
        name = obj.get("name") or obj.get("option")
        named_secret = isinstance(name, str) and "value" in obj and is_sensitive_name(name)
        out = {}
        for key, value in obj.items():
            if (
                isinstance(value, str)
                and value
                and ((named_secret and key == "value") or is_sensitive_name(str(key)))
            ):
                out[key] = REDACTED
                continue
            out[key] = redact_obj(value)
        return out
    if isinstance(obj, list):
        return [redact_obj(v) for v in obj]
    if isinstance(obj, str):
        return redact_keys(obj)
    return obj
