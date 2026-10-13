"""
workbook_tools.py — a stored Excel workbook as the screens show it, and the
same workbook handed out protected by a password.

view()           one sheet a page at a time: the first row is the header,
                 columns that are empty on every row are left out, and an
                 optional search keeps the rows that contain the text.
protected_url()  an encrypted copy (the AES-256 protection Excel applies with
                 Protect Workbook > Encrypt with Password, as on the validation
                 team's own files) behind a short-lived link. The password is
                 used once to encrypt and is never stored or logged; the copies
                 are removed after an hour.

Callers check who may open the workbook before calling either.
"""
import io
import uuid
from collections import OrderedDict
from datetime import date, datetime, timedelta, timezone

import boto3
from openpyxl import load_workbook

import api_util
from api_util import ApiError

DOWNLOADS_PREFIX = f"{api_util.PRIVATE_ROOT}/Downloads"
KEEP_COPIES = timedelta(hours=1)
LINK_SECONDS = 600
PAGE, MAX_PAGE = 100, 500
PASSWORD_MIN, PASSWORD_MAX = 8, 255
XLSX_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

# A warm Lambda keeps the last few workbooks it read, so paging does not
# download and parse the file again (keyed by ETag: a new file is a new entry).
_CACHE = OrderedDict()
_CACHE_SIZE = 4


def is_workbook(name):
    return str(name or "").lower().endswith(".xlsx")


def _plain(value):
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        return value
    if value is None or isinstance(value, (int, float)):
        return value
    return str(value)


def _empty(value):
    return value is None or (isinstance(value, str) and not value.strip())


def parse(content):
    """[{name, headers, rows, levels, columns}] for every sheet: rows hold only
    the columns some row uses; levels give (indent, bold) of each row's first
    cell, so an outline such as the Summary keeps its shape."""
    wb = load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    sheets = []
    try:
        for ws in wb.worksheets:
            rows, levels = [], []
            for cells in ws.iter_rows():
                values = [_plain(c.value) for c in cells]
                if all(_empty(v) for v in values):
                    continue
                first = cells[0] if cells else None
                align, font = getattr(first, "alignment", None), getattr(first, "font", None)
                rows.append(values)
                levels.append((int(getattr(align, "indent", 0) or 0), bool(getattr(font, "b", False))))
            header = rows[0] if rows else []
            data, levels = rows[1:], levels[1:]
            width = max([len(header)] + [len(r) for r in data])
            used = [i for i in range(width)
                    if any(i < len(r) and not _empty(r[i]) for r in data)] if data else list(range(len(header)))
            pick = lambda r: [r[i] if i < len(r) else None for i in used]  # noqa: E731
            sheets.append({
                "name": ws.title,
                "headers": [str(h) if h is not None else "" for h in pick(header)],
                "rows": [pick(r) for r in data],
                "levels": levels,
                "hidden_columns": width - len(used),
            })
    finally:
        wb.close()
    return sheets


def _workbook(bucket, key):
    s3 = boto3.client("s3")
    try:
        etag = s3.head_object(Bucket=bucket, Key=key)["ETag"]
    except Exception:  # noqa: BLE001 - any failure to find it reads as "not found"
        raise ApiError("File not found", 404)
    tag = (bucket, key, etag)
    if tag in _CACHE:
        _CACHE.move_to_end(tag)
        return _CACHE[tag]
    content = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    try:
        sheets = parse(content)
    except Exception:  # noqa: BLE001 - not a workbook openpyxl can read
        raise ApiError("This file cannot be shown here; download it instead", 422)
    _CACHE[tag] = sheets
    while len(_CACHE) > _CACHE_SIZE:
        _CACHE.popitem(last=False)
    return sheets


def _int(value, default, low, high):
    try:
        return max(low, min(high, int(value)))
    except (TypeError, ValueError):
        return default


def view(bucket, key, sheet=None, offset=0, limit=PAGE, query=""):
    """One page of one sheet (the first sheet unless `sheet` names another)."""
    if not is_workbook(key):
        raise ApiError("Only Excel workbooks (.xlsx) can be shown here")
    sheets = _workbook(bucket, key)
    if not sheets:
        raise ApiError("The workbook has no sheets", 422)
    chosen = next((s for s in sheets if s["name"] == sheet), sheets[0])
    needle = str(query or "").strip().lower()
    order = range(len(chosen["rows"]))
    if needle:
        order = [i for i in order
                 if any(needle in str(v).lower() for v in chosen["rows"][i] if not _empty(v))]
    total = len(order)
    limit = _int(limit, PAGE, 1, MAX_PAGE)
    offset = _int(offset, 0, 0, max(0, total - 1))
    page = list(order[offset:offset + limit])
    return {
        "sheets": [{"name": s["name"], "rows": len(s["rows"])} for s in sheets],
        "sheet": chosen["name"],
        "headers": chosen["headers"],
        "rows": [chosen["rows"][i] for i in page],
        "levels": [chosen["levels"][i] for i in page],
        "hidden_columns": chosen["hidden_columns"],
        "total": total,
        "offset": offset,
        "limit": limit,
    }


def check_password(value):
    if not isinstance(value, str) or not PASSWORD_MIN <= len(value) <= PASSWORD_MAX:
        raise ApiError(f"The password must be {PASSWORD_MIN} to {PASSWORD_MAX} characters long")
    return value


def encrypt(content, password):
    """The workbook bytes encrypted with the password (ECMA-376 agile
    encryption: AES-256, SHA-512), as Excel itself writes them."""
    from msoffcrypto.format.ooxml import OOXMLFile
    out = io.BytesIO()
    try:
        OOXMLFile(io.BytesIO(content)).encrypt(check_password(password), out)
    except ApiError:
        raise
    except Exception:  # noqa: BLE001 - the library's message is not useful to the caller
        raise ApiError("This file cannot be protected with a password", 422)
    return out.getvalue()


def _sweep(s3, bucket):
    """Remove the protected copies older than KEEP_COPIES."""
    cutoff = datetime.now(timezone.utc) - KEEP_COPIES
    old = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{DOWNLOADS_PREFIX}/"):
        old += [{"Key": o["Key"]} for o in page.get("Contents", []) if o["LastModified"] < cutoff]
    for i in range(0, len(old), 1000):
        s3.delete_objects(Bucket=bucket, Delete={"Objects": old[i:i + 1000], "Quiet": True})


def protected_url(bucket, key, file_name, password):
    """A short-lived link to a copy of the workbook that opens only with the password."""
    if not is_workbook(key):
        raise ApiError("Only Excel workbooks (.xlsx) can be protected with a password")
    check_password(password)
    s3 = boto3.client("s3")
    try:
        content = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception:  # noqa: BLE001
        raise ApiError("File not found", 404)
    locked = encrypt(content, password)
    _sweep(s3, bucket)
    copy = f"{DOWNLOADS_PREFIX}/{uuid.uuid4().hex}/{api_util.safe_segment(file_name, 'workbook.xlsx')}"
    s3.put_object(Bucket=bucket, Key=copy, Body=locked, ContentType=XLSX_TYPE)
    return api_util.presign_get(bucket, copy, file_name, expires=LINK_SECONDS)
