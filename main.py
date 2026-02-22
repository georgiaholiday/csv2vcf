"""
csv_to_vcf.py
=============
A professional-grade CSV → VCF (vCard) converter.
Supports 1000+ contacts in a single batch export.

Author  : Professional Python Developer
Version : 3.0.0
"""

import csv
import os
import sys
import shutil
import codecs
import argparse
import logging
import traceback
from pathlib import Path
from datetime import datetime
from typing import Optional

# ─────────────────────────────────────────────────────────────────────────────
# Logging — timestamped, two handlers: console (INFO) + file (DEBUG)
# ─────────────────────────────────────────────────────────────────────────────
LOG_FILE = "csv_to_vcf.log"

log = logging.getLogger("csv_to_vcf")
log.setLevel(logging.DEBUG)

_console = logging.StreamHandler(sys.stdout)
_console.setLevel(logging.INFO)
_console.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)-8s %(message)s", "%H:%M:%S"))

_file_h = logging.FileHandler(LOG_FILE, encoding="utf-8")
_file_h.setLevel(logging.DEBUG)
_file_h.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)-8s %(message)s"))

log.addHandler(_console)
log.addHandler(_file_h)


# ─────────────────────────────────────────────────────────────────────────────
# Custom Exceptions — each error class carries a clear, descriptive message
# ─────────────────────────────────────────────────────────────────────────────
class CSVtoVCFError(Exception):
    """Base exception for all converter errors."""


class InputFileError(CSVtoVCFError):
    """Raised for any problem with the input CSV file."""


class OutputFileError(CSVtoVCFError):
    """Raised for any problem writing the output VCF file."""


class EncodingError(CSVtoVCFError):
    """Raised when the CSV encoding cannot be determined or read."""


class EmptyFileError(CSVtoVCFError):
    """Raised when the CSV is empty or has no data rows."""


class NoUsableColumnsError(CSVtoVCFError):
    """Raised when none of the CSV columns match any known field."""


# ─────────────────────────────────────────────────────────────────────────────
# COLUMN MAP — maps CSV header variants to internal keys
# Both Persian and English column names are supported.
# ─────────────────────────────────────────────────────────────────────────────
COLUMN_ALIASES: dict[str, list[str]] = {
    "first_name"   : ["first name", "firstname", "first", "fname", "نام"],
    "last_name"    : ["last name", "lastname", "last", "lname", "نام خانوادگی", "family"],
    "full_name"    : ["name", "full name", "fullname", "contact name", "display name", "نام کامل"],
    "phone"        : ["phone", "mobile", "cell", "telephone", "tel", "phone number",
                      "mobile number", "شماره", "شماره موبایل", "شماره تلفن"],
    "phone2"       : ["phone 2", "phone2", "secondary phone", "other phone", "home phone"],
    "email"        : ["email", "e-mail", "mail", "ایمیل"],
    "email2"       : ["email 2", "email2", "secondary email", "other email"],
    "organization" : ["organization", "company", "org", "employer", "شرکت", "سازمان"],
    "title"        : ["title", "job title", "position", "role", "عنوان شغلی"],
    "address"      : ["address", "street", "addr", "آدرس"],
    "city"         : ["city", "شهر"],
    "state"        : ["state", "province", "استان"],
    "zip_code"     : ["zip", "postal code", "zip code", "کد پستی"],
    "country"      : ["country", "کشور"],
    "website"      : ["website", "url", "web", "homepage", "وبسایت"],
    "birthday"     : ["birthday", "birth date", "dob", "تولد", "تاریخ تولد"],
    "note"         : ["note", "notes", "memo", "comment", "یادداشت"],
}

# Encoding candidates tried in order during auto-detection
ENCODING_CANDIDATES = ["utf-8-sig", "utf-8", "cp1256", "cp1252", "latin-1", "iso-8859-1"]


# ─────────────────────────────────────────────────────────────────────────────
# Input Validation
# ─────────────────────────────────────────────────────────────────────────────

def validate_input_path(path: Path) -> None:
    """
    Validate the input CSV file thoroughly before opening it.
    Raises a specific exception with a human-readable message on any problem.
    """
    if not path.exists():
        raise InputFileError(
            f"File not found: '{path}'\n"
            f"  → Make sure the path is correct and the file exists."
        )
    if path.is_dir():
        raise InputFileError(
            f"Expected a file but got a directory: '{path}'\n"
            f"  → Specify the CSV file, not the folder."
        )
    if not os.access(path, os.R_OK):
        raise InputFileError(
            f"Permission denied: cannot read '{path}'\n"
            f"  → Check file permissions (try: chmod +r \"{path}\")."
        )
    if path.stat().st_size == 0:
        raise EmptyFileError(
            f"The file is completely empty (0 bytes): '{path}'\n"
            f"  → Nothing to convert."
        )
    if path.suffix.lower() not in (".csv", ".txt", ".tsv"):
        log.warning(
            f"Unexpected extension '{path.suffix}' — expected .csv. "
            "Will try anyway but results may vary."
        )


def validate_output_path(path: Path) -> None:
    """
    Ensure the output directory exists and is writable.
    Raises OutputFileError on any problem.
    """
    parent = path.parent
    try:
        parent.mkdir(parents=True, exist_ok=True)
    except PermissionError:
        raise OutputFileError(
            f"Cannot create output directory: '{parent}'\n"
            f"  → Run with elevated permissions or choose a different output path."
        )
    except OSError as e:
        raise OutputFileError(f"Cannot create output directory '{parent}': {e}")

    if not os.access(parent, os.W_OK):
        raise OutputFileError(
            f"No write permission in directory: '{parent}'\n"
            f"  → Choose a different output location."
        )

    # Rough disk space check — fail early before processing thousands of rows
    free_bytes = shutil.disk_usage(parent).free
    if free_bytes < 10 * 1024:
        raise OutputFileError(
            f"Disk is nearly full ({free_bytes} bytes free).\n"
            f"  → Free up space and try again."
        )


def detect_encoding(path: Path) -> str:
    """
    Try ENCODING_CANDIDATES in order and return the first that reads without errors.
    Raises EncodingError if none work.
    """
    for enc in ENCODING_CANDIDATES:
        try:
            with open(path, encoding=enc, errors="strict") as f:
                f.read()
            log.debug(f"Detected encoding: {enc}")
            return enc
        except (UnicodeDecodeError, LookupError):
            continue

    raise EncodingError(
        f"Cannot read '{path}' with any known encoding: {ENCODING_CANDIDATES}\n"
        f"  → Pass the correct encoding manually: --encoding cp1256\n"
        f"  → Common values: utf-8, cp1256 (Farsi/Arabic Excel), cp1252 (Windows)"
    )


def detect_delimiter(path: Path, encoding: str) -> str:
    """
    Sniff the delimiter from the first line of the file.
    Falls back to ',' on any error.
    """
    try:
        with open(path, encoding=encoding, errors="replace") as f:
            first_line = f.readline()
        candidates = {",": first_line.count(","), ";": first_line.count(";"),
                      "\t": first_line.count("\t"), "|": first_line.count("|")}
        best = max(candidates, key=candidates.get)   # type: ignore[arg-type]
        if candidates[best] == 0:
            log.warning("Could not auto-detect delimiter — falling back to ','.")
            return ","
        log.debug(f"Auto-detected delimiter: {repr(best)}")
        return best
    except Exception as e:
        log.warning(f"Delimiter detection failed ({e}) — falling back to ','.")
        return ","


# ─────────────────────────────────────────────────────────────────────────────
# Column Mapping
# ─────────────────────────────────────────────────────────────────────────────

def normalize_headers(headers: list[str]) -> dict[str, str]:
    """
    Build {internal_key -> actual_csv_column} mapping, case-insensitively.
    Raises NoUsableColumnsError if no recognizable columns are found.
    """
    if not headers:
        raise NoUsableColumnsError(
            "CSV file has no headers.\n"
            "  → The first row must contain column names."
        )

    headers_lower = {h.strip().lower(): h for h in headers if h.strip()}
    normalized: dict[str, str] = {}

    for key, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            if alias.lower() in headers_lower:
                normalized[key] = headers_lower[alias.lower()]
                break

    has_name    = any(k in normalized for k in ("first_name", "last_name", "full_name"))
    has_contact = any(k in normalized for k in ("phone", "email"))

    if not has_name and not has_contact:
        raise NoUsableColumnsError(
            f"No recognizable columns found in the CSV.\n"
            f"  CSV headers : {list(headers_lower.keys())}\n"
            f"  → Rename columns to match known names, e.g.:\n"
            f"    Name  : 'First Name', 'Last Name', 'Name', 'نام'\n"
            f"    Phone : 'Phone', 'Mobile', 'شماره'\n"
            f"    Email : 'Email', 'ایمیل'"
        )
    if not has_name:
        log.warning("No name column detected — contacts will be labelled by phone/email.")
    if not has_contact:
        log.warning("No phone or email column detected.")

    return normalized


# ─────────────────────────────────────────────────────────────────────────────
# Field Helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_field(row: dict, col_map: dict[str, str], key: str) -> str:
    """Safely read and strip a field from a row. Returns '' if missing."""
    col = col_map.get(key)
    if not col:
        return ""
    val = row.get(col, "")
    return str(val).strip() if val is not None else ""


def build_full_name(row: dict, col_map: dict[str, str]) -> tuple[str, str, str]:
    """Return (full_name, first, last) from whatever name columns are available."""
    first = get_field(row, col_map, "first_name")
    last  = get_field(row, col_map, "last_name")
    full  = get_field(row, col_map, "full_name")

    if first or last:
        return f"{first} {last}".strip(), first, last
    if full:
        parts = full.split(maxsplit=1)
        return full, parts[0], parts[1] if len(parts) > 1 else ""
    return "", "", ""


def sanitize_phone(phone: str) -> str:
    """Keep only valid phone characters; warn if everything is stripped."""
    if not phone:
        return ""
    allowed = set("0123456789+()-. ")
    cleaned = "".join(ch for ch in phone if ch in allowed).strip()
    if phone and not cleaned:
        log.debug(f"Phone '{phone}' was entirely stripped during sanitization.")
    return cleaned


def escape_vcard_text(text: str) -> str:
    """Escape special chars in vCard text fields per RFC 6350."""
    return (
        text
        .replace("\\", "\\\\")
        .replace("\r\n", "\\n").replace("\n", "\\n").replace("\r", "\\n")
        .replace(",", "\\,")
        .replace(";", "\\;")
    )


# ─────────────────────────────────────────────────────────────────────────────
# vCard Builder
# ─────────────────────────────────────────────────────────────────────────────

def row_to_vcard(
    row: dict,
    col_map: dict[str, str],
    row_num: int,
    vcard_version: str = "3.0",
) -> Optional[str]:
    """
    Convert one CSV row to a vCard string.
    Returns None if the row has no usable data.
    Never raises — all per-row errors are caught and logged internally.
    """
    try:
        full_name, first, last = build_full_name(row, col_map)
        phone  = sanitize_phone(get_field(row, col_map, "phone"))
        phone2 = sanitize_phone(get_field(row, col_map, "phone2"))
        email  = get_field(row, col_map, "email")

        # Need at least one of: name, phone, or email
        if not full_name and not phone and not email:
            log.debug(f"Row {row_num}: skipped — no name, phone, or email.")
            return None

        # Fallback display name when no name column exists
        if not full_name:
            full_name = phone or email
            first, last = full_name, ""

        email2       = get_field(row, col_map, "email2")
        organization = get_field(row, col_map, "organization")
        title        = get_field(row, col_map, "title")
        address      = get_field(row, col_map, "address")
        city         = get_field(row, col_map, "city")
        state        = get_field(row, col_map, "state")
        zip_code     = get_field(row, col_map, "zip_code")
        country      = get_field(row, col_map, "country")
        website      = get_field(row, col_map, "website")
        birthday     = get_field(row, col_map, "birthday")
        note         = get_field(row, col_map, "note")

        lines: list[str] = ["BEGIN:VCARD", f"VERSION:{vcard_version}"]

        # Name fields
        lines.append(f"N:{escape_vcard_text(last)};{escape_vcard_text(first)};;;")
        lines.append(f"FN:{escape_vcard_text(full_name)}")

        # Organization
        if organization:
            lines.append(f"ORG:{escape_vcard_text(organization)}")
        if title:
            lines.append(f"TITLE:{escape_vcard_text(title)}")

        # Phone numbers
        if phone:
            tag = "TEL;TYPE=CELL,VOICE" if vcard_version == "4.0" else "TEL;TYPE=CELL"
            lines.append(f"{tag}:{phone}")
        if phone2:
            tag = "TEL;TYPE=HOME,VOICE" if vcard_version == "4.0" else "TEL;TYPE=HOME"
            lines.append(f"{tag}:{phone2}")

        # Email addresses
        if email:
            tag = "EMAIL;TYPE=WORK" if vcard_version == "4.0" else "EMAIL;TYPE=INTERNET"
            lines.append(f"{tag}:{email}")
        if email2:
            tag = "EMAIL;TYPE=HOME" if vcard_version == "4.0" else "EMAIL;TYPE=INTERNET"
            lines.append(f"{tag}:{email2}")

        # Address — ADR: PO;Extended;Street;City;State;ZIP;Country
        if any([address, city, state, zip_code, country]):
            lines.append(
                f"ADR;TYPE=HOME:;;{escape_vcard_text(address)};"
                f"{escape_vcard_text(city)};{escape_vcard_text(state)};"
                f"{escape_vcard_text(zip_code)};{escape_vcard_text(country)}"
            )

        # Website — add https:// if no scheme present
        if website:
            if not website.startswith(("http://", "https://", "ftp://")):
                website = "https://" + website
            lines.append(f"URL:{website}")

        if birthday:
            lines.append(f"BDAY:{birthday}")

        if note:
            lines.append(f"NOTE:{escape_vcard_text(note)}")

        lines.append("END:VCARD")
        return "\r\n".join(lines)   # RFC 6350 mandates CRLF line endings

    except Exception as exc:
        # One bad row must never abort the entire conversion
        log.warning(f"Row {row_num}: unexpected error — {exc}")
        log.debug(traceback.format_exc())
        return None


# ─────────────────────────────────────────────────────────────────────────────
# File Writer
# ─────────────────────────────────────────────────────────────────────────────

def _write_vcf(vcards: list[str], path: Path) -> None:
    """
    Write vCard list to a UTF-8 .vcf file.
    Raises OutputFileError with a clear message on any write problem.
    """
    try:
        with open(path, "w", encoding="utf-8", newline="") as f:
            f.write("\r\n\r\n".join(vcards))
            f.write("\r\n")   # trailing newline after last vCard
    except PermissionError:
        raise OutputFileError(
            f"Permission denied: cannot write to '{path}'\n"
            f"  → The file may be open in another program, or write-protected."
        )
    except OSError as e:
        if e.errno == 28:   # ENOSPC
            raise OutputFileError(
                f"Disk full! Could not write to '{path}'.\n"
                f"  → Free up space and try again."
            )
        raise OutputFileError(f"Write failed for '{path}': {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Main Conversion Engine
# ─────────────────────────────────────────────────────────────────────────────

def convert_csv_to_vcf(
    input_path: str,
    output_path: str,
    encoding: str = "",         # empty = auto-detect
    delimiter: str = "",        # empty = auto-detect
    vcard_version: str = "3.0",
    chunk_size: int = 0,        # 0 = single output file
) -> dict:
    """
    Full conversion pipeline: validate → read → convert → write.

    Returns a summary dict:
        total_read, total_written, total_skipped, output_files
    """
    src = Path(input_path)
    dst = Path(output_path)

    # Step 1 — Validate input file
    validate_input_path(src)

    # Step 2 — Resolve encoding
    if not encoding:
        log.info("No encoding specified — auto-detecting...")
        encoding = detect_encoding(src)
    else:
        try:
            codecs.lookup(encoding)
        except LookupError:
            raise EncodingError(
                f"Unknown encoding: '{encoding}'\n"
                f"  → Valid examples: utf-8, utf-8-sig, cp1256, cp1252, latin-1"
            )
        log.info(f"Using encoding: {encoding}")

    # Step 3 — Resolve delimiter
    if not delimiter:
        log.info("No delimiter specified — auto-detecting...")
        delimiter = detect_delimiter(src, encoding)
    elif len(delimiter) != 1:
        raise InputFileError(
            f"Delimiter must be exactly one character, got: {repr(delimiter)}"
        )

    # Step 4 — Prepare output path
    if dst.is_dir() or not dst.suffix:
        dst = dst / f"{src.stem}.vcf"
    validate_output_path(dst)

    log.info(
        f"Input: {src}  |  Output: {dst}  |  "
        f"Encoding: {encoding}  |  Delimiter: {repr(delimiter)}  |  vCard: {vcard_version}"
    )

    # Step 5 — Open CSV
    try:
        csv_file = open(src, newline="", encoding=encoding, errors="replace")
    except OSError as e:
        raise InputFileError(f"Cannot open '{src}': {e}")

    vcards: list[str] = []
    total_read    = 0
    total_skipped = 0
    skipped_rows: list[int] = []

    with csv_file:
        # Step 6 — Build DictReader
        try:
            reader = csv.DictReader(csv_file, delimiter=delimiter)
        except csv.Error as e:
            raise InputFileError(
                f"Failed to parse CSV structure: {e}\n"
                f"  → Try specifying the delimiter manually: --delimiter ;"
            )

        # Step 7 — Read headers
        try:
            headers = list(reader.fieldnames or [])
        except csv.Error as e:
            raise InputFileError(f"Cannot read CSV headers: {e}")

        col_map = normalize_headers(headers)
        log.info(f"Recognized fields: {list(col_map.keys())}")

        # Step 8 — Process every row
        for row_num, row in enumerate(reader, start=2):
            total_read += 1

            # Warn on column count mismatches (misaligned rows)
            actual = len([v for v in row.values() if v is not None])
            if actual != len(headers):
                log.debug(
                    f"Row {row_num}: column count mismatch "
                    f"(expected {len(headers)}, got {actual}) — data may be misaligned."
                )

            vcard = row_to_vcard(row, col_map, row_num, vcard_version)
            if vcard:
                vcards.append(vcard)
            else:
                total_skipped += 1
                skipped_rows.append(row_num)

    # Step 9 — Sanity checks before writing
    total_written = len(vcards)

    if total_read == 0:
        raise EmptyFileError(
            f"CSV has headers but zero data rows: '{src}'\n"
            f"  → Add at least one contact row below the header."
        )

    if total_written == 0:
        raise EmptyFileError(
            f"Every row was skipped — no valid contacts found.\n"
            f"  → {total_skipped} rows had no name, phone, or email.\n"
            f"  → Problematic rows: {skipped_rows[:20]}{'...' if len(skipped_rows) > 20 else ''}\n"
            f"  → Check your CSV structure and column names."
        )

    if skipped_rows:
        log.info(f"{len(skipped_rows)} empty/invalid rows skipped (see {LOG_FILE} for row numbers).")
        log.debug(f"Skipped row numbers: {skipped_rows}")

    log.info(f"Read: {total_read}  |  To write: {total_written}  |  Skipped: {total_skipped}")

    # Step 10 — Write VCF output
    output_files: list[str] = []

    if chunk_size > 0 and total_written > chunk_size:
        # Split into numbered files — useful when phone apps have import limits
        chunks = [vcards[i:i + chunk_size] for i in range(0, total_written, chunk_size)]
        for idx, chunk in enumerate(chunks, start=1):
            chunk_path = dst.parent / f"{dst.stem}_part{idx:03d}{dst.suffix}"
            _write_vcf(chunk, chunk_path)
            output_files.append(str(chunk_path))
            log.info(f"  → {chunk_path.name}  ({len(chunk)} contacts)")
    else:
        # Single file — handles 10,000+ contacts without issues
        _write_vcf(vcards, dst)
        output_files.append(str(dst))
        log.info(f"  → {dst.name}  ({total_written} contacts)")

    return {
        "total_read"    : total_read,
        "total_written" : total_written,
        "total_skipped" : total_skipped,
        "output_files"  : output_files,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Sample CSV Generator
# ─────────────────────────────────────────────────────────────────────────────

def generate_sample_csv(output_path: str = "sample_contacts.csv") -> None:
    """Create a realistic sample CSV for testing (includes intentionally tricky rows)."""
    rows = [
        ["First Name", "Last Name", "Phone", "Phone 2", "Email",
         "Organization", "Title", "Address", "City", "Country", "Birthday", "Note"],
        ["Ali",   "Ahmadi",   "+98-912-111-2233", "+98-21-44556677",
         "ali@example.com", "Tech Co", "Developer", "Valiasr St", "Tehran", "Iran",
         "1990-05-15", "VIP customer"],
        ["Sara",  "Hosseini", "+98-935-999-8877", "",
         "sara@company.ir", "Design Studio", "UI Designer", "Hafez Ave", "Isfahan", "Iran",
         "1995-11-20", ""],
        ["John",  "Smith",    "+1-310-555-0100",  "+1-310-555-0101",
         "john@mail.com", "Global Corp", "Manager", "123 Main St", "Los Angeles", "USA",
         "1985-03-22", "Conference lead"],
        # Intentionally empty row — should be skipped cleanly without crash
        ["", "", "", "", "", "", "", "", "", "", "", ""],
        ["Maria", "Garcia",   "+34-600-123-456", "",
         "maria@empresa.es", "Empresa SL", "CEO", "Calle Mayor", "Madrid", "Spain",
         "", "Long-time partner; important,client"],
    ]
    try:
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows(rows)
        log.info(f"Sample CSV created: {output_path}")
    except PermissionError:
        raise OutputFileError(
            f"Cannot write sample to '{output_path}' — permission denied.\n"
            f"  → Try a different location, e.g.: --output /tmp/sample.csv"
        )
    except OSError as e:
        raise OutputFileError(f"Cannot write sample CSV: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="csv_to_vcf",
        description="Convert CSV contacts → VCF (vCard). Supports 1000+ contacts.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python csv_to_vcf.py contacts.csv
  python csv_to_vcf.py contacts.csv -o output/contacts.vcf
  python csv_to_vcf.py contacts.csv --version 4.0
  python csv_to_vcf.py contacts.csv --chunk 500
  python csv_to_vcf.py contacts.csv --encoding cp1256 --delimiter ";"
  python csv_to_vcf.py --sample

Exit codes:
  0  Success
  2  Input file error
  3  Output file error
  4  Encoding error
  5  Empty file
  6  No usable columns
  7  Interrupted by user
  99 Unexpected error (see csv_to_vcf.log)
        """,
    )
    parser.add_argument("input",       nargs="?",            help="Input CSV file path")
    parser.add_argument("-o","--output", default="",         help="Output VCF path")
    parser.add_argument("--version",   choices=["3.0","4.0"], default="3.0",
                        help="vCard version (default: 3.0)")
    parser.add_argument("--delimiter", default="",
                        help="CSV delimiter (default: auto-detect)")
    parser.add_argument("--encoding",  default="",
                        help="CSV encoding (default: auto-detect)")
    parser.add_argument("--chunk",     type=int, default=0, metavar="N",
                        help="Split output into files of N contacts each")
    parser.add_argument("--sample",    action="store_true",
                        help="Generate a sample CSV for testing and exit")
    parser.add_argument("-v","--verbose", action="store_true",
                        help="Show debug-level output")
    return parser


def main() -> None:
    parser = build_arg_parser()
    args   = parser.parse_args()

    if args.verbose:
        _console.setLevel(logging.DEBUG)

    # ── Sample mode ──────────────────────────────────────────────────────────
    if args.sample:
        try:
            generate_sample_csv("sample_contacts.csv")
            print("\n✓ sample_contacts.csv created.")
            print("  Run: python csv_to_vcf.py sample_contacts.csv\n")
        except CSVtoVCFError as e:
            print(f"\n✗ Error: {e}\n", file=sys.stderr)
            sys.exit(3)
        sys.exit(0)

    if not args.input:
        parser.print_help()
        sys.exit(1)

    src = Path(args.input)
    dst = args.output or str(src.with_suffix(".vcf"))

    # ── Run conversion ────────────────────────────────────────────────────────
    start = datetime.now()
    try:
        result = convert_csv_to_vcf(
            input_path    = str(src),
            output_path   = dst,
            encoding      = args.encoding,
            delimiter     = args.delimiter,
            vcard_version = args.version,
            chunk_size    = args.chunk,
        )

    # Specific exit codes for each error category — scriptable and debuggable
    except InputFileError as e:
        print(f"\n✗ Input Error:\n  {e}\n", file=sys.stderr)
        sys.exit(2)
    except OutputFileError as e:
        print(f"\n✗ Output Error:\n  {e}\n", file=sys.stderr)
        sys.exit(3)
    except EncodingError as e:
        print(f"\n✗ Encoding Error:\n  {e}\n", file=sys.stderr)
        sys.exit(4)
    except EmptyFileError as e:
        print(f"\n✗ Empty File:\n  {e}\n", file=sys.stderr)
        sys.exit(5)
    except NoUsableColumnsError as e:
        print(f"\n✗ Column Mapping Error:\n  {e}\n", file=sys.stderr)
        sys.exit(6)
    except KeyboardInterrupt:
        print("\n\n⚠  Interrupted by user.\n", file=sys.stderr)
        sys.exit(7)
    except Exception as e:
        # Unexpected bug — full traceback goes to log, summary to terminal
        log.debug(traceback.format_exc())
        print(
            f"\n✗ Unexpected error: {e}\n"
            f"  → Full traceback saved to: {LOG_FILE}\n"
            f"  → Please report this issue.\n",
            file=sys.stderr,
        )
        sys.exit(99)

    # ── Success Summary ───────────────────────────────────────────────────────
    elapsed = (datetime.now() - start).total_seconds()
    print("\n" + "═" * 52)
    print("  ✓  CSV → VCF Conversion Complete")
    print("═" * 52)
    print(f"  Rows read      : {result['total_read']}")
    print(f"  Contacts saved : {result['total_written']}")
    print(f"  Skipped rows   : {result['total_skipped']}")
    print(f"  Time elapsed   : {elapsed:.2f}s")
    print(f"  Log file       : {LOG_FILE}")
    print(f"  Output file(s) :")
    for f in result["output_files"]:
        try:
            size_kb = Path(f).stat().st_size / 1024
            print(f"    • {f}  ({size_kb:.1f} KB)")
        except OSError:
            print(f"    • {f}")
    print("═" * 52 + "\n")


if __name__ == "__main__":
    main()