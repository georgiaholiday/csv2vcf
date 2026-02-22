"""
csv_to_vcf.py
Professional-grade CSV to VCF (vCard) converter with Android and WhatsApp Web support.
Supports 1000+ contacts in a single batch export with Persian/Arabic character support.

Author  : Professional Python Developer
Version : 4.0.0
Features:
    - Android-compatible VCF output (UTF-8 BOM encoding)
    - WhatsApp Web link generation (wa.me format)
    - International phone number formatting
    - Persian/Arabic character support
    - Auto-detection of encoding and delimiter
    - Chunked output for large contact lists
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
from typing import Optional, Dict, List, Tuple


# ============================================================================
# LOGGING SETUP
# Timestamped logging with two handlers: console (INFO) and file (DEBUG)
# ============================================================================
LOG_FILE = "csv_to_vcf.log"
logger = logging.getLogger("csv_to_vcf")
logger.setLevel(logging.DEBUG)

# Console handler - shows INFO level and above
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(
    logging.Formatter("[%(asctime)s] %(levelname)-8s %(message)s", "%H:%M:%S")
)

# File handler - logs DEBUG level for troubleshooting
file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(
    logging.Formatter("[%(asctime)s] %(levelname)-8s %(message)s")
)

logger.addHandler(console_handler)
logger.addHandler(file_handler)


# ============================================================================
# CUSTOM EXCEPTIONS
# Each exception class provides clear, descriptive error messages
# ============================================================================
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


# ============================================================================
# COLUMN MAPPING
# Maps CSV header variants to internal keys (supports Persian and English)
# ============================================================================
COLUMN_ALIASES: Dict[str, List[str]] = {
    "first_name": [
        "first name", "firstname", "first", "fname", "نام"
    ],
    "last_name": [
        "last name", "lastname", "last", "lname", "نام خانوادگی", "family"
    ],
    "full_name": [
        "name", "full name", "fullname", "contact name", 
        "display name", "نام کامل"
    ],
    "phone": [
        "phone", "mobile", "cell", "telephone", "tel", "phone number",
        "mobile number", "شماره", "شماره موبایل", "شماره تلفن"
    ],
    "phone2": [
        "phone 2", "phone2", "secondary phone", "other phone", 
        "home phone"
    ],
    "email": [
        "email", "e-mail", "mail", "ایمیل"
    ],
    "email2": [
        "email 2", "email2", "secondary email", "other email"
    ],
    "organization": [
        "organization", "company", "org", "employer", "شرکت", "سازمان"
    ],
    "title": [
        "title", "job title", "position", "role", "عنوان شغلی"
    ],
    "address": [
        "address", "street", "addr", "آدرس"
    ],
    "city": [
        "city", "شهر"
    ],
    "state": [
        "state", "province", "استان"
    ],
    "zip_code": [
        "zip", "postal code", "zip code", "کد پستی"
    ],
    "country": [
        "country", "کشور"
    ],
    "website": [
        "website", "url", "web", "homepage", "وبسایت"
    ],
    "birthday": [
        "birthday", "birth date", "dob", "تولد", "تاریخ تولد"
    ],
    "note": [
        "note", "notes", "memo", "comment", "یادداشت"
    ],
}

# Encoding candidates tried in order during auto-detection
ENCODING_CANDIDATES = [
    "utf-8-sig",    # UTF-8 with BOM (best for Android)
    "utf-8",        # Standard UTF-8
    "cp1256",       # Arabic/Persian Windows
    "cp1252",       # Western European Windows
    "latin-1",      # ISO-8859-1
    "iso-8859-1"    # ISO Latin-1
]


# ============================================================================
# INPUT VALIDATION FUNCTIONS
# ============================================================================
def validate_input_path(path: Path) -> None:
    """
    Validate the input CSV file thoroughly before opening it.
    Raises specific exceptions with human-readable messages for any problem.
    
    Args:
        path: Path to the input CSV file
        
    Raises:
        InputFileError: If file doesn't exist, is a directory, or has permission issues
        EmptyFileError: If file is 0 bytes
    """
    # Check if file exists
    if not path.exists():
        raise InputFileError(
            f"File not found: '{path}'\n"
            f"  → Make sure the path is correct and the file exists."
        )
    
    # Check if it's a directory instead of a file
    if path.is_dir():
        raise InputFileError(
            f"Expected a file but got a directory: '{path}'\n"
            f"  → Specify the CSV file, not the folder."
        )
    
    # Check read permissions
    if not os.access(path, os.R_OK):
        raise InputFileError(
            f"Permission denied: cannot read '{path}'\n"
            f"  → Check file permissions (try: chmod +r \"{path}\")."
        )
    
    # Check if file is empty
    if path.stat().st_size == 0:
        raise EmptyFileError(
            f"The file is completely empty (0 bytes): '{path}'\n"
            f"  → Nothing to convert."
        )
    
    # Warn about unexpected file extensions
    if path.suffix.lower() not in (".csv", ".txt", ".tsv"):
        logger.warning(
            f"Unexpected extension '{path.suffix}' — expected .csv. "
            "Will try anyway but results may vary."
        )


def validate_output_path(path: Path) -> None:
    """
    Ensure the output directory exists and is writable.
    Also checks for sufficient disk space.
    
    Args:
        path: Path to the output file
        
    Raises:
        OutputFileError: If directory cannot be created or has permission issues
    """
    parent = path.parent
    
    # Create output directory if it doesn't exist
    try:
        parent.mkdir(parents=True, exist_ok=True)
    except PermissionError:
        raise OutputFileError(
            f"Cannot create output directory: '{parent}'\n"
            f"  → Run with elevated permissions or choose a different output path."
        )
    except OSError as e:
        raise OutputFileError(f"Cannot create output directory '{parent}': {e}")
    
    # Check write permissions
    if not os.access(parent, os.W_OK):
        raise OutputFileError(
            f"No write permission in directory: '{parent}'\n"
            f"  → Choose a different output location."
        )
    
    # Check disk space (minimum 10KB)
    free_bytes = shutil.disk_usage(parent).free
    if free_bytes < 10 * 1024:
        raise OutputFileError(
            f"Disk is nearly full ({free_bytes} bytes free).\n"
            f"  → Free up space and try again."
        )


# ============================================================================
# ENCODING AND DELIMITER DETECTION
# ============================================================================
def detect_encoding(path: Path) -> str:
    """
    Try ENCODING_CANDIDATES in order and return the first that reads without errors.
    
    Args:
        path: Path to the CSV file
        
    Returns:
        The detected encoding name
        
    Raises:
        EncodingError: If no encoding can read the file
    """
    for encoding in ENCODING_CANDIDATES:
        try:
            with open(path, encoding=encoding, errors="strict") as f:
                f.read()
            logger.debug(f"Detected encoding: {encoding}")
            return encoding
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
    Counts occurrences of common delimiters and returns the most frequent one.
    
    Args:
        path: Path to the CSV file
        encoding: File encoding
        
    Returns:
        Detected delimiter character (default: ',')
    """
    try:
        with open(path, encoding=encoding, errors="replace") as f:
            first_line = f.readline()
        
        # Count occurrences of common delimiters
        candidates = {
            ",": first_line.count(","),
            ";": first_line.count(";"),
            "\t": first_line.count("\t"),
            "|": first_line.count("|")
        }
        
        # Return the most frequent delimiter
        best = max(candidates, key=candidates.get)  # type: ignore[arg-type]
        
        if candidates[best] == 0:
            logger.warning("Could not auto-detect delimiter — falling back to ','.")
            return ","
        
        logger.debug(f"Auto-detected delimiter: {repr(best)}")
        return best
        
    except Exception as e:
        logger.warning(f"Delimiter detection failed ({e}) — falling back to ','.")
        return ","


# ============================================================================
# PHONE NUMBER NORMALIZATION FOR ANDROID
# ============================================================================
def normalize_phone_for_android(phone: str) -> str:
    """
    Convert phone numbers to international format for better Android compatibility.
    Handles Iranian numbers (09, 989, etc.) and adds country code.
    
    Args:
        phone: Raw phone number string
        
    Returns:
        Normalized phone number in international format
    """
    if not phone:
        return ""
    
    # Remove all non-numeric characters except +
    cleaned = ''.join(c for c in phone if c in '0123456789+')
    
    # Handle Iranian phone numbers
    if cleaned.startswith('09'):
        # Convert 09xx to +989xx
        cleaned = '+98' + cleaned[1:]
    elif cleaned.startswith('989'):
        # Convert 989xx to +989xx
        cleaned = '+' + cleaned
    elif cleaned.startswith('0098'):
        # Convert 0098 to +98
        cleaned = '+98' + cleaned[4:]
    elif cleaned.startswith('9') and len(cleaned) == 10:
        # Handle 9xxxxxxxxx format (Iranian without 0)
        cleaned = '+98' + cleaned
    
    return cleaned


# ============================================================================
# COLUMN MAPPING
# ============================================================================
def normalize_headers(headers: List[str]) -> Dict[str, str]:
    """
    Build mapping from internal keys to actual CSV column names.
    Case-insensitive matching with support for Persian and English aliases.
    
    Args:
        headers: List of CSV header names
        
    Returns:
        Dictionary mapping internal keys to actual CSV column names
        
    Raises:
        NoUsableColumnsError: If no recognizable columns are found
    """
    if not headers:
        raise NoUsableColumnsError(
            "CSV file has no headers.\n"
            "  → The first row must contain column names."
        )
    
    # Create case-insensitive lookup dictionary
    headers_lower = {h.strip().lower(): h for h in headers if h.strip()}
    normalized: Dict[str, str] = {}
    
    # Match each internal key to its CSV column
    for key, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            if alias.lower() in headers_lower:
                normalized[key] = headers_lower[alias.lower()]
                break
    
    # Check if we have at least name and contact info
    has_name = any(k in normalized for k in ("first_name", "last_name", "full_name"))
    has_contact = any(k in normalized for k in ("phone", "email"))
    
    if not has_name and not has_contact:
        raise NoUsableColumnsError(
            f"No recognizable columns found in the CSV.\n"
            f"  CSV headers: {list(headers_lower.keys())}\n"
            f"  → Rename columns to match known names, e.g.:\n"
            f"    Name  : 'First Name', 'Last Name', 'Name', 'نام'\n"
            f"    Phone : 'Phone', 'Mobile', 'شماره'\n"
            f"    Email : 'Email', 'ایمیل'"
        )
    
    if not has_name:
        logger.warning("No name column detected — contacts will be labelled by phone/email.")
    if not has_contact:
        logger.warning("No phone or email column detected.")
    
    return normalized


# ============================================================================
# FIELD HELPER FUNCTIONS
# ============================================================================
def get_field(row: Dict, col_map: Dict[str, str], key: str) -> str:
    """
    Safely read and strip a field from a CSV row.
    
    Args:
        row: CSV row as dictionary
        col_map: Column mapping dictionary
        key: Internal field key
        
    Returns:
        Stripped field value or empty string if missing
    """
    col = col_map.get(key)
    if not col:
        return ""
    
    val = row.get(col, "")
    return str(val).strip() if val is not None else ""


def build_full_name(row: Dict, col_map: Dict[str, str]) -> Tuple[str, str, str]:
    """
    Build full name from available name columns.
    
    Args:
        row: CSV row as dictionary
        col_map: Column mapping dictionary
        
    Returns:
        Tuple of (full_name, first_name, last_name)
    """
    first = get_field(row, col_map, "first_name")
    last = get_field(row, col_map, "last_name")
    full = get_field(row, col_map, "full_name")
    
    # Build from first and last name
    if first or last:
        return f"{first} {last}".strip(), first, last
    
    # Use full name if available
    if full:
        parts = full.split(maxsplit=1)
        return full, parts[0], parts[1] if len(parts) > 1 else ""
    
    return "", "", ""


def sanitize_phone(phone: str) -> str:
    """
    Keep only valid phone characters and normalize for Android.
    
    Args:
        phone: Raw phone number
        
    Returns:
        Sanitized phone number
    """
    if not phone:
        return ""
    
    # Normalize for Android first
    phone = normalize_phone_for_android(phone)
    
    # Keep only valid phone characters
    allowed = set("0123456789+()-. ")
    cleaned = ''.join(ch for ch in phone if ch in allowed).strip()
    
    if phone and not cleaned:
        logger.debug(f"Phone '{phone}' was entirely stripped during sanitization.")
    
    return cleaned


def escape_vcard_text(text: str) -> str:
    """
    Escape special characters in vCard text fields per RFC 6350.
    
    Args:
        text: Text to escape
        
    Returns:
        Escaped text
    """
    return (
        text
        .replace("\\", "\\\\")
        .replace("\r\n", "\\n").replace("\n", "\\n").replace("\r", "\\n")
        .replace(",", "\\,")
        .replace(";", "\\;")
    )


# ============================================================================
# VCARD BUILDER
# ============================================================================
def row_to_vcard(
    row: Dict,
    col_map: Dict[str, str],
    row_num: int,
    vcard_version: str = "3.0"
) -> Optional[str]:
    """
    Convert one CSV row to a vCard string.
    
    Args:
        row: CSV row as dictionary
        col_map: Column mapping dictionary
        row_num: Row number (for logging)
        vcard_version: vCard version (3.0 or 4.0)
        
    Returns:
        vCard string or None if row has no usable data
        
    Note:
        Never raises exceptions - all errors are caught and logged internally
    """
    try:
        # Build name fields
        full_name, first, last = build_full_name(row, col_map)
        phone = sanitize_phone(get_field(row, col_map, "phone"))
        phone2 = sanitize_phone(get_field(row, col_map, "phone2"))
        email = get_field(row, col_map, "email")
        
        # Skip if no essential data
        if not full_name and not phone and not email:
            logger.debug(f"Row {row_num}: skipped — no name, phone, or email.")
            return None
        
        # Fallback display name when no name column exists
        if not full_name:
            full_name = phone or email
            first, last = full_name, ""
        
        # Get optional fields
        email2 = get_field(row, col_map, "email2")
        organization = get_field(row, col_map, "organization")
        title = get_field(row, col_map, "title")
        address = get_field(row, col_map, "address")
        city = get_field(row, col_map, "city")
        state = get_field(row, col_map, "state")
        zip_code = get_field(row, col_map, "zip_code")
        country = get_field(row, col_map, "country")
        website = get_field(row, col_map, "website")
        birthday = get_field(row, col_map, "birthday")
        note = get_field(row, col_map, "note")
        
        # Build vCard lines
        lines: List[str] = ["BEGIN:VCARD", f"VERSION:{vcard_version}"]
        
        # Name fields (N and FN are required by RFC 6350)
        lines.append(f"N:{escape_vcard_text(last)};{escape_vcard_text(first)};;;")
        lines.append(f"FN:{escape_vcard_text(full_name)}")
        
        # Organization and title
        if organization:
            lines.append(f"ORG:{escape_vcard_text(organization)}")
        if title:
            lines.append(f"TITLE:{escape_vcard_text(title)}")
        
        # Phone numbers - use Android-friendly format
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
        
        # Address (ADR format: PO Box;Extended;Street;City;State;ZIP;Country)
        if any([address, city, state, zip_code, country]):
            lines.append(
                f"ADR;TYPE=HOME:;;{escape_vcard_text(address)};"
                f"{escape_vcard_text(city)};{escape_vcard_text(state)};"
                f"{escape_vcard_text(zip_code)};{escape_vcard_text(country)}"
            )
        
        # Website - add https:// if no scheme present
        if website:
            if not website.startswith(("http://", "https://", "ftp://")):
                website = "https://" + website
            lines.append(f"URL:{website}")
        
        # Birthday
        if birthday:
            lines.append(f"BDAY:{birthday}")
        
        # Notes
        if note:
            lines.append(f"NOTE:{escape_vcard_text(note)}")
        
        lines.append("END:VCARD")
        
        # RFC 6350 mandates CRLF line endings
        return "\r\n".join(lines)
        
    except Exception as exc:
        # One bad row must never abort the entire conversion
        logger.warning(f"Row {row_num}: unexpected error — {exc}")
        logger.debug(traceback.format_exc())
        return None


# ============================================================================
# FILE WRITER
# ============================================================================
def _write_vcf(vcards: List[str], path: Path) -> None:
    """
    Write vCard list to a UTF-8 BOM .vcf file for Android compatibility.
    
    Args:
        vcards: List of vCard strings
        path: Output file path
        
    Raises:
        OutputFileError: With clear message on any write problem
    """
    try:
        # Use UTF-8 with BOM for better Android compatibility
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            f.write("\r\n\r\n".join(vcards))
            f.write("\r\n")  # trailing newline after last vCard
    except PermissionError:
        raise OutputFileError(
            f"Permission denied: cannot write to '{path}'\n"
            f"  → The file may be open in another program, or write-protected."
        )
    except OSError as e:
        if e.errno == 28:  # ENOSPC - Disk full
            raise OutputFileError(
                f"Disk full! Could not write to '{path}'.\n"
                f"  → Free up space and try again."
            )
        raise OutputFileError(f"Write failed for '{path}': {e}")


def _write_whatsapp_file(contacts: List[Dict], path: Path) -> str:
    """
    Create a WhatsApp share file with wa.me links for easy contact addition.
    
    Args:
        contacts: List of contact dictionaries with 'name' and 'phone'
        path: Output file path (will be modified to .whatsapp.txt)
        
    Returns:
        Path to the created WhatsApp file
    """
    whatsapp_path = path.with_suffix('.whatsapp.txt')
    
    try:
        with open(whatsapp_path, 'w', encoding='utf-8') as f:
            f.write("📱 WhatsApp Contact Links\n")
            f.write("=" * 60 + "\n\n")
            
            for contact in contacts:
                name = contact.get('name', '')
                phone = contact.get('phone', '')
                
                if phone:
                    # Clean phone number for wa.me link
                    clean_phone = ''.join(c for c in phone if c in '0123456789+')
                    
                    # Remove + for wa.me link
                    wa_phone = clean_phone.replace('+', '')
                    
                    f.write(f"👤 {name}\n")
                    f.write(f"📞 {clean_phone}\n")
                    f.write(f"🔗 https://wa.me/{wa_phone}\n")
                    f.write(f"   Click to chat on WhatsApp Web\n")
                    f.write("-" * 60 + "\n\n")
        
        logger.info(f"WhatsApp share file created: {whatsapp_path.name}")
        return str(whatsapp_path)
        
    except Exception as e:
        logger.warning(f"Failed to create WhatsApp file: {e}")
        return ""


# ============================================================================
# MAIN CONVERSION ENGINE
# ============================================================================
def convert_csv_to_vcf(
    input_path: str,
    output_path: str,
    encoding: str = "",
    delimiter: str = "",
    vcard_version: str = "3.0",
    chunk_size: int = 0,
    whatsapp: bool = False
) -> Dict:
    """
    Full conversion pipeline: validate → read → convert → write.
    
    Args:
        input_path: Path to input CSV file
        output_path: Path to output VCF file
        encoding: CSV encoding (empty = auto-detect)
        delimiter: CSV delimiter (empty = auto-detect)
        vcard_version: vCard version (3.0 or 4.0)
        chunk_size: Split output into files of N contacts (0 = single file)
        whatsapp: Create WhatsApp share file with wa.me links
        
    Returns:
        Summary dictionary with:
            - total_read: Total rows read
            - total_written: Total contacts written
            - total_skipped: Total rows skipped
            - output_files: List of output file paths
            - whatsapp_file: WhatsApp file path (if created)
    
    Raises:
        Various CSVtoVCFError subclasses for specific error conditions
    """
    src = Path(input_path)
    dst = Path(output_path)
    
    # Step 1: Validate input file
    validate_input_path(src)
    
    # Step 2: Resolve encoding
    if not encoding:
        logger.info("No encoding specified — auto-detecting...")
        encoding = detect_encoding(src)
    else:
        try:
            codecs.lookup(encoding)
        except LookupError:
            raise EncodingError(
                f"Unknown encoding: '{encoding}'\n"
                f"  → Valid examples: utf-8, utf-8-sig, cp1256, cp1252, latin-1"
            )
        logger.info(f"Using encoding: {encoding}")
    
    # Step 3: Resolve delimiter
    if not delimiter:
        logger.info("No delimiter specified — auto-detecting...")
        delimiter = detect_delimiter(src, encoding)
    elif len(delimiter) != 1:
        raise InputFileError(
            f"Delimiter must be exactly one character, got: {repr(delimiter)}"
        )
    
    # Step 4: Prepare output path
    if dst.is_dir() or not dst.suffix:
        dst = dst / f"{src.stem}.vcf"
    validate_output_path(dst)
    
    logger.info(
        f"Input: {src}  |  Output: {dst}  |  "
        f"Encoding: {encoding}  |  Delimiter: {repr(delimiter)}  |  "
        f"vCard: {vcard_version}"
    )
    
    # Step 5: Open CSV file
    try:
        csv_file = open(src, newline="", encoding=encoding, errors="replace")
    except OSError as e:
        raise InputFileError(f"Cannot open '{src}': {e}")
    
    vcards: List[str] = []
    contacts_for_whatsapp: List[Dict] = []
    total_read = 0
    total_skipped = 0
    skipped_rows: List[int] = []
    
    with csv_file:
        # Step 6: Build DictReader
        try:
            reader = csv.DictReader(csv_file, delimiter=delimiter)
        except csv.Error as e:
            raise InputFileError(
                f"Failed to parse CSV structure: {e}\n"
                f"  → Try specifying the delimiter manually: --delimiter ';'"
            )
        
        # Step 7: Read headers
        try:
            headers = list(reader.fieldnames or [])
        except csv.Error as e:
            raise InputFileError(f"Cannot read CSV headers: {e}")
        
        col_map = normalize_headers(headers)
        logger.info(f"Recognized fields: {list(col_map.keys())}")
        
        # Step 8: Process every row
        for row_num, row in enumerate(reader, start=2):
            total_read += 1
            
            # Warn on column count mismatches
            actual = len([v for v in row.values() if v is not None])
            if actual != len(headers):
                logger.debug(
                    f"Row {row_num}: column count mismatch "
                    f"(expected {len(headers)}, got {actual}) — data may be misaligned."
                )
            
            # Convert row to vCard
            vcard = row_to_vcard(row, col_map, row_num, vcard_version)
            if vcard:
                vcards.append(vcard)
                
                # Collect contact info for WhatsApp file
                if whatsapp:
                    full_name, _, _ = build_full_name(row, col_map)
                    phone = sanitize_phone(get_field(row, col_map, "phone"))
                    
                    if phone:
                        contacts_for_whatsapp.append({
                            'name': full_name or phone,
                            'phone': phone
                        })
            else:
                total_skipped += 1
                skipped_rows.append(row_num)
    
    # Step 9: Sanity checks before writing
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
        logger.info(f"{len(skipped_rows)} empty/invalid rows skipped (see {LOG_FILE} for details).")
        logger.debug(f"Skipped row numbers: {skipped_rows}")
    
    logger.info(f"Read: {total_read}  |  To write: {total_written}  |  Skipped: {total_skipped}")
    
    # Step 10: Write VCF output
    output_files: List[str] = []
    
    if chunk_size > 0 and total_written > chunk_size:
        # Split into numbered files (useful when phone apps have import limits)
        chunks = [vcards[i:i + chunk_size] for i in range(0, total_written, chunk_size)]
        for idx, chunk in enumerate(chunks, start=1):
            chunk_path = dst.parent / f"{dst.stem}_part{idx:03d}{dst.suffix}"
            _write_vcf(chunk, chunk_path)
            output_files.append(str(chunk_path))
            logger.info(f"  → {chunk_path.name}  ({len(chunk)} contacts)")
    else:
        # Single file (handles 10,000+ contacts without issues)
        _write_vcf(vcards, dst)
        output_files.append(str(dst))
        logger.info(f"  → {dst.name}  ({total_written} contacts)")
    
    # Step 11: Create WhatsApp share file if requested
    whatsapp_file = ""
    if whatsapp and contacts_for_whatsapp:
        whatsapp_file = _write_whatsapp_file(contacts_for_whatsapp, dst)
        if whatsapp_file:
            output_files.append(whatsapp_file)
    
    return {
        "total_read": total_read,
        "total_written": total_written,
        "total_skipped": total_skipped,
        "output_files": output_files,
        "whatsapp_file": whatsapp_file
    }


# ============================================================================
# SAMPLE CSV GENERATOR
# ============================================================================
def generate_sample_csv(output_path: str = "sample_contacts.csv") -> None:
    """
    Create a realistic sample CSV for testing (includes intentionally tricky rows).
    
    Args:
        output_path: Path to save sample CSV
        
    Raises:
        OutputFileError: If file cannot be written
    """
    rows = [
        # Header row
        ["First Name", "Last Name", "Phone", "Phone 2", "Email",
         "Organization", "Title", "Address", "City", "Country", "Birthday", "Note"],
        # Sample contacts
        ["Ali", "Ahmadi", "+98-912-111-2233", "+98-21-44556677",
         "ali@example.com", "Tech Co", "Developer", "Valiasr St", "Tehran", "Iran",
         "1990-05-15", "VIP customer"],
        ["Sara", "Hosseini", "+98-935-999-8877", "",
         "sara@company.ir", "Design Studio", "UI Designer", "Hafez Ave", "Isfahan", "Iran",
         "1995-11-20", ""],
        ["John", "Smith", "+1-310-555-0100", "+1-310-555-0101",
         "john@mail.com", "Global Corp", "Manager", "123 Main St", "Los Angeles", "USA",
         "1985-03-22", "Conference lead"],
        # Intentionally empty row (should be skipped)
        ["", "", "", "", "", "", "", "", "", "", "", ""],
        ["Maria", "Garcia", "+34-600-123-456", "",
         "maria@empresa.es", "Empresa SL", "CEO", "Calle Mayor", "Madrid", "Spain",
         "", "Long-time partner; important,client"],
    ]
    
    try:
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows(rows)
        logger.info(f"Sample CSV created: {output_path}")
    except PermissionError:
        raise OutputFileError(
            f"Cannot write sample to '{output_path}' — permission denied.\n"
            f"  → Try a different location, e.g.: --output /tmp/sample.csv"
        )
    except OSError as e:
        raise OutputFileError(f"Cannot write sample CSV: {e}")


# ============================================================================
# COMMAND-LINE INTERFACE
# ============================================================================
def build_arg_parser() -> argparse.ArgumentParser:
    """
    Build and configure the argument parser for command-line usage.
    
    Returns:
        Configured ArgumentParser instance
    """
    parser = argparse.ArgumentParser(
        prog="csv_to_vcf",
        description="Convert CSV contacts to VCF (vCard) format with Android and WhatsApp support.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python csv_to_vcf.py contacts.csv
  python csv_to_vcf.py contacts.csv -o output/contacts.vcf
  python csv_to_vcf.py contacts.csv --version 4.0
  python csv_to_vcf.py contacts.csv --chunk 500
  python csv_to_vcf.py contacts.csv --encoding cp1256 --delimiter ";"
  python csv_to_vcf.py contacts.csv --whatsapp
  python csv_to_vcf.py --sample

Exit codes:
  0   Success
  2   Input file error
  3   Output file error
  4   Encoding error
  5   Empty file
  6   No usable columns
  7   Interrupted by user
  99  Unexpected error (see csv_to_vcf.log)
"""
    )
    
    parser.add_argument("input", nargs="?", help="Input CSV file path")
    parser.add_argument("-o", "--output", default="", help="Output VCF path")
    parser.add_argument(
        "--version", choices=["3.0", "4.0"], default="3.0",
        help="vCard version (default: 3.0)"
    )
    parser.add_argument(
        "--delimiter", default="",
        help="CSV delimiter (default: auto-detect)"
    )
    parser.add_argument(
        "--encoding", default="",
        help="CSV encoding (default: auto-detect)"
    )
    parser.add_argument(
        "--chunk", type=int, default=0, metavar="N",
        help="Split output into files of N contacts each"
    )
    parser.add_argument(
        "--whatsapp", action="store_true",
        help="Also create WhatsApp share file with wa.me links"
    )
    parser.add_argument(
        "--sample", action="store_true",
        help="Generate a sample CSV for testing and exit"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Show debug-level output"
    )
    
    return parser


def main() -> None:
    """
    Main entry point for command-line execution.
    Handles argument parsing, conversion, and error reporting.
    """
    parser = build_arg_parser()
    args = parser.parse_args()
    
    # Enable verbose logging if requested
    if args.verbose:
        console_handler.setLevel(logging.DEBUG)
    
    # Sample mode: generate sample CSV and exit
    if args.sample:
        try:
            generate_sample_csv("sample_contacts.csv")
            print("\n✓ sample_contacts.csv created.")
            print("  Run: python csv_to_vcf.py sample_contacts.csv\n")
        except CSVtoVCFError as e:
            print(f"\n✗ Error: {e}\n", file=sys.stderr)
            sys.exit(3)
        sys.exit(0)
    
    # Show help if no input file provided
    if not args.input:
        parser.print_help()
        sys.exit(1)
    
    src = Path(args.input)
    dst = args.output or str(src.with_suffix(".vcf"))
    
    # Run conversion
    start = datetime.now()
    try:
        result = convert_csv_to_vcf(
            input_path=str(src),
            output_path=dst,
            encoding=args.encoding,
            delimiter=args.delimiter,
            vcard_version=args.version,
            chunk_size=args.chunk,
            whatsapp=args.whatsapp
        )
    
    # Handle specific error types with appropriate exit codes
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
        print("\n\n⚠ Interrupted by user.\n", file=sys.stderr)
        sys.exit(7)
    except Exception as e:
        # Unexpected bug - full traceback goes to log, summary to terminal
        logger.debug(traceback.format_exc())
        print(
            f"\n✗ Unexpected error: {e}\n"
            f"  → Full traceback saved to: {LOG_FILE}\n"
            f"  → Please report this issue.\n",
            file=sys.stderr,
        )
        sys.exit(99)
    
    # Success summary
    elapsed = (datetime.now() - start).total_seconds()
    
    print("\n" + "═" * 52)
    print("  ✓ CSV → VCF Conversion Complete")
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


# ============================================================================
# SCRIPT ENTRY POINT
# ============================================================================
if __name__ == "__main__":
    main()