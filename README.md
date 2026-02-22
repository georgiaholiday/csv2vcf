# csv_to_vcf 📇

A professional-grade Python tool to convert CSV contact files into VCF (vCard) format.  
Handles **1000+ contacts** in a single batch, supports both **Persian and English** column names, and provides detailed, actionable error messages for every failure case.

---

## Features

- ✅ Converts any CSV file to a standard `.vcf` vCard file
- ✅ Supports **vCard 3.0 and 4.0**
- ✅ Auto-detects file **encoding** (`utf-8`, `cp1256`, `cp1252`, etc.)
- ✅ Auto-detects CSV **delimiter** (`,` `;` `\t` `|`)
- ✅ Recognizes column names in **Persian and English**
- ✅ Optional **chunked output** (split into files of N contacts)
- ✅ Full **error handling** — bad rows are skipped, never crash the whole run
- ✅ Detailed **log file** (`csv_to_vcf.log`) for debugging
- ✅ Zero external dependencies — standard library only

---

## Requirements

- Python **3.10** or higher
- No pip installs needed

---

## Installation

```bash
git clone https://github.com/yourname/csv_to_vcf.git
cd csv_to_vcf
```

That's it. No virtual environment or dependencies required.

---

## Quick Start

```bash
# Generate a sample CSV to test with
python csv_to_vcf.py --sample

# Convert it
python csv_to_vcf.py sample_contacts.csv
```

Output: `sample_contacts.vcf` in the same directory.

---

## Usage

```
python csv_to_vcf.py [input] [options]
```

### Arguments

| Argument | Description |
|---|---|
| `input` | Path to the source CSV file |
| `-o`, `--output` | Output VCF path (default: same folder as input) |
| `--version` | vCard version: `3.0` (default) or `4.0` |
| `--encoding` | CSV encoding (default: auto-detect) |
| `--delimiter` | CSV column delimiter (default: auto-detect) |
| `--chunk N` | Split output into files of N contacts each |
| `--sample` | Generate a sample CSV and exit |
| `-v`, `--verbose` | Show debug-level output in terminal |

### Examples

```bash
# Basic — output next to input file
python csv_to_vcf.py contacts.csv

# Custom output path
python csv_to_vcf.py contacts.csv -o exports/my_contacts.vcf

# Use vCard 4.0 format
python csv_to_vcf.py contacts.csv --version 4.0

# Split into 500-contact chunks (useful for iOS/Android import limits)
python csv_to_vcf.py contacts.csv --chunk 500

# Farsi Excel file (Windows encoding, semicolon delimiter)
python csv_to_vcf.py contacts.csv --encoding cp1256 --delimiter ";"

# Show all debug info in terminal
python csv_to_vcf.py contacts.csv -v
```

---

## CSV Format

The first row must be a header row. Column names are matched **case-insensitively** and support common variations in both **Persian** and **English**.

### Supported Columns

| Field | Accepted Column Names |
|---|---|
| First Name | `First Name`, `firstname`, `fname`, `نام` |
| Last Name | `Last Name`, `lastname`, `lname`, `نام خانوادگی`, `family` |
| Full Name | `Name`, `Full Name`, `fullname`, `نام کامل` |
| Phone | `Phone`, `Mobile`, `Cell`, `Tel`, `شماره`, `شماره موبایل` |
| Phone 2 | `Phone 2`, `phone2`, `Home Phone`, `Secondary Phone` |
| Email | `Email`, `e-mail`, `mail`, `ایمیل` |
| Email 2 | `Email 2`, `email2`, `Secondary Email` |
| Organization | `Organization`, `Company`, `org`, `شرکت`, `سازمان` |
| Job Title | `Title`, `Job Title`, `Position`, `Role`, `عنوان شغلی` |
| Address | `Address`, `Street`, `آدرس` |
| City | `City`, `شهر` |
| State / Province | `State`, `Province`, `استان` |
| ZIP / Postal Code | `Zip`, `Postal Code`, `کد پستی` |
| Country | `Country`, `کشور` |
| Website | `Website`, `URL`, `web`, `وبسایت` |
| Birthday | `Birthday`, `Birth Date`, `dob`, `تولد` |
| Note | `Note`, `Notes`, `memo`, `یادداشت` |

### Minimal Example

```csv
First Name,Last Name,Phone,Email
Ali,Ahmadi,+98-912-111-2233,ali@example.com
Sara,Hosseini,+98-935-999-8877,sara@company.ir
```

### Full Example

```csv
First Name,Last Name,Phone,Phone 2,Email,Organization,Title,Address,City,Country,Birthday,Note
Ali,Ahmadi,+98-912-111-2233,+98-21-44556677,ali@example.com,Tech Co,Developer,Valiasr St,Tehran,Iran,1990-05-15,VIP customer
```

> Only the columns you include are used. All others are safely ignored.

---

## Error Handling

Every error produces a clear message with a suggested fix. Bad rows are skipped individually — they never abort the full conversion.

| Situation | Behaviour |
|---|---|
| File not found | `InputFileError` with exact path |
| No read permission | `InputFileError` with chmod hint |
| File is 0 bytes | `EmptyFileError` |
| Unknown encoding | `EncodingError` with encoding list |
| No recognizable columns | `NoUsableColumnsError` with column list |
| All rows empty | `EmptyFileError` with problematic row numbers |
| Disk full on write | `OutputFileError` |
| No write permission | `OutputFileError` with path |
| Individual bad row | Logged and skipped — conversion continues |
| Unexpected bug | Full traceback written to `csv_to_vcf.log` |

### Exit Codes

Useful when running from scripts or CI pipelines.

| Code | Meaning |
|---|---|
| `0` | Success |
| `2` | Input file error |
| `3` | Output file error |
| `4` | Encoding error |
| `5` | Empty file or all rows skipped |
| `6` | No usable columns found |
| `7` | Interrupted by user (Ctrl+C) |
| `99` | Unexpected internal error |

---

## Output

### Single File (default)

```
contacts.vcf
```

### Chunked Files (`--chunk 500`)

```
contacts_part001.vcf   (500 contacts)
contacts_part002.vcf   (500 contacts)
contacts_part003.vcf   (remaining contacts)
```

### Log File

Every run writes to `csv_to_vcf.log` in the working directory:

```
[08:41:31] INFO     Detected encoding: utf-8-sig
[08:41:31] INFO     Auto-detected delimiter: ','
[08:41:31] INFO     Recognized fields: ['first_name', 'last_name', 'phone', ...]
[08:41:31] INFO     1 empty/invalid rows skipped (see csv_to_vcf.log)
[08:41:31] INFO     Read: 5  |  To write: 4  |  Skipped: 1
```

---

## vCard Output Format

Each contact is written as a standard vCard block, compatible with Android, iOS, Outlook, Google Contacts, and Thunderbird.

```
BEGIN:VCARD
VERSION:3.0
N:Ahmadi;Ali;;;
FN:Ali Ahmadi
ORG:Tech Co
TITLE:Developer
TEL;TYPE=CELL:+98-912-111-2233
TEL;TYPE=HOME:+98-21-44556677
EMAIL;TYPE=INTERNET:ali@example.com
ADR;TYPE=HOME:;;Valiasr St;Tehran;;;Iran
BDAY:1990-05-15
NOTE:VIP customer
END:VCARD
```

---

## Common Issues

**My Farsi CSV shows garbled characters**
```bash
python csv_to_vcf.py contacts.csv --encoding cp1256
```

**My CSV uses semicolons instead of commas**
```bash
python csv_to_vcf.py contacts.csv --delimiter ";"
```

**Some contacts are missing from the output**  
Run with `-v` to see which rows were skipped and why:
```bash
python csv_to_vcf.py contacts.csv -v
```
Also check `csv_to_vcf.log` for the exact row numbers.

**I want to import into an app that limits contacts per file**
```bash
python csv_to_vcf.py contacts.csv --chunk 200
```

---

## License

MIT — free to use, modify, and distribute.