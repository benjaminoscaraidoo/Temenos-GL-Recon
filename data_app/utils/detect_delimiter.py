import csv

def detect_delimiter(file_path):
    """
    Auto-detect delimiter: ',', '|', or ';'
    Reads only a small sample for performance (important for multi-GB files).
    """

    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        sample_size=5000
        sample = f.read(sample_size)

    sniffer = csv.Sniffer()

    try:
        dialect = sniffer.sniff(sample, delimiters=[",", "|", ";"])
        return dialect.delimiter
    except Exception:
        # fallback (safe default for banking files)
        return "|"