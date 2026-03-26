"""Download all historically available 10-K and 10-Q filings from SEC EDGAR."""

import argparse
import json
import ssl
import time
from pathlib import Path

import httpx

SSL_CONTEXT = ssl.create_default_context()

BASE_URL = "https://data.sec.gov"
ARCHIVES_URL = "https://www.sec.gov/Archives/edgar/data"
FILING_TYPES = {"10-K", "10-Q"}
HEADERS = {
    "User-Agent": "SECFilingsDownloader research@example.com",
    "Accept-Encoding": "gzip, deflate",
}
# SEC asks for max 10 req/sec; we stay well under that
REQUEST_DELAY = 0.15


def pad_cik(cik: str) -> str:
    """Pad CIK to 10 digits with leading zeros."""
    return cik.lstrip("0").zfill(10)


def get_submissions(cik: str) -> dict:
    """Fetch the full submissions JSON, including paginated history."""
    url = f"{BASE_URL}/submissions/CIK{pad_cik(cik)}.json"
    resp = httpx.get(url, headers=HEADERS, verify=SSL_CONTEXT, timeout=30)
    resp.raise_for_status()
    return resp.json()


def collect_filings(submissions: dict) -> list[dict]:
    """Extract all 10-K and 10-Q filing metadata from submissions data."""
    filings: list[dict] = []

    def extract_from_block(block: dict) -> None:
        forms = block["form"]
        accession_numbers = block["accessionNumber"]
        primary_documents = block["primaryDocument"]
        filing_dates = block["filingDate"]
        for i, form in enumerate(forms):
            if form in FILING_TYPES:
                filings.append(
                    {
                        "form": form,
                        "accessionNumber": accession_numbers[i],
                        "primaryDocument": primary_documents[i],
                        "filingDate": filing_dates[i],
                    }
                )

    # Recent filings
    extract_from_block(submissions["filings"]["recent"])

    # Older filings in paginated files
    for file_ref in submissions["filings"]["files"]:
        filename = file_ref["name"]
        url = f"{BASE_URL}/submissions/{filename}"
        print(f"  Fetching older filings index: {filename}")
        time.sleep(REQUEST_DELAY)
        resp = httpx.get(url, headers=HEADERS, verify=SSL_CONTEXT, timeout=30)
        resp.raise_for_status()
        extract_from_block(resp.json())

    return filings


def resolve_document_url(filing: dict, cik: str) -> tuple[str, str]:
    """Return (url, extension) for the actual filing document.

    For older filings where primaryDocument is empty, fall back to the
    full submission text file ({accession}.txt).
    """
    accession_no_dashes = filing["accessionNumber"].replace("-", "")
    cik_num = cik.lstrip("0")
    base = f"{ARCHIVES_URL}/{cik_num}/{accession_no_dashes}"

    primary = filing["primaryDocument"]
    if primary:
        ext = Path(primary).suffix or ".html"
        return f"{base}/{primary}", ext

    # Old filings: no primaryDocument, download the full submission text
    return f"{base}/{filing['accessionNumber']}.txt", ".txt"


def download_filing(
    filing: dict, cik: str, output_dir: Path, client: httpx.Client
) -> None:
    """Download a single filing document."""
    doc_url, ext = resolve_document_url(filing, cik)

    form_dir = output_dir / filing["form"]
    form_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{filing['filingDate']}_{filing['accessionNumber']}{ext}"
    dest = form_dir / filename

    if dest.exists():
        print(f"  Skipping (exists): {dest}")
        return

    for attempt in range(3):
        time.sleep(REQUEST_DELAY)
        try:
            resp = client.get(doc_url, timeout=120)
            resp.raise_for_status()
            break
        except httpx.TimeoutException:
            if attempt < 2:
                print(f"  Timeout, retrying ({attempt + 2}/3)...")
                time.sleep(2)
            else:
                raise

    dest.write_bytes(resp.content)
    print(f"  Downloaded: {dest} ({len(resp.content):,} bytes)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download 10-K and 10-Q filings from SEC EDGAR"
    )
    parser.add_argument("cik", help="CIK number of the company (e.g. 0000200406)")
    parser.add_argument("output_dir", help="Output directory for downloaded filings")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cik = args.cik
    output_dir = Path(args.output_dir)

    padded = pad_cik(cik)
    print(f"Fetching filing index for CIK {padded}...")
    submissions = get_submissions(cik)
    company_name = submissions.get("name", "Unknown")
    print(f"Company: {company_name}")

    print("Collecting 10-K and 10-Q filings...")
    filings = collect_filings(submissions)

    # Sort by date
    filings.sort(key=lambda f: f["filingDate"])

    # Summary
    count_10k = sum(1 for f in filings if f["form"] == "10-K")
    count_10q = sum(1 for f in filings if f["form"] == "10-Q")
    print(f"\nFound {len(filings)} filings: {count_10k} 10-K, {count_10q} 10-Q")
    if filings:
        print(f"Date range: {filings[0]['filingDate']} to {filings[-1]['filingDate']}")

    # Save metadata
    output_dir.mkdir(parents=True, exist_ok=True)
    meta_path = output_dir / "filings_metadata.json"
    meta_path.write_text(json.dumps(filings, indent=2))
    print(f"Metadata saved to {meta_path}")

    # Download
    print("\nDownloading filings...")
    with httpx.Client(
        headers=HEADERS, verify=SSL_CONTEXT, follow_redirects=True
    ) as client:
        for i, filing in enumerate(filings, 1):
            print(f"[{i}/{len(filings)}] {filing['form']} - {filing['filingDate']}")
            try:
                download_filing(filing, cik, output_dir, client)
            except (httpx.HTTPStatusError, httpx.TimeoutException) as e:
                print(f"  ERROR: {e}")

    print("\nDone!")


if __name__ == "__main__":
    main()
