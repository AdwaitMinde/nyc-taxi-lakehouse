import argparse
import sys
import time
from datetime import date
from pathlib import Path

import requests

CDN_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DATASETS = ["yellow", "green", "fhv", "fhvhv"]
DEFAULT_MONTHS = 12
TIMEOUT = 30
MAX_RETRIES = 3
RETRY_BACKOFF_SEC = 5


def month_iter_backward(start_year: int, start_month: int, count: int):
    """Yield (year, month) tuples walking backward from start, `count` times."""
    y, m = start_year, start_month
    for _ in range(count):
        yield y, m
        m -= 1
        if m == 0:
            m = 12
            y -= 1


def url_for(dataset: str, year: int, month: int) -> str:
    return f"{CDN_BASE}/{dataset}_tripdata_{year:04d}-{month:02d}.parquet"


def head_exists(url: str) -> bool:
    try:
        resp = requests.head(url, timeout=TIMEOUT, allow_redirects=True)
        return resp.status_code == 200
    except requests.RequestException:
        return False


def find_latest_available_month(probe_dataset: str = "yellow") -> tuple[int, int]:
    """
    TLC publishes with ~2 month lag. Start probing from (today - 2 months)
    and walk backward until a file actually exists on the CDN.
    """
    today = date.today()
    y, m = today.year, today.month
    # jump back 2 months as the expected starting point
    for _ in range(2):
        m -= 1
        if m == 0:
            m = 12
            y -= 1

    # walk backward up to 6 months looking for the first real file
    for y2, m2 in month_iter_backward(y, m, 6):
        url = url_for(probe_dataset, y2, m2)
        print(f"  probing {url} ...", end=" ")
        if head_exists(url):
            print("found")
            return y2, m2
        print("not yet published")
    raise RuntimeError(
        "Could not find a published month in the last 6 candidates. "
        "TLC's publishing schedule may have shifted -- check "
        "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page manually."
    )


def download_file(url: str, dest: Path, dry_run: bool = False) -> bool:
    if dest.exists() and dest.stat().st_size > 0:
        print(f"  already have {dest.name}, skipping")
        return True

    if dry_run:
        print(f"  [dry-run] would download {url}")
        return True

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with requests.get(url, stream=True, timeout=TIMEOUT) as resp:
                if resp.status_code == 404:
                    print(f"  404 not found: {url}")
                    return False
                resp.raise_for_status()
                tmp_path = dest.with_suffix(dest.suffix + ".part")
                with open(tmp_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        f.write(chunk)
                tmp_path.rename(dest)
                size_mb = dest.stat().st_size / (1024 * 1024)
                print(f"  downloaded {dest.name} ({size_mb:.1f} MB)")
                return True
        except requests.RequestException as e:
            print(f"  attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SEC)
    print(f"  giving up on {url} after {MAX_RETRIES} attempts")
    return False


def main():
    parser = argparse.ArgumentParser(description="Download NYC TLC trip data to local disk")
    parser.add_argument("--months", type=int, default=DEFAULT_MONTHS,
                         help=f"Number of months to pull (default {DEFAULT_MONTHS})")
    parser.add_argument("--datasets", type=str, default=",".join(DATASETS),
                         help="Comma-separated dataset list (yellow,green,fhv,fhvhv)")
    parser.add_argument("--out", type=str, default="../data/raw",
                         help="Output directory (default ../data/raw relative to script)")
    parser.add_argument("--dry-run", action="store_true",
                         help="Print what would be downloaded without downloading")
    args = parser.parse_args()

    datasets = [d.strip() for d in args.datasets.split(",") if d.strip()]
    invalid = [d for d in datasets if d not in DATASETS]
    if invalid:
        print(f"Unknown dataset(s): {invalid}. Valid options: {DATASETS}")
        sys.exit(1)

    out_dir = Path(__file__).parent / args.out
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Determining latest available month on TLC CDN...")
    latest_y, latest_m = find_latest_available_month()
    print(f"Latest available month: {latest_y:04d}-{latest_m:02d}\n")

    months = list(month_iter_backward(latest_y, latest_m, args.months))
    months.reverse()  # oldest first, nicer log order

    print(f"Will fetch {len(months)} months x {len(datasets)} datasets "
          f"= {len(months) * len(datasets)} files")
    print(f"Range: {months[0][0]:04d}-{months[0][1]:02d} through "
          f"{months[-1][0]:04d}-{months[-1][1]:02d}\n")

    results = {"ok": [], "missing": []}

    for dataset in datasets:
        dataset_dir = out_dir / dataset
        dataset_dir.mkdir(parents=True, exist_ok=True)
        print(f"=== {dataset} ===")
        for y, m in months:
            url = url_for(dataset, y, m)
            dest = dataset_dir / f"{dataset}_tripdata_{y:04d}-{m:02d}.parquet"
            ok = download_file(url, dest, dry_run=args.dry_run)
            (results["ok"] if ok else results["missing"]).append(f"{dataset} {y:04d}-{m:02d}")
        print()

    print("=== Summary ===")
    print(f"Succeeded: {len(results['ok'])}")
    print(f"Missing/failed: {len(results['missing'])}")
    if results["missing"]:
        print("Missing files:")
        for item in results["missing"]:
            print(f"  - {item}")
    print(f"\nFiles saved under: {out_dir.resolve()}")


if __name__ == "__main__":
    main()
