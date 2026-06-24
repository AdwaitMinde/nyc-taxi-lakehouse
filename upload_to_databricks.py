import argparse
import subprocess
import sys
from pathlib import Path

DATASETS = ["yellow", "green", "fhv", "fhvhv"]
VOLUME_PATH = "dbfs:/Volumes/nyc_taxi/raw/trip_data"


def run_cmd(cmd: list[str], dry_run: bool = False) -> bool:
    printable = " ".join(cmd)
    print(f"  $ {printable}")
    if dry_run:
        return True
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  FAILED (exit {result.returncode})")
        if result.stdout.strip():
            print(f"  stdout: {result.stdout.strip()}")
        if result.stderr.strip():
            print(f"  stderr: {result.stderr.strip()}")
        return False
    if result.stdout.strip():
        print(f"  {result.stdout.strip()}")
    return True


def main():
    parser = argparse.ArgumentParser(description="Upload local TLC parquet files to a Unity Catalog volume")
    parser.add_argument("--profile", type=str, required=True,
                         help="Databricks CLI auth profile name (e.g. adwait1301)")
    parser.add_argument("--local-root", type=str, default="../data/raw",
                         help="Local directory containing dataset subfolders (default ../data/raw)")
    parser.add_argument("--volume-path", type=str, default=VOLUME_PATH,
                         help=f"Destination volume path (default {VOLUME_PATH})")
    parser.add_argument("--datasets", type=str, default=",".join(DATASETS),
                         help="Comma-separated dataset list to upload")
    parser.add_argument("--dry-run", action="store_true",
                         help="Print commands without executing them")
    args = parser.parse_args()

    datasets = [d.strip() for d in args.datasets.split(",") if d.strip()]
    local_root = Path(__file__).parent / args.local_root

    if not local_root.exists():
        print(f"Local root not found: {local_root.resolve()}")
        print("Did you run download_tlc_data.py first?")
        sys.exit(1)

    print(f"Databricks CLI profile: {args.profile}")
    print(f"Local source root:      {local_root.resolve()}")
    print(f"Destination volume:     {args.volume_path}\n")

    results = {"ok": [], "failed": []}

    for dataset in datasets:
        local_dir = local_root / dataset
        if not local_dir.exists():
            print(f"=== {dataset}: skipped, no local folder at {local_dir} ===\n")
            results["failed"].append(dataset)
            continue

        file_count = len(list(local_dir.glob("*.parquet")))
        print(f"=== {dataset}: {file_count} file(s) ===")

        dest = f"{args.volume_path}/{dataset}"
        cmd = [
            "databricks", "fs", "cp",
            "-r",
            str(local_dir),
            dest,
            "--overwrite",
            "--profile", args.profile,
        ]
        ok = run_cmd(cmd, dry_run=args.dry_run)
        (results["ok"] if ok else results["failed"]).append(dataset)
        print()

    print("=== Summary ===")
    print(f"Uploaded: {results['ok']}")
    if results["failed"]:
        print(f"Failed/skipped: {results['failed']}")
        sys.exit(1)
    else:
        print(f"\nAll datasets uploaded under {args.volume_path}")
        print("Verify in the Databricks UI: Catalog > nyc_taxi > raw > trip_data")


if __name__ == "__main__":
    main()