#!/usr/bin/env python3
"""Usage: python3 analyze.py results/some_run.csv"""
import csv
import sys
from collections import defaultdict


def parse_cpu(value: str) -> float:
    return float(value.strip().strip("%"))


def parse_mem(value: str) -> float:
    value = value.strip()
    if value.endswith("GiB"):
        return float(value[:-3]) * 1024
    if value.endswith("MiB"):
        return float(value[:-3])
    if value.endswith("KiB"):
        return float(value[:-3]) / 1024
    if value.endswith("B"):
        return float(value[:-1]) / (1024 * 1024)
    return 0.0


def main(path: str) -> None:
    by_container = defaultdict(list)
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            cpu = parse_cpu(row["cpu_perc"])
            mem = parse_mem(row["mem_usage"].split("/")[0])
            by_container[row["container"]].append((cpu, mem))

    print(f"\nSummary for {path}\n" + "-" * 60)
    for container, samples in sorted(by_container.items()):
        cpus = [c for c, _ in samples]
        mems = [m for _, m in samples]
        print(
            f"{container:30s} "
            f"avg_cpu={sum(cpus)/len(cpus):6.2f}%  peak_cpu={max(cpus):6.2f}%  "
            f"avg_mem={sum(mems)/len(mems):7.1f}MiB  peak_mem={max(mems):7.1f}MiB  "
            f"(n={len(samples)})"
        )
    print()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 analyze.py <csv_path>")
        sys.exit(1)
    main(sys.argv[1])