#!/usr/bin/env python3
"""Compare PFS handler traversal performance across two amifuse checkouts."""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import sys
import time
import types
from pathlib import Path


def _walk_tree(bridge):
    dir_paths = []
    file_entries = []
    stack = ["/"]
    while stack:
        current = stack.pop()
        entries = bridge.list_dir_path(current)
        dir_names = []
        for entry in entries:
            child = "/" + entry["name"] if current == "/" else current + "/" + entry["name"]
            if entry["dir_type"] > 0:
                dir_paths.append(child)
                dir_names.append(child)
            else:
                file_entries.append(
                    {
                        "path": child,
                        "size": int(entry.get("size", 0)),
                    }
                )
        stack.extend(reversed(sorted(dir_names)))
    file_entries.sort(key=lambda item: item["path"])
    return dir_paths, file_entries


def _run_worker(image: str, driver: str, read_size: int, read_count: int):
    repo_root = Path.cwd()
    amitools_root = repo_root / "amitools"
    if str(amitools_root) not in sys.path:
        sys.path.insert(0, str(amitools_root))
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    if "fuse" not in sys.modules:
        fake_fuse = types.ModuleType("fuse")

        class _DummyOperations:
            pass

        class _DummyLoggingMixIn:
            pass

        class _DummyFuseError(RuntimeError):
            pass

        fake_fuse.FUSE = object
        fake_fuse.FuseOSError = _DummyFuseError
        fake_fuse.LoggingMixIn = _DummyLoggingMixIn
        fake_fuse.Operations = _DummyOperations
        sys.modules["fuse"] = fake_fuse
    from amifuse.fuse_fs import HandlerBridge

    start = time.perf_counter()
    bridge = HandlerBridge(Path(image), Path(driver))
    init_s = time.perf_counter() - start

    start = time.perf_counter()
    dir_paths, file_entries = _walk_tree(bridge)
    walk_s = time.perf_counter() - start

    sample_files = file_entries[:read_count]
    sample_bytes = 0
    start = time.perf_counter()
    for entry in sample_files:
        sample_bytes += len(bridge.read_file(entry["path"], min(entry["size"], read_size), 0))
    read_s = time.perf_counter() - start

    return {
        "dir_count": len(dir_paths),
        "file_count": len(file_entries),
        "init_s": init_s,
        "walk_s": walk_s,
        "read_s": read_s,
        "total_s": init_s + walk_s + read_s,
        "sample_bytes": sample_bytes,
        "sample_files": [entry["path"] for entry in sample_files],
    }


def _summarize(name: str, repo: Path, runs):
    fields = ("init_s", "walk_s", "read_s", "total_s")
    summary = {
        "name": name,
        "repo": str(repo.resolve()),
        "repeats": len(runs),
        "dir_count": runs[0]["dir_count"],
        "file_count": runs[0]["file_count"],
        "sample_bytes": runs[0]["sample_bytes"],
        "sample_files": runs[0]["sample_files"],
        "runs": runs,
    }
    for field in fields:
        values = [run[field] for run in runs]
        summary[f"{field}_min"] = min(values)
        summary[f"{field}_avg"] = statistics.fmean(values)
        summary[f"{field}_max"] = max(values)
    return summary


def _run_repo(repo: Path, image: str, driver: str, repeat: int, read_size: int, read_count: int):
    runs = []
    script_path = Path(__file__).resolve()
    for _ in range(repeat):
        try:
            proc = subprocess.run(
                [
                    sys.executable,
                    str(script_path),
                    "--worker",
                    "--image",
                    image,
                    "--driver",
                    driver,
                    "--read-size",
                    str(read_size),
                    "--read-count",
                    str(read_count),
                ],
                cwd=repo,
                capture_output=True,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(
                f"worker failed for {repo}\nstdout:\n{exc.stdout}\nstderr:\n{exc.stderr}"
            ) from exc
        lines = [line for line in proc.stdout.splitlines() if line.strip()]
        if not lines:
            raise RuntimeError(f"worker produced no output for {repo}")
        runs.append(json.loads(lines[-1]))
    return runs


def _print_summary(summary):
    print(
        f"{summary['name']:8s} "
        f"init={summary['init_s_avg']:.3f}s "
        f"walk={summary['walk_s_avg']:.3f}s "
        f"read={summary['read_s_avg']:.3f}s "
        f"total={summary['total_s_avg']:.3f}s "
        f"dirs={summary['dir_count']} files={summary['file_count']}"
    )


def _parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--baseline", default="../amifuse-0.2", help="baseline checkout")
    parser.add_argument("--candidate", default=".", help="candidate checkout")
    parser.add_argument("--image", default="pfs.hdf", help="disk image path, relative to each repo")
    parser.add_argument("--driver", default="pfs3aio", help="filesystem driver path, relative to each repo")
    parser.add_argument("--repeat", type=int, default=3, help="number of runs per repo")
    parser.add_argument("--read-size", type=int, default=4096, help="bytes to read per sampled file")
    parser.add_argument("--read-count", type=int, default=10, help="number of files to sample after the walk")
    parser.add_argument("--json", action="store_true", help="print machine-readable summary")
    return parser.parse_args()


def main():
    args = _parse_args()
    if args.worker:
        result = _run_worker(args.image, args.driver, args.read_size, args.read_count)
        print(json.dumps(result, sort_keys=True))
        return 0

    baseline_repo = Path(args.baseline)
    candidate_repo = Path(args.candidate)
    baseline = _summarize(
        "baseline",
        baseline_repo,
        _run_repo(baseline_repo, args.image, args.driver, args.repeat, args.read_size, args.read_count),
    )
    candidate = _summarize(
        "candidate",
        candidate_repo,
        _run_repo(candidate_repo, args.image, args.driver, args.repeat, args.read_size, args.read_count),
    )
    ratio = candidate["total_s_avg"] / baseline["total_s_avg"]

    if args.json:
        print(
            json.dumps(
                {
                    "baseline": baseline,
                    "candidate": candidate,
                    "total_ratio": ratio,
                },
                indent=2,
                sort_keys=True,
            )
        )
    else:
        _print_summary(baseline)
        _print_summary(candidate)
        print(f"ratio    candidate/baseline total={ratio:.3f}x")
        print("sample   " + ", ".join(candidate["sample_files"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
