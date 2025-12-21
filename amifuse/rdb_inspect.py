import argparse
import json
import sys
from pathlib import Path
from typing import Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
AMITOOLS_PATH = REPO_ROOT / "amitools"

# Prefer local checkout of amitools if it is not installed
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(AMITOOLS_PATH) not in sys.path:
    sys.path.insert(0, str(AMITOOLS_PATH))

from amitools.fs.blkdev.RawBlockDevice import RawBlockDevice  # type: ignore  # noqa: E402
from amitools.fs.rdb.RDisk import RDisk  # type: ignore  # noqa: E402
import amitools.fs.DosType as DosType  # type: ignore  # noqa: E402


def open_rdisk(
    image: Path, block_size: Optional[int] = None
) -> Tuple[RawBlockDevice, RDisk]:
    """Open an RDB image read-only and return the block device + parsed RDisk.

    Scans blocks 0-15 for the RDB signature (RDSK), as the RDB can be located
    at any of these blocks depending on the disk geometry.
    """
    from amitools.fs.block.rdb.RDBlock import RDBlock

    initial_block_size = block_size or 512
    blkdev = RawBlockDevice(str(image), read_only=True, block_bytes=initial_block_size)
    blkdev.open()

    # Scan blocks 0-15 for RDB signature
    rdb_blk_num = None
    for blk_num in range(16):
        rdb = RDBlock(blkdev, blk_num)
        if rdb.read():
            rdb_blk_num = blk_num
            # Check if we need to adjust block size
            if block_size is None and rdb.block_size != blkdev.block_bytes:
                blkdev.close()
                blkdev = RawBlockDevice(
                    str(image), read_only=True, block_bytes=rdb.block_size
                )
                blkdev.open()
            break

    if rdb_blk_num is None:
        blkdev.close()
        raise IOError(f"No valid RDB found in blocks 0-15 at {image}")

    # Create RDisk and open - need to set rdb manually since we already found it
    rdisk = RDisk(blkdev)
    rdisk.rdb = RDBlock(blkdev, rdb_blk_num)
    if not rdisk.rdb.read():
        blkdev.close()
        raise IOError(f"Failed to read RDB at block {rdb_blk_num} in {image}")

    if not rdisk.open():
        blkdev.close()
        raise IOError(f"Failed to parse RDB at {image}")

    return blkdev, rdisk


def format_fs_summary(rdisk: RDisk):
    lines = []
    for fs in rdisk.fs:
        dt = fs.fshd.dos_type
        dt_str = DosType.num_to_tag_str(dt)
        lines.append(
            f"FS #{fs.num}: {dt_str}/0x{dt:08x} version={fs.fshd.get_version_string()} "
            f"size={len(fs.get_data())} flags={fs.get_flags_info()}"
        )
    return lines


def extract_fs(rdisk: RDisk, index: int, out_path: Path) -> Path:
    fs = rdisk.get_filesystem(index)
    if fs is None:
        raise IndexError(f"Filesystem #{index} not found")
    data = fs.get_data()
    out_path.write_bytes(data)
    return out_path


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Inspect an RDB image and optionally extract filesystem drivers."
    )
    parser.add_argument("image", type=Path, help="Path to the RDB image (raw file)")
    parser.add_argument(
        "--block-size",
        type=int,
        help="Block size in bytes (defaults to auto/512).",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Show full partition details (matching amitools' rdbtool).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Dump the parsed RDB as JSON instead of text summary.",
    )
    parser.add_argument(
        "--extract-fs",
        type=int,
        metavar="N",
        help="Extract filesystem entry N (0-based) to a host file.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        help="Output path when extracting a filesystem (default derives from dostype).",
    )
    args = parser.parse_args(argv)

    blkdev = None
    rdisk = None
    try:
        blkdev, rdisk = open_rdisk(args.image, block_size=args.block_size)

        if args.json:
            print(json.dumps(rdisk.get_desc(), indent=2))
        else:
            for line in rdisk.get_info(full=args.full):
                print(line)
            fs_lines = format_fs_summary(rdisk)
            if fs_lines:
                print("\nFilesystem drivers:")
                for line in fs_lines:
                    print(" ", line)

        if args.extract_fs is not None:
            fs_obj = rdisk.get_filesystem(args.extract_fs)
            if fs_obj is None:
                raise SystemExit(f"No filesystem #{args.extract_fs} in RDB.")
            dt = fs_obj.fshd.dos_type
            default_name = f"fs{args.extract_fs}_{DosType.num_to_tag_str(dt)}.bin"
            out_path = args.out or Path(default_name)
            saved_to = extract_fs(rdisk, args.extract_fs, out_path)
            print(f"Wrote filesystem #{args.extract_fs} ({hex(dt)}) to {saved_to}")
    finally:
        if rdisk is not None:
            rdisk.close()
        if blkdev is not None:
            blkdev.close()


if __name__ == "__main__":
    main()
