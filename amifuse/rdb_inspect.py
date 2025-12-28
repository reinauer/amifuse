import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple, Union

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


# ADF geometry constants
ADF_DD_SIZE = 901120   # 80 cylinders * 2 heads * 11 sectors * 512 bytes
ADF_HD_SIZE = 1802240  # 80 cylinders * 2 heads * 22 sectors * 512 bytes


@dataclass
class ADFInfo:
    """Information about an ADF (floppy) image."""
    dos_type: int           # DOS type from boot block (0x444F5300-0x444F5307)
    is_hd: bool             # True if HD floppy (22 sectors/track), False for DD (11)
    cylinders: int          # Always 80 for floppies
    heads: int              # Always 2 for floppies
    sectors_per_track: int  # 11 for DD, 22 for HD
    block_size: int         # Always 512
    total_blocks: int       # Total number of blocks


# MBR partition type for Amiga RDB partition (used by Emu68)
MBR_TYPE_AMIGA_RDB = 0x76


@dataclass
class MBRPartition:
    """Information about a single MBR partition entry."""
    index: int              # Partition index (0-3)
    bootable: bool          # Boot indicator (0x80 = bootable)
    partition_type: int     # Partition type byte
    start_lba: int          # Starting LBA sector
    num_sectors: int        # Number of sectors


@dataclass
class MBRInfo:
    """Information about an MBR partition table."""
    partitions: list        # List of MBRPartition entries (only non-empty ones)
    has_amiga_partitions: bool  # True if any 0x76 partitions exist


@dataclass
class MBRContext:
    """Context for an RDB opened within an MBR partition.

    Stored so callers can understand the disk layout and adjust block
    operations accordingly.
    """
    mbr_info: MBRInfo           # Full MBR partition table info
    mbr_partition: MBRPartition  # The specific 0x76 partition containing the RDB
    offset_blocks: int          # Block offset from start of disk


def detect_mbr(image: Path) -> Optional[MBRInfo]:
    """Detect and parse MBR partition table.

    Returns MBRInfo if a valid MBR is found, None otherwise.
    Only returns partitions that are non-empty (have sectors).
    """
    try:
        with open(image, 'rb') as f:
            block0 = f.read(512)
    except OSError:
        return None

    if len(block0) < 512:
        return None

    # Check MBR signature at offset 0x1FE-0x1FF
    if block0[0x1FE:0x200] != b'\x55\xAA':
        return None

    partitions = []
    has_amiga = False

    # Parse 4 partition entries starting at offset 0x1BE
    for i in range(4):
        offset = 0x1BE + i * 16
        entry = block0[offset:offset + 16]

        bootable = entry[0] == 0x80
        partition_type = entry[4]
        # LBA values are little-endian
        start_lba = int.from_bytes(entry[8:12], 'little')
        num_sectors = int.from_bytes(entry[12:16], 'little')

        # Skip empty partitions
        if partition_type == 0 or num_sectors == 0:
            continue

        part = MBRPartition(
            index=i,
            bootable=bootable,
            partition_type=partition_type,
            start_lba=start_lba,
            num_sectors=num_sectors,
        )
        partitions.append(part)

        if partition_type == MBR_TYPE_AMIGA_RDB:
            has_amiga = True

    if not partitions:
        return None

    return MBRInfo(partitions=partitions, has_amiga_partitions=has_amiga)


class OffsetBlockDevice:
    """Block device wrapper that adds an offset to all block operations.

    This allows treating an MBR partition as if it were a standalone disk,
    so RDB parsing can work within the partition boundaries.
    """

    def __init__(self, base_blkdev, offset_blocks: int, num_blocks: int):
        """Create an offset block device.

        Args:
            base_blkdev: The underlying block device (must be open)
            offset_blocks: Starting block offset within base device
            num_blocks: Number of blocks in this slice
        """
        self.base = base_blkdev
        self.offset = offset_blocks
        self.num_blocks = num_blocks
        self.block_bytes = base_blkdev.block_bytes
        self.block_longs = self.block_bytes // 4

    def read_block(self, blk_num: int, num_blks: int = 1) -> bytes:
        """Read blocks with offset applied."""
        if blk_num + num_blks > self.num_blocks:
            raise IOError(f"Read beyond partition: {blk_num}+{num_blks} > {self.num_blocks}")
        return self.base.read_block(self.offset + blk_num, num_blks)

    def write_block(self, blk_num: int, data: bytes, num_blks: int = 1):
        """Write blocks with offset applied."""
        if blk_num + num_blks > self.num_blocks:
            raise IOError(f"Write beyond partition: {blk_num}+{num_blks} > {self.num_blocks}")
        self.base.write_block(self.offset + blk_num, data, num_blks)

    def flush(self):
        """Flush underlying device."""
        self.base.flush()

    def close(self):
        """Close the underlying base device."""
        self.base.close()

    def open(self):
        """Open does nothing - base device should already be open."""
        pass


def detect_adf(image: Path) -> Optional[ADFInfo]:
    """Detect if image is an ADF (Amiga floppy disk) based on content.

    Checks:
    1. First 3 bytes are "DOS" (0x44 0x4F 0x53)
    2. 4th byte is 0-7 (DOS type variant)
    3. File size matches DD (901120) or HD (1802240) floppy size

    Returns ADFInfo if detected, None otherwise.
    """
    try:
        size = os.path.getsize(image)
    except OSError:
        return None

    # Check file size matches floppy geometry
    if size == ADF_DD_SIZE:
        is_hd = False
        sectors_per_track = 11
    elif size == ADF_HD_SIZE:
        is_hd = True
        sectors_per_track = 22
    else:
        return None

    # Read first 4 bytes and check for DOS signature
    try:
        with open(image, 'rb') as f:
            header = f.read(4)
    except OSError:
        return None

    if len(header) < 4:
        return None

    # Check for "DOS" signature (bytes 0-2) and valid variant (byte 3)
    if header[0:3] != b'DOS':
        return None

    variant = header[3]
    if variant > 7:
        return None

    # Build DOS type: 'DOS\x00' = 0x444F5300, 'DOS\x01' = 0x444F5301, etc.
    dos_type = 0x444F5300 | variant

    total_blocks = 80 * 2 * sectors_per_track

    return ADFInfo(
        dos_type=dos_type,
        is_hd=is_hd,
        cylinders=80,
        heads=2,
        sectors_per_track=sectors_per_track,
        block_size=512,
        total_blocks=total_blocks,
    )


def _scan_for_rdb(blkdev, block_size: Optional[int] = None):
    """Scan blocks 0-15 for RDB signature.

    Returns (rdb_block, new_block_size) where:
    - rdb_block: The RDBlock object if found, None otherwise
    - new_block_size: If non-None, caller must reopen device with this block size
                      and call _scan_for_rdb again
    """
    from amitools.fs.block.rdb.RDBlock import RDBlock

    for blk_num in range(16):
        rdb = RDBlock(blkdev, blk_num)
        if rdb.read():
            # Check if we need to adjust block size
            if block_size is None and rdb.block_size != blkdev.block_bytes:
                # Need to reopen with correct block size
                return None, rdb.block_size
            return rdb, None
    return None, None


def open_rdisk(
    image: Path, block_size: Optional[int] = None, mbr_partition_index: Optional[int] = None
) -> Tuple[Union[RawBlockDevice, 'OffsetBlockDevice'], RDisk, Optional[MBRContext]]:
    """Open an RDB image read-only and return the block device + parsed RDisk.

    Scans blocks 0-15 for the RDB signature (RDSK), as the RDB can be located
    at any of these blocks depending on the disk geometry.

    For MBR-partitioned disks (e.g., Emu68 SD cards), if no direct RDB is found
    but the disk has MBR partitions of type 0x76, the RDB is searched within
    those partitions. The returned block device will be an OffsetBlockDevice
    that maps to the partition boundaries.

    Args:
        image: Path to the disk image
        block_size: Force a specific block size (default: auto-detect)
        mbr_partition_index: For MBR disks with multiple 0x76 partitions,
            select which one to use (0-based). Default: first 0x76 partition.

    Returns:
        Tuple of (block_device, rdisk, mbr_context).
        mbr_context is None for direct RDB disks, or MBRContext for MBR partitions.
    """
    initial_block_size = block_size or 512
    blkdev = RawBlockDevice(str(image), read_only=True, block_bytes=initial_block_size)
    blkdev.open()

    # First try direct RDB scan
    rdb_block, new_block_size = _scan_for_rdb(blkdev, block_size)

    if new_block_size is not None:
        # Need to reopen with correct block size and rescan
        blkdev.close()
        blkdev = RawBlockDevice(str(image), read_only=True, block_bytes=new_block_size)
        blkdev.open()
        rdb_block, _ = _scan_for_rdb(blkdev, block_size)

    if rdb_block is not None:
        # Found direct RDB
        rdisk = RDisk(blkdev)
        rdisk.rdb = rdb_block
        if not rdisk.open():
            blkdev.close()
            raise IOError(f"Failed to parse RDB at {image}")
        return blkdev, rdisk, None

    # No direct RDB - check for MBR with 0x76 partitions
    mbr_info = detect_mbr(image)
    if mbr_info is not None and mbr_info.has_amiga_partitions:
        # Find 0x76 partitions
        amiga_parts = [p for p in mbr_info.partitions if p.partition_type == MBR_TYPE_AMIGA_RDB]

        if mbr_partition_index is not None:
            if mbr_partition_index >= len(amiga_parts):
                blkdev.close()
                raise IOError(
                    f"MBR partition index {mbr_partition_index} out of range "
                    f"(found {len(amiga_parts)} Amiga partitions)"
                )
            amiga_parts = [amiga_parts[mbr_partition_index]]

        # Try each 0x76 partition until we find one with a valid RDB
        for mbr_part in amiga_parts:
            offset_dev = OffsetBlockDevice(blkdev, mbr_part.start_lba, mbr_part.num_sectors)

            rdb_block, new_block_size = _scan_for_rdb(offset_dev, block_size)

            if new_block_size is not None:
                # Need to reopen base device with new block size and rescan
                blkdev.close()
                blkdev = RawBlockDevice(str(image), read_only=True, block_bytes=new_block_size)
                blkdev.open()
                offset_dev = OffsetBlockDevice(blkdev, mbr_part.start_lba, mbr_part.num_sectors)
                rdb_block, _ = _scan_for_rdb(offset_dev, block_size)

            if rdb_block is not None:
                # Found RDB in this partition
                rdisk = RDisk(offset_dev)
                rdisk.rdb = rdb_block
                if not rdisk.open():
                    continue  # Try next partition

                mbr_ctx = MBRContext(
                    mbr_info=mbr_info,
                    mbr_partition=mbr_part,
                    offset_blocks=mbr_part.start_lba,
                )
                return offset_dev, rdisk, mbr_ctx

        # No valid RDB found in any 0x76 partition
        blkdev.close()
        raise IOError(
            f"MBR with {len(amiga_parts)} Amiga partition(s) found, "
            f"but none contain a valid RDB: {image}"
        )

    # Check for other partition types to give helpful error messages
    error_msg = f"No valid RDB found in blocks 0-15 at {image}"
    try:
        block0 = blkdev.read_block(0)
        block1 = blkdev.read_block(1)
        if len(block0) >= 512 and block0[0x1FE:0x200] == b'\x55\xAA':
            error_msg = f"MBR partition table detected but no Amiga (0x76) partitions found: {image}"
        elif len(block1) >= 8 and block1[0:8] == b'EFI PART':
            error_msg = f"GPT partition table detected (not supported): {image}"
    except Exception:
        pass

    blkdev.close()
    raise IOError(error_msg)


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


def format_mbr_info(mbr_ctx: MBRContext) -> List[str]:
    """Format MBR partition info as a list of lines for display."""
    lines = []
    lines.append("MBR Partition Table detected (Emu68-style)")
    lines.append(f"  Active partition: MBR slot {mbr_ctx.mbr_partition.index}")
    lines.append(f"  Partition offset: {mbr_ctx.offset_blocks} sectors ({mbr_ctx.offset_blocks * 512 // 1024 // 1024} MB)")
    lines.append(f"  Partition size: {mbr_ctx.mbr_partition.num_sectors} sectors ({mbr_ctx.mbr_partition.num_sectors * 512 // 1024 // 1024} MB)")
    lines.append("")
    lines.append("  All MBR partitions:")
    for p in mbr_ctx.mbr_info.partitions:
        type_str = "Amiga RDB" if p.partition_type == MBR_TYPE_AMIGA_RDB else f"0x{p.partition_type:02x}"
        boot_str = " (bootable)" if p.bootable else ""
        size_mb = p.num_sectors * 512 // 1024 // 1024
        lines.append(f"    [{p.index}] Type: {type_str}{boot_str}, Start: {p.start_lba}, Size: {p.num_sectors} ({size_mb} MB)")
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
    mbr_ctx = None
    try:
        blkdev, rdisk, mbr_ctx = open_rdisk(args.image, block_size=args.block_size)

        if args.json:
            desc = rdisk.get_desc()
            if mbr_ctx is not None:
                desc["mbr"] = {
                    "partition_index": mbr_ctx.mbr_partition.index,
                    "offset_blocks": mbr_ctx.offset_blocks,
                    "partition_size": mbr_ctx.mbr_partition.num_sectors,
                    "all_partitions": [
                        {
                            "index": p.index,
                            "type": p.partition_type,
                            "bootable": p.bootable,
                            "start_lba": p.start_lba,
                            "num_sectors": p.num_sectors,
                        }
                        for p in mbr_ctx.mbr_info.partitions
                    ],
                }
            print(json.dumps(desc, indent=2))
        else:
            # Show MBR info if present
            if mbr_ctx is not None:
                for line in format_mbr_info(mbr_ctx):
                    print(line)
                print()

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
