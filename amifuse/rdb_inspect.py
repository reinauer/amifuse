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
    """Context for an RDB opened alongside or within an MBR partition.

    For Emu68-style disks, the RDB lives inside an MBR partition of type 0x76.
    For Parceiro-style disks, the MBR and RDB coexist at the disk level: the
    MBR occupies block 0 and the RDB starts at block 1+, with no block offset.
    """
    mbr_info: MBRInfo           # Full MBR partition table info
    mbr_partition: Optional[MBRPartition]  # 0x76 partition (Emu68), None for Parceiro
    offset_blocks: int          # Block offset from start of disk (0 for Parceiro)
    scheme: str = "emu68"       # "emu68" or "parceiro"


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


def _lenient_rdisk_open(rdisk) -> List[str]:
    """Open an RDisk leniently, tolerating corrupt filesystem blocks.

    Replicates the logic of RDisk.open() but continues past corrupt
    filesystem (LSEG) blocks instead of failing.  Partition blocks are
    still required to be valid.

    Returns a list of warning strings (empty if everything parsed cleanly).
    """
    from amitools.fs.block.Block import Block
    from amitools.fs.block.rdb.PartitionBlock import PartitionBlock
    from amitools.fs.rdb.Partition import Partition
    from amitools.fs.rdb.FileSystem import FileSystem

    rdb = rdisk.rdb
    if rdb.block_size != rdisk.rawblk.block_bytes:
        raise ValueError(
            "block size mismatch: rdb=%d != device=%d"
            % (rdb.block_size, rdisk.rawblk.block_bytes)
        )
    rdisk.block_bytes = rdb.block_size
    rdisk.used_blks = [rdb.blk_num]
    warnings = []

    # Read partitions (critical — fail on errors)
    part_blk = rdb.part_list
    rdisk.parts = []
    num = 0
    while part_blk != Block.no_blk:
        p = Partition(rdisk.rawblk, part_blk, num, rdb.log_drv.cyl_blks, rdisk)
        num += 1
        if not p.read():
            raise IOError(f"Corrupt partition block at block {part_blk}")
        rdisk.parts.append(p)
        rdisk.used_blks.append(p.get_blk_num())
        part_blk = p.get_next_partition_blk()

    # Read filesystems (non-critical — warn on errors)
    fs_blk = rdb.fs_list
    rdisk.fs = []
    num = 0
    while fs_blk != PartitionBlock.no_blk:
        fs = FileSystem(rdisk.rawblk, fs_blk, num)
        num += 1
        if not fs.read():
            # The FSHD itself may have read OK (fs.fshd.valid) even though
            # the LSEG data chain is corrupt.
            if fs.fshd is not None and fs.fshd.valid:
                dt = fs.fshd.dos_type
                dt_str = DosType.num_to_tag_str(dt)
                warnings.append(
                    f"Filesystem #{fs.num} ({dt_str}/0x{dt:08x}): "
                    f"corrupt data block in LSEG chain (driver data unavailable)"
                )
                # We can still follow the next-FS pointer from the header
                fs_blk = fs.fshd.next
            else:
                warnings.append(
                    f"Corrupt filesystem header at block {fs_blk} "
                    f"(remaining filesystem entries skipped)"
                )
                break
            continue
        rdisk.fs.append(fs)
        rdisk.used_blks += fs.get_blk_nums()
        fs_blk = fs.get_next_fs_blk()

    rdisk.valid = True
    rdisk.max_blks = rdb.log_drv.rdb_blk_hi + 1
    return warnings


def open_rdisk(
    image: Path, block_size: Optional[int] = None, mbr_partition_index: Optional[int] = None
) -> Tuple[Union[RawBlockDevice, 'OffsetBlockDevice'], RDisk, Optional[MBRContext]]:
    """Open an RDB image read-only and return the block device + parsed RDisk.

    Scans blocks 0-15 for the RDB signature (RDSK), as the RDB can be located
    at any of these blocks depending on the disk geometry.

    Supports three disk layouts:
    - Plain RDB: RDSK at block 0 (standard Amiga hard disk)
    - Parceiro-style: MBR at block 0, RDSK at block 1+, coexisting at the
      disk level.  Non-Amiga partitions (e.g. FAT32) live in MBR entries.
    - Emu68-style: MBR with 0x76 (Amiga RDB) partition, RDSK inside that
      partition.

    Tolerates corrupt filesystem driver (LSEG) blocks in the RDB — partition
    data is parsed strictly, but corrupt FS entries are skipped with warnings
    stored in ``rdisk.rdb_warnings``.

    Args:
        image: Path to the disk image
        block_size: Force a specific block size (default: auto-detect)
        mbr_partition_index: For MBR disks with multiple 0x76 partitions,
            select which one to use (0-based). Default: first 0x76 partition.

    Returns:
        Tuple of (block_device, rdisk, mbr_context).
        mbr_context is None for plain RDB disks, or MBRContext for MBR disks.
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
        # Found direct RDB — try strict open first, then lenient fallback
        rdisk = RDisk(blkdev)
        rdisk.rdb = rdb_block
        rdisk.rdb_warnings = []
        if not rdisk.open():
            # Strict open failed — try lenient parse (tolerates corrupt FS blocks)
            rdisk2 = RDisk(blkdev)
            rdisk2.rdb = rdb_block
            try:
                rdisk2.rdb_warnings = _lenient_rdisk_open(rdisk2)
            except IOError:
                blkdev.close()
                raise IOError(f"Failed to parse RDB at {image}")
            rdisk = rdisk2

        # Check for Parceiro-style MBR+RDB coexistence
        mbr_ctx = None
        mbr_info = detect_mbr(image)
        if mbr_info is not None and rdb_block.blk_num > 0:
            mbr_ctx = MBRContext(
                mbr_info=mbr_info,
                mbr_partition=None,
                offset_blocks=0,
                scheme="parceiro",
            )
        return blkdev, rdisk, mbr_ctx

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
                rdisk.rdb_warnings = []
                if not rdisk.open():
                    # Try lenient parse
                    rdisk2 = RDisk(offset_dev)
                    rdisk2.rdb = rdb_block
                    try:
                        rdisk2.rdb_warnings = _lenient_rdisk_open(rdisk2)
                    except IOError:
                        continue  # Try next partition
                    rdisk = rdisk2

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


def find_partition_mbr_index(
    image: Path, block_size: Optional[int], partition_name: str
) -> Optional[int]:
    """Find which 0x76 MBR partition index contains a named Amiga partition.

    For MBR images with multiple 0x76 partitions, searches each RDB for the
    named partition.  Returns the 0-based index into the list of 0x76
    partitions, or None if the image is not MBR, has only one 0x76 partition,
    or the partition is found in the default (first) RDB.
    """
    mbr_info = detect_mbr(image)
    if mbr_info is None or not mbr_info.has_amiga_partitions:
        return None
    amiga_parts = [p for p in mbr_info.partitions if p.partition_type == MBR_TYPE_AMIGA_RDB]
    if len(amiga_parts) <= 1:
        return None
    for idx in range(len(amiga_parts)):
        try:
            blkdev, rdisk, _ = open_rdisk(image, block_size=block_size, mbr_partition_index=idx)
            try:
                part = rdisk.find_partition_by_string(str(partition_name))
                if part is not None:
                    return idx
            finally:
                rdisk.close()
                blkdev.close()
        except IOError:
            continue
    return None


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


_MBR_TYPE_NAMES = {
    MBR_TYPE_AMIGA_RDB: "Amiga RDB",
    0x01: "FAT12",
    0x04: "FAT16 <32M",
    0x06: "FAT16",
    0x07: "NTFS/exFAT",
    0x0B: "W95 FAT32",
    0x0C: "W95 FAT32 (LBA)",
    0x0E: "W95 FAT16 (LBA)",
    0x0F: "W95 Extended (LBA)",
    0x82: "Linux swap",
    0x83: "Linux",
    0xEE: "GPT protective",
}


def format_mbr_info(mbr_ctx: MBRContext) -> List[str]:
    """Format MBR partition info as a list of lines for display."""
    lines = []
    if mbr_ctx.scheme == "parceiro":
        lines.append("MBR + RDB coexistence detected (Parceiro-style)")
        lines.append("  MBR at block 0, RDB at block 1+")
    else:
        lines.append("MBR Partition Table detected (Emu68-style)")
        lines.append(f"  Active partition: MBR slot {mbr_ctx.mbr_partition.index}")
        lines.append(f"  Partition offset: {mbr_ctx.offset_blocks} sectors ({mbr_ctx.offset_blocks * 512 // 1024 // 1024} MB)")
        lines.append(f"  Partition size: {mbr_ctx.mbr_partition.num_sectors} sectors ({mbr_ctx.mbr_partition.num_sectors * 512 // 1024 // 1024} MB)")
    lines.append("")
    lines.append("  MBR partitions:")
    for p in mbr_ctx.mbr_info.partitions:
        type_str = _MBR_TYPE_NAMES.get(p.partition_type, f"0x{p.partition_type:02x}")
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

    # Detect if MBR with multiple 0x76 partitions
    mbr_info = detect_mbr(args.image)
    multi_rdb = False
    amiga_parts = []
    if mbr_info and mbr_info.has_amiga_partitions:
        amiga_parts = [p for p in mbr_info.partitions if p.partition_type == MBR_TYPE_AMIGA_RDB]
        if len(amiga_parts) > 1:
            multi_rdb = True

    if args.json:
        # JSON mode: collect all RDBs into a single JSON structure
        all_rdbs = []
        for rdb_idx in range(len(amiga_parts) if multi_rdb else 1):
            mbr_partition_index = rdb_idx if multi_rdb else None
            try:
                blkdev, rdisk, mbr_ctx = open_rdisk(
                    args.image, block_size=args.block_size, mbr_partition_index=mbr_partition_index
                )
            except IOError:
                continue
            try:
                desc = rdisk.get_desc()
                if mbr_ctx is not None:
                    mbr_desc = {
                        "scheme": mbr_ctx.scheme,
                        "offset_blocks": mbr_ctx.offset_blocks,
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
                    if mbr_ctx.mbr_partition is not None:
                        mbr_desc["partition_index"] = mbr_ctx.mbr_partition.index
                        mbr_desc["partition_size"] = mbr_ctx.mbr_partition.num_sectors
                    desc["mbr"] = mbr_desc
                warnings = getattr(rdisk, 'rdb_warnings', [])
                if warnings:
                    desc["warnings"] = warnings
                all_rdbs.append(desc)
            finally:
                rdisk.close()
                blkdev.close()
        if len(all_rdbs) == 1:
            print(json.dumps(all_rdbs[0], indent=2))
        else:
            print(json.dumps(all_rdbs, indent=2))
    elif multi_rdb:
        # Text mode with multiple RDBs: show MBR table once, then each RDB
        try:
            blkdev, rdisk, mbr_ctx = open_rdisk(
                args.image, block_size=args.block_size, mbr_partition_index=0
            )
        except IOError as e:
            raise SystemExit(f"Error: {e}")
        for line in format_mbr_info(mbr_ctx):
            print(line)
        rdisk.close()
        blkdev.close()

        for rdb_idx in range(len(amiga_parts)):
            try:
                blkdev, rdisk, mbr_ctx = open_rdisk(
                    args.image, block_size=args.block_size, mbr_partition_index=rdb_idx
                )
            except IOError as e:
                print(f"\nMBR partition [{amiga_parts[rdb_idx].index}]: Error: {e}")
                continue
            try:
                print(f"\n=== Amiga RDB in MBR partition [{mbr_ctx.mbr_partition.index}]"
                      f" (offset: {mbr_ctx.offset_blocks} sectors) ===\n")
                for line in rdisk.get_info(full=args.full):
                    print(line)
                fs_lines = format_fs_summary(rdisk)
                if fs_lines:
                    print("\nFilesystem drivers:")
                    for line in fs_lines:
                        print(" ", line)
                warnings = getattr(rdisk, 'rdb_warnings', [])
                if warnings:
                    print("\nWarnings:")
                    for w in warnings:
                        print(f"  {w}")
            finally:
                rdisk.close()
                blkdev.close()
    else:
        # Single RDB: original behavior
        blkdev = None
        rdisk = None
        mbr_ctx = None
        try:
            blkdev, rdisk, mbr_ctx = open_rdisk(args.image, block_size=args.block_size)

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

            warnings = getattr(rdisk, 'rdb_warnings', [])
            if warnings:
                print("\nWarnings:")
                for w in warnings:
                    print(f"  {w}")
        finally:
            if rdisk is not None:
                rdisk.close()
            if blkdev is not None:
                blkdev.close()

    if args.extract_fs is not None:
        # Extract from first RDB (use --mbr-partition for specific one in future)
        blkdev, rdisk, _ = open_rdisk(args.image, block_size=args.block_size)
        try:
            fs_obj = rdisk.get_filesystem(args.extract_fs)
            if fs_obj is None:
                raise SystemExit(f"No filesystem #{args.extract_fs} in RDB.")
            dt = fs_obj.fshd.dos_type
            default_name = f"fs{args.extract_fs}_{DosType.num_to_tag_str(dt)}.bin"
            out_path = args.out or Path(default_name)
            saved_to = extract_fs(rdisk, args.extract_fs, out_path)
            print(f"Wrote filesystem #{args.extract_fs} ({hex(dt)}) to {saved_to}")
        finally:
            rdisk.close()
            blkdev.close()


if __name__ == "__main__":
    main()
