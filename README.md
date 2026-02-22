# amifuse

Mount Amiga filesystem images on macOS/Linux/Windows using native AmigaOS filesystem handlers via FUSE.

amifuse runs actual Amiga filesystem drivers (like PFS3) through m68k CPU emulation, allowing you to read Amiga hard disk images without relying on reverse-engineered implementations.

![amifuse](https://raw.githubusercontent.com/reinauer/amifuse/main/Docs/amifuse.png)

## Requirements

- **macOS**: [macFUSE](https://osxfuse.github.io/)
- **Linux**: FUSE for Linux
- **Windows**: [WinFSP](https://winfsp.dev/)
- **Python 3.9+**
- **7z**: Required for `make unpack` (install via `brew install p7zip` on macOS)
- A **filesystem handler**: e.g. [pfs3aio](https://aminet.net/package/disk/misc/pfs3aio) (or use `make download`)

## Installation

```bash
# Clone the repository with submodules
git clone --recursive https://github.com/reinauer/amifuse.git
cd amifuse

# Or if already cloned, initialize submodules
git submodule update --init
```

### With virtual environment (recommended)

```bash
python3 -m venv .venv
source .venv/bin/activate   # On Windows: .venv\Scripts\activate

pip install -e './amitools[vamos]'   # Install amitools from submodule (includes machine68k)
pip install -e .                     # Install amifuse
```

### Without virtual environment

```bash
pip install --user -e './amitools[vamos]'
pip install --user -e .
```

### macOS-specific

Install macFUSE from https://osxfuse.github.io/ or via Homebrew/MacPorts:

```bash
# Homebrew
brew install --cask macfuse

# MacPorts
port install macfuse +fs_link
```

You may need to reboot and allow the kernel extension in System Preferences > Security & Privacy.

### Linux-specific

```bash
# Debian/Ubuntu
sudo apt install fuse libfuse-dev

# Fedora
sudo dnf install fuse fuse-devel
```

### Running from source (without installing)

To run amifuse directly from a local checkout (useful for development/debugging):

```bash
python3 -m amifuse mount disk.hdf
```

## Quick Start

To download a test PFS3 disk image and the pfs3aio handler:

```bash
make download   # Downloads pfs.7z and pfs3aio to Downloads/
make unpack     # Extracts pfs.hdf and copies pfs3aio to current directory
```

Then mount with:

```bash
# macOS: auto-mounts to /Volumes/<partition_name>, uses embedded driver from RDB
amifuse mount pfs.hdf

# Linux: requires explicit mountpoint
mkdir -p ./mnt
amifuse mount pfs.hdf --mountpoint ./mnt
```

## Usage

amifuse uses subcommands for different operations:

```bash
amifuse inspect <image>                    # Inspect RDB partitions
amifuse mount <image>                      # Mount a filesystem
```

### Inspecting Disk Images

View partition information and embedded filesystem drivers:

```bash
# Show partition summary
amifuse inspect /path/to/disk.hdf

# Show full partition details
amifuse inspect --full /path/to/disk.hdf
```

| Argument | Description |
|----------|-------------|
| `image` | Path to the RDB image file |
| `--block-size` | Block size in bytes (default: auto-detect or 512) |
| `--full` | Show full partition details |

### Mounting Filesystems

```bash
amifuse mount /path/to/disk.hdf
```

| Argument | Required | Description |
|----------|----------|-------------|
| `image` | Yes | Path to the Amiga hard disk image file |
| `--driver` | No | Path to filesystem handler binary (default: extract from RDB if available) |
| `--mountpoint` | macOS/Windows: No, Linux: Yes | Mount location (macOS: `/Volumes/<partition>`, Windows: first free drive letter) |
| `--partition` | No | Partition name (e.g., `DH0`) or index (default: first partition) |
| `--block-size` | No | Override block size (default: auto-detect or 512) |
| `--volname` | No | Override the volume name shown in Finder |
| `--debug` | No | Enable debug logging of FUSE operations |
| `--profile` | No | Enable cProfile profiling and write stats to `profile.txt` on exit |
| `--write` | No | Enable read-write mode (experimental, use with caution) |
| `--icons` | No | Convert Amiga .info icons to native icons (experimental, macOS only) |

### Examples

```bash
# macOS: Mount using embedded filesystem driver from RDB (simplest)
amifuse mount disk.hdf

# macOS: Mount with explicit driver
amifuse mount pfs.hdf --driver pfs3aio

# Mount a specific partition by name
amifuse mount multi-partition.hdf --partition DH0

# Mount a specific partition by index
amifuse mount multi-partition.hdf --partition 2

# Linux: Explicit mountpoint required
mkdir -p ./mnt
amifuse mount disk.hdf --mountpoint ./mnt

# Mount an ADF floppy image (requires explicit driver)
amifuse mount workbench.adf --driver L/FastFileSystem

# Enable native icons (macOS only, converts Amiga .info files)
amifuse mount disk.hdf --icons

# Browse the filesystem
ls /Volumes/PDH0   # macOS
ls ./mnt           # Linux

# Unmount when done (Ctrl+C in the terminal, or:)
umount /Volumes/PDH0   # macOS
umount ./mnt           # Linux
```

## Additional Tools

### rdb-inspect

Inspect RDB (Rigid Disk Block) images to view partition information and embedded filesystem drivers.

```bash
# Show partition summary
rdb-inspect /path/to/disk.hdf

# Show full partition details
rdb-inspect --full /path/to/disk.hdf

# Output as JSON
rdb-inspect --json /path/to/disk.hdf

# Extract embedded filesystem driver #0 to a file
rdb-inspect --extract-fs 0 --out pfs3.bin /path/to/disk.hdf
```

| Argument | Description |
|----------|-------------|
| `image` | Path to the RDB image file |
| `--block-size` | Block size in bytes (default: auto-detect or 512) |
| `--full` | Show full partition details |
| `--json` | Output parsed RDB as JSON |
| `--extract-fs N` | Extract filesystem entry N (0-based) to a file |
| `--out` | Output path for extracted filesystem (default: auto-derived) |

### driver-info

Inspect Amiga filesystem handler binaries to verify they can be relocated and display segment information.

```bash
# Inspect a filesystem handler
driver-info pfs3aio

# Use a custom base address
driver-info --base 0x200000 pfs3aio
```

| Argument | Description |
|----------|-------------|
| `binary` | Path to the filesystem handler binary |
| `--base` | Base address for relocation (default: 0x100000) |
| `--padding` | Padding between segments when relocating |

## Supported Image Formats

- **HDF/RDB** - Hard disk images with Rigid Disk Block. Filesystem drivers can be embedded in the RDB or specified via `--driver`.
- **Emu68-style MBR images** - Disk images with MBR partition table containing an RDB partition, as used by Emu68 on Raspberry Pi.
- **ADF** - Amiga Disk File floppy images (DD and HD). Requires `--driver` since ADFs don't contain embedded drivers.

## Supported Filesystems

Currently tested with:
- **PFS3** (Professional File System 3) via `pfs3aio` handler
- **SFS** (Smart File System 1.279) via `SmartFileSystem` handler
- **FFS/OFS** (Fast/Old File System) via `L:FastFileSystem` from Workbench
- **BFFS** (Berkeley Fast File System) via Chris Hooper's [BFFSFilesystem](https://github.com/cdhooper/bffs) handler

Other Amiga filesystem handlers may work but have not been tested. Reports are
welcome.

## Icon Support

The `--icons` flag enables conversion of Amiga `.info` icon files to native Finder icons:

- Folder and file icons from `.info` files are displayed in Finder
- Supports Traditional, NewIcons, and GlowIcons formats
- The `.info` files are hidden in directory listings
- Volume icons are displayed on the Desktop

*** This feature is experimental and macOS-only. ***

## Notes

- The filesystem is mounted **read-only** by default; use `--write` for experimental read-write support
- The mount runs in the foreground; press Ctrl+C to unmount
- macOS Finder/Spotlight indexing is automatically disabled to improve performance
- First directory traversal may be slow as the handler processes each path; subsequent accesses are cached
