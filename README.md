# amifuse

Mount Amiga filesystem images on macOS/Linux using native AmigaOS filesystem handlers via FUSE.

amifuse runs actual Amiga filesystem drivers (like PFS3) through m68k CPU emulation, allowing you to read Amiga hard disk images without relying on reverse-engineered implementations.

## Requirements

- **macOS**: [macFUSE](https://osxfuse.github.io/) (or FUSE for Linux)
- **Python 3.10+**
- **7z**: Required for `make unpack` (install via `brew install p7zip` on macOS)
- A **filesystem handler**: e.g. [pfs3aio](https://aminet.net/package/disk/misc/pfs3aio) (or use `make download`)

## Installation

```bash
# Clone the repository with submodules
git clone --recursive https://github.com/YOUR_USER/amifuse.git
cd amifuse

# Or if already cloned, initialize submodules
git submodule update --init
```

### With virtual environment (recommended)

```bash
python3 -m venv .venv
source .venv/bin/activate   # On Windows: .venv\Scripts\activate

pip install fusepy          # FUSE Python bindings
pip install machine68k      # m68k CPU emulator (required by amitools)
pip install -e ./amitools[vamos]
```

### Without virtual environment

```bash
pip install --user fusepy
pip install --user machine68k
pip install --user -e ./amitools[vamos]
```

### macOS-specific

Install macFUSE from https://osxfuse.github.io/ or via Homebrew:

```bash
brew install --cask macfuse
```

You may need to reboot and allow the kernel extension in System Preferences > Security & Privacy.

### Linux-specific

```bash
# Debian/Ubuntu
sudo apt install fuse libfuse-dev

# Fedora
sudo dnf install fuse fuse-devel
```

## Quick Start

To download a test PFS3 disk image and the pfs3aio handler:

```bash
make download   # Downloads pfs.7z and pfs3aio to Downloads/
make unpack     # Extracts pfs.hdf and copies pfs3aio to current directory
```

Then mount with:

```bash
mkdir -p ./mnt
python3 -m amifuse.fuse_fs --driver pfs3aio --image pfs.hdf --mountpoint ./mnt
```

## Usage

```bash
python3 -m amifuse.fuse_fs \
    --driver pfs3aio \
    --image /path/to/disk.hdf \
    --mountpoint ./mnt
```

### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `--driver` | Yes | Path to the Amiga filesystem handler binary (e.g., `pfs3aio`) |
| `--image` | Yes | Path to the Amiga hard disk image file |
| `--mountpoint` | Yes | Directory where the filesystem will be mounted |
| `--block-size` | No | Override block size (default: auto-detect or 512) |
| `--volname` | No | Override the volume name shown in Finder |
| `--debug` | No | Enable debug logging of FUSE operations |
| `--profile` | No | Enable cProfile profiling and write stats to `profile.txt` on exit |
| `--write` | No | Enable read-write mode (experimental, use with caution) |

### Example

```bash
# Create mount point
mkdir -p ./mnt

# Mount a PFS3 formatted disk image
python3 -m amifuse.fuse_fs \
    --driver pfs3aio \
    --image ~/Documents/FS-UAE/Hard\ Drives/pfs.hdf \
    --mountpoint ./mnt

# Browse the filesystem
ls ./mnt
find ./mnt -type f

# Unmount when done (Ctrl+C in the terminal, or:)
umount ./mnt
```

## Supported Filesystems

Currently tested with:
- **PFS3** (Professional File System 3) via `pfs3aio` handler

Other Amiga filesystem handlers may work but have not been tested.

## Notes

- The filesystem is mounted **read-only** by default; use `--write` for experimental read-write support
- The mount runs in the foreground; press Ctrl+C to unmount
- macOS Finder/Spotlight indexing is automatically disabled to improve performance
- First directory traversal may be slow as the handler processes each path; subsequent accesses are cached

## Troubleshooting

**Slow Filesystem access**
Yes, this code is incredibly slow. Please help me make it faster.

**"Mountpoint is already a mount"**
```bash
umount -f ./mnt
```

**High CPU usage**
This can happen when Finder or Spotlight are indexing the mount. The filesystem automatically rejects macOS metadata queries, but initial indexing attempts may still occur.

**Permission denied**
Ensure macFUSE is installed and your user has permission to use FUSE mounts.
