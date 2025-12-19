"""
Read-only FUSE bridge that forwards basic filesystem ops into the Amiga handler
via our vamos bootstrap: readdir/stat/read are turned into LOCATE/EXAMINE/
FINDINPUT/SEEK/READ packets.
"""

import argparse
import errno
import os
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    from fuse import FUSE, FuseOSError, LoggingMixIn, Operations  # type: ignore
except ImportError as e:
    raise SystemExit(
        "Python FUSE bindings not found. Install fusepy (pip install fusepy) "
        "or build the local python-fuse bindings and add them to PYTHONPATH."
    ) from e

from .driver_runtime import BlockDeviceBackend
from .vamos_runner import VamosHandlerRuntime
from .bootstrap import BootstrapAllocator
from .startup_runner import HandlerLauncher
from amitools.vamos.astructs.access import AccessStruct  # type: ignore
from amitools.vamos.libstructs.dos import FileInfoBlockStruct, FileHandleStruct  # type: ignore


def _parse_fib(mem, fib_addr: int) -> Dict:
    """Decode a FileInfoBlock into a simple dict."""
    fib = AccessStruct(mem, FileInfoBlockStruct, fib_addr)
    dir_type = fib.r_s("fib_DirEntryType")
    size = fib.r_s("fib_Size")
    name_bytes = mem.r_block(fib_addr + 8, 108)  # fib_FileName starts at offset 8
    name_len = name_bytes[0]
    name = name_bytes[1 : 1 + name_len].decode("latin-1", errors="ignore")
    return {
        "dir_type": dir_type,
        "size": size,
        "name": name,
    }


import threading

class HandlerBridge:
    """Maintains the handler state and issues DOS packets synchronously."""

    def __init__(self, image: Path, driver: Path, block_size: Optional[int] = None):
        self._lock = threading.RLock()  # Reentrant lock for thread safety
        self.backend = BlockDeviceBackend(image, block_size=block_size)
        self.backend.open()
        self.vh = VamosHandlerRuntime()
        self.vh.setup()
        self.vh.set_scsi_backend(self.backend)
        seg_baddr = self.vh.load_handler(driver)
        # Build DeviceNode/FSSM using seglist bptr
        ba = BootstrapAllocator(self.vh, image)
        boot = ba.alloc_all(handler_seglist_baddr=seg_baddr, handler_seglist_bptr=seg_baddr, handler_name="PFS0:")
        entry_addr = self.vh.slm.seg_loader.infos[seg_baddr].seglist.get_segment().get_addr()
        self.launcher = HandlerLauncher(self.vh, boot, entry_addr)
        self.state = self.launcher.launch_with_startup()
        # run startup to completion
        self._run_until_replies()
        # Save the current PC/SP as the "main loop" state for later restarts
        # This is where the handler should be after processing startup
        self.state.main_loop_pc = self.state.pc
        self.state.main_loop_sp = self.state.sp
        self.state.initialized = True
        print(f"[amifuse] Saved main_loop_pc=0x{self.state.pc:x}, main_loop_sp=0x{self.state.sp:x}")
        self.mem = self.vh.alloc.get_mem()
        # cache a best-effort volume name
        self._volname = None
        self._fib_mem = None
        self._read_buf_mem = None
        self._read_buf_size = 0
        print(
            f"[amifuse] handler loaded seg_baddr=0x{seg_baddr:x} entry=0x{entry_addr:x} "
            f"port=0x{self.state.port_addr:x} reply=0x{self.state.reply_port_addr:x}"
        )

    def _run_until_replies(self, max_iters: int = 50, cycles: int = 200_000, sleep_base: float = 0.0005, sleep_max: float = 0.01):
        """Run handler bursts until at least one reply is queued or iterations exhausted."""
        from amitools.vamos.lib.ExecLibrary import ExecLibrary
        replies = []
        sleep_time = sleep_base
        for i in range(max_iters):
            self.launcher.run_burst(self.state, max_cycles=cycles)
            rs = self.state.run_state
            # Check for replies first - if we have them, we're done
            replies = self.launcher.poll_replies(self.state.reply_port_addr)
            if replies:
                break
            # If handler is blocked in WaitPort with no messages, stop spinning
            if ExecLibrary._waitport_blocked_sp is not None:
                # Handler is waiting for a message that isn't there yet
                # This shouldn't happen if caller queued a message before calling us
                break
            if getattr(rs, "error", None):
                # Error during run - might be WaitPort block, check if we have replies
                replies = self.launcher.poll_replies(self.state.reply_port_addr)
                break
            # Yield with exponential backoff to avoid tight polling loops.
            if sleep_base > 0:
                time.sleep(sleep_time)
                sleep_time = min(sleep_time * 2, sleep_max)
        return replies

    def _alloc_fib(self):
        if self._fib_mem is None:
            self._fib_mem = self.vh.alloc.alloc_struct(
                FileInfoBlockStruct, label="FUSE_FIB"
            )
        self.mem.w_block(self._fib_mem.addr, b"\x00" * FileInfoBlockStruct.get_size())
        return self._fib_mem

    def _alloc_read_buf(self, size: int):
        if self._read_buf_mem is None or size > self._read_buf_size:
            self._read_buf_mem = self.vh.alloc.alloc_memory(size, label="FUSE_READBUF")
            self._read_buf_size = size
        self.mem.w_block(self._read_buf_mem.addr, b"\x00" * size)
        return self._read_buf_mem

    def _alloc_bstr(self, text: str):
        return self.launcher.alloc_bstr(text, label="FUSE_BSTR")

    def locate(self, lock_bptr: int, name: str):
        with self._lock:
            _, name_bptr = self._alloc_bstr(name)
            self.launcher.send_locate(self.state, lock_bptr, name_bptr)
            replies = self._run_until_replies()
            return replies[-1][2] if replies else 0, replies[-1][3] if replies else -1

    def locate_path(self, path: str) -> Tuple[int, int]:
        """Return a lock BPTR for the given absolute path."""
        with self._lock:
            parts = [p for p in path.split("/") if p]
            lock = 0
            res2 = 0
            if not parts:
                return lock, res2
            for comp in parts:
                _, name_bptr = self._alloc_bstr(comp)
                self.launcher.send_locate(self.state, lock, name_bptr)
                replies = self._run_until_replies()
                lock = replies[-1][2] if replies else 0
                res2 = replies[-1][3] if replies else -1
                if lock == 0:
                    break
            return lock, res2

    def open_file(self, path: str) -> Optional[int]:
        """Open a file via FINDINPUT and return the FileHandle address."""
        with self._lock:
            parts = [p for p in path.split("/") if p]
            if not parts:
                return None
            name = parts[-1]
            dir_path = "/" + "/".join(parts[:-1])
            dir_lock, _ = self.locate_path(dir_path)
            if dir_lock == 0 and dir_path != "/":
                return None
            _, name_bptr = self._alloc_bstr(name)
            fh_addr, _, _ = self.launcher.send_findinput(self.state, name_bptr, dir_lock)
            replies = self._run_until_replies()
            if not replies or replies[-1][2] == 0:
                return None
            return fh_addr

    def list_dir(self, lock_bptr: int) -> List[Dict]:
        with self._lock:
            # ensure we have a lock; lock_bptr=0 -> root
            if lock_bptr == 0:
                lock_bptr, _ = self.locate(0, "")
            fib_mem = self._alloc_fib()
            # First Examine returns info about the directory itself, not contents
            self.launcher.send_examine(self.state, lock_bptr, fib_mem.addr)
            replies = self._run_until_replies()
            entries: List[Dict] = []
            if not replies or replies[-1][2] == 0:
                return entries
            # Don't add the first entry - it's the directory itself, not a child
            # Iterate via ExamineNext to get actual directory contents
            for _ in range(256):
                self.launcher.send_examine_next(self.state, lock_bptr, fib_mem.addr)
                replies = self._run_until_replies()
                if not replies or replies[-1][2] == 0:
                    break
                entry = _parse_fib(self.mem, fib_mem.addr)
                if not entry["name"]:
                    break
                entries.append(entry)
            return entries

    def list_dir_path(self, path: str) -> List[Dict]:
        """List directory contents by path."""
        with self._lock:
            if path == "/":
                return self.list_dir(0)
            lock, _ = self.locate_path(path)
            if lock == 0:
                return []
            return self.list_dir(lock)

    def stat_path(self, path: str) -> Optional[Dict]:
        with self._lock:
            lock, _ = self.locate_path(path)
            if lock == 0 and path != "/":
                return None
            if path == "/":
                return {"dir_type": 2, "size": 0, "name": ""}
            fib_mem = self._alloc_fib()
            self.launcher.send_examine(self.state, lock, fib_mem.addr)
            replies = self._run_until_replies()
            if not replies or replies[-1][2] == 0:
                return None
            return _parse_fib(self.mem, fib_mem.addr)

    def volume_name(self) -> str:
        """Best-effort name: RDB drive name, else first dir entry, else fallback."""
        with self._lock:
            if self._volname:
                return self._volname
            # try root FIB name (usually the volume name)
            try:
                fib_mem = self._alloc_fib()
                # lock_bptr=0 -> current volume root
                root_lock, _ = self.locate(0, "")
                if root_lock == 0:
                    root_lock = 0
                self.launcher.send_examine(self.state, root_lock, fib_mem.addr)
                replies = self._run_until_replies()
                if replies and replies[-1][2]:
                    info = _parse_fib(self.mem, fib_mem.addr)
                    if info.get("name"):
                        self._volname = info["name"]
                        return self._volname
            except Exception:
                pass
            # try RDB partition name
            try:
                if self.backend.rdb and self.backend.rdb.parts:
                    name = self.backend.rdb.parts[0].part_blk.drv_name
                    if name:
                        self._volname = name
                        return name
            except Exception:
                pass
            # try root listing
            entries = self.list_dir(0)
            for ent in entries:
                if ent.get("name"):
                    self._volname = ent["name"]
                    return ent["name"]
            self._volname = "AmigaFS"
            return self._volname

    def read_file(self, path: str, size: int, offset: int) -> bytes:
        with self._lock:
            # split into parent lock + name
            parts = [p for p in path.split("/") if p]
            if not parts:
                return b""
            name = parts[-1]
            dir_path = "/" + "/".join(parts[:-1])
            dir_lock, _ = self.locate_path(dir_path)
            if dir_lock == 0 and dir_path != "/":
                return b""
            _, name_bptr = self._alloc_bstr(name)
            fh_addr, _, _ = self.launcher.send_findinput(self.state, name_bptr, dir_lock)
            replies = self._run_until_replies()
            if not replies or replies[-1][2] == 0:
                return b""
            # optional seek
            if offset:
                self.launcher.send_seek_handle(self.state, fh_addr, offset, 0)  # OFFSET_BEGINNING
                self._run_until_replies()
            buf_mem = self._alloc_read_buf(size)
            self.launcher.send_read_handle(self.state, fh_addr, buf_mem.addr, size)
            replies = self._run_until_replies()
            if not replies or replies[-1][2] <= 0:
                return b""
            nread = min(replies[-1][2], size)
            return bytes(self.mem.r_block(buf_mem.addr, nread))

    def seek_handle(self, fh_addr: int, offset: int, mode: int = 0):
        with self._lock:
            self.launcher.send_seek_handle(self.state, fh_addr, offset, mode)
            self._run_until_replies()

    def read_handle(self, fh_addr: int, size: int) -> bytes:
        with self._lock:
            buf_mem = self._alloc_read_buf(size)
            self.launcher.send_read_handle(self.state, fh_addr, buf_mem.addr, size)
            replies = self._run_until_replies()
            if not replies or replies[-1][2] <= 0:
                return b""
            nread = min(replies[-1][2], size)
            return bytes(self.mem.r_block(buf_mem.addr, nread))


    def read_handle_at(self, fh_addr: int, offset: int, size: int) -> bytes:
        with self._lock:
            self.launcher.send_seek_handle(self.state, fh_addr, offset, 0)  # OFFSET_BEGINNING
            self._run_until_replies()
            buf_mem = self._alloc_read_buf(size)
            self.launcher.send_read_handle(self.state, fh_addr, buf_mem.addr, size)
            replies = self._run_until_replies()
            if not replies or replies[-1][2] <= 0:
                return b""
            nread = min(replies[-1][2], size)
            return bytes(self.mem.r_block(buf_mem.addr, nread))

    def close_file(self, fh_addr: int):
        with self._lock:
            self.launcher.send_end_handle(self.state, fh_addr)
            self._run_until_replies()


class AmigaFuseFS(Operations):
    # macOS special files we should reject immediately without calling handler
    _MACOS_SPECIAL = frozenset([
        "._.", ".hidden", ".Trashes", ".Spotlight-V100", ".fseventsd",
        ".metadata_never_index", ".com.apple.timemachine.donotpresent",
        ".VolumeIcon.icns", ".DS_Store", "Icon\r", ".ql_disablethumbnails",
        ".localized", ".TemporaryItems", ".DocumentRevisions-V100",
        ".vol", ".file", ".hotfiles.btree", ".quota.user", ".quota.group",
        ".apdisk", ".com.apple.NetBootX", "mach_kernel", ".PKInstallSandboxManager",
        ".PKInstallSandboxManager-SystemSoftware", ".Trashes.501", "Backups.backupdb",
    ])

    def __init__(
        self,
        bridge: HandlerBridge,
        debug: bool = False,
    ):
        self.bridge = bridge
        self._debug = debug
        self._stat_cache: Dict[str, Tuple[float, Dict]] = {}  # path -> (timestamp, stat_result)
        self._cache_ttl = 3600.0  # Cache for 1 hour - read-only FS never changes
        self._neg_cache: Dict[str, float] = {}  # path -> timestamp for ENOENT results
        self._neg_cache_ttl = 3600.0  # Cache negative results for 1 hour
        self._dir_cache: Dict[str, Tuple[float, List[str]]] = {}  # path -> (timestamp, entries)
        self._dir_cache_ttl = 3600.0  # Cache directory listings for 1 hour
        self._fh_lock = threading.Lock()
        self._fh_cache: Dict[int, Dict[str, object]] = {}
        self._next_fh = 1
        self._last_op_time = time.time()
        self._op_count = 0

    def _is_macos_special(self, path: str) -> bool:
        """Return True if path is a macOS special file we should reject."""
        name = path.rsplit("/", 1)[-1]
        if name.startswith("._"):  # AppleDouble resource fork files
            return True
        return name in self._MACOS_SPECIAL

    def _get_cached_stat(self, path: str) -> Optional[Dict]:
        """Return cached stat result if still valid, else None."""
        if path in self._stat_cache:
            ts, result = self._stat_cache[path]
            if time.time() - ts < self._cache_ttl:
                return result
            del self._stat_cache[path]
        return None

    def _set_cached_stat(self, path: str, result: Dict):
        """Cache a stat result."""
        self._stat_cache[path] = (time.time(), result)

    def _is_neg_cached(self, path: str) -> bool:
        """Return True if path is in negative cache (known non-existent)."""
        if path in self._neg_cache:
            if time.time() - self._neg_cache[path] < self._neg_cache_ttl:
                return True
            del self._neg_cache[path]
        return False

    def _set_neg_cached(self, path: str):
        """Cache a negative (ENOENT) result."""
        self._neg_cache[path] = time.time()

    def _track_op(self, op: str, path: str, cached: bool = False):
        """Track operations for debugging."""
        if not self._debug:
            return
        self._op_count += 1
        now = time.time()
        elapsed = now - self._last_op_time
        # Report every 10 seconds
        if elapsed > 10.0:
            rate = self._op_count / elapsed
            print(f"[FUSE] {self._op_count} ops in {elapsed:.1f}s ({rate:.1f}/s), last: {op}({path}) cached={cached}")
            self._op_count = 0
            self._last_op_time = now

    def _root_stat(self):
        now = int(time.time())
        return {
            "st_mode": (0o755 | 0o040000),  # drwxr-xr-x
            "st_nlink": 2,
            "st_size": 0,
            "st_ctime": now,
            "st_mtime": now,
            "st_atime": now,
        }

    # --- FUSE operations ---
    def getattr(self, path, fh=None):
        # Reject macOS special files immediately without calling handler
        if self._is_macos_special(path):
            self._track_op("getattr", path, cached=True)
            raise FuseOSError(errno.ENOENT)
        if path == "/":
            self._track_op("getattr", path, cached=True)
            return self._root_stat()
        # Check negative cache first (known non-existent paths)
        if self._is_neg_cached(path):
            self._track_op("getattr", path, cached=True)
            raise FuseOSError(errno.ENOENT)
        # Check positive cache
        cached = self._get_cached_stat(path)
        if cached is not None:
            self._track_op("getattr", path, cached=True)
            return cached
        self._track_op("getattr", path, cached=False)
        info = self.bridge.stat_path(path)
        if not info:
            # Cache negative result
            self._set_neg_cached(path)
            raise FuseOSError(errno.ENOENT)
        is_dir = info["dir_type"] >= 0
        mode = (0o755 if is_dir else 0o444) | (0o040000 if is_dir else 0o100000)
        now = int(time.time())
        result = {
            "st_mode": mode,
            "st_nlink": 2 if is_dir else 1,
            "st_size": info["size"],
            "st_ctime": now,
            "st_mtime": now,
            "st_atime": now,
        }
        self._set_cached_stat(path, result)
        return result

    def readdir(self, path, fh):
        # Check directory cache first
        if path in self._dir_cache:
            ts, cached_entries = self._dir_cache[path]
            if time.time() - ts < self._dir_cache_ttl:
                self._track_op("readdir", path, cached=True)
                return cached_entries
            del self._dir_cache[path]

        self._track_op("readdir", path, cached=False)
        entries = [".", ".."]
        dir_entries = self.bridge.list_dir_path(path)
        now = time.time()
        for ent in dir_entries:
            name = ent["name"]
            entries.append(name)
            # Pre-populate stat cache from directory listing
            child_path = path.rstrip("/") + "/" + name if path != "/" else "/" + name
            is_dir = ent["dir_type"] >= 0
            mode = (0o755 if is_dir else 0o444) | (0o040000 if is_dir else 0o100000)
            stat_result = {
                "st_mode": mode,
                "st_nlink": 2 if is_dir else 1,
                "st_size": ent["size"],
                "st_ctime": int(now),
                "st_mtime": int(now),
                "st_atime": int(now),
            }
            self._stat_cache[child_path] = (now, stat_result)

        # Cache the directory listing
        self._dir_cache[path] = (now, entries)
        return entries

    def open(self, path, flags):
        # read-only
        if flags & (os.O_WRONLY | os.O_RDWR):
            raise FuseOSError(errno.EACCES)
        fh_addr = self.bridge.open_file(path)
        if fh_addr is None:
            info = self.bridge.stat_path(path)
            if info and info.get("dir_type", 0) >= 0:
                raise FuseOSError(errno.EISDIR)
            raise FuseOSError(errno.ENOENT)
        with self._fh_lock:
            handle = self._next_fh
            self._next_fh += 1
            self._fh_cache[handle] = {
                "fh_addr": fh_addr,
                "pos": 0,
                "lock": threading.Lock(),
                "closed": False,
            }
        return handle

    def read(self, path, size, offset, fh):
        with self._fh_lock:
            entry = self._fh_cache.get(fh)
        if entry is None:
            data = self.bridge.read_file(path, size, offset)
            if data is None:
                raise FuseOSError(errno.EIO)
            return data
        fh_addr = entry["fh_addr"]
        with entry["lock"]:
            if entry.get("closed"):
                raise FuseOSError(errno.EIO)
            if offset != entry["pos"]:
                data = self.bridge.read_handle_at(fh_addr, offset, size)
            else:
                data = self.bridge.read_handle(fh_addr, size)
            if data is None:
                raise FuseOSError(errno.EIO)
            entry["pos"] = offset + len(data)
            return data

    def release(self, path, fh):
        with self._fh_lock:
            entry = self._fh_cache.get(fh)
            if not entry:
                return 0
            entry["closed"] = True
            del self._fh_cache[fh]
        with entry["lock"]:
            self.bridge.close_file(entry["fh_addr"])
        return 0


def mount_fuse(
    image: Path,
    driver: Path,
    mountpoint: Path,
    block_size: Optional[int],
    volname_opt: Optional[str] = None,
    debug: bool = False,
):
    if mountpoint.exists() and os.path.ismount(mountpoint):
        raise SystemExit(
            f"Mountpoint {mountpoint} is already a mount; unmount it first (e.g. umount -f {mountpoint})."
        )
    bridge = HandlerBridge(image, driver, block_size=block_size)
    volname = volname_opt or bridge.volume_name()
    # Multi-threaded mode with caching to minimize macOS polling.
    FUSE(
        AmigaFuseFS(bridge, debug=debug),
        str(mountpoint),
        foreground=True,
        ro=True,
        allow_other=False,
        nothreads=False,  # Multi-threaded; handler bridge serializes access
        fsname=f"amifuse:{volname}",
        volname=volname,
        subtype="amifuse",
        # macOS-specific options to reduce Finder/Spotlight polling
        local=True,  # Tell macOS this is a local FS
        noappledouble=True,  # Disable AppleDouble ._ files
        noapplexattr=True,  # Disable Apple extended attributes
        # Kernel-level caching (in seconds) - dramatically reduces getattr calls
        default_permissions=True,  # Let kernel handle permission checks
    )


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Mount an Amiga filesystem image read-only via FUSE (skeleton)."
    )
    parser.add_argument("--driver", required=True, type=Path, help="Filesystem binary")
    parser.add_argument("--image", required=True, type=Path, help="Disk image file")
    parser.add_argument("--mountpoint", required=True, type=Path, help="Mount location")
    parser.add_argument(
        "--block-size", type=int, help="Override block size (defaults to auto/512)."
    )
    parser.add_argument(
        "--volname", type=str, help="Override macFUSE volume name (defaults to partition name)."
    )
    parser.add_argument(
        "--debug", action="store_true", help="Enable debug logging of FUSE operations."
    )
    args = parser.parse_args(argv)
    mount_fuse(args.image, args.driver, args.mountpoint, args.block_size, args.volname, args.debug)


if __name__ == "__main__":
    main()
