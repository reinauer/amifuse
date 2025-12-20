"""
FUSE bridge that forwards basic filesystem ops into the Amiga handler
via our vamos bootstrap. Read/write is experimental and enabled with --write.
"""

import cProfile
import pstats

import argparse
import errno
import os
import shutil
import signal
import subprocess
import sys
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
from .startup_runner import HandlerLauncher, OFFSET_BEGINNING
from amitools.vamos.astructs.access import AccessStruct  # type: ignore
from amitools.vamos.libstructs.dos import FileInfoBlockStruct, FileHandleStruct  # type: ignore


def _parse_fib(mem, fib_addr: int) -> Dict:
    """Decode a FileInfoBlock into a simple dict.

    Uses direct memory reads with known offsets from FileInfoBlockStruct:
      fib_DiskKey: ULONG at 0
      fib_DirEntryType: LONG at 4
      fib_FileName: ARRAY(UBYTE,108) at 8
      fib_Protection: LONG at 116
      fib_EntryType: LONG at 120
      fib_Size: ULONG at 124
    """
    dir_type = mem.r32s(fib_addr + 4)   # fib_DirEntryType (signed LONG)
    size = mem.r32(fib_addr + 124)      # fib_Size (unsigned ULONG)
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

    def __init__(
        self,
        image: Path,
        driver: Path,
        block_size: Optional[int] = None,
        read_only: bool = True,
        debug: bool = False,
    ):
        self._lock = threading.RLock()  # Reentrant lock for thread safety
        self._debug = debug
        self.backend = BlockDeviceBackend(image, block_size=block_size, read_only=read_only)
        self.backend.open()
        self.vh = VamosHandlerRuntime()
        self.vh.setup()
        self.vh.set_scsi_backend(self.backend)
        seg_baddr = self.vh.load_handler(driver)
        # Build DeviceNode/FSSM using seglist bptr
        ba = BootstrapAllocator(self.vh, image)
        boot = ba.alloc_all(handler_seglist_baddr=seg_baddr, handler_seglist_bptr=seg_baddr, handler_name="DH0:")
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
        if self._debug:
            print(f"[amifuse] Saved main_loop_pc=0x{self.state.pc:x}, main_loop_sp=0x{self.state.sp:x}")
        self.mem = self.vh.alloc.get_mem()
        # cache a best-effort volume name
        self._volname = None
        self._fib_mem = None
        self._read_buf_mem = None
        self._read_buf_size = 0
        self._bstr_ring = []
        self._bstr_sizes = []
        self._bstr_index = 0
        self._bstr_ring_size = 8
        self._fh_pool: List[int] = []
        self._fh_mem: Dict[int, object] = {}
        self._neg_cache: Dict[str, float] = {}
        # Short negative cache for handler lookups; tune for RW.
        # A newly created file might be invisible for up to TTL seconds, then it will appear normally.
        self._neg_cache_ttl = 10.0
        self._write_enabled = not read_only
        if self._write_enabled:
            self._neg_cache_ttl = 0.0
        print(
            f"[amifuse] handler loaded seg_baddr=0x{seg_baddr:x} entry=0x{entry_addr:x} "
            f"port=0x{self.state.port_addr:x} reply=0x{self.state.reply_port_addr:x}"
        )

    def _run_until_replies(self, max_iters: int = 50, cycles: int = 200_000, sleep_base: float = 0.0005, sleep_max: float = 0.01):
        """Run handler bursts until at least one reply is queued or iterations exhausted."""
        from amitools.vamos.lib.ExecLibrary import ExecLibrary
        replies = []
        sleep_time = sleep_base
        # If we previously blocked in WaitPort, reset to the saved return address.
        waitport_sp = ExecLibrary._waitport_blocked_sp
        if waitport_sp is not None:
            try:
                # Only resume from WaitPort if there is a message pending.
                has_pending = self.vh.slm.exec_impl.port_mgr.has_msg(self.state.port_addr)
                if has_pending:
                    ret_addr = self.mem.r32(waitport_sp)
                    if ret_addr != 0:
                        self.state.pc = ret_addr
                        self.state.sp = waitport_sp + 4
                        ExecLibrary._waitport_blocked_sp = None
                        ExecLibrary._waitport_blocked_port = None
            except Exception:
                pass
        # Guard against re-entering at the exit trap addresses (0x400/0x402).
        # The emulator uses low memory addresses as trap vectors; if PC lands there,
        # it means the handler tried to exit. Reset to main loop to keep it alive.
        if self.state.pc <= 0x1000 and getattr(self.state, "main_loop_pc", 0):
            self.state.pc = self.state.main_loop_pc
            self.state.sp = self.state.main_loop_sp
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

    def _log_replies(self, label: str, replies):
        if not self._debug:
            return
        for _, pkt_addr, res1, res2 in replies:
            print(f"[amifuse][{label}] pkt=0x{pkt_addr:x} res1={res1} res2={res2}")

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
        encoded = text.encode("latin-1", errors="replace")
        if len(encoded) > 255:
            encoded = encoded[:255]
        data = bytes([len(encoded)]) + encoded
        if not self._bstr_ring:
            self._bstr_ring = [None] * self._bstr_ring_size
            self._bstr_sizes = [0] * self._bstr_ring_size
        idx = self._bstr_index
        self._bstr_index = (idx + 1) % self._bstr_ring_size
        mem_obj = self._bstr_ring[idx]
        if mem_obj is None or len(data) > self._bstr_sizes[idx]:
            mem_obj = self.vh.alloc.alloc_memory(len(data), label=f"FUSE_BSTR_{idx}")
            self._bstr_ring[idx] = mem_obj
            self._bstr_sizes[idx] = len(data)
        self.mem.w_block(mem_obj.addr, data)
        return mem_obj.addr, mem_obj.addr >> 2

    def _alloc_fh(self) -> int:
        if self._fh_pool:
            addr = self._fh_pool.pop()
            mem_obj = self._fh_mem.get(addr)
            if mem_obj:
                self.mem.w_block(addr, b"\x00" * FileHandleStruct.get_size())
            return addr
        mem_obj = self.vh.alloc.alloc_struct(FileHandleStruct, label="FUSE_FH")
        self.mem.w_block(mem_obj.addr, b"\x00" * FileHandleStruct.get_size())
        self._fh_mem[mem_obj.addr] = mem_obj
        return mem_obj.addr

    def _free_fh(self, fh_addr: int):
        if fh_addr in self._fh_mem:
            self._fh_pool.append(fh_addr)

    def _is_neg_cached(self, path: str) -> bool:
        if self._neg_cache_ttl <= 0:
            return False
        if path in self._neg_cache:
            if time.time() - self._neg_cache[path] < self._neg_cache_ttl:
                return True
            del self._neg_cache[path]
        return False

    def _set_neg_cached(self, path: str):
        if self._neg_cache_ttl <= 0:
            return
        self._neg_cache[path] = time.time()

    def locate(self, lock_bptr: int, name: str):
        with self._lock:
            _, name_bptr = self._alloc_bstr(name)
            self.launcher.send_locate(self.state, lock_bptr, name_bptr)
            replies = self._run_until_replies()
            return replies[-1][2] if replies else 0, replies[-1][3] if replies else -1

    def free_lock(self, lock_bptr: int):
        with self._lock:
            if lock_bptr:
                self.launcher.send_free_lock(self.state, lock_bptr)
                self._run_until_replies()

    def locate_path(self, path: str) -> Tuple[int, int, List[int]]:
        """Return (lock BPTR, res2, locks_to_free) for the given absolute path."""
        with self._lock:
            if path and path != "/" and self._is_neg_cached(path):
                return 0, -1, []
            parts = [p for p in path.split("/") if p]
            lock = 0
            res2 = 0
            locks: List[int] = []
            if not parts:
                return lock, res2, locks
            for comp in parts:
                _, name_bptr = self._alloc_bstr(comp)
                self.launcher.send_locate(self.state, lock, name_bptr)
                replies = self._run_until_replies()
                lock = replies[-1][2] if replies else 0
                res2 = replies[-1][3] if replies else -1
                if lock == 0:
                    break
                locks.append(lock)
            if lock == 0 and path and path != "/":
                self._set_neg_cached(path)
            return lock, res2, locks

    def open_file(self, path: str, flags: int = os.O_RDONLY) -> Optional[Tuple[int, int]]:
        """Open a file via FINDINPUT/FINDUPDATE/FINDOUTPUT and return the FileHandle address."""
        with self._lock:
            if path and path != "/" and self._is_neg_cached(path):
                return None
            parts = [p for p in path.split("/") if p]
            if not parts:
                return None
            name = parts[-1]
            dir_path = "/" + "/".join(parts[:-1])
            dir_lock, _, locks = self.locate_path(dir_path)
            if dir_path == "/" and dir_lock == 0:
                dir_lock, _ = self.locate(0, "")
                if dir_lock:
                    locks.append(dir_lock)
            if dir_lock == 0 and dir_path != "/":
                if path and path != "/":
                    self._set_neg_cached(path)
                return None
            _, name_bptr = self._alloc_bstr(name)
            mode = flags & getattr(os, 'O_ACCMODE', 3)
            if mode == os.O_RDONLY:
                fh_addr = self._alloc_fh()
                self.launcher.send_findinput(self.state, name_bptr, dir_lock, fh_addr)
            else:
                if not self._write_enabled:
                    return None
                if flags & os.O_TRUNC:
                    fh_addr = self._alloc_fh()
                    self.launcher.send_findoutput(self.state, name_bptr, dir_lock, fh_addr)
                else:
                    fh_addr = self._alloc_fh()
                    self.launcher.send_findupdate(self.state, name_bptr, dir_lock, fh_addr)
            replies = self._run_until_replies()
            self._log_replies("find", replies)
            if not replies or replies[-1][2] == 0:
                if path and path != "/":
                    self._set_neg_cached(path)
                self._free_fh(fh_addr)
                for l in reversed(locks):
                    self.free_lock(l)
                return None
            if path:
                self._neg_cache.pop(path, None)
            if self._write_enabled:
                # Writing may create new entries; clear stale negative cache.
                self._neg_cache.clear()
            return fh_addr, dir_lock

    def list_dir(self, lock_bptr: int) -> List[Dict]:
        with self._lock:
            # ensure we have a lock; lock_bptr=0 -> root
            root_lock = 0
            if lock_bptr == 0:
                root_lock, _ = self.locate(0, "")
                lock_bptr = root_lock
            fib_mem = self._alloc_fib()
            # First Examine returns info about the directory itself, not contents
            self.launcher.send_examine(self.state, lock_bptr, fib_mem.addr)
            replies = self._run_until_replies()
            entries: List[Dict] = []
            if not replies or replies[-1][2] == 0:
                if root_lock:
                    self.free_lock(root_lock)
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
            if root_lock:
                self.free_lock(root_lock)
            return entries

    def list_dir_path(self, path: str) -> List[Dict]:
        """List directory contents by path."""
        with self._lock:
            if path == "/":
                return self.list_dir(0)
            lock, _, locks = self.locate_path(path)
            if lock == 0:
                return []
            entries = self.list_dir(lock)
            for l in reversed(locks):
                self.free_lock(l)
            return entries

    def stat_path(self, path: str) -> Optional[Dict]:
        with self._lock:
            lock, _, locks = self.locate_path(path)
            if lock == 0 and path != "/":
                return None
            if path == "/":
                return {"dir_type": 2, "size": 0, "name": ""}
            fib_mem = self._alloc_fib()
            self.launcher.send_examine(self.state, lock, fib_mem.addr)
            replies = self._run_until_replies()
            if not replies or replies[-1][2] == 0:
                for l in reversed(locks):
                    self.free_lock(l)
                return None
            info = _parse_fib(self.mem, fib_mem.addr)
            for l in reversed(locks):
                self.free_lock(l)
            return info

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
                self.launcher.send_examine(self.state, root_lock, fib_mem.addr)
                replies = self._run_until_replies()
                if replies and replies[-1][2]:
                    info = _parse_fib(self.mem, fib_mem.addr)
                    if info.get("name"):
                        self._volname = info["name"]
                        if root_lock:
                            self.free_lock(root_lock)
                        return self._volname
                if root_lock:
                    self.free_lock(root_lock)
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
            if path and path != "/" and self._is_neg_cached(path):
                return b""
            # split into parent lock + name
            parts = [p for p in path.split("/") if p]
            if not parts:
                return b""
            name = parts[-1]
            dir_path = "/" + "/".join(parts[:-1])
            dir_lock, _, locks = self.locate_path(dir_path)
            if dir_lock == 0 and dir_path != "/":
                if path and path != "/":
                    self._set_neg_cached(path)
                return b""
            _, name_bptr = self._alloc_bstr(name)
            fh_addr = self._alloc_fh()
            self.launcher.send_findinput(self.state, name_bptr, dir_lock, fh_addr)
            replies = self._run_until_replies()
            self._log_replies("findinput", replies)
            if not replies or replies[-1][2] == 0:
                if path and path != "/":
                    self._set_neg_cached(path)
                self._free_fh(fh_addr)
                for l in reversed(locks):
                    self.free_lock(l)
                return b""
            # optional seek
            if offset:
                self.launcher.send_seek_handle(self.state, fh_addr, offset, OFFSET_BEGINNING)
                self._run_until_replies()
            buf_mem = self._alloc_read_buf(size)
            self.launcher.send_read_handle(self.state, fh_addr, buf_mem.addr, size)
            replies = self._run_until_replies()
            self._log_replies("read", replies)
            if not replies or replies[-1][2] <= 0:
                self.launcher.send_end_handle(self.state, fh_addr)
                self._run_until_replies()
                self._free_fh(fh_addr)
                for l in reversed(locks):
                    self.free_lock(l)
                return b""
            nread = min(replies[-1][2], size)
            data = bytes(self.mem.r_block(buf_mem.addr, nread))
            self.launcher.send_end_handle(self.state, fh_addr)
            self._run_until_replies()
            self._free_fh(fh_addr)
            for l in reversed(locks):
                self.free_lock(l)
            return data

    def seek_handle(self, fh_addr: int, offset: int, mode: int = OFFSET_BEGINNING):
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
            self.launcher.send_seek_handle(self.state, fh_addr, offset, OFFSET_BEGINNING)
            self._run_until_replies()
            buf_mem = self._alloc_read_buf(size)
            self.launcher.send_read_handle(self.state, fh_addr, buf_mem.addr, size)
            replies = self._run_until_replies()
            if not replies or replies[-1][2] <= 0:
                return b""
            nread = min(replies[-1][2], size)
            return bytes(self.mem.r_block(buf_mem.addr, nread))


    def write_handle(self, fh_addr: int, data: bytes) -> int:
        with self._lock:
            buf_mem = self._alloc_read_buf(len(data))
            self.mem.w_block(buf_mem.addr, data)
            self.launcher.send_write_handle(self.state, fh_addr, buf_mem.addr, len(data))
            replies = self._run_until_replies()
            self._log_replies("write", replies)
            if not replies:
                return -1
            return replies[-1][2]

    def write_handle_at(self, fh_addr: int, offset: int, data: bytes) -> int:
        with self._lock:
            self.launcher.send_seek_handle(self.state, fh_addr, offset, OFFSET_BEGINNING)
            replies = self._run_until_replies()
            self._log_replies("seek", replies)
            buf_mem = self._alloc_read_buf(len(data))
            self.mem.w_block(buf_mem.addr, data)
            self.launcher.send_write_handle(self.state, fh_addr, buf_mem.addr, len(data))
            replies = self._run_until_replies()
            self._log_replies("write", replies)
            if not replies:
                return -1
            return replies[-1][2]

    def set_handle_size(self, fh_addr: int, size: int, mode: int = OFFSET_BEGINNING) -> int:
        with self._lock:
            self.launcher.send_set_file_size(self.state, fh_addr, size, mode)
            replies = self._run_until_replies()
            self._log_replies("setsize", replies)
            if not replies:
                return -1
            return replies[-1][2]

    def delete_object(self, parent_lock_bptr: int, name: str) -> Tuple[int, int]:
        with self._lock:
            _, name_bptr = self._alloc_bstr(name)
            self.launcher.send_delete_object(self.state, parent_lock_bptr, name_bptr)
            replies = self._run_until_replies()
            self._log_replies("delete", replies)
            if not replies:
                return 0, -1
            return replies[-1][2], replies[-1][3]

    def rename_object(
        self, src_lock_bptr: int, src_name: str, dst_lock_bptr: int, dst_name: str
    ) -> Tuple[int, int]:
        with self._lock:
            _, src_bptr = self._alloc_bstr(src_name)
            _, dst_bptr = self._alloc_bstr(dst_name)
            self.launcher.send_rename_object(
                self.state, src_lock_bptr, src_bptr, dst_lock_bptr, dst_bptr
            )
            replies = self._run_until_replies()
            self._log_replies("rename", replies)
            if not replies:
                return 0, -1
            return replies[-1][2], replies[-1][3]

    def create_dir(self, parent_lock_bptr: int, name: str) -> Tuple[int, int]:
        with self._lock:
            _, name_bptr = self._alloc_bstr(name)
            self.launcher.send_create_dir(self.state, parent_lock_bptr, name_bptr)
            replies = self._run_until_replies()
            self._log_replies("mkdir", replies)
            if not replies:
                return 0, -1
            return replies[-1][2], replies[-1][3]

    def close_file(self, fh_addr: int):
        with self._lock:
            self.launcher.send_end_handle(self.state, fh_addr)
            replies = self._run_until_replies()
            self._log_replies("end", replies)
            if self._write_enabled:
                self.launcher.send_flush(self.state)
                flush_replies = self._run_until_replies()
                self._log_replies("flush", flush_replies)
            self._free_fh(fh_addr)


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
        self._uid = getattr(os, 'getuid', lambda: 0)()
        self._gid = getattr(os, 'getgid', lambda: 0)()
        self._stat_cache: Dict[str, Tuple[float, Dict]] = {}  # path -> (timestamp, stat_result)
        self._cache_ttl = 3600.0  # Cache for 1 hour - read-only FS never changes
        self._neg_cache: Dict[str, float] = {}  # path -> timestamp for ENOENT results
        self._neg_cache_ttl = 3600.0  # Cache negative results for 1 hour
        self._dir_cache: Dict[str, Tuple[float, List[str]]] = {}  # path -> (timestamp, entries)
        self._dir_cache_ttl = 3600.0  # Cache directory listings for 1 hour
        if self.bridge._write_enabled:
            self._cache_ttl = 0.0
            self._neg_cache_ttl = 0.0
            self._dir_cache_ttl = 0.0
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
        if self._neg_cache_ttl <= 0:
            return False
        if path in self._neg_cache:
            if time.time() - self._neg_cache[path] < self._neg_cache_ttl:
                return True
            del self._neg_cache[path]
        return False

    def _set_neg_cached(self, path: str):
        """Cache a negative (ENOENT) result."""
        if self._neg_cache_ttl <= 0:
            return
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
            "st_uid": self._uid,
            "st_gid": self._gid,
        }

    def _split_path(self, path: str) -> Tuple[str, str]:
        parts = [p for p in path.split("/") if p]
        if not parts:
            return "/", ""
        name = parts[-1]
        dir_path = "/" + "/".join(parts[:-1]) if len(parts) > 1 else "/"
        return dir_path, name

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
        file_mode = 0o644 if self.bridge._write_enabled else 0o444
        mode = (0o755 if is_dir else file_mode) | (0o040000 if is_dir else 0o100000)
        now = int(time.time())
        result = {
            "st_mode": mode,
            "st_nlink": 2 if is_dir else 1,
            "st_size": info["size"],
            "st_ctime": now,
            "st_mtime": now,
            "st_atime": now,
            "st_uid": self._uid,
            "st_gid": self._gid,
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
            file_mode = 0o644 if self.bridge._write_enabled else 0o444
            mode = (0o755 if is_dir else file_mode) | (0o040000 if is_dir else 0o100000)
            stat_result = {
                "st_mode": mode,
                "st_nlink": 2 if is_dir else 1,
                "st_size": ent["size"],
                "st_ctime": int(now),
                "st_mtime": int(now),
                "st_atime": int(now),
                "st_uid": self._uid,
                "st_gid": self._gid,
            }
            self._stat_cache[child_path] = (now, stat_result)

        # Cache the directory listing
        self._dir_cache[path] = (now, entries)
        return entries

    def open(self, path, flags):
        if self._debug:
            print(f"[FUSE][open] path={path} flags=0x{flags:x}")
        if not self.bridge._write_enabled and (flags & (os.O_WRONLY | os.O_RDWR)):
            raise FuseOSError(errno.EROFS)
        opened = self.bridge.open_file(path, flags)
        if opened is None:
            info = self.bridge.stat_path(path)
            if info and info.get("dir_type", 0) >= 0:
                raise FuseOSError(errno.EISDIR)
            raise FuseOSError(errno.ENOENT)
        fh_addr, parent_lock = opened
        with self._fh_lock:
            handle = self._next_fh
            self._next_fh += 1
            self._fh_cache[handle] = {
                "fh_addr": fh_addr,
                "parent_lock": parent_lock,
                "pos": None,
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
            if entry["pos"] is None or offset != entry["pos"]:
                data = self.bridge.read_handle_at(fh_addr, offset, size)
            else:
                data = self.bridge.read_handle(fh_addr, size)
            if data is None:
                raise FuseOSError(errno.EIO)
            entry["pos"] = offset + len(data)
            return data

    def write(self, path, data, offset, fh):
        if self._debug:
            print(f"[FUSE][write] path={path} offset={offset} size={len(data)} fh={fh}")
        if not self.bridge._write_enabled:
            raise FuseOSError(errno.EROFS)
        with self._fh_lock:
            entry = self._fh_cache.get(fh)
        if entry is None:
            raise FuseOSError(errno.EIO)
        fh_addr = entry["fh_addr"]
        with entry["lock"]:
            if entry.get("closed"):
                raise FuseOSError(errno.EIO)
            if entry["pos"] is None or offset != entry["pos"]:
                written = self.bridge.write_handle_at(fh_addr, offset, data)
            else:
                written = self.bridge.write_handle(fh_addr, data)
            if written < 0:
                raise FuseOSError(errno.EIO)
            entry["pos"] = offset + written
        return written

    def truncate(self, path, length, fh=None):
        if self._debug:
            print(f"[FUSE][truncate] path={path} length={length} fh={fh}")
        if not self.bridge._write_enabled:
            raise FuseOSError(errno.EROFS)
        if fh is None:
            fh = self.open(path, os.O_WRONLY)
            temp_handle = True
        else:
            temp_handle = False
        try:
            with self._fh_lock:
                entry = self._fh_cache.get(fh)
            if entry is None:
                raise FuseOSError(errno.EIO)
            fh_addr = entry["fh_addr"]
            with entry["lock"]:
                if entry.get("closed"):
                    raise FuseOSError(errno.EIO)
                size = self.bridge.set_handle_size(fh_addr, length, OFFSET_BEGINNING)
                if size < 0:
                    raise FuseOSError(errno.EIO)
                if entry["pos"] is not None:
                    entry["pos"] = min(entry["pos"], length)
        finally:
            if temp_handle:
                self.release(path, fh)
        return 0

    def create(self, path, mode, fi=None):
        if self._debug:
            print(f"[FUSE][create] path={path} mode=0o{mode:o}")
        if not self.bridge._write_enabled:
            raise FuseOSError(errno.EROFS)
        if self._is_macos_special(path):
            raise FuseOSError(errno.ENOENT)
        opened = self.bridge.open_file(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
        if opened is None:
            raise FuseOSError(errno.EIO)
        fh_addr, parent_lock = opened
        with self._fh_lock:
            handle = self._next_fh
            self._next_fh += 1
            self._fh_cache[handle] = {
                "fh_addr": fh_addr,
                "parent_lock": parent_lock,
                "pos": None,
                "lock": threading.Lock(),
                "closed": False,
            }
        # Prime stat/dir cache so immediate getattr after create doesn't fail.
        now = time.time()
        self._stat_cache[path] = (
            now,
            {
                "st_mode": 0o100644,
                "st_nlink": 1,
                "st_size": 0,
                "st_ctime": int(now),
                "st_mtime": int(now),
                "st_atime": int(now),
                "st_uid": self._uid,
                "st_gid": self._gid,
            },
        )
        parent_path, _ = self._split_path(path)
        self._dir_cache.pop(parent_path, None)
        return handle

    def unlink(self, path):
        if not self.bridge._write_enabled:
            raise FuseOSError(errno.EROFS)
        dir_path, name = self._split_path(path)
        if not name:
            raise FuseOSError(errno.EINVAL)
        lock_bptr, _, locks = self.bridge.locate_path(dir_path)
        if lock_bptr == 0 and dir_path != "/":
            raise FuseOSError(errno.ENOENT)
        res1, _ = self.bridge.delete_object(lock_bptr, name)
        if res1 == 0:
            raise FuseOSError(errno.EIO)
        self._stat_cache.pop(path, None)
        self._dir_cache.pop(dir_path, None)
        for l in reversed(locks):
            self.bridge.free_lock(l)
        return 0

    def rmdir(self, path):
        return self.unlink(path)

    def mkdir(self, path, mode):
        if not self.bridge._write_enabled:
            raise FuseOSError(errno.EROFS)
        dir_path, name = self._split_path(path)
        if not name:
            raise FuseOSError(errno.EINVAL)
        parent_lock, _, locks = self.bridge.locate_path(dir_path)
        if parent_lock == 0 and dir_path != "/":
            raise FuseOSError(errno.ENOENT)
        res1, _ = self.bridge.create_dir(parent_lock, name)
        if res1 == 0:
            raise FuseOSError(errno.EIO)
        self._dir_cache.pop(dir_path, None)
        for l in reversed(locks):
            self.bridge.free_lock(l)
        return 0

    def rename(self, old, new):
        if self._debug:
            print(f"[FUSE][rename] old={old} new={new}")
        if not self.bridge._write_enabled:
            raise FuseOSError(errno.EROFS)
        src_dir, src_name = self._split_path(old)
        dst_dir, dst_name = self._split_path(new)
        if not src_name or not dst_name:
            raise FuseOSError(errno.EINVAL)
        src_lock, _, src_locks = self.bridge.locate_path(src_dir)
        dst_lock, _, dst_locks = self.bridge.locate_path(dst_dir)
        if (src_lock == 0 and src_dir != "/") or (dst_lock == 0 and dst_dir != "/"):
            raise FuseOSError(errno.ENOENT)
        res1, _ = self.bridge.rename_object(src_lock, src_name, dst_lock, dst_name)
        if res1 == 0:
            raise FuseOSError(errno.EIO)
        self._stat_cache.pop(old, None)
        self._stat_cache.pop(new, None)
        self._dir_cache.pop(src_dir, None)
        self._dir_cache.pop(dst_dir, None)
        for l in reversed(src_locks):
            self.bridge.free_lock(l)
        for l in reversed(dst_locks):
            self.bridge.free_lock(l)
        return 0

    def flush(self, path, fh):
        return 0

    def fsync(self, path, fdatasync, fh):
        return 0

    def chmod(self, path, mode):
        if not self.bridge._write_enabled:
            raise FuseOSError(errno.EROFS)
        # Amiga filesystems doesn't support Unix-style chmod; accept and ignore.
        return 0

    def chown(self, path, uid, gid):
        if not self.bridge._write_enabled:
            raise FuseOSError(errno.EROFS)
        # Ignore ownership changes; keep host uid/gid.
        return 0

    def utimens(self, path, times=None):
        if not self.bridge._write_enabled:
            raise FuseOSError(errno.EROFS)
        # Ignore timestamp updates for now.
        return 0

    def access(self, path, mode):
        if self._is_macos_special(path):
            raise FuseOSError(errno.ENOENT)
        if not self.bridge._write_enabled and (mode & os.W_OK):
            raise FuseOSError(errno.EROFS)
        return 0

    def listxattr(self, path):
        return []

    def getxattr(self, path, name, position=0):
        enoattr = getattr(errno, "ENOATTR", errno.ENODATA)
        raise FuseOSError(enoattr)

    def setxattr(self, path, name, value, options, position=0):
        if not self.bridge._write_enabled:
            raise FuseOSError(errno.EROFS)
        return 0

    def removexattr(self, path, name):
        if not self.bridge._write_enabled:
            raise FuseOSError(errno.EROFS)
        return 0

    def release(self, path, fh):
        with self._fh_lock:
            entry = self._fh_cache.get(fh)
            if not entry:
                return 0
            entry["closed"] = True
            del self._fh_cache[fh]
        with entry["lock"]:
            self.bridge.close_file(entry["fh_addr"])
            parent_lock = entry.get("parent_lock", 0)
            if parent_lock:
                self.bridge.free_lock(parent_lock)
        return 0


def mount_fuse(
    image: Path,
    driver: Path,
    mountpoint: Path,
    block_size: Optional[int],
    volname_opt: Optional[str] = None,
    debug: bool = False,
    write: bool = False,
):
    if mountpoint.exists() and os.path.ismount(mountpoint):
        raise SystemExit(
            f"Mountpoint {mountpoint} is already a mount; unmount it first (e.g. umount -f {mountpoint})."
        )

    if write:
        # Guard against accidental writes without explicit intent.
        print("[amifuse] write mode enabled; ensure the image is backed up")

    def _unmount_mountpoint():
        if not mountpoint.exists() or not os.path.ismount(mountpoint):
            return
        if sys.platform.startswith("darwin"):
            cmd = ["umount", "-f", str(mountpoint)]
        else:
            if shutil.which("fusermount"):
                cmd = ["fusermount", "-u", str(mountpoint)]
            else:
                cmd = ["umount", "-f", str(mountpoint)]
        subprocess.run(cmd, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def _start_signal_watcher():
        def _watch():
            sig = signal.sigwait({signal.SIGINT, signal.SIGTERM})
            if debug:
                print(f"[amifuse] signal {sig} received; unmounting {mountpoint}")
            _unmount_mountpoint()
            os._exit(0)

        try:
            signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT, signal.SIGTERM})
            threading.Thread(target=_watch, daemon=True).start()
        except (AttributeError, ValueError):
            def _handler(signum, _frame):
                if debug:
                    print(f"[amifuse] signal {signum} received; unmounting {mountpoint}")
                _unmount_mountpoint()
                os._exit(0)

            signal.signal(signal.SIGINT, _handler)
            signal.signal(signal.SIGTERM, _handler)

    _start_signal_watcher()
    bridge = HandlerBridge(
        image, driver, block_size=block_size, read_only=not write, debug=debug
    )
    volname = volname_opt or bridge.volume_name()
    # Multi-threaded mode with caching to minimize macOS polling.
    use_threads = not write
    fuse_kwargs = {
        "foreground": True,
        "ro": not write,
        "allow_other": False,
        "nothreads": not use_threads,
        "fsname": f"amifuse:{volname}",
        "subtype": "amifuse",
        "default_permissions": True,  # Let kernel handle permission checks
    }
    if sys.platform.startswith("darwin"):
        # macOS-specific options to reduce Finder/Spotlight polling
        fuse_kwargs.update({
            "volname": volname,  # Volume name shown in Finder
            "local": True,  # Tell macOS this is a local FS (not network)
            "noappledouble": True,  # Disable AppleDouble ._ files
            "noapplexattr": True,  # Disable Apple extended attributes
        })
    FUSE(AmigaFuseFS(bridge, debug=debug), str(mountpoint), **fuse_kwargs)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Mount an Amiga filesystem image via FUSE (experimental)."
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
    parser.add_argument(
        "--profile", action="store_true", help="Enable profiling and write stats to profile.txt."
    )
    parser.add_argument(
        "--write", action="store_true", help="Enable read-write mode (experimental)."
    )
 
    args = parser.parse_args(argv)

    if args.profile:
        profiler = cProfile.Profile()
        profiler.enable()

    mount_fuse(args.image, args.driver, args.mountpoint, args.block_size, args.volname, args.debug, args.write)

    if args.profile:
        profiler.disable()
        with open("profile.txt", "w") as f:
            stats = pstats.Stats(profiler, stream=f)
            stats.sort_stats(pstats.SortKey.CUMULATIVE)
            stats.print_stats()

if __name__ == "__main__":
    main()
