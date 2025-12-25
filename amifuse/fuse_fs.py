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
from amitools.vamos.libstructs.dos import FileInfoBlockStruct, FileHandleStruct, DosPacketStruct  # type: ignore


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


class OffsetBlockDeviceBackend:
    """Wraps a BlockDeviceBackend to present a partition as a logical device starting at block 0."""

    def __init__(self, backend, start_block: int, num_blocks: int):
        self.backend = backend
        self.start_block = start_block
        self.num_blocks = num_blocks
        self.block_size = backend.block_size
        self.read_only = backend.read_only
        # Forward RDB for volume name lookup
        self.rdb = getattr(backend, 'rdb', None)
        # Geometry
        self.heads = backend.heads
        self.secs = backend.secs
        # Calculate logical cylinders
        blk_per_cyl = self.heads * self.secs
        self.cyls = num_blocks // blk_per_cyl if blk_per_cyl else 0
        self.total_blocks = num_blocks

    def close(self):
        self.backend.close()

    def read_blocks(self, blk_num: int, num_blks: int = 1) -> bytes:
        if blk_num + num_blks > self.total_blocks:
            num_blks = max(0, self.total_blocks - blk_num)
            if num_blks == 0:
                return b''
        return self.backend.read_blocks(self.start_block + blk_num, num_blks)

    def write_blocks(self, blk_num: int, data: bytes, num_blks: int = 1):
        if blk_num + num_blks > self.total_blocks:
            num_blks = max(0, self.total_blocks - blk_num)
            if num_blks == 0:
                return
            data = data[:num_blks * self.block_size]
        self.backend.write_blocks(self.start_block + blk_num, data, num_blks)

    def sync(self):
        self.backend.sync()


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
        trace: bool = False,
        partition: Optional[str] = None,
    ):
        self._lock = threading.RLock()  # Reentrant lock for thread safety
        self._debug = debug
        self.backend = BlockDeviceBackend(image, block_size=block_size, read_only=read_only)
        self.backend.open()
        self.vh = VamosHandlerRuntime()
        # Use 68020 CPU for compatibility with SFS and other modern handlers
        self.vh.setup(cpu="68020")
        if trace:
            self.vh.enable_trace()
        self.mem = self.vh.alloc.get_mem()
        seg_baddr = self.vh.load_handler(driver)
        # Build DeviceNode/FSSM using seglist bptr
        ba = BootstrapAllocator(self.vh, image, partition=partition)
        boot = ba.alloc_all(handler_seglist_baddr=seg_baddr, handler_seglist_bptr=seg_baddr, handler_name="DH0:")
        self._partition_index = boot["part"].num if boot.get("part") else 0

        # For FFS compatibility: wrap backend to present partition starting at block 0,
        # and patch DosEnvec to set LowCyl=0, Surfaces=0 (triggers FFS auto-detect).
        if boot.get("part") and boot.get("env_addr"):
            part = boot["part"]
            blk_per_cyl = self.backend.heads * self.backend.secs
            dos_env = part.part_blk.dos_env
            start_cyl = dos_env.low_cyl
            end_cyl = dos_env.high_cyl
            num_cyl = end_cyl - start_cyl + 1
            start_blk = start_cyl * blk_per_cyl
            num_blk = num_cyl * blk_per_cyl

            if debug:
                print(f"[amifuse] Partition geometry:")
                print(f"[amifuse]   Original: LowCyl={start_cyl} HighCyl={end_cyl} Surfaces={dos_env.surfaces} BlkPerTrack={dos_env.blk_per_trk}")
                print(f"[amifuse]   Calculated: start_blk={start_blk} num_blk={num_blk} num_cyl={num_cyl}")

            # Wrap backend so partition appears to start at block 0
            self.backend = OffsetBlockDeviceBackend(self.backend, start_blk, num_blk)

            # Patch DosEnvec for FFS compatibility
            env_addr = boot["env_addr"]
            self.mem.w32(env_addr + 0x24, 0)  # de_LowCyl = 0
            self.mem.w32(env_addr + 0x28, num_cyl - 1)  # de_HighCyl = logical cylinders - 1
            if debug:
                print(f"[amifuse]   Patched DosEnvec: LowCyl=0 HighCyl={num_cyl - 1}")

        self.vh.set_scsi_backend(self.backend, debug=debug)

        # Handler entry point is at segment start (byte 0).
        # AmigaOS starts C/assembler handlers at the first byte of the first segment.
        # The handler's own startup code will set up registers and call WaitPort/GetMsg
        # to retrieve the startup packet from pr_MsgPort.
        seg_info = self.vh.slm.seg_loader.infos[seg_baddr]
        seglist = seg_info.seglist
        seg_addr = seglist.get_segment().get_addr()
        self.launcher = HandlerLauncher(self.vh, boot, seg_addr)
        self.state = self.launcher.launch_with_startup(debug=debug)
        # run startup to completion
        self._run_until_replies()
        self._update_handler_port_from_startup()
        # Let the handler settle into its message wait loop.
        self._capture_main_loop_state()
        if not self.state.initialized:
            self.state.initialized = True
        if self._debug:
            print(f"[amifuse] Saved main_loop_pc=0x{self.state.pc:x}, main_loop_sp=0x{self.state.sp:x}")
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
        if self._debug:
            print(
                f"[amifuse] handler loaded seg_baddr=0x{seg_baddr:x} seg_addr=0x{seg_addr:x} "
                f"port=0x{self.state.port_addr:x} reply=0x{self.state.reply_port_addr:x}"
            )

    def _run_until_replies(self, max_iters: int = 50, cycles: int = 200_000, sleep_base: float = 0.0005, sleep_max: float = 0.01):
        """Run handler bursts until at least one reply is queued or iterations exhausted."""
        from amitools.vamos.lib.ExecLibrary import ExecLibrary
        replies = []
        sleep_time = sleep_base

        # Check if handler has crashed - if so, don't try to run it
        if self.state.crashed:
            return []

        # Guard against re-entering at the exit trap addresses (0x400/0x402).
        # The emulator uses low memory addresses as trap vectors; if PC lands there,
        # it means the handler tried to exit. Reset to main loop to keep it alive.
        if self.state.pc <= 0x1000 and getattr(self.state, "main_loop_pc", 0):
            self.state.pc = self.state.main_loop_pc
            self.state.sp = self.state.main_loop_sp
        for i in range(max_iters):
            self.launcher.run_burst(self.state, max_cycles=cycles)
            rs = self.state.run_state
            # Check if handler crashed during this burst
            if self.state.crashed:
                return []
            # Check for replies first - if we have them, we're done
            replies = self.launcher.poll_replies(self.state.reply_port_addr)
            if replies:
                break
            # If handler is blocked in WaitPort/Wait with no messages, stop spinning
            if ExecLibrary._waitport_blocked_sp is not None or ExecLibrary._wait_blocked_sp is not None:
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

    def _capture_main_loop_state(self, max_iters: int = 10, cycles: int = 200_000):
        """Run until the handler blocks in Wait/WaitPort and capture restart PC/SP."""
        from amitools.vamos.lib.ExecLibrary import ExecLibrary
        for _ in range(max_iters):
            self.launcher.run_burst(self.state, max_cycles=cycles)
            if self.state.main_loop_pc:
                return
            waitport_sp = ExecLibrary._waitport_blocked_sp
            wait_sp = ExecLibrary._wait_blocked_sp
            waitport_ret = ExecLibrary._waitport_blocked_ret
            wait_ret = ExecLibrary._wait_blocked_ret
            blocked_sp = waitport_sp if waitport_sp is not None else wait_sp
            blocked_ret = waitport_ret if waitport_ret is not None else wait_ret
            if blocked_sp is not None:
                ret_addr = blocked_ret if blocked_ret is not None else self.mem.r32(blocked_sp)
                if ret_addr >= 0x800:
                    self.state.main_loop_pc = ret_addr
                    self.state.main_loop_sp = blocked_sp + 4
                return

    def _update_handler_port_from_startup(self):
        pkt = AccessStruct(self.mem, DosPacketStruct, self.state.stdpkt_addr)
        res1 = pkt.r_s("dp_Res1")
        res2 = pkt.r_s("dp_Res2")
        if res1:
            alt_port = pkt.r_s("dp_Arg4")
            if alt_port and alt_port != self.state.port_addr:
                pmgr = self.vh.slm.exec_impl.port_mgr
                if not pmgr.has_port(alt_port):
                    pmgr.register_port(alt_port)
                self.state.port_addr = alt_port
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
                    if self._debug:
                        print(f"[amifuse][open_file] FINDOUTPUT name={name!r} dir_lock=0x{dir_lock:x} fh_addr=0x{fh_addr:x}", flush=True)
                    self.launcher.send_findoutput(self.state, name_bptr, dir_lock, fh_addr)
                else:
                    fh_addr = self._alloc_fh()
                    if self._debug:
                        print(f"[amifuse][open_file] FINDUPDATE name={name!r} dir_lock=0x{dir_lock:x} fh_addr=0x{fh_addr:x}", flush=True)
                    self.launcher.send_findupdate(self.state, name_bptr, dir_lock, fh_addr)
            replies = self._run_until_replies()
            self._log_replies("find", replies)
            if not replies or replies[-1][2] == 0:
                if self._debug:
                    res2 = replies[-1][3] if replies else -1
                    print(f"[amifuse][open_file] FAILED: replies={bool(replies)} res1={replies[-1][2] if replies else 'none'} res2={res2}", flush=True)
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
            # Note: ACTION_FLUSH is for flushing volume buffers, not file-specific.
            # We call flush_volume() on unmount instead of after every file close.
            self._free_fh(fh_addr)

    def flush_volume(self):
        """Flush the handler's buffers to disk. Call on unmount."""
        with self._lock:
            if self.state.crashed:
                return
            if self._debug:
                print("[amifuse] Flushing volume buffers to disk...", flush=True)
            self.launcher.send_flush(self.state)
            replies = self._run_until_replies()
            self._log_replies("flush_volume", replies)
            # Sync the underlying file to disk
            self.backend.sync()
            if self._debug:
                if replies and replies[-1][2] != 0:
                    print("[amifuse] Volume flush complete", flush=True)
                else:
                    print("[amifuse] Volume flush may have failed", flush=True)


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

    def _check_handler_alive(self):
        """Check if handler is alive and raise EIO if crashed."""
        if self.bridge.state.crashed:
            if self._debug and not getattr(self, '_crash_reported', False):
                import sys
                print(f"[FUSE] Handler crashed - all operations returning EIO", file=sys.stderr, flush=True)
                self._crash_reported = True
            raise FuseOSError(errno.EIO)

    def _log_op(self, op: str, path: str, extra: str = ""):
        """Log operation if debug is enabled."""
        if self._debug:
            import sys
            msg = f"[FUSE][{op}] {path}"
            if extra:
                msg += f" {extra}"
            print(msg, file=sys.stderr, flush=True)

    # --- FUSE operations ---
    def getattr(self, path, fh=None):
        # Don't log getattr - too noisy. Only log on errors.
        self._check_handler_alive()
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
        self._check_handler_alive()
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
        self._log_op("open", path, f"flags=0x{flags:x}")
        self._check_handler_alive()
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
                "dirty": False,  # Track if file was written to
            }
        return handle

    def read(self, path, size, offset, fh):
        self._check_handler_alive()
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
        self._log_op("write", path, f"offset={offset} size={len(data)} fh={fh}")
        self._check_handler_alive()
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
            entry["dirty"] = True  # Mark as needing flush on close
        return written

    def truncate(self, path, length, fh=None):
        self._check_handler_alive()
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
                entry["dirty"] = True  # Truncate modifies the file
        finally:
            if temp_handle:
                self.release(path, fh)
        return 0

    def create(self, path, mode, fi=None):
        self._log_op("create", path, f"mode=0o{mode:o}")
        try:
            self._check_handler_alive()
            if not self.bridge._write_enabled:
                raise FuseOSError(errno.EROFS)
            if self._is_macos_special(path):
                raise FuseOSError(errno.ENOENT)
            opened = self.bridge.open_file(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
            if opened is None:
                self._log_op("create", path, "FAILED: open_file returned None")
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
                    "dirty": True,  # New files are always dirty
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
            self._log_op("create", path, f"SUCCESS handle={handle} fh_addr=0x{fh_addr:x}")
            return handle
        except FuseOSError:
            raise
        except Exception as e:
            self._log_op("create", path, f"EXCEPTION: {type(e).__name__}: {e}")
            raise FuseOSError(errno.EIO) from e

    def unlink(self, path):
        self._log_op("unlink", path, "")
        self._check_handler_alive()
        if not self.bridge._write_enabled:
            raise FuseOSError(errno.EROFS)
        dir_path, name = self._split_path(path)
        if not name:
            raise FuseOSError(errno.EINVAL)
        lock_bptr, _, locks = self.bridge.locate_path(dir_path)
        # For root directory, get a proper lock
        if dir_path == "/" and lock_bptr == 0:
            lock_bptr, _ = self.bridge.locate(0, "")
            if lock_bptr:
                locks.append(lock_bptr)
        if lock_bptr == 0:
            self._log_op("unlink", path, f"parent dir not found: {dir_path}")
            raise FuseOSError(errno.ENOENT)
        res1, res2 = self.bridge.delete_object(lock_bptr, name)
        self._log_op("unlink", path, f"delete_object res1={res1} res2={res2}")
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
        self._check_handler_alive()
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
        self._check_handler_alive()
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
        self._log_op("flush", path, f"fh={fh}")
        return 0

    def fsync(self, path, fdatasync, fh):
        self._log_op("fsync", path, f"fh={fh} fdatasync={fdatasync}")
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
        self._log_op("access", path, f"mode={mode}")
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
        self._log_op("release", path, f"fh={fh}")
        with self._fh_lock:
            entry = self._fh_cache.get(fh)
            if not entry:
                return 0
            entry["closed"] = True
            del self._fh_cache[fh]
        with entry["lock"]:
            self._log_op("release", path, f"closing fh_addr=0x{entry['fh_addr']:x}")
            self.bridge.close_file(entry["fh_addr"])
            parent_lock = entry.get("parent_lock", 0)
            if parent_lock:
                self.bridge.free_lock(parent_lock)
        return 0

    def destroy(self, path):
        """Called when filesystem is unmounted. Flush handler buffers."""
        print("[amifuse] Unmounting - flushing volume...", flush=True)
        if self.bridge._write_enabled:
            self.bridge.flush_volume()
        print("[amifuse] Unmount complete.", flush=True)


def get_partition_info(image: Path, block_size: Optional[int], partition: Optional[str]) -> dict:
    """Get partition name and dostype from RDB."""
    from .rdb_inspect import open_rdisk
    blkdev, rdisk = open_rdisk(image, block_size=block_size)
    try:
        if partition is None:
            part = rdisk.get_partition(0)
        else:
            part = rdisk.find_partition_by_string(str(partition))
        if part is None:
            return {"name": "AmigaFS", "dostype": None}
        return {
            "name": str(part.get_drive_name()),
            "dostype": part.part_blk.dos_env.dos_type,
        }
    finally:
        rdisk.close()
        blkdev.close()


def get_partition_name(image: Path, block_size: Optional[int], partition: Optional[str]) -> str:
    """Get the partition name from RDB without starting the handler."""
    return get_partition_info(image, block_size, partition)["name"]


def extract_embedded_driver(image: Path, block_size: Optional[int], partition: Optional[str]) -> Optional[Path]:
    """Extract filesystem driver from RDB if available for the partition's dostype.

    Returns path to temp file containing the driver, or None if not found.
    """
    import tempfile
    import amitools.fs.DosType as DosType
    from .rdb_inspect import open_rdisk

    blkdev, rdisk = open_rdisk(image, block_size=block_size)
    try:
        # Get the partition and its dostype
        if partition is None:
            part = rdisk.get_partition(0)
        else:
            part = rdisk.find_partition_by_string(str(partition))
        if part is None:
            return None

        target_dostype = part.part_blk.dos_env.dos_type
        dt_str = DosType.num_to_tag_str(target_dostype)

        # Search filesystem blocks for matching dostype
        for fs in rdisk.fs:
            if fs.fshd.dos_type == target_dostype:
                # Found matching filesystem - extract to temp file
                data = fs.get_data()
                # Create temp file that persists until program exits
                fd, temp_path = tempfile.mkstemp(suffix=f"_{dt_str}.handler", prefix="amifuse_")
                os.write(fd, data)
                os.close(fd)
                return Path(temp_path), dt_str, target_dostype

        return None
    finally:
        rdisk.close()
        blkdev.close()


def mount_fuse(
    image: Path,
    driver: Optional[Path],
    mountpoint: Optional[Path],
    block_size: Optional[int],
    volname_opt: Optional[str] = None,
    debug: bool = False,
    trace: bool = False,
    write: bool = False,
    partition: Optional[str] = None,
):
    # Get partition name for display and auto-mountpoint
    try:
        part_name = get_partition_name(image, block_size, partition)
    except IOError as e:
        raise SystemExit(f"Error: {e}")

    # If no driver specified, try to extract from RDB
    temp_driver = None
    driver_desc = None
    if driver is None:
        try:
            result = extract_embedded_driver(image, block_size, partition)
        except IOError as e:
            raise SystemExit(f"Error: {e}")
        if result is None:
            raise SystemExit(
                "No embedded filesystem driver found for this partition.\n"
                "You need to specify a filesystem handler with --driver"
            )
        temp_driver, dt_str, dostype = result
        driver = temp_driver
        driver_desc = f"{dt_str}/0x{dostype:08x} (from RDB)"
    else:
        driver_desc = str(driver)

    # Auto-create mountpoint on macOS/Windows if not specified
    if mountpoint is None:
        if sys.platform.startswith("darwin"):
            mountpoint = Path(f"/Volumes/{volname_opt or part_name}")
        elif sys.platform.startswith("win"):
            # Find first available drive letter (starting from A:)
            import string
            for letter in string.ascii_uppercase:
                drive = f"{letter}:"
                if not os.path.exists(drive):
                    mountpoint = Path(drive)
                    break
            if mountpoint is None:
                raise SystemExit("No available drive letter found. Use --mountpoint to specify one.")
        else:
            raise SystemExit("--mountpoint is required on Linux")

    if mountpoint.exists() and os.path.ismount(mountpoint):
        raise SystemExit(
            f"Mountpoint {mountpoint} is already a mount; unmount it first (e.g. umount -f {mountpoint})."
        )

    # Create mountpoint directory if it doesn't exist
    # On macOS /Volumes, FUSE creates the mount point automatically
    if not mountpoint.exists():
        if sys.platform.startswith("darwin") and str(mountpoint).startswith("/Volumes/"):
            # macFUSE will create the mount point in /Volumes
            pass
        else:
            mountpoint.mkdir(parents=True)

    # Print startup banner
    print(f"amifuse {__version__} - Copyright (C) 2025 by Stefan Reinauer")
    print(f"Mounting partition '{part_name}' from {image}")
    print(f"Filesystem driver: {driver_desc}")
    print(f"Mount point: {mountpoint}")

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

    bridge = HandlerBridge(
        image,
        driver,
        block_size=block_size,
        read_only=not write,
        debug=debug,
        trace=trace,
        partition=partition
    )

    # Let default signal handling work - FUSE will call destroy() on unmount
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
    try:
        FUSE(AmigaFuseFS(bridge, debug=debug), str(mountpoint), **fuse_kwargs)
    finally:
        # Clean up temp driver file if we extracted one
        if temp_driver is not None and temp_driver.exists():
            temp_driver.unlink()


__version__ = "0.1.0"


def cmd_inspect(args):
    """Handle the 'inspect' subcommand."""
    from .rdb_inspect import open_rdisk, format_fs_summary

    blkdev = None
    rdisk = None
    try:
        try:
            blkdev, rdisk = open_rdisk(args.image, block_size=args.block_size)
        except IOError as e:
            raise SystemExit(f"Error: {e}")
        for line in rdisk.get_info(full=args.full):
            print(line)
        fs_lines = format_fs_summary(rdisk)
        if fs_lines:
            print("\nFilesystem drivers:")
            for line in fs_lines:
                print(" ", line)
    finally:
        if rdisk is not None:
            rdisk.close()
        if blkdev is not None:
            blkdev.close()


def cmd_mount(args):
    """Handle the 'mount' subcommand."""
    if args.profile:
        profiler = cProfile.Profile()
        profiler.enable()

    mount_fuse(
        args.image, args.driver, args.mountpoint,
        args.block_size, args.volname, args.debug, args.trace, args.write,
        partition=args.partition
    )

    if args.profile:
        profiler.disable()
        with open("profile.txt", "w") as f:
            stats = pstats.Stats(profiler, stream=f)
            stats.sort_stats(pstats.SortKey.CUMULATIVE)
            stats.print_stats()


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=f"amifuse {__version__} - Copyright (C) 2025 by Stefan Reinauer\n\n"
        "Mount Amiga filesystem images via FUSE.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # inspect subcommand
    inspect_parser = subparsers.add_parser(
        "inspect", help="Inspect RDB partitions and filesystems."
    )
    inspect_parser.add_argument("image", type=Path, help="Disk image file")
    inspect_parser.add_argument(
        "--block-size", type=int, help="Override block size (defaults to auto/512)."
    )
    inspect_parser.add_argument(
        "--full", action="store_true", help="Show full partition details."
    )
    inspect_parser.set_defaults(func=cmd_inspect)

    # mount subcommand
    mount_parser = subparsers.add_parser(
        "mount", help="Mount an Amiga filesystem image via FUSE."
    )
    mount_parser.add_argument("image", type=Path, help="Disk image file")
    mount_parser.add_argument("--driver", type=Path, help="Filesystem binary (default: extract from RDB if available)")
    mount_parser.add_argument("--mountpoint", type=Path, help="Mount location (default: /Volumes/<partition> on macOS, first free drive letter on Windows)")
    mount_parser.add_argument(
        "--partition", type=str, help="Partition name (e.g. DH0) or index (defaults to first)."
    )
    mount_parser.add_argument(
        "--block-size", type=int, help="Override block size (defaults to auto/512)."
    )
    mount_parser.add_argument(
        "--volname", type=str, help="Override macFUSE volume name (defaults to partition name)."
    )
    mount_parser.add_argument(
        "--debug", action="store_true", help="Enable debug logging of FUSE operations."
    )
    mount_parser.add_argument(
        "--trace",
        action="store_true",
        help="Enable vamos instruction tracing (very noisy).",
    )
    mount_parser.add_argument(
        "--profile", action="store_true", help="Enable profiling and write stats to profile.txt."
    )
    mount_parser.add_argument(
        "--write", action="store_true", help="Enable read-write mode (experimental)."
    )
    mount_parser.set_defaults(func=cmd_mount)

    args = parser.parse_args(argv)
    args.func(args)

if __name__ == "__main__":
    main()
