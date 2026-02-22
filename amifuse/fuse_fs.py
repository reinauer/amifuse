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
from .process_mgr import ProcessManager
from .startup_runner import HandlerLauncher, OFFSET_BEGINNING, _get_block_state, _clear_all_block_state
from amitools.vamos.astructs.access import AccessStruct  # type: ignore
from amitools.vamos.libstructs.dos import FileInfoBlockStruct, FileHandleStruct, DosPacketStruct  # type: ignore
from amitools.vamos.lib.dos.DosProtection import DosProtection  # type: ignore


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
    protection = mem.r32(fib_addr + 116)  # fib_Protection (LONG)
    size = mem.r32(fib_addr + 124)      # fib_Size (unsigned ULONG)
    name_bytes = mem.r_block(fib_addr + 8, 108)  # fib_FileName starts at offset 8
    name_len = name_bytes[0]
    name = name_bytes[1 : 1 + name_len].decode("latin-1", errors="ignore")
    return {
        "dir_type": dir_type,
        "size": size,
        "name": name,
        "protection": protection,
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
        trace: bool = False,
        partition: Optional[str] = None,
        adf_info=None,
    ):
        self._lock = threading.RLock()  # Reentrant lock for thread safety
        self._debug = debug
        self._adf_info = adf_info
        # For MBR images with multiple 0x76 partitions, find the right one
        mbr_idx = None
        if partition and adf_info is None:
            from .rdb_inspect import find_partition_mbr_index
            mbr_idx = find_partition_mbr_index(image, block_size, partition)
        self.backend = BlockDeviceBackend(
            image, block_size=block_size, read_only=read_only, adf_info=adf_info,
            mbr_partition_index=mbr_idx,
        )
        self.backend.open()
        self.vh = VamosHandlerRuntime()
        # Use 68020 CPU for compatibility with SFS and other modern handlers
        self.vh.setup(cpu="68020")
        if trace:
            self.vh.enable_trace()
        self.mem = self.vh.alloc.get_mem()
        seg_baddr = self.vh.load_handler(driver)
        # Build DeviceNode/FSSM using seglist bptr
        ba = BootstrapAllocator(
            self.vh, image, partition=partition, adf_info=adf_info,
            mbr_partition_index=mbr_idx,
        )
        handler_name = "DF0:" if adf_info else "DH0:"
        boot = ba.alloc_all(handler_seglist_baddr=seg_baddr, handler_seglist_bptr=seg_baddr, handler_name=handler_name)
        self._partition_index = boot["part"].num if boot.get("part") else 0

        # Log partition geometry for debugging.
        if boot.get("part") and debug:
            part = boot["part"]
            if adf_info:
                # For ADF, use the synthetic info
                print(f"[amifuse] ADF geometry:")
                floppy_type = "HD" if adf_info.is_hd else "DD"
                print(f"[amifuse]   Type: {floppy_type} floppy")
                print(f"[amifuse]   Cylinders={adf_info.cylinders} Heads={adf_info.heads} Sectors={adf_info.sectors_per_track}")
                print(f"[amifuse]   Total blocks: {adf_info.total_blocks}")
            else:
                dos_env = part.part_blk.dos_env
                # Use partition geometry from DosEnvec, not disk geometry
                blk_per_cyl = dos_env.surfaces * dos_env.blk_per_trk
                start_cyl = dos_env.low_cyl
                end_cyl = dos_env.high_cyl
                num_cyl = end_cyl - start_cyl + 1
                start_blk = start_cyl * blk_per_cyl
                num_blk = num_cyl * blk_per_cyl

                print(f"[amifuse] Partition geometry:")
                print(f"[amifuse]   DosEnvec: LowCyl={start_cyl} HighCyl={end_cyl} Surfaces={dos_env.surfaces} BlkPerTrack={dos_env.blk_per_trk}")
                print(f"[amifuse]   Calculated: blk_per_cyl={blk_per_cyl} start_blk={start_blk} num_blk={num_blk}")

        # Check that the partition data is within the image file
        if boot.get("part") and not adf_info:
            dos_env = boot["part"].part_blk.dos_env
            blk_per_cyl = dos_env.surfaces * dos_env.blk_per_trk
            part_end_byte = (dos_env.high_cyl + 1) * blk_per_cyl * self.backend.block_size
            image_size = os.path.getsize(str(image))
            if part_end_byte > image_size:
                part_start_byte = dos_env.low_cyl * blk_per_cyl * self.backend.block_size
                part_name = partition or f"#{boot['part'].num}"
                if part_start_byte >= image_size:
                    raise SystemExit(
                        f"Partition '{part_name}' starts at byte {part_start_byte:,} "
                        f"but image is only {image_size:,} bytes ({image_size/1024/1024:.0f} MB). "
                        f"The image appears to be truncated."
                    )
                else:
                    print(
                        f"[amifuse] WARNING: Partition extends to byte {part_end_byte:,} "
                        f"but image is only {image_size:,} bytes. Data may be incomplete."
                    )

        self.vh.set_scsi_backend(self.backend, debug=debug)

        # Handler entry point is at segment start (byte 0).
        # AmigaOS starts C/assembler handlers at the first byte of the first segment.
        # The handler's own startup code will set up registers and call WaitPort/GetMsg
        # to retrieve the startup packet from pr_MsgPort.
        seg_info = self.vh.slm.seg_loader.infos[seg_baddr]
        seglist = seg_info.seglist
        seg_addr = seglist.get_segment().get_addr()
        seg_size = seglist.get_segment().get_size()
        if debug:
            print(f"[amifuse] Segment: addr=0x{seg_addr:x} size={seg_size} end=0x{seg_addr+seg_size:x}")
        self.launcher = HandlerLauncher(self.vh, boot, seg_addr)
        self.state = self.launcher.launch_with_startup(debug=debug)

        # Initialize ProcessManager early for multi-process support (e.g., SFS)
        # This must happen before _run_until_replies since SFS creates children during startup
        self.proc_mgr = ProcessManager(
            vh=self.vh,
            machine=self.vh.machine,
            exec_impl=self.vh.slm.exec_impl,
            parent_proc_addr=self.state.process_addr
        )

        # run startup to completion
        # Use moderate cycle count for startup to allow child processes to run.
        # SFS creates child processes that need to run and register ports.
        # Smaller bursts give us more chances to interleave child execution.
        # Use small bursts for startup to frequently yield and run children
        replies = self._run_until_replies(cycles=10_000, max_iters=2000, drain_non_essential=False)
        # Check if startup succeeded - res1=0 means the handler rejected the disk
        pkt = AccessStruct(self.mem, DosPacketStruct, self.state.stdpkt_addr)
        startup_res1 = pkt.r_s("dp_Res1")
        startup_res2 = pkt.r_s("dp_Res2")
        if self._debug:
            print(f"[amifuse] Startup packet result: res1={startup_res1} res2={startup_res2}")
        if startup_res1 == 0:
            # Handler rejected the disk - likely invalid filesystem signature
            error_msgs = {
                218: "Not a valid DOS disk (NDOS)",
                225: "Not a DOS disk",
                226: "Wrong disk type",
                303: "Object not found",
            }
            error_desc = error_msgs.get(startup_res2, f"error code {startup_res2}")
            raise SystemExit(f"Filesystem handler rejected the disk: {error_desc}")
        self._update_handler_port_from_startup()
        # Capture main loop state from the current blocking state BEFORE
        # clearing it.  run_burst() during _run_until_replies may have
        # already set main_loop_pc; if not, we grab it from the raw
        # blocking variables.  This is critical for WaitPort-based handlers
        # (e.g. PFS3 on large partitions) where clearing the blocking
        # state and setting D0=0 would cause a NULL-message crash when the
        # handler resumes from the WaitPort return address.
        from amitools.vamos.machine.regs import REG_D0
        cpu = self.vh.machine.cpu
        if not self.state.main_loop_pc:
            from amitools.vamos.lib.ExecLibrary import ExecLibrary as _EL
            _wps = _get_block_state(_EL, '_waitport_blocked_sp')
            _ws = _get_block_state(_EL, '_wait_blocked_sp')
            _wpr = _get_block_state(_EL, '_waitport_blocked_ret')
            _wr = _get_block_state(_EL, '_wait_blocked_ret')
            _wm = _get_block_state(_EL, '_wait_blocked_mask')
            _bsp = _wps if _wps is not None else _ws
            _bret = _wpr if _wpr is not None else _wr
            if _bsp is not None:
                _ret = _bret if _bret is not None else self.mem.r32(_bsp)
                if _ret >= 0x800:
                    self.state.main_loop_pc = _ret
                    self.state.main_loop_sp = _bsp + 4
                    if _wm is not None and _wm != 0:
                        self.state.wait_mask = _wm
                    self.state.initialized = True
                    if self._debug:
                        mask_str = f" wait_mask=0x{self.state.wait_mask:x}" if self.state.wait_mask else ""
                        print(f"[amifuse] Captured main_loop from blocking state: pc=0x{_ret:x}, sp=0x{_bsp+4:x}{mask_str}")
        _clear_all_block_state()
        cpu.w_reg(REG_D0, 0)
        # Let the handler settle into its message wait loop (only if not
        # already captured above).
        self._capture_main_loop_state()
        if not self.state.initialized:
            self.state.initialized = True
        if self._debug:
            print(f"[amifuse] Saved main_loop_pc=0x{self.state.pc:x}, main_loop_sp=0x{self.state.sp:x}")
        # Flush pending timer events so the handler can complete delayed disk
        # validation.  PFS3 uses TR_ADDREQUEST to schedule deferred volume
        # validation; the timer signal must fire before the volume becomes
        # accessible.  We deliver all pending signals from the handler's
        # wait mask so it can process any queued timer or I/O events.
        self._flush_pending_signals()
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

    def _run_until_replies(self, max_iters: int = 50, cycles: int = 200_000, sleep_base: float = 0.0005, sleep_max: float = 0.01, drain_non_essential: bool = True):
        """Run handler bursts until at least one reply is queued or iterations exhausted."""
        from amitools.vamos.lib.ExecLibrary import ExecLibrary
        replies = []
        sleep_time = sleep_base

        # Check if handler has crashed - if so, don't try to run it
        if self.state.crashed:
            if self._debug:
                print("[amifuse] _run_until_replies: handler crashed, returning empty")
            return []

        # Guard against re-entering at the exit trap addresses (0x400/0x402).
        # The emulator uses low memory addresses as trap vectors; if PC lands there,
        # it means the handler tried to exit. Reset to main loop to keep it alive.
        if self.state.pc <= 0x1000 and getattr(self.state, "main_loop_pc", 0):
            self.state.pc = self.state.main_loop_pc
            self.state.sp = self.state.main_loop_sp

        # If handler is at main_loop_pc and there's a pending message, set D0 to wake it
        # This handles the case where handler is blocked waiting but blocked state was cleared
        if self.state.main_loop_pc and self.state.pc == self.state.main_loop_pc:
            pmgr = self.vh.slm.exec_impl.port_mgr
            has_msg = pmgr.has_msg(self.state.port_addr)
            if self._debug:
                # Show all ports and their queue status, including sigbit and memory list head
                from amitools.vamos.libstructs.exec_ import MsgPortStruct, ListStruct
                port_status = []
                for addr, port in pmgr.ports.items():
                    qlen = len(port.queue) if port.queue else 0
                    # Read mp_SigBit from memory (offset 15 in MsgPort)
                    try:
                        sigbit = self.mem.r8(addr + 15)  # mp_SigBit offset
                        # Read mp_MsgList.lh_Head to see if message is in memory
                        lst_off = MsgPortStruct.sdef.find_field_def_by_name("mp_MsgList").offset
                        lh_head = self.mem.r32(addr + lst_off)
                        lh_tail_addr = addr + lst_off + ListStruct.sdef.find_field_def_by_name("lh_Tail").offset
                        # If lh_Head == lh_Tail address, list is empty
                        list_empty = (lh_head == lh_tail_addr or lh_head == 0)
                    except:
                        sigbit = -1
                        lh_head = 0
                        list_empty = True
                    list_str = "empty" if list_empty else f"msg@0x{lh_head:x}"
                    port_status.append(f"0x{addr:x}:py{qlen}/sig{sigbit}/{list_str}")
                print(f"[amifuse] Ports: {' '.join(port_status)}")
            if has_msg:
                # Only deliver the DOS port signal (bit 8) during normal
                # packet processing.  Timer and other signals were already
                # handled during _flush_pending_signals at init time.
                # Delivering ALL signals here causes a timer storm: the
                # handler processes the timer, re-arms it, and SendIO
                # queues another reply, triggering the timer on every
                # single packet until the handler state gets corrupted.
                dos_signal = 0x100  # bit 8 = DOS port signal
                self.vh.machine.cpu.w_reg(0, dos_signal)  # REG_D0
                # Also set tc_SigRecvd in case handler checks the task structure
                from amitools.vamos.libstructs.exec_ import TaskStruct
                sigrecvd_off = TaskStruct.sdef.find_field_def_by_name("tc_SigRecvd").offset
                proc_addr = self.state.process_addr
                self.mem.w32(proc_addr + sigrecvd_off, dos_signal)
                if self._debug:
                    print(f"[amifuse] Resuming: D0=0x{dos_signal:x} tc_SigRecvd@0x{proc_addr + sigrecvd_off:x}=0x{dos_signal:x}")

        for i in range(max_iters):
            old_pc = self.state.pc
            self.launcher.run_burst(self.state, max_cycles=cycles)
            rs = self.state.run_state
            new_pc = self.state.pc
            # Log first few iterations to debug handler movement
            if self._debug and i < 3:
                cycles_run = getattr(rs, 'cycles', 0) if rs else 0
                print(f"[amifuse] iter {i}: PC 0x{old_pc:x}->0x{new_pc:x} cycles={cycles_run} done={getattr(rs, 'done', False)} error={getattr(rs, 'error', None)}")
            # Check if handler crashed during this burst
            if self.state.crashed:
                if self._debug:
                    print(f"[amifuse] _run_until_replies: handler crashed at iter {i}")
                return []
            # Check for replies first - if we have them, we're done
            replies = self.launcher.poll_replies(self.state.reply_port_addr, debug=self._debug)
            if replies:
                if self._debug and i > 5:
                    print(f"[amifuse] _run_until_replies: got {len(replies)} replies after {i} iters")
                break

            # Run any child processes that were created (e.g., SFS DosList handler)
            # This allows children to initialize and register their ports
            if hasattr(self, 'proc_mgr'):
                if self._debug and i < 5:
                    print(f"[amifuse] iter {i}: calling run_all_ready_children")
                num_children = self.proc_mgr.run_all_ready_children(cycles_per_child=cycles // 4)
                if num_children > 0 and self._debug:
                    print(f"[amifuse] Ran {num_children} child process(es)")
                # CRITICAL: Clear global blocking state after running children.
                # Otherwise the child's blocking state gets applied to the parent.
                _clear_all_block_state()
            else:
                if self._debug and i < 5:
                    print(f"[amifuse] iter {i}: no proc_mgr")

            # Check for error FIRST - errors take precedence over blocked state
            if getattr(rs, "error", None):
                # Error during run - might be WaitPort block or FindPort yield
                # Check if we have replies first
                replies = self.launcher.poll_replies(self.state.reply_port_addr, debug=self._debug)
                if replies:
                    break
                # No replies - run children and continue (don't break)
                if hasattr(self, 'proc_mgr'):
                    self.proc_mgr.run_all_ready_children(cycles_per_child=cycles // 2)
                    _clear_all_block_state()  # Clear child's blocking state
                continue
            # If handler exited (done=True) without blocking, stop looping - it's not coming back
            if getattr(rs, "done", False) and not self.state.initialized:
                # Handler exited during startup without entering main loop
                # The startup packet results are in the packet struct - let caller check them
                break
            # Yield with exponential backoff to avoid tight polling loops.
            if sleep_base > 0:
                time.sleep(sleep_time)
                sleep_time = min(sleep_time * 2, sleep_max)
        if self._debug and not replies:
            print(f"[amifuse] _run_until_replies: exhausted {max_iters} iters with no replies, pc=0x{self.state.pc:x}")

        # Drain stale timer/IO messages that accumulated during packet
        # processing.  SendIO queues replies synchronously; without this
        # drain, the next _run_until_replies would deliver timer signals
        # alongside the DOS signal, causing a timer storm.
        # During startup (drain_non_essential=False), keep timer messages
        # so _flush_pending_signals can deliver them for deferred init.
        if drain_non_essential:
            pmgr = self.vh.slm.exec_impl.port_mgr
            dos_port = self.state.port_addr
            reply_port = self.state.reply_port_addr
            for port_addr in list(pmgr.ports.keys()):
                if port_addr != dos_port and port_addr != reply_port:
                    while pmgr.has_msg(port_addr):
                        pmgr.get_msg(port_addr)

        return replies

    def _capture_main_loop_state(self, max_iters: int = 50, cycles: int = 10_000):
        """Run until the handler blocks in Wait/WaitPort and capture restart PC/SP."""
        if self.state.main_loop_pc:
            return  # Already captured (e.g. during _run_until_replies)
        from amitools.vamos.lib.ExecLibrary import ExecLibrary
        for i in range(max_iters):
            if self.state.pc < 0x800 or self.state.pc > 0xFFFFFF:
                print(f"[_capture] ERROR: invalid state.pc=0x{self.state.pc:x} before run_burst", file=sys.stderr)
                return
            rs = self.launcher.run_burst(self.state, max_cycles=cycles)

            # Run child processes - SFS needs DosList handler to run during init
            if hasattr(self, 'proc_mgr'):
                self.proc_mgr.run_all_ready_children(cycles_per_child=cycles // 2)
                # CRITICAL: Clear global blocking state after running children
                _clear_all_block_state()

            if self.state.main_loop_pc:
                return
            waitport_sp = _get_block_state(ExecLibrary, '_waitport_blocked_sp')
            wait_sp = _get_block_state(ExecLibrary, '_wait_blocked_sp')
            waitport_ret = _get_block_state(ExecLibrary, '_waitport_blocked_ret')
            wait_ret = _get_block_state(ExecLibrary, '_wait_blocked_ret')
            blocked_sp = waitport_sp if waitport_sp is not None else wait_sp
            blocked_ret = waitport_ret if waitport_ret is not None else wait_ret
            if blocked_sp is not None:
                ret_addr = blocked_ret if blocked_ret is not None else self.mem.r32(blocked_sp)
                if ret_addr >= 0x800:
                    self.state.main_loop_pc = ret_addr
                    self.state.main_loop_sp = blocked_sp + 4
                    # Capture Wait mask for _flush_pending_signals
                    wait_mask = _get_block_state(ExecLibrary, '_wait_blocked_mask')
                    if wait_mask is not None and wait_mask != 0:
                        self.state.wait_mask = wait_mask
                    if self._debug:
                        mask_str = f" wait_mask=0x{self.state.wait_mask:x}" if self.state.wait_mask else ""
                        print(f"[amifuse] _capture_main_loop_state: captured at iter {i}, pc=0x{ret_addr:x}, sp=0x{blocked_sp+4:x}{mask_str}")
                return
            # Check if handler crashed (invalid PC)
            if rs and hasattr(rs, 'pc') and rs.pc < 0x800:
                print(f"[amifuse] _capture_main_loop_state: handler crashed at iter {i}, pc=0x{rs.pc:x}", file=sys.stderr)
                return

    def _flush_pending_signals(self, max_rounds: int = 20):
        """Deliver pending signals so the handler can complete deferred init.

        PFS3 uses Signal(FindTask(NULL), ...) to schedule deferred volume
        validation steps.  Each self-signal causes Wait() to return
        immediately, letting the handler continue validation within one
        burst.  When validation finishes (or the handler needs to wait for
        real time / IO), it calls Wait() without pending self-signals and
        blocks.

        We deliver an initial timer signal (bit 19) to kick off the first
        validation cycle.  Subsequent rounds only fire if the handler
        blocked again with pending signals (port messages or self-signals).
        """
        if not self.state.main_loop_pc:
            return
        from amitools.vamos.libstructs.exec_ import ExecLibraryStruct, TaskStruct, MsgPortStruct
        from amitools.vamos.lib.ExecLibrary import ExecLibrary
        from amitools.vamos.lib.lexec.signalfunc import SignalFunc
        from amitools.vamos.machine.regs import REG_D0

        # If we don't have a wait mask yet, run the handler once to capture it.
        if not self.state.wait_mask:
            _clear_all_block_state()
            self.state.pc = self.state.main_loop_pc
            self.state.sp = self.state.main_loop_sp
            self.vh.machine.cpu.w_reg(REG_D0, 0)
            self.launcher.run_burst(self.state, max_cycles=100_000)
            wait_mask = _get_block_state(ExecLibrary, '_wait_blocked_mask')
            if wait_mask and wait_mask != 0:
                self.state.wait_mask = wait_mask
            _clear_all_block_state()

        wait_mask = self.state.wait_mask
        if not wait_mask:
            return

        if self._debug:
            print(f"[amifuse] _flush_pending_signals: wait_mask=0x{wait_mask:x}")

        pmgr = self.vh.slm.exec_impl.port_mgr
        exec_base = self.mem.r32(4)
        this_task_off = ExecLibraryStruct.sdef.find_field_def_by_name("ThisTask").offset
        this_task = self.mem.r32(exec_base + this_task_off)
        sigrecvd_off = TaskStruct.sdef.find_field_def_by_name("tc_SigRecvd").offset
        mp_sigbit_off = MsgPortStruct.sdef.find_field_def_by_name("mp_SigBit").offset

        for rnd in range(max_rounds):
            # --- Gather all pending signals ---
            # 1. Self-signals set by handler via Signal(FindTask(NULL), ...)
            self_signals = SignalFunc._fallback_signals

            # 2. tc_SigRecvd from m68k memory (IO completion, etc.)
            tc_sigrecvd = self.mem.r32(this_task + sigrecvd_off)

            # 3. Signals from ports with actual messages
            port_signals = 0
            for port_addr, port in pmgr.ports.items():
                has_msg = (port.queue is not None and len(port.queue) > 0)
                if not has_msg:
                    try:
                        mp_msglist_off = MsgPortStruct.sdef.find_field_def_by_name("mp_MsgList").offset
                        list_addr = port_addr + mp_msglist_off
                        lh_head = self.mem.r32(list_addr)
                        has_msg = (lh_head != 0 and lh_head != list_addr + 4)
                    except Exception:
                        pass
                if has_msg:
                    try:
                        sigbit = self.mem.read(0, port_addr + mp_sigbit_off)
                        if 0 <= sigbit < 32:
                            port_signals |= 1 << sigbit
                    except Exception:
                        pass

            # 4. On the first round, inject the timer/validation signal to
            #    kick off the deferred validation cycle.
            timer_signal = (1 << 19) if rnd == 0 else 0

            # Combine and mask to what the handler is waiting for.
            combined = self_signals | tc_sigrecvd | port_signals | timer_signal
            actual_signals = combined & wait_mask

            # If no signals to deliver (and past the first round), we're done.
            if actual_signals == 0 and rnd > 0:
                if self._debug:
                    print(f"[amifuse] _flush round {rnd}: no pending signals, done")
                break

            # Ensure at least the timer signal on first round.
            if actual_signals == 0:
                actual_signals = (1 << 19) & wait_mask
                if actual_signals == 0:
                    actual_signals = wait_mask  # Last resort

            if self._debug:
                print(f"[amifuse] _flush round {rnd}: signals=0x{actual_signals:x} "
                      f"(self=0x{self_signals:x} tc=0x{tc_sigrecvd:x} "
                      f"ports=0x{port_signals:x} timer=0x{timer_signal:x})")

            # Clear delivered signals from _fallback_signals and tc_SigRecvd,
            # mimicking how Wait() atomically clears returned signals.
            SignalFunc._fallback_signals &= ~actual_signals
            self.mem.w32(this_task + sigrecvd_off, tc_sigrecvd & ~actual_signals)

            # Set up Wait() return: D0 = returned signals
            self.state.pc = self.state.main_loop_pc
            self.state.sp = self.state.main_loop_sp
            self.vh.machine.cpu.w_reg(REG_D0, actual_signals)
            _clear_all_block_state()

            old_pc = self.state.pc
            self.launcher.run_burst(self.state, max_cycles=2_000_000)
            rs = self.state.run_state
            if self._debug:
                cycles_run = getattr(rs, 'cycles', 0) if rs else 0
                print(f"[amifuse] _flush round {rnd}: PC 0x{old_pc:x}->0x{self.state.pc:x} "
                      f"cycles={cycles_run} error={getattr(rs, 'error', None)} crashed={self.state.crashed}")

            # Drain any spurious replies (timer completion etc.)
            self.launcher.poll_replies(self.state.reply_port_addr)
            _clear_all_block_state()
            if self.state.crashed:
                break

            # Capture updated wait_mask if handler changed it
            new_mask = _get_block_state(ExecLibrary, '_wait_blocked_mask')
            if new_mask and new_mask != 0:
                wait_mask = new_mask
                self.state.wait_mask = new_mask

        # Drain stale messages from ALL non-essential ports (timer, disk
        # change).  After initialization, timer signals must NOT interfere
        # with normal DOS packet processing.
        dos_port = self.state.port_addr
        reply_port = self.state.reply_port_addr
        for port_addr in list(pmgr.ports.keys()):
            if port_addr != dos_port and port_addr != reply_port:
                while pmgr.has_msg(port_addr):
                    pmgr.get_msg(port_addr)

        # Clean up fallback signals to avoid stale signals during normal
        # DOS packet processing.
        SignalFunc._fallback_signals = 0

        # After the flush rounds, the handler is blocked in Wait() and
        # run_burst has set state.pc to the Wait() return address (= the
        # main loop entry).  Update main_loop_pc/sp to match so subsequent
        # _run_until_replies calls work correctly.
        if self.state.pc >= 0x800:
            self.state.main_loop_pc = self.state.pc
            self.state.main_loop_sp = self.state.sp

        # Ensure clean blocking state for normal operation.
        _clear_all_block_state()

    def _update_handler_port_from_startup(self):
        pkt = AccessStruct(self.mem, DosPacketStruct, self.state.stdpkt_addr)
        res1 = pkt.r_s("dp_Res1")
        res2 = pkt.r_s("dp_Res2")
        if res1:
            # Check dp_Port - many handlers set this to their message port in the reply
            dp_port = pkt.r_s("dp_Port")
            alt_port = pkt.r_s("dp_Arg4")
            if self._debug:
                print(f"[amifuse] Startup reply: dp_Port=0x{dp_port:x} dp_Arg4=0x{alt_port:x} (current=0x{self.state.port_addr:x})")
            # Prefer dp_Port if it's different and valid
            new_port = None
            if dp_port and dp_port != self.state.reply_port_addr and dp_port != self.state.port_addr:
                new_port = dp_port
            elif alt_port and alt_port != self.state.port_addr:
                new_port = alt_port
            if new_port:
                pmgr = self.vh.slm.exec_impl.port_mgr
                if not pmgr.has_port(new_port):
                    pmgr.register_port(new_port)
                if self._debug:
                    print(f"[amifuse] Switching to handler port 0x{new_port:x}")
                self.state.port_addr = new_port
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
            if self._debug:
                if replies:
                    print(f"[amifuse] locate(lock=0x{lock_bptr:x}, name='{name}'): res1=0x{replies[-1][2]:x} res2={replies[-1][3]}")
                else:
                    print(f"[amifuse] locate(lock=0x{lock_bptr:x}, name='{name}'): NO REPLIES")
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
                if self._debug:
                    print(f"[amifuse] list_dir: obtained root_lock=0x{root_lock:x}")
            fib_mem = self._alloc_fib()
            # First Examine returns info about the directory itself, not contents
            self.launcher.send_examine(self.state, lock_bptr, fib_mem.addr)
            replies = self._run_until_replies()
            entries: List[Dict] = []
            if not replies or replies[-1][2] == 0:
                if self._debug:
                    res2 = replies[-1][3] if replies else -1
                    print(f"[amifuse] list_dir: Examine FAILED lock=0x{lock_bptr:x} res2={res2}")
                if root_lock:
                    self.free_lock(root_lock)
                return entries
            if self._debug:
                dir_info = _parse_fib(self.mem, fib_mem.addr)
                print(f"[amifuse] list_dir: Examine OK dir_name='{dir_info['name']}' type={dir_info['dir_type']}")
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
                return {"dir_type": 2, "size": 0, "name": "", "protection": 0}
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
    # Note: "Icon\r" and ".VolumeIcon.icns" are NOT in this list - we handle them for custom icons
    _MACOS_SPECIAL = frozenset([
        "._.", ".hidden", ".Trashes", ".Spotlight-V100", ".fseventsd",
        ".metadata_never_index", ".com.apple.timemachine.donotpresent",
        ".DS_Store", ".ql_disablethumbnails",
        ".localized", ".TemporaryItems", ".DocumentRevisions-V100",
        ".vol", ".file", ".hotfiles.btree", ".quota.user", ".quota.group",
        ".apdisk", ".com.apple.NetBootX", "mach_kernel", ".PKInstallSandboxManager",
        ".PKInstallSandboxManager-SystemSoftware", ".Trashes.501", "Backups.backupdb",
    ])
    def __init__(
        self,
        bridge: HandlerBridge,
        debug: bool = False,
        icons: bool = False,
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
        # Icon support - use platform-specific handler
        self._icons_enabled = icons
        self._icon_parser = None
        self._icon_cache = None
        self._icon_existence_cache = None
        self._icon_handler = None
        if icons:
            from .icon_parser import IconParser
            from .icon_cache import IconCache, IconExistenceCache
            from . import platform
            self._icon_parser = IconParser(debug=debug)
            self._icon_cache = IconCache()
            self._icon_existence_cache = IconExistenceCache()
            self._icon_handler = platform.get_icon_handler(icons_enabled=True, debug=debug)

    def _is_macos_special(self, path: str) -> bool:
        """Return True if path is a macOS special file we should reject."""
        name = path.rsplit("/", 1)[-1]
        if name.startswith("._"):  # AppleDouble resource fork files
            return True
        # Icon files are handled specially when icons enabled
        if self._icon_handler:
            from . import platform
            icon_file, volume_icon_file = platform.get_icon_file_names()
            if icon_file and name == icon_file:
                return False  # Let it through - we handle it in getattr
            if volume_icon_file and name == volume_icon_file:
                return False  # Let it through - we handle it in getattr
        return name in self._MACOS_SPECIAL

    def _is_icon_file(self, path: str) -> bool:
        """Return True if path is the virtual icon file for custom folder icons."""
        if self._icon_handler:
            return self._icon_handler.is_icon_file(path)
        return False

    def _is_volume_icon_file(self, path: str) -> bool:
        """Return True if path is the virtual volume icon file."""
        if self._icon_handler:
            return self._icon_handler.is_volume_icon_file(path)
        return False

    def _get_parent_dir(self, path: str) -> str:
        """Get the parent directory of a path."""
        if path == "/" or "/" not in path.lstrip("/"):
            return "/"
        return "/" + path.lstrip("/").rsplit("/", 1)[0]

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
        self._check_handler_alive()
        # Reject macOS special files immediately without calling handler
        if self._is_macos_special(path):
            self._track_op("getattr", path, cached=True)
            raise FuseOSError(errno.ENOENT)
        if path == "/":
            self._track_op("getattr", path, cached=True)
            return self._root_stat()
        # Handle virtual Icon file for custom folder icons (platform-specific)
        if self._icon_handler and self._is_icon_file(path):
            parent_dir = self._get_parent_dir(path)
            # Check if parent directory has a custom icon
            if self._has_valid_icon(parent_dir):
                icns_data = self._get_icon_for_path(parent_dir)
                icns_size = len(icns_data) if icns_data else 0
                return self._icon_handler.get_icon_file_stat(icns_size, self._uid, self._gid)
            raise FuseOSError(errno.ENOENT)
        # Handle virtual volume icon file (platform-specific)
        # Note: macFUSE's volicon module handles this at mount time, but we keep
        # this as a fallback for older versions or non-macOS systems
        if self._icon_handler and self._is_volume_icon_file(path):
            if self._has_valid_icon("/"):
                icns_data = self._get_icon_for_path("/")
                if icns_data:
                    return self._icon_handler.get_volume_icon_stat(len(icns_data), self._uid, self._gid)
            raise FuseOSError(errno.ENOENT)
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
        # Convert Amiga protection bits to Unix mode
        prot = DosProtection(info.get("protection", 0))
        base_mode = prot.to_host_mode()
        # For read-only mounts, strip write bits
        if not self.bridge._write_enabled:
            base_mode &= ~(0o222)
        mode = base_mode | (0o040000 if is_dir else 0o100000)
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
        # Set UF_HIDDEN on all .info files when icons mode is enabled
        # This hides the Amiga icon files since we display icons via xattrs
        if self._icons_enabled and (path.endswith(".info") or path.lower().endswith(".info")):
            result["st_flags"] = result.get("st_flags", 0) | 0x8000  # UF_HIDDEN
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
            # Convert Amiga protection bits to Unix mode
            prot = DosProtection(ent.get("protection", 0))
            base_mode = prot.to_host_mode()
            # For read-only mounts, strip write bits
            if not self.bridge._write_enabled:
                base_mode &= ~(0o222)
            mode = base_mode | (0o040000 if is_dir else 0o100000)
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
            # Set UF_HIDDEN on .info files when icons mode is enabled
            if self._icons_enabled and (name.endswith(".info") or name.lower().endswith(".info")):
                stat_result["st_flags"] = 0x8000  # UF_HIDDEN
            self._stat_cache[child_path] = (now, stat_result)

        # Add virtual icon files if this directory has a custom icon
        if self._icon_handler and self._has_valid_icon(path):
            from . import platform
            icon_file, _ = platform.get_icon_file_names()
            if icon_file:
                entries.append(icon_file)

        # Add virtual volume icon file at root if Disk.info exists
        if self._icon_handler and path == "/" and self._has_valid_icon("/"):
            from . import platform
            _, volume_icon_file = platform.get_icon_file_names()
            if volume_icon_file:
                entries.append(volume_icon_file)

        # Cache the directory listing
        self._dir_cache[path] = (now, entries)
        return entries

    def open(self, path, flags):
        self._log_op("open", path, f"flags=0x{flags:x}")
        self._check_handler_alive()
        if not self.bridge._write_enabled and (flags & (os.O_WRONLY | os.O_RDWR)):
            raise FuseOSError(errno.EROFS)
        # Handle virtual Icon\r file - return a special handle
        if self._icons_enabled and self._is_icon_file(path):
            parent_dir = self._get_parent_dir(path)
            if self._has_valid_icon(parent_dir):
                with self._fh_lock:
                    handle = self._next_fh
                    self._next_fh += 1
                    self._fh_cache[handle] = {
                        "virtual_icon": True,
                        "parent_dir": parent_dir,
                        "pos": 0,
                        "lock": threading.Lock(),
                        "closed": False,
                    }
                return handle
            raise FuseOSError(errno.ENOENT)
        # Handle virtual .VolumeIcon.icns file - return a special handle
        if self._icons_enabled and self._is_volume_icon_file(path):
            if self._has_valid_icon("/"):
                icns_data = self._get_icon_for_path("/")
                if icns_data:
                    with self._fh_lock:
                        handle = self._next_fh
                        self._next_fh += 1
                        self._fh_cache[handle] = {
                            "virtual_volume_icon": True,
                            "icns_data": icns_data,
                            "pos": 0,
                            "lock": threading.Lock(),
                            "closed": False,
                        }
                    return handle
            raise FuseOSError(errno.ENOENT)
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
        # Virtual Icon\r file returns empty content (icon is in ResourceFork)
        if entry.get("virtual_icon"):
            return b""
        # Virtual .VolumeIcon.icns file returns actual ICNS data
        if entry.get("virtual_volume_icon"):
            icns_data = entry.get("icns_data", b"")
            end = min(offset + size, len(icns_data))
            if offset >= len(icns_data):
                return b""
            return icns_data[offset:end]
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
        if self._debug:
            print(f"[FUSE][listxattr] path={path} icons_enabled={self._icons_enabled}", flush=True)
        if not self._icons_enabled or not self._icon_handler:
            return []

        # For Icon\r files, check if parent has a valid icon
        # For other paths, check if the path itself has a valid icon
        if self._is_icon_file(path):
            parent_dir = self._get_parent_dir(path)
            has_icon = self._has_valid_icon(parent_dir)
        else:
            has_icon = self._has_valid_icon(path)

        result = self._icon_handler.get_listxattr_for_path(path, has_icon)
        if self._debug and result:
            print(f"[FUSE][listxattr] {path} -> {result}", flush=True)
        return result

    def getxattr(self, path, name, position=0):
        if self._debug:
            print(f"[FUSE][getxattr] path={path} name={name} position={position}", flush=True)
        enoattr = getattr(errno, "ENOATTR", errno.ENODATA)

        if not self._icons_enabled or not self._icon_handler:
            raise FuseOSError(enoattr)

        # For Icon\r files, get icon data from parent directory
        # For other paths, get icon data from the path itself
        if self._is_icon_file(path):
            icon_source_path = self._get_parent_dir(path)
        else:
            icon_source_path = path

        # Get icon data and has_icon status
        icns_data = self._get_icon_for_path(icon_source_path)
        has_icon = self._has_valid_icon(icon_source_path)

        # Use DarwinIconHandler to get the xattr value
        result = self._icon_handler.get_xattr_value(path, name, icns_data, has_icon, position)

        if result is not None:
            if self._debug:
                print(f"[FUSE][getxattr] {name} for {path}: {len(result)} bytes", flush=True)
            return result

        raise FuseOSError(enoattr)

    def _find_info_file(self, path: str) -> Optional[str]:
        """Find the .info file for a given path using case-insensitive matching.

        For root ("/"), looks for Disk.info.
        For other paths, looks for path.info.

        Returns:
            The actual path to the .info file, or None if not found.
        """
        if path == "/":
            # Root directory uses Disk.info for its icon
            target_name = "disk.info"
            dir_path = "/"
        else:
            # Normal files/directories: append .info suffix
            # Get parent directory and target name
            if "/" in path[1:]:  # Has subdirectories
                dir_path = path.rsplit("/", 1)[0] or "/"
                base_name = path.rsplit("/", 1)[1]
            else:
                dir_path = "/"
                base_name = path[1:]  # Remove leading /
            target_name = (base_name + ".info").lower()

        # List directory and find matching .info file case-insensitively
        try:
            entries = self.bridge.list_dir_path(dir_path)
            for entry in entries:
                name = entry.get("name", "")
                if name.lower() == target_name:
                    if dir_path == "/":
                        return "/" + name
                    else:
                        return dir_path + "/" + name
        except Exception:
            pass

        return None

    def _has_valid_icon(self, path: str) -> bool:
        """Check if the file at path has a valid .info icon file that can be converted.

        This actually tries to generate the ICNS to ensure consistency with _get_icon_for_path.
        """
        if not self._icons_enabled:
            return False

        # Check existence cache first
        cached = self._icon_existence_cache.get(path)
        if cached is not None:
            return cached

        # Actually try to get the icon - this ensures we can parse it
        icns_data = self._get_icon_for_path(path)
        if icns_data:
            self._icon_existence_cache.put(path, True)
            return True

        self._icon_existence_cache.put(path, False)
        return False

    def _get_icon_for_path(self, path: str) -> Optional[bytes]:
        """Get ICNS data for a file by parsing its .info file."""
        if not self._icons_enabled:
            return None

        # Check ICNS cache first (keyed by base path)
        cached = self._icon_cache.get(path)
        if cached is not None:
            return cached

        # Find the .info file using case-insensitive matching
        info_path = self._find_info_file(path)
        if not info_path:
            return None

        info_stat = self._get_cached_stat(info_path)
        if info_stat is None:
            info_stat = self.bridge.stat_path(info_path)
        if not info_stat:
            return None

        # Read the entire .info file
        # info_stat could be from bridge.stat_path (has 'size') or from _stat_cache (has 'st_size')
        if isinstance(info_stat, dict):
            file_size = info_stat.get("size") or info_stat.get("st_size", 0)
        else:
            file_size = 0
        if file_size <= 0 or file_size > 1024 * 1024:  # Max 1MB
            return None

        data = self.bridge.read_file(info_path, file_size, 0)
        if not data:
            if self._debug:
                print(f"[FUSE] Failed to read icon data from {info_path}", flush=True)
            return None

        if self._debug:
            print(f"[FUSE] Parsing icon: {info_path} ({len(data)} bytes)", flush=True)

        # Parse the icon
        icon_info = self._icon_parser.parse(data)
        if not icon_info:
            if self._debug:
                print(f"[FUSE] Failed to parse icon from {info_path}", flush=True)
            return None

        # Convert to ICNS
        from .icon_parser import create_icns
        aspect_ratio = icon_info.get("aspect_ratio", 1.0)
        icns_data = create_icns(icon_info["rgba"], icon_info["width"], icon_info["height"],
                                debug=self._debug, aspect_ratio=aspect_ratio)

        # Cache the result
        self._icon_cache.put(path, icns_data)

        if self._debug:
            print(f"[FUSE] Generated {len(icns_data)} byte ICNS for {path} "
                  f"({icon_info['width']}x{icon_info['height']} {icon_info['format']})", flush=True)
            # Save ICNS to temp file for verification
            safe_name = path.replace("/", "_").lstrip("_")
            icns_path = f"/tmp/amifuse_icon_{safe_name}.icns"
            with open(icns_path, "wb") as f:
                f.write(icns_data)
            print(f"[FUSE] DEBUG: Saved ICNS to {icns_path}", flush=True)
            # Also save resource fork for verification
            from .resource_fork import build_resource_fork
            rfork_data = build_resource_fork(icns_data, 0)
            rfork_path = f"/tmp/amifuse_icon_{safe_name}.rsrc"
            with open(rfork_path, "wb") as f:
                f.write(rfork_data)
            print(f"[FUSE] DEBUG: Saved resource fork ({len(rfork_data)} bytes) to {rfork_path}", flush=True)

        return icns_data

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
        # Virtual Icon\r files have no real handle to close
        if entry.get("virtual_icon"):
            return 0
        # Virtual .VolumeIcon.icns files have no real handle to close
        if entry.get("virtual_volume_icon"):
            return 0
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
    from .rdb_inspect import open_rdisk, find_partition_mbr_index
    mbr_idx = find_partition_mbr_index(image, block_size, partition) if partition else None
    blkdev, rdisk, _mbr_ctx = open_rdisk(image, block_size=block_size, mbr_partition_index=mbr_idx)
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
    from .rdb_inspect import open_rdisk, find_partition_mbr_index

    mbr_idx = find_partition_mbr_index(image, block_size, partition) if partition else None
    blkdev, rdisk, _mbr_ctx = open_rdisk(image, block_size=block_size, mbr_partition_index=mbr_idx)
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
    icons: bool = False,
):
    import amitools.fs.DosType as DosType
    from .rdb_inspect import detect_adf

    # First, check if this is an ADF (floppy disk image)
    adf_info = detect_adf(image)

    if adf_info is not None:
        # ADF floppy detected - use DF0 as the partition name
        dt_str = DosType.num_to_tag_str(adf_info.dos_type)
        floppy_type = "HD" if adf_info.is_hd else "DD"
        part_name = "DF0"  # Will use actual volume name from handler later

        # ADF files don't have embedded drivers - user must specify one
        if driver is None:
            raise SystemExit(
                f"ADF floppy image detected ({floppy_type}, {dt_str}).\n"
                "Floppy images don't contain embedded filesystem drivers.\n"
                "You need to specify a filesystem handler with --driver\n"
                "For FFS/OFS floppies, use the L:FastFileSystem from a Workbench disk."
            )
        driver_desc = str(driver)
        temp_driver = None
    else:
        # HDF/RDB image - get partition info
        adf_info = None  # Ensure it's None for later checks
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
    from . import platform as plat
    if mountpoint is None:
        mountpoint = plat.get_default_mountpoint(volname_opt or part_name)
        if mountpoint is None:
            raise SystemExit("--mountpoint is required on this platform")

    if mountpoint.exists() and os.path.ismount(mountpoint):
        raise SystemExit(
            f"Mountpoint {mountpoint} is already a mount; unmount it first (e.g. umount -f {mountpoint})."
        )

    # Create mountpoint directory if it doesn't exist
    if not mountpoint.exists():
        if not plat.should_auto_create_mountpoint(mountpoint):
            mountpoint.mkdir(parents=True)

    # Print startup banner
    print(__banner__)
    if adf_info is not None:
        floppy_type = "HD" if adf_info.is_hd else "DD"
        print(f"Mounting ADF floppy ({floppy_type}) from {image}")
    else:
        print(f"Mounting partition '{part_name}' from {image}")
    print(f"Filesystem driver: {driver_desc}")
    print(f"Mount point: {mountpoint}")

    if write:
        # Guard against accidental writes without explicit intent.
        print("[amifuse] write mode enabled; ensure the image is backed up")

    if icons:
        print("[amifuse] icon mode enabled; Amiga icons will appear as macOS custom icons")

    def _unmount_mountpoint():
        if not mountpoint.exists() or not os.path.ismount(mountpoint):
            return
        cmd = plat.get_unmount_command(mountpoint)
        subprocess.run(cmd, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    bridge = HandlerBridge(
        image,
        driver,
        block_size=block_size,
        read_only=not write,
        debug=debug,
        trace=trace,
        partition=partition,
        adf_info=adf_info,
    )

    # Let default signal handling work - FUSE will call destroy() on unmount
    volname = volname_opt or bridge.volume_name()

    # Pre-generate volume icon if platform requires it at mount time
    temp_volicon = None
    if icons:
        temp_volicon = plat.pre_generate_volume_icon(bridge, debug=debug)
        if temp_volicon:
            print(f"[amifuse] Volume icon generated: {temp_volicon}")

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
    # Add platform-specific mount options
    platform_opts = plat.get_mount_options(
        volname=volname,
        volicon_path=str(temp_volicon) if temp_volicon else None,
        icons_enabled=icons
    )
    fuse_kwargs.update(platform_opts)

    if debug:
        print(f"[amifuse] FUSE options: {fuse_kwargs}", flush=True)

    try:
        FUSE(AmigaFuseFS(bridge, debug=debug, icons=icons), str(mountpoint), **fuse_kwargs)
    finally:
        # Clean up temp driver file if we extracted one
        if temp_driver is not None and temp_driver.exists():
            temp_driver.unlink()
        # Clean up temp volume icon file
        if temp_volicon is not None and temp_volicon.exists():
            temp_volicon.unlink()


def format_volume(
    image: Path,
    driver: Optional[Path],
    block_size: Optional[int],
    partition: str,
    volname: str = "Empty",
    debug: bool = False,
):
    import amitools.fs.DosType as DosType

    # Get partition dostype from RDB
    part_info = get_partition_info(image, block_size, partition)
    dostype = part_info["dostype"]
    if dostype is None:
        raise SystemExit(f"Could not determine dostype for partition '{partition}'.")
    dt_str = DosType.num_to_tag_str(dostype)

    # Resolve driver
    temp_driver = None
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
        temp_driver, _, _ = result
        driver = temp_driver
        driver_desc = f"{dt_str}/0x{dostype:08x} (from RDB)"
    else:
        driver_desc = str(driver)

    print(__banner__)
    print(f"Formatting partition '{partition}' in {image}")
    print(f"Filesystem driver: {driver_desc}")
    print(f"Volume name: {volname}")
    print(f"DOS type: {dt_str} (0x{dostype:08x})")

    try:
        bridge = HandlerBridge(
            image,
            driver,
            block_size=block_size,
            read_only=False,
            debug=debug,
            partition=partition,
        )

        # Inhibit the volume so the handler releases it for formatting
        bridge.launcher.send_inhibit(bridge.state, True)
        replies = bridge._run_until_replies()
        if debug and replies:
            _, _, r1, r2 = replies[-1]
            print(f"[amifuse] INHIBIT reply: res1={r1} res2={r2}")

        # Allocate volume name BSTR and send ACTION_FORMAT
        _, volname_bptr = bridge.launcher.alloc_bstr(volname, label="FormatVolName")
        bridge.launcher.send_format(bridge.state, volname_bptr, dostype)
        replies = bridge._run_until_replies()

        if not replies:
            raise SystemExit("Format failed: no reply from handler.")

        _, pkt_addr, res1, res2 = replies[-1]
        if res1 == 0:
            error_msgs = {
                205: "Object in use",
                218: "Not a valid DOS disk",
                225: "Not a DOS disk",
                226: "Wrong disk type",
                232: "Disk is write-protected",
            }
            error_desc = error_msgs.get(res2, f"error code {res2}")
            raise SystemExit(f"Format failed: {error_desc}")

        if debug:
            print(f"[amifuse] FORMAT reply: res1={res1} res2={res2}")

        # Uninhibit to re-mount the newly formatted volume
        bridge.launcher.send_inhibit(bridge.state, False)
        bridge._run_until_replies()

        # Flush to ensure data is written
        bridge.launcher.send_flush(bridge.state)
        bridge._run_until_replies()

        print(f"Format complete. Volume '{volname}' created on partition '{partition}'.")
    finally:
        if temp_driver is not None and temp_driver.exists():
            temp_driver.unlink()


__version__ = "v0.3.0"
__banner__ = f"amifuse {__version__} - Copyright (C) 2025-2026 by Stefan Reinauer"


def _inspect_rdisk(rdisk, full=False):
    """Print RDB info, filesystem drivers, and warnings for a single RDisk."""
    from .rdb_inspect import format_fs_summary
    for line in rdisk.get_info(full=full):
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


def cmd_inspect(args):
    """Handle the 'inspect' subcommand."""
    import amitools.fs.DosType as DosType
    from .rdb_inspect import (
        open_rdisk, format_mbr_info, detect_adf, detect_mbr, MBR_TYPE_AMIGA_RDB,
    )

    # First check for ADF
    adf_info = detect_adf(args.image)
    if adf_info is not None:
        dt_str = DosType.num_to_tag_str(adf_info.dos_type)
        floppy_type = "HD" if adf_info.is_hd else "DD"
        print(f"ADF Floppy Image: {args.image}")
        print(f"  Type: {floppy_type} ({adf_info.sectors_per_track} sectors/track)")
        print(f"  Geometry: {adf_info.cylinders} cylinders, {adf_info.heads} heads")
        print(f"  Block size: {adf_info.block_size} bytes")
        print(f"  Total blocks: {adf_info.total_blocks}")
        print(f"  DOS type: {dt_str} (0x{adf_info.dos_type:08x})")
        print("\nNote: ADF files don't contain embedded filesystem drivers.")
        print("Use --driver to specify a filesystem handler when mounting.")
        return

    # Detect if MBR with multiple 0x76 partitions
    mbr_info = detect_mbr(args.image)
    multi_rdb = False
    amiga_parts = []
    if mbr_info and mbr_info.has_amiga_partitions:
        amiga_parts = [p for p in mbr_info.partitions if p.partition_type == MBR_TYPE_AMIGA_RDB]
        if len(amiga_parts) > 1:
            multi_rdb = True

    if multi_rdb:
        # Show MBR table once using first partition's context
        try:
            blkdev, rdisk, mbr_ctx = open_rdisk(args.image, block_size=args.block_size, mbr_partition_index=0)
        except IOError as e:
            raise SystemExit(f"Error: {e}")
        for line in format_mbr_info(mbr_ctx):
            print(line)
        rdisk.close()
        blkdev.close()

        # Show each RDB
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
                _inspect_rdisk(rdisk, full=args.full)
            finally:
                rdisk.close()
                blkdev.close()
    else:
        # Single RDB (or non-MBR): existing behavior
        blkdev = None
        rdisk = None
        mbr_ctx = None
        try:
            try:
                blkdev, rdisk, mbr_ctx = open_rdisk(args.image, block_size=args.block_size)
            except IOError as e:
                raise SystemExit(f"Error: {e}")
            if mbr_ctx is not None:
                for line in format_mbr_info(mbr_ctx):
                    print(line)
                print()
            _inspect_rdisk(rdisk, full=args.full)
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
        partition=args.partition,
        icons=args.icons
    )

    if args.profile:
        profiler.disable()
        with open("profile.txt", "w") as f:
            stats = pstats.Stats(profiler, stream=f)
            stats.sort_stats(pstats.SortKey.CUMULATIVE)
            stats.print_stats()


def cmd_format(args):
    """Handle the 'format' subcommand."""
    format_volume(
        args.image, args.driver, args.block_size,
        partition=args.partition,
        volname=args.volname,
        debug=args.debug,
    )


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="amifuse",
        description=f"{__banner__}\n\n"
        "Mount Amiga filesystem images via FUSE.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
commands:
  inspect <image>           Inspect RDB partitions and filesystems.
    --block-size N            Override block size (defaults to auto/512).
    --full                    Show full partition details.

  mount <image>             Mount an Amiga filesystem image via FUSE.
    --driver PATH             Filesystem binary (default: extract from RDB).
    --mountpoint PATH         Mount location (default: /Volumes/<partition>).
    --partition NAME          Partition name (e.g. DH0) or index (defaults to first).
    --block-size N            Override block size (defaults to auto/512).
    --volname NAME            Override macFUSE volume name (defaults to partition name).
    --write                   Enable read-write mode (experimental).
    --icons                   Convert Amiga .info icons to native macOS icons (experimental).
    --debug                   Enable debug logging of FUSE operations.
    --trace                   Enable vamos instruction tracing (very noisy).
    --profile                 Enable profiling and write stats to profile.txt.

  format <image> <partition> [volname]
                              Format an Amiga partition.
    --driver PATH             Filesystem binary (default: extract from RDB).
    --block-size N            Override block size (defaults to auto/512).
    --debug                   Enable debug logging.
""",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=__banner__,
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
    mount_parser.add_argument(
        "--icons", action="store_true",
        help="Convert Amiga .info icons to native macOS icons (experimental)."
    )
    mount_parser.set_defaults(func=cmd_mount)

    # format subcommand
    format_parser = subparsers.add_parser(
        "format", help="Format an Amiga partition."
    )
    format_parser.add_argument("image", type=Path, help="Disk image file")
    format_parser.add_argument("partition", type=str, help="Partition name (e.g. DH0) or index.")
    format_parser.add_argument("volname", nargs="?", default="Empty", help="Volume name (default: Empty)")
    format_parser.add_argument("--driver", type=Path, help="Filesystem binary (default: extract from RDB if available)")
    format_parser.add_argument(
        "--block-size", type=int, help="Override block size (defaults to auto/512)."
    )
    format_parser.add_argument(
        "--debug", action="store_true", help="Enable debug logging."
    )
    format_parser.set_defaults(func=cmd_format)

    args = parser.parse_args(argv)
    args.func(args)

if __name__ == "__main__":
    main()
