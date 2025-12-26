"""
Launch a filesystem handler with a minimal DOS-style bootstrap:

- build Process + MsgPort in vamos memory
- queue ACTION_STARTUP packet (with DeviceNode/FileSysStartupMsg)
- run the handler for a short burst and surface replies
"""

from dataclasses import dataclass
from typing import Dict, Tuple

from amitools.vamos.astructs.access import AccessStruct  # type: ignore
from amitools.vamos.libstructs.exec_ import (  # type: ignore
    ExecLibraryStruct,
    ListStruct,
    MsgPortFlags,
    MsgPortStruct,
    NodeType,
    TaskState,
)
from amitools.vamos.libstructs.dos import (
    DosPacketStruct,
    MessageStruct,
    ProcessStruct,
    FileHandleStruct,
)  # type: ignore
from amitools.vamos.machine.regs import REG_A6, REG_A7, REG_D0  # type: ignore
from amitools.vamos.schedule.stack import Stack  # type: ignore
from amitools.vamos.schedule.task import Task  # type: ignore
from .amiga_structs import DeviceNodeStruct  # type: ignore
from amitools.vamos.libstructs.exec_ import NodeStruct  # type: ignore
from amitools.vamos.lib.DosLibrary import DosLibrary  # type: ignore
from .handler_stub import build_entry_stub  # type: ignore

# Dos packet opcodes we care about
ACTION_STARTUP = 0
ACTION_DISK_INFO = 25
ACTION_READ = ord("R")
ACTION_WRITE = ord("W")
ACTION_SEEK = 1008
ACTION_END = 1007
ACTION_SET_FILE_SIZE = 1022
ACTION_FREE_LOCK = 15
ACTION_FINDUPDATE = 1004
ACTION_FINDINPUT = 1005
ACTION_FINDOUTPUT = 1006
ACTION_DELETE_OBJECT = 16
ACTION_RENAME_OBJECT = 17
ACTION_CREATE_DIR = 22
ACTION_FLUSH = 27
OFFSET_BEGINNING = -1
OFFSET_CURRENT = 0
OFFSET_END = 1
SHARED_LOCK = -2
EXCLUSIVE_LOCK = -1


@dataclass
class HandlerLaunchState:
    process_addr: int
    port_addr: int
    reply_port_addr: int
    stack: Stack
    stdpkt_addr: int
    msg_addr: int
    task: Task
    run_state: object
    pc: int
    sp: int
    started: bool
    entry_stub_pc: int = 0  # Original entry stub address for reset
    main_loop_pc: int = 0   # PC to restart from when waiting for messages
    main_loop_sp: int = 0   # SP to use when restarting
    initialized: bool = False  # True after startup is complete
    exit_count: int = 0  # Count consecutive exits without WaitPort block
    crashed: bool = False  # True if handler hit an unrecoverable error
    last_error_pc: int = 0  # PC at time of crash for diagnostics
    consecutive_errors: int = 0  # Count of consecutive errors without successful reply


class HandlerLauncher:
    def __init__(
        self,
        vh,
        boot_info: Dict,
        segment_addr: int,
    ):
        self.vh = vh
        self.alloc = vh.alloc
        self.mem = vh.alloc.get_mem()
        self.exec_impl = vh.slm.exec_impl
        # ExecBase is stored at address 4 on Amiga systems
        self.exec_base_addr = self.mem.r32(4)
        self.boot = boot_info
        self.segment_addr = segment_addr
        # Scratch packet buffers to avoid unbounded allocs (we serialize requests).
        self._stdpkt_ring = []
        self._stdpkt_sizes = []
        self._stdpkt_index = 0
        self._stdpkt_ring_size = 8
        # Cache mp_SigBit offset for signal computation
        self._mp_sigbit_offset = MsgPortStruct.sdef.find_field_def_by_name("mp_SigBit").offset

    def _compute_pending_signals(self, mask: int = 0xFFFFFFFF) -> int:
        """Compute pending signals from all message ports AND tc_SigRecvd, ANDed with mask.

        Signals can come from two sources:
        1. Message ports with pending messages (DOS packets)
        2. tc_SigRecvd - signals set directly by device drivers (IO completion)
        """
        from amitools.vamos.libstructs.exec_ import ExecLibraryStruct, TaskStruct

        pending = 0

        # First, include any signals already set in tc_SigRecvd (e.g., IO completion)
        try:
            exec_base = self.mem.r32(4)
            if exec_base != 0:
                this_task_off = ExecLibraryStruct.sdef.find_field_def_by_name("ThisTask").offset
                this_task = self.mem.r32(exec_base + this_task_off)
                if this_task != 0:
                    sigrecvd_off = TaskStruct.sdef.find_field_def_by_name("tc_SigRecvd").offset
                    pending = self.mem.r32(this_task + sigrecvd_off)
        except Exception:
            pass

        # Then add signals from message ports with pending messages
        port_mgr = self.exec_impl.port_mgr
        for port_addr, port in port_mgr.ports.items():
            try:
                if port.queue is not None and len(port.queue) > 0:
                    sigbit = self.mem.read(0, port_addr + self._mp_sigbit_offset)
                    if 0 <= sigbit < 32:
                        pending |= 1 << sigbit
            except Exception:
                continue
        return pending & mask

    def _clear_signals_from_task(self, signals: int):
        """Clear the specified signals from tc_SigRecvd.

        This mimics what Wait() does when it returns - it atomically clears
        the returned signals from tc_SigRecvd. When we resume from a Wait()
        block without actually running Wait(), we must clear signals ourselves.
        """
        from amitools.vamos.libstructs.exec_ import ExecLibraryStruct, TaskStruct
        try:
            exec_base = self.mem.r32(4)
            if exec_base != 0:
                this_task_off = ExecLibraryStruct.sdef.find_field_def_by_name("ThisTask").offset
                this_task = self.mem.r32(exec_base + this_task_off)
                if this_task != 0:
                    sigrecvd_off = TaskStruct.sdef.find_field_def_by_name("tc_SigRecvd").offset
                    current = self.mem.r32(this_task + sigrecvd_off)
                    self.mem.w32(this_task + sigrecvd_off, current & ~signals)
        except Exception:
            pass

    # ---- low-level helpers ----

    def _write_bstr(self, text: str, label: str) -> int:
        # Use latin-1 encoding for Amiga compatibility (handles chars 0-255)
        encoded = text.encode("latin-1", errors="replace")
        data = bytes([len(encoded)]) + encoded
        mem_obj = self.alloc.alloc_memory(len(data), label=label)
        self.mem.w_block(mem_obj.addr, data)
        return mem_obj.addr

    def _init_msgport(self, port_addr: int, task_addr: int, sigbit: int = None):
        mp = AccessStruct(self.mem, MsgPortStruct, port_addr)
        # zero first to clear garbage
        self.mem.w_block(port_addr, b"\x00" * MsgPortStruct.get_size())
        mp.w_s("mp_Node.ln_Type", NodeType.NT_MSGPORT)
        mp.w_s("mp_Flags", MsgPortFlags.PA_SIGNAL)
        if sigbit is None:
            sigbit = self._alloc_signal_bit()
        else:
            if 0 <= sigbit < len(self.exec_impl._signals):
                self.exec_impl._signals[sigbit] = True
        mp.w_s("mp_SigBit", sigbit)
        mp.w_s("mp_SigTask", task_addr)
        # Initialize mp_MsgList as a proper empty Amiga list
        # An empty list has: lh_Head -> &lh_Tail, lh_Tail = 0, lh_TailPred -> &lh_Head
        lst_off = MsgPortStruct.sdef.find_field_def_by_name("mp_MsgList").offset
        list_addr = port_addr + lst_off
        lst = AccessStruct(self.mem, ListStruct, list_addr)
        # Get addresses of list header fields
        lh_head_addr = list_addr + ListStruct.sdef.find_field_def_by_name("lh_Head").offset
        lh_tail_addr = list_addr + ListStruct.sdef.find_field_def_by_name("lh_Tail").offset
        # Empty list: Head points to Tail address, TailPred points to Head address
        lst.w_s("lh_Head", lh_tail_addr)  # Points to end marker
        lst.w_s("lh_Tail", 0)             # End marker is 0
        lst.w_s("lh_TailPred", lh_head_addr)  # Points back to start
        lst.w_s("lh_Type", NodeType.NT_MESSAGE)
        return sigbit

    def _create_port(self, name: str, task_addr: int) -> int:
        port_mem = self.alloc.alloc_memory(MsgPortStruct.get_size(), label=name)
        self._init_msgport(port_mem.addr, task_addr)
        if not self.exec_impl.port_mgr.has_port(port_mem.addr):
            self.exec_impl.port_mgr.register_port(port_mem.addr)
        return port_mem.addr

    def _set_exec_this_task(self, proc_addr: int, stack: Stack):
        exec_struct = AccessStruct(self.mem, ExecLibraryStruct, self.exec_base_addr)
        exec_struct.w_s("ThisTask", proc_addr)
        exec_struct.w_s("SysStkLower", stack.get_lower())
        exec_struct.w_s("SysStkUpper", stack.get_upper())
        # keep exec impl in sync for StackSwap
        self.exec_impl.stk_lower = stack.get_lower()
        self.exec_impl.stk_upper = stack.get_upper()

    def _create_process(self, name="handler") -> Tuple[int, int, Stack]:
        stack = Stack.alloc(self.alloc, 32 * 1024, name=name + "_stack")
        # clear stack to avoid garbage parameters being consumed by the handler
        self.mem.w_block(stack.get_lower(), b"\x00" * stack.get_size())
        proc_mem = self.alloc.alloc_memory(ProcessStruct.get_size(), label="HandlerProcess")
        # clear struct to avoid garbage pointers
        self.mem.w_block(proc_mem.addr, b"\x00" * ProcessStruct.get_size())
        proc = AccessStruct(self.mem, ProcessStruct, proc_mem.addr)
        name_cstr = self.alloc.alloc_memory(len(name) + 1, label="HandlerName")
        self.mem.w_cstr(name_cstr.addr, name)
        proc.w_s("pr_Task.tc_Node.ln_Type", NodeType.NT_PROCESS)
        proc.w_s("pr_Task.tc_Node.ln_Name", name_cstr.addr)
        proc.w_s("pr_Task.tc_Node.ln_Pri", 0)
        proc.w_s("pr_Task.tc_State", TaskState.TS_READY)
        proc.w_s("pr_Task.tc_Flags", 0)
        proc.w_s("pr_Task.tc_IDNestCnt", 0)
        proc.w_s("pr_Task.tc_TDNestCnt", 0)
        proc.w_s("pr_Task.tc_SPReg", stack.get_initial_sp())
        proc.w_s("pr_Task.tc_SPLower", stack.get_lower())
        proc.w_s("pr_Task.tc_SPUpper", stack.get_upper())
        proc.w_s("pr_StackSize", stack.get_size())
        proc.w_s("pr_StackBase", stack.get_lower())
        proc.w_s("pr_GlobVec", 0xFFFFFFFF)
        # NOTE: Do NOT set pr_CLI - SFS checks this to distinguish handler vs CLI
        # invocation, and rejects startup if pr_CLI is set.
        # dummy file handles for stdio
        fh_in_mem = self.alloc.alloc_struct(FileHandleStruct, label="NullFHIn")
        fh_out_mem = self.alloc.alloc_struct(FileHandleStruct, label="NullFHOut")
        fh_in = AccessStruct(self.mem, FileHandleStruct, fh_in_mem.addr)
        fh_out = AccessStruct(self.mem, FileHandleStruct, fh_out_mem.addr)
        fh_in.w_s("fh_Type", 0)
        fh_out.w_s("fh_Type", 0)
        proc.w_s("pr_CIS", fh_in_mem.addr >> 2)
        proc.w_s("pr_COS", fh_out_mem.addr >> 2)
        proc.w_s("pr_ConsoleTask", 0)
        proc.w_s("pr_WindowPtr", 0xFFFFFFFF)
        port_addr = proc.s_get_addr("pr_MsgPort")
        # FFS TaskWait uses a fixed SIGMASK (0x100), so set mp_SigBit=8.
        sigbit = self._init_msgport(port_addr, proc_mem.addr, sigbit=8)
        # advertise signal allocation in task
        proc.w_s("pr_Task.tc_SigAlloc", 1 << sigbit)
        proc.w_s("pr_Task.tc_SigWait", 0)
        proc.w_s("pr_Task.tc_SigRecvd", 0)
        self._set_exec_this_task(proc_mem.addr, stack)
        # register MsgPort with exec port manager so WaitPort/GetMsg see it
        if not self.exec_impl.port_mgr.has_port(port_addr):
            self.exec_impl.port_mgr.register_port(port_addr)
        return proc_mem.addr, port_addr, stack

    def alloc_bstr(self, text: str, label: str = "bstr") -> Tuple[int, int]:
        """Return (addr, bptr) of a freshly allocated BSTR."""
        addr = self._write_bstr(text, label)
        return addr, addr >> 2

    def _build_std_packet(
        self, dest_port_addr: int, reply_port_addr: int, pkt_type: int, args
    ) -> Tuple[int, int]:
        """Allocate contiguous StandardPacket = Message + DosPacket."""
        total = MessageStruct.get_size() + DosPacketStruct.get_size()
        if not self._stdpkt_ring:
            self._stdpkt_ring = [None] * self._stdpkt_ring_size
            self._stdpkt_sizes = [0] * self._stdpkt_ring_size
        idx = self._stdpkt_index
        self._stdpkt_index = (idx + 1) % self._stdpkt_ring_size
        sp_mem = self._stdpkt_ring[idx]
        if sp_mem is None or total > self._stdpkt_sizes[idx]:
            sp_mem = self.alloc.alloc_memory(total, label=f"stdpkt_scratch_{idx}")
            self._stdpkt_ring[idx] = sp_mem
            self._stdpkt_sizes[idx] = total
        sp_addr = sp_mem.addr
        self.mem.w_block(sp_addr, b"\x00" * total)
        msg = AccessStruct(self.mem, MessageStruct, sp_addr)
        pkt = AccessStruct(self.mem, DosPacketStruct, sp_addr + MessageStruct.get_size())
        # message
        msg.w_s("mn_Node.ln_Type", NodeType.NT_MESSAGE)
        msg.w_s("mn_Node.ln_Succ", 0)
        msg.w_s("mn_Node.ln_Pred", 0)
        msg.w_s("mn_ReplyPort", reply_port_addr)
        msg.w_s("mn_Length", total)
        # packet
        pkt.w_s("dp_Link", sp_addr)  # BPTR to message in StandardPacket
        pkt.w_s("dp_Port", reply_port_addr)
        pkt.w_s("dp_Type", pkt_type)
        for i, val in enumerate(args[:7], start=1):
            pkt.w_s(f"dp_Arg{i}", val)
        # link message name to packet
        msg.w_s("mn_Node.ln_Name", sp_addr + MessageStruct.get_size())
        # queue message to destination port - both Python queue and Amiga memory
        self.exec_impl.port_mgr.put_msg(dest_port_addr, sp_addr)
        # Also link the message into the port's mp_MsgList in memory
        # This is necessary because some handlers (like FFS) read mp_MsgList directly
        self._link_msg_to_port(dest_port_addr, sp_addr)
        return sp_addr + MessageStruct.get_size(), sp_addr

    def _link_msg_to_port(self, port_addr: int, msg_addr: int):
        """Link a message into a port's mp_MsgList in Amiga memory.

        This ensures handlers that read mp_MsgList directly (without calling GetMsg)
        can find the message.

        NOTE: We always reinitialize the list as a single-element list. The vamos
        GetMsg trap removes from the Python queue but doesn't unlink from the
        memory list. By always reinitializing, we avoid list corruption from
        stale entries.
        """
        from amitools.vamos.libstructs.exec_ import MsgPortStruct, ListStruct, NodeStruct
        from amitools.vamos.astructs.access import AccessStruct

        # Get mp_MsgList offset in MsgPort
        msglist_off = MsgPortStruct.sdef.find_field_def_by_name("mp_MsgList").offset
        list_addr = port_addr + msglist_off

        lst = AccessStruct(self.mem, ListStruct, list_addr)

        # Get node addresses in list structure
        lh_head_addr = list_addr + ListStruct.sdef.find_field_def_by_name("lh_Head").offset
        lh_tail_addr = list_addr + ListStruct.sdef.find_field_def_by_name("lh_Tail").offset

        # Access the message node (mn_Node is at offset 0 of Message)
        msg_node = AccessStruct(self.mem, NodeStruct, msg_addr)

        # Always reinitialize as single-element list to avoid stale entry issues
        # lh_Head points to first node, lh_Tail is 0 (end marker), lh_TailPred points to last node
        lst.w_s("lh_Head", msg_addr)
        lst.w_s("lh_TailPred", msg_addr)
        # Message node: ln_Succ points to lh_Tail (end of list), ln_Pred points to lh_Head
        msg_node.w_s("ln_Succ", lh_tail_addr)
        msg_node.w_s("ln_Pred", lh_head_addr)

    # ---- public orchestration ----

    def launch_with_startup(self, extra_packets=None, debug=False) -> HandlerLaunchState:
        proc_addr, port_addr, stack = self._create_process(name="pfs_handler")
        reply_port = self._create_port("caller_port", proc_addr)
        # fill DeviceNode dn_Task now that we have a port
        dn = AccessStruct(self.mem, DeviceNodeStruct, self.boot["dn_addr"])
        dn.w_s("dn_Task", port_addr)
        # startup packet args per pfs3: Arg1=mount name, Arg2=FSSM BPTR, Arg3=DeviceNode BPTR
        startup_pkt, startup_msg = self._build_std_packet(
            port_addr,
            reply_port,
            ACTION_STARTUP,
            [
                self.boot["dn_name_addr"] >> 2,
                self.boot["fssm_addr"] >> 2,
                self.boot["dn_addr"] >> 2,
            ],
        )
        # queue any additional packets before starting the handler
        if extra_packets:
            for pkt_type, args in extra_packets:
                self._build_std_packet(port_addr, reply_port, pkt_type, args)
        # Build entry stub that jumps to segment start.
        # Handler's own startup code will set up registers and call WaitPort/GetMsg
        # to retrieve the startup packet from pr_MsgPort.
        stub_pc = build_entry_stub(self.mem, self.alloc, self.segment_addr)
        start_regs = {REG_A6: self.exec_base_addr}
        task = Task("handler_task", stub_pc, stack, start_regs=start_regs)
        return HandlerLaunchState(
            process_addr=proc_addr,
            port_addr=port_addr,
            stack=stack,
            stdpkt_addr=startup_pkt,
            msg_addr=startup_msg,
            task=task,
            run_state=task.run_state,
            pc=stub_pc,
            sp=stack.get_initial_sp(),
            started=False,
            reply_port_addr=reply_port,
            entry_stub_pc=stub_pc,
        )

    def setup_resume_if_blocked(self, state: HandlerLaunchState, debug: bool = False) -> bool:
        """Check if handler is blocked waiting and set up resume state if signal/message pending.

        This should be called before run_burst to handle the case where a message
        was queued or signal was set since the last burst. Returns True if resume was set up.
        """
        from amitools.vamos.lib.ExecLibrary import ExecLibrary

        waitport_sp = ExecLibrary._waitport_blocked_sp
        wait_sp = ExecLibrary._wait_blocked_sp
        waitport_ret = ExecLibrary._waitport_blocked_ret
        wait_ret = ExecLibrary._wait_blocked_ret
        wait_mask = ExecLibrary._wait_blocked_mask

        blocked_sp = waitport_sp if waitport_sp is not None else wait_sp
        if blocked_sp is None:
            return False

        # Check if there's something pending that would wake the handler
        has_pending = False

        if wait_sp is not None and wait_mask is not None:
            # Wait() blocked - check if any signals in the mask are pending
            # This includes both message port signals AND direct signals (like IO completion)
            pending = self._compute_pending_signals(wait_mask)
            has_pending = pending != 0
        else:
            # WaitPort blocked - check for message on the specific port
            waitport_port = ExecLibrary._waitport_blocked_port
            if waitport_sp is not None and waitport_port is not None:
                check_port = waitport_port
            else:
                check_port = state.port_addr
            has_pending = self.exec_impl.port_mgr.has_msg(check_port)

        if not has_pending:
            return False

        blocked_ret = waitport_ret if waitport_ret is not None else wait_ret
        mem = self.vh.alloc.get_mem()
        cpu = self.vh.machine.cpu

        try:
            ret_addr = blocked_ret if blocked_ret is not None else mem.r32(blocked_sp)
        except Exception:
            ret_addr = 0

        if ret_addr == 0:
            return False

        # Set up resume state
        state.pc = ret_addr
        state.sp = blocked_sp + 4

        # Set D0 appropriately for Wait() or WaitPort()
        if wait_mask is not None:
            # Wait() resume - set D0 to ALL pending signals (not masked)
            # We're resuming because there's a message, so report the message signal
            # even if handler's mask didn't include it - otherwise handler spins forever
            pending = self._compute_pending_signals(0xFFFFFFFF)
            cpu.w_reg(REG_D0, pending)
            # CRITICAL: Clear the returned signals from tc_SigRecvd, just like Wait() would.
            # If we don't clear, the handler's next Wait() call will see the same signals
            # and return immediately, causing an infinite GetMsg/Wait loop.
            self._clear_signals_from_task(pending)
        elif waitport_sp is not None:
            # WaitPort()/WaitPkt() resume - set D0 appropriately
            waitport_port = ExecLibrary._waitport_blocked_port
            if waitport_port is not None:
                msg_addr = self.exec_impl.port_mgr.peek_msg(waitport_port)
                if DosLibrary._waitpkt_blocked and msg_addr:
                    # WaitPkt() resume - extract packet from message
                    msg = AccessStruct(self.mem, MessageStruct, msg_addr)
                    pkt_addr = msg.r_s("mn_Node.ln_Name")
                    d0_val = pkt_addr if pkt_addr else 0
                else:
                    # WaitPort() resume - return message address
                    d0_val = msg_addr if msg_addr else 0
                cpu.w_reg(REG_D0, d0_val)

        # Clear blocked state
        ExecLibrary._waitport_blocked_sp = None
        ExecLibrary._waitport_blocked_port = None
        ExecLibrary._waitport_blocked_ret = None
        ExecLibrary._wait_blocked_mask = None
        ExecLibrary._wait_blocked_sp = None
        ExecLibrary._wait_blocked_ret = None
        DosLibrary._waitpkt_blocked = False

        return True

    def run_burst(self, state: HandlerLaunchState, max_cycles=200000, debug: bool = False):
        """Run the handler from its current PC/SP for a limited number of cycles."""
        import sys
        # If handler has already crashed, don't try to run it
        if state.crashed:
            return state.run_state

        # Check if we need to resume from a blocked Wait/WaitPort
        self.setup_resume_if_blocked(state, debug=debug)

        cpu = self.vh.machine.cpu
        mem = self.vh.alloc.get_mem()
        ram_end = self.vh.machine.get_ram_total()

        def _pc_valid(pc: int) -> bool:
            # 0x800 is the minimum valid code address; below this is Amiga system
            # vectors, trap handlers, and reserved memory that cannot contain handler code.
            return 0x800 <= pc < ram_end

        # Validate PC before attempting to run - if it's garbage, handler is dead
        if not _pc_valid(state.pc):
            state.crashed = True
            state.last_error_pc = state.pc
            return state.run_state

        # Always restore A6 to ExecBase before entering handler code.
        # We can re-enter the handler after WaitPort/exit traps, and A6 may be 0.
        set_regs = {REG_A6: self.exec_base_addr}
        try:
            cpu.w_reg(REG_A6, self.exec_base_addr)
        except Exception:
            pass
        if not state.started:
            state.started = True
        # machine.run() pushes an exit trap address at SP. When resuming after
        # Wait/WaitPort, we must NOT offset SP by -4 or the handler's stack will
        # be corrupted (the handler expects SP to be at state.sp, not state.sp-4).
        # The exit trap overwrites whatever is at state.sp (typically a saved
        # register), but this is acceptable since the handler will restore/reuse
        # that location anyway. The critical thing is that rts pops from the
        # correct address (state.sp + 4 = the real return address).
        run_sp = state.sp
        run_state = self.vh.machine.run(
            state.pc,
            sp=run_sp,
            set_regs=set_regs,
            max_cycles=max_cycles,
            cycles_per_run=max_cycles,
            name="handler_burst",
        )
        state.run_state = run_state
        new_pc = cpu.r_pc()
        new_sp = cpu.r_reg(REG_A7)

        # Detect crash: if error occurred and new_pc is garbage, handler is dead
        if run_state.error and not _pc_valid(new_pc):
            state.consecutive_errors += 1
            # After first error, mark as crashed - don't try to recover
            # The handler's internal state is corrupted and restarting won't help
            if state.consecutive_errors == 1:
                # Dump CPU state for debugging
                d_regs = [cpu.r_reg(i) for i in range(8)]
                a_regs = [cpu.r_reg(8 + i) for i in range(8)]
                print(f"\n[amifuse] FATAL: Handler crashed", file=sys.stderr)
                print(f"[amifuse]   Initial PC=0x{state.pc:x} crashed at PC=0x{new_pc:x}", file=sys.stderr)
                print(f"[amifuse]   main_loop_pc=0x{state.main_loop_pc:x} entry_stub_pc=0x{state.entry_stub_pc:x}", file=sys.stderr)
                print(f"[amifuse]   D0-D7: {' '.join(f'{r:08x}' for r in d_regs)}", file=sys.stderr)
                print(f"[amifuse]   A0-A7: {' '.join(f'{r:08x}' for r in a_regs)}", file=sys.stderr)
                if a_regs[6] == 0:
                    print(f"[amifuse]   NOTE: A6 (ExecBase) is NULL - handler lost ExecBase reference", file=sys.stderr)
                print(f"[amifuse]   Restart amifuse to recover.", file=sys.stderr)
            state.crashed = True
            state.last_error_pc = new_pc
            return run_state
        # Check if WaitPort or Wait blocked (saved state in ExecLibrary class variable)
        from amitools.vamos.lib.ExecLibrary import ExecLibrary
        waitport_sp = ExecLibrary._waitport_blocked_sp
        wait_sp = ExecLibrary._wait_blocked_sp
        waitport_ret = ExecLibrary._waitport_blocked_ret
        wait_ret = ExecLibrary._wait_blocked_ret
        wait_mask = ExecLibrary._wait_blocked_mask
        # Use whichever blocking call was triggered
        blocked_sp = waitport_sp if waitport_sp is not None else wait_sp
        blocked_ret = waitport_ret if waitport_ret is not None else wait_ret
        # Capture updated PC/SP for the next burst, but only if the run was
        # interrupted mid-execution (cycle limit). If run_state.done is True,
        # the handler returned via the exit trap (PC would be 0x402 = hw_exc_addr,
        # which is invalid for re-entry). If there's an error, the PC might also
        # be at an invalid location.
        # Check error first since _terminate_run sets both error and done
        if run_state.error:
            # WaitPort/Wait or other blocking call failed - handler is waiting for a message.
            if blocked_sp is not None:
                ret_addr = blocked_ret if blocked_ret is not None else mem.r32(blocked_sp)
                # Save as main loop PC if not yet set - this is where handler waits for messages
                if (not state.initialized or state.main_loop_pc == 0) and _pc_valid(ret_addr):
                    state.main_loop_pc = ret_addr
                    state.main_loop_sp = blocked_sp + 4
                    state.initialized = True
                # Check if there's something pending that would wake the handler
                if wait_mask is not None:
                    # Wait() blocked - check for ANY pending signals (messages OR IO completion)
                    pending = self._compute_pending_signals(wait_mask)
                    has_pending = pending != 0
                else:
                    # WaitPort blocked - check for message on the port
                    has_pending = self.exec_impl.port_mgr.has_msg(state.port_addr)
                if has_pending:
                    # Resume from blocking call return address (saved on stack)
                    if _pc_valid(ret_addr):
                        state.pc = ret_addr
                        state.sp = blocked_sp + 4
                    elif state.main_loop_pc and _pc_valid(state.main_loop_pc):
                        state.pc = state.main_loop_pc
                        state.sp = state.main_loop_sp
                    # CRITICAL: Set D0 before resuming from Wait() or WaitPort()
                    if wait_mask is not None:
                        # Wait() resume - set D0 to pending signals
                        if wait_mask == 0:
                            pending = self._compute_pending_signals(0xFFFFFFFF)
                        else:
                            pending = self._compute_pending_signals(wait_mask)
                        cpu.w_reg(REG_D0, pending)
                        # Clear returned signals from tc_SigRecvd (like Wait() would)
                        self._clear_signals_from_task(pending)
                    elif waitport_sp is not None:
                        # WaitPort()/WaitPkt() resume - set D0 appropriately
                        waitport_port = ExecLibrary._waitport_blocked_port
                        if waitport_port is not None:
                            msg_addr = self.exec_impl.port_mgr.peek_msg(waitport_port)
                            if DosLibrary._waitpkt_blocked and msg_addr:
                                # WaitPkt() resume - extract packet from message
                                msg = AccessStruct(self.mem, MessageStruct, msg_addr)
                                pkt_addr = msg.r_s("mn_Node.ln_Name")
                                cpu.w_reg(REG_D0, pkt_addr if pkt_addr else 0)
                            else:
                                # WaitPort() resume - return message address
                                cpu.w_reg(REG_D0, msg_addr if msg_addr else 0)
                    # Clear both blocking states
                    ExecLibrary._waitport_blocked_port = None
                    ExecLibrary._waitport_blocked_sp = None
                    ExecLibrary._waitport_blocked_ret = None
                    ExecLibrary._wait_blocked_mask = None
                    ExecLibrary._wait_blocked_sp = None
                    ExecLibrary._wait_blocked_ret = None
                    DosLibrary._waitpkt_blocked = False
                else:
                    # No message pending - save restart point for later
                    if _pc_valid(ret_addr):
                        state.pc = ret_addr
                        state.sp = blocked_sp + 4
                    elif state.main_loop_pc and _pc_valid(state.main_loop_pc):
                        state.pc = state.main_loop_pc
                        state.sp = state.main_loop_sp
            elif state.initialized and state.main_loop_pc != 0:
                # Fall back to saved main loop PC
                state.pc = state.main_loop_pc
                state.sp = state.main_loop_sp
            # else: keep current PC (error happened during initialization)
        elif run_state.done:
            # Handler exited via exit trap.
            # Check if WaitPort/Wait blocked - if so, we can get the return address from the saved SP
            if blocked_sp is not None:
                ret_addr = blocked_ret if blocked_ret is not None else mem.r32(blocked_sp)
                # Save as main loop PC if not yet initialized
                if (not state.initialized or state.main_loop_pc == 0) and _pc_valid(ret_addr):
                    state.main_loop_pc = ret_addr
                    state.main_loop_sp = blocked_sp + 4  # Pop return address
                    state.initialized = True
                # Only restart immediately if there's something pending (message or signal).
                # Otherwise, leave state pointing to Wait/WaitPort return - caller
                # will queue a message before calling run_burst again.
                if wait_mask is not None:
                    # Wait() blocked - check for ANY pending signals (messages OR IO completion)
                    pending = self._compute_pending_signals(wait_mask)
                    has_pending = pending != 0
                else:
                    # WaitPort blocked - check for message on the port
                    has_pending = self.exec_impl.port_mgr.has_msg(state.port_addr)
                if has_pending:
                    # Restart from return address (where Wait/WaitPort was called)
                    if _pc_valid(ret_addr):
                        state.pc = ret_addr
                        state.sp = blocked_sp + 4  # Pop the return address
                    elif state.main_loop_pc and _pc_valid(state.main_loop_pc):
                        state.pc = state.main_loop_pc
                        state.sp = state.main_loop_sp
                    # CRITICAL: Set D0 before resuming from Wait() or WaitPort()
                    if wait_mask is not None:
                        # Wait() resume - set D0 to pending signals
                        if wait_mask == 0:
                            pending = self._compute_pending_signals(0xFFFFFFFF)
                        else:
                            pending = self._compute_pending_signals(wait_mask)
                        cpu.w_reg(REG_D0, pending)
                        # Clear returned signals from tc_SigRecvd (like Wait() would)
                        self._clear_signals_from_task(pending)
                    elif waitport_sp is not None:
                        # WaitPort()/WaitPkt() resume - set D0 appropriately
                        waitport_port = ExecLibrary._waitport_blocked_port
                        if waitport_port is not None:
                            msg_addr = self.exec_impl.port_mgr.peek_msg(waitport_port)
                            if DosLibrary._waitpkt_blocked and msg_addr:
                                # WaitPkt() resume - extract packet from message
                                msg = AccessStruct(self.mem, MessageStruct, msg_addr)
                                pkt_addr = msg.r_s("mn_Node.ln_Name")
                                cpu.w_reg(REG_D0, pkt_addr if pkt_addr else 0)
                            else:
                                # WaitPort() resume - return message address
                                cpu.w_reg(REG_D0, msg_addr if msg_addr else 0)
                    # Clear the blocked states
                    ExecLibrary._waitport_blocked_port = None
                    ExecLibrary._waitport_blocked_sp = None
                    ExecLibrary._waitport_blocked_ret = None
                    ExecLibrary._wait_blocked_mask = None
                    ExecLibrary._wait_blocked_sp = None
                    ExecLibrary._wait_blocked_ret = None
                    DosLibrary._waitpkt_blocked = False
                    state.exit_count = 0  # Reset exit counter
                else:
                    # No message pending - save restart point but don't spin.
                    # Next run_burst will restart from here when a message is queued.
                    if _pc_valid(ret_addr):
                        state.pc = ret_addr
                        state.sp = blocked_sp + 4
                    elif state.main_loop_pc and _pc_valid(state.main_loop_pc):
                        state.pc = state.main_loop_pc
                        state.sp = state.main_loop_sp
                    # Keep blocked state so we know handler is waiting
                    # Don't clear: ExecLibrary._waitport_blocked_port/sp or _wait_blocked_*
            else:
                # Normal exit trap (RTS to run_exit_addr) without WaitPort block.
                # For FFS, this means startup processing is complete. Check if there's
                # a reply we can read from the packet.
                has_pending = self.exec_impl.port_mgr.has_msg(state.port_addr)
                if has_pending and state.initialized and state.main_loop_pc != 0:
                    # Messages waiting and we have a valid restart point
                    state.pc = state.main_loop_pc
                    state.sp = state.main_loop_sp
                # else: Startup complete or no restart point - let caller check packet results
        else:
            if _pc_valid(new_pc):
                state.pc = new_pc
                state.sp = new_sp
            elif state.main_loop_pc and _pc_valid(state.main_loop_pc):
                state.pc = state.main_loop_pc
                state.sp = state.main_loop_sp
        return run_state

    def send_disk_info(self, state: HandlerLaunchState, info_buf_addr: int):
        # Arg1 = InfoData* (APTR)
        return self.send_packet(state, ACTION_DISK_INFO, [info_buf_addr])

    def send_read(self, state: HandlerLaunchState, buf_addr: int, offset_bytes: int, length_bytes: int):
        # Arg1 = window (not used), Arg2 = offset (block), Arg3 = buf, Arg4 = length
        # pfs3 uses TD style; offset in bytes, length in bytes; Arg1 ignored
        return self.send_packet(
            state,
            ACTION_READ,
            [
                0,
                offset_bytes,
                buf_addr,
                length_bytes,
            ],
        )

    def poll_replies(self, port_addr: int, debug: bool = False):
        """Return a list of (msg_addr, pkt_addr, res1, res2) for all queued replies."""
        results = []
        pmgr = self.exec_impl.port_mgr
        has_port = pmgr.has_port(port_addr)
        has_msg = pmgr.has_msg(port_addr) if has_port else False
        if debug:
            print(f"[poll_replies] port=0x{port_addr:x} has_port={has_port} has_msg={has_msg}")
        while has_port and has_msg:
            msg_addr = pmgr.get_msg(port_addr)
            msg = AccessStruct(self.mem, MessageStruct, msg_addr)
            pkt_addr = msg.r_s("mn_Node.ln_Name")
            pkt = AccessStruct(self.mem, DosPacketStruct, pkt_addr)
            res1 = pkt.r_s("dp_Res1")
            res2 = pkt.r_s("dp_Res2")
            if debug:
                print(f"[poll_replies] got msg=0x{msg_addr:x} pkt=0x{pkt_addr:x} res1=0x{res1:x} res2={res2}")
            results.append((msg_addr, pkt_addr, res1, res2))
            has_msg = pmgr.has_msg(port_addr)
        return results

    def send_packet(self, state: HandlerLaunchState, pkt_type: int, args):
        """Queue a packet to the handler and return the msg/pkt addresses."""
        pkt_addr, msg_addr = self._build_std_packet(
            state.port_addr, state.reply_port_addr, pkt_type, args
        )
        return pkt_addr, msg_addr

    def send_locate(
        self,
        state: HandlerLaunchState,
        lock_bptr: int,
        name_bptr: int,
        mode: int = SHARED_LOCK,
    ):
        """ACTION_LOCATE_OBJECT: returns a new lock BPTR in dp_Res1."""
        return self.send_packet(state, 8, [lock_bptr, name_bptr, mode])

    def send_examine(self, state: HandlerLaunchState, lock_bptr: int, fib_addr: int):
        """ACTION_EXAMINE_OBJECT"""
        return self.send_packet(state, 23, [lock_bptr, fib_addr >> 2])

    def send_examine_next(self, state: HandlerLaunchState, lock_bptr: int, fib_addr: int):
        """ACTION_EXAMINE_NEXT"""
        return self.send_packet(state, 24, [lock_bptr, fib_addr >> 2])

    def send_free_lock(self, state: HandlerLaunchState, lock_bptr: int):
        """ACTION_FREE_LOCK: release a lock returned by LOCATE."""
        return self.send_packet(state, ACTION_FREE_LOCK, [lock_bptr])

    def send_findinput(
        self, state: HandlerLaunchState, name_bptr: int, dir_lock_bptr: int = 0, fh_addr: int = None
    ):
        """ACTION_FINDINPUT: allocate a FileHandle if needed, return (fh_addr, pkt_addr, msg_addr)."""
        if fh_addr is None:
            fh_mem = self.alloc.alloc_struct(FileHandleStruct, label="FindInputFH")
            self.mem.w_block(fh_mem.addr, b"\x00" * FileHandleStruct.get_size())
            fh_addr = fh_mem.addr
        pkt_addr, msg_addr = self.send_packet(
            state,
            ACTION_FINDINPUT,
            [
                fh_addr >> 2,  # BPTR to FileHandle
                dir_lock_bptr,
                name_bptr,
            ],
        )
        return fh_addr, pkt_addr, msg_addr

    def send_findupdate(
        self, state: HandlerLaunchState, name_bptr: int, dir_lock_bptr: int = 0, fh_addr: int = None
    ):
        """ACTION_FINDUPDATE: open or create a file for read/write."""
        if fh_addr is None:
            fh_mem = self.alloc.alloc_struct(FileHandleStruct, label="FindUpdateFH")
            self.mem.w_block(fh_mem.addr, b"\x00" * FileHandleStruct.get_size())
            fh_addr = fh_mem.addr
        pkt_addr, msg_addr = self.send_packet(
            state,
            ACTION_FINDUPDATE,
            [
                fh_addr >> 2,  # BPTR to FileHandle
                dir_lock_bptr,
                name_bptr,
            ],
        )
        return fh_addr, pkt_addr, msg_addr

    def send_findoutput(
        self, state: HandlerLaunchState, name_bptr: int, dir_lock_bptr: int = 0, fh_addr: int = None, debug: bool = False
    ):
        """ACTION_FINDOUTPUT: open or create a file for output (truncate)."""
        if fh_addr is None:
            fh_mem = self.alloc.alloc_struct(FileHandleStruct, label="FindOutputFH")
            self.mem.w_block(fh_mem.addr, b"\x00" * FileHandleStruct.get_size())
            fh_addr = fh_mem.addr
        pkt_addr, msg_addr = self.send_packet(
            state,
            ACTION_FINDOUTPUT,
            [
                fh_addr >> 2,  # BPTR to FileHandle
                dir_lock_bptr,
                name_bptr,
            ],
        )
        return fh_addr, pkt_addr, msg_addr

    def send_delete_object(self, state: HandlerLaunchState, lock_bptr: int, name_bptr: int):
        """ACTION_DELETE_OBJECT: delete a file or directory."""
        return self.send_packet(state, ACTION_DELETE_OBJECT, [lock_bptr, name_bptr])

    def send_rename_object(
        self,
        state: HandlerLaunchState,
        src_lock_bptr: int,
        src_name_bptr: int,
        dst_lock_bptr: int,
        dst_name_bptr: int,
    ):
        """ACTION_RENAME_OBJECT: rename or move an object."""
        return self.send_packet(
            state,
            ACTION_RENAME_OBJECT,
            [
                src_lock_bptr,
                src_name_bptr,
                dst_lock_bptr,
                dst_name_bptr,
            ],
        )

    def send_create_dir(self, state: HandlerLaunchState, lock_bptr: int, name_bptr: int):
        """ACTION_CREATE_DIR: create a directory, returns new lock in res1."""
        return self.send_packet(state, ACTION_CREATE_DIR, [lock_bptr, name_bptr])

    def send_read_handle(
        self, state: HandlerLaunchState, fh_addr: int, buf_addr: int, length_bytes: int
    ):
        """ACTION_READ using a FileHandle filled by FINDINPUT; uses fh_Args as the fileentry pointer."""
        fh = AccessStruct(self.mem, FileHandleStruct, fh_addr)
        fileentry_ptr = fh.r_s("fh_Args")
        return self.send_packet(
            state,
            ACTION_READ,
            [
                fileentry_ptr,
                buf_addr,
                length_bytes,
            ],
        )

    def send_write_handle(
        self, state: HandlerLaunchState, fh_addr: int, buf_addr: int, length_bytes: int
    ):
        """ACTION_WRITE using a FileHandle filled by FINDOUTPUT/FINDUPDATE."""
        fh = AccessStruct(self.mem, FileHandleStruct, fh_addr)
        fileentry_ptr = fh.r_s("fh_Args")
        return self.send_packet(
            state,
            ACTION_WRITE,
            [
                fileentry_ptr,
                buf_addr,
                length_bytes,
            ],
        )

    def send_set_file_size(
        self, state: HandlerLaunchState, fh_addr: int, size: int, mode: int = OFFSET_BEGINNING
    ):
        """ACTION_SET_FILE_SIZE on a FileHandle; mode defaults to OFFSET_BEGINNING."""
        fh = AccessStruct(self.mem, FileHandleStruct, fh_addr)
        fileentry_ptr = fh.r_s("fh_Args")
        return self.send_packet(
            state,
            ACTION_SET_FILE_SIZE,
            [
                fileentry_ptr,
                size,
                mode,
            ],
        )

    def send_seek_handle(self, state: HandlerLaunchState, fh_addr: int, offset: int, mode: int = OFFSET_BEGINNING):
        """ACTION_SEEK on a FileHandle; mode defaults to OFFSET_BEGINNING."""
        fh = AccessStruct(self.mem, FileHandleStruct, fh_addr)
        fileentry_ptr = fh.r_s("fh_Args")
        return self.send_packet(
            state,
            ACTION_SEEK,
            [
                fileentry_ptr,
                offset,
                mode,
            ],
        )

    def send_end_handle(self, state: HandlerLaunchState, fh_addr: int):
        """ACTION_END on a FileHandle; closes the file handle in the handler."""
        fh = AccessStruct(self.mem, FileHandleStruct, fh_addr)
        fileentry_ptr = fh.r_s("fh_Args")
        return self.send_packet(
            state,
            ACTION_END,
            [
                fileentry_ptr,
            ],
        )

    def send_flush(self, state: HandlerLaunchState):
        """ACTION_FLUSH: flush filesystem buffers."""
        return self.send_packet(state, ACTION_FLUSH, [])

    def _alloc_signal_bit(self) -> int:
        """Reserve a signal bit from exec's bitmap."""
        # mirror ExecLibrary.AllocSignal logic
        for idx, used in enumerate(self.exec_impl._signals):
            if not used:
                self.exec_impl._signals[idx] = True
                return idx
        return 0  # fallback; should not happen
