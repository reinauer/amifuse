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
    CLIStruct,
    DosPacketStruct,
    MessageStruct,
    ProcessStruct,
    FileHandleStruct,
)  # type: ignore
from amitools.vamos.machine.regs import REG_A0, REG_A6, REG_A7  # type: ignore
from amitools.vamos.schedule.stack import Stack  # type: ignore
from amitools.vamos.schedule.task import Task  # type: ignore
from .amiga_structs import DeviceNodeStruct  # type: ignore
from amitools.vamos.libstructs.exec_ import NodeStruct  # type: ignore
from .handler_stub import build_entry_stub  # type: ignore

# Dos packet opcodes we care about
ACTION_STARTUP = 0
ACTION_DISK_INFO = 25
ACTION_READ = ord("R")
ACTION_SEEK = 1008
ACTION_END = 1007


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


class HandlerLauncher:
    def __init__(self, vh, boot_info: Dict, handler_entry_addr: int):
        self.vh = vh
        self.alloc = vh.alloc
        self.mem = vh.alloc.get_mem()
        self.exec_impl = vh.slm.exec_impl
        # ExecBase is stored at address 4 on Amiga systems
        self.exec_base_addr = self.mem.r32(4)
        self.boot = boot_info
        self.handler_entry_addr = handler_entry_addr

    # ---- low-level helpers ----

    def _write_bstr(self, text: str, label: str) -> int:
        # Use latin-1 encoding for Amiga compatibility (handles chars 0-255)
        encoded = text.encode("latin-1", errors="replace")
        data = bytes([len(encoded)]) + encoded
        mem_obj = self.alloc.alloc_memory(len(data), label=label)
        self.mem.w_block(mem_obj.addr, data)
        return mem_obj.addr

    def _init_msgport(self, port_addr: int, task_addr: int):
        mp = AccessStruct(self.mem, MsgPortStruct, port_addr)
        # zero first to clear garbage
        self.mem.w_block(port_addr, b"\x00" * MsgPortStruct.get_size())
        mp.w_s("mp_Node.ln_Type", NodeType.NT_MSGPORT)
        mp.w_s("mp_Flags", MsgPortFlags.PA_SIGNAL)
        sigbit = self._alloc_signal_bit()
        mp.w_s("mp_SigBit", sigbit)
        mp.w_s("mp_SigTask", task_addr)
        lst_off = MsgPortStruct.sdef.find_field_def_by_name("mp_MsgList").offset
        lst = AccessStruct(self.mem, ListStruct, port_addr + lst_off)
        lst.w_s("lh_Head", 0)
        lst.w_s("lh_Tail", 0)
        lst.w_s("lh_TailPred", 0)
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
        # Minimal CLI to placate handlers expecting a CLI/process context
        cli_mem = self.alloc.alloc_memory(CLIStruct.get_size(), label="HandlerCLI")
        self.mem.w_block(cli_mem.addr, b"\x00" * CLIStruct.get_size())
        cli = AccessStruct(self.mem, CLIStruct, cli_mem.addr)
        cmd_name_bstr = self._write_bstr(name, "CLICommandName")
        prompt_bstr = self._write_bstr("", "CLIPrompt")
        cli.w_s("cli_CommandName", cmd_name_bstr >> 2)
        cli.w_s("cli_SetName", cmd_name_bstr >> 2)
        cli.w_s("cli_Prompt", prompt_bstr >> 2)
        cli.w_s("cli_DefaultStack", stack.get_size() >> 2)
        proc.w_s("pr_CLI", cli_mem.addr >> 2)
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
        sigbit = self._init_msgport(port_addr, proc_mem.addr)
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
        sp_mem = self.alloc.alloc_memory(total, label=f"stdpkt_{pkt_type}")
        self.mem.w_block(sp_mem.addr, b"\x00" * total)
        msg = AccessStruct(self.mem, MessageStruct, sp_mem.addr)
        pkt = AccessStruct(self.mem, DosPacketStruct, sp_mem.addr + MessageStruct.get_size())
        # message
        msg.w_s("mn_Node.ln_Type", NodeType.NT_MESSAGE)
        msg.w_s("mn_Node.ln_Succ", 0)
        msg.w_s("mn_Node.ln_Pred", 0)
        msg.w_s("mn_ReplyPort", reply_port_addr)
        msg.w_s("mn_Length", total)
        # packet
        pkt.w_s("dp_Link", sp_mem.addr)  # BPTR to message in StandardPacket
        pkt.w_s("dp_Port", reply_port_addr)
        pkt.w_s("dp_Type", pkt_type)
        for i, val in enumerate(args[:7], start=1):
            pkt.w_s(f"dp_Arg{i}", val)
        # link message name to packet
        msg.w_s("mn_Node.ln_Name", sp_mem.addr + MessageStruct.get_size())
        # queue message to destination port
        self.exec_impl.port_mgr.put_msg(dest_port_addr, sp_mem.addr)
        return sp_mem.addr + MessageStruct.get_size(), sp_mem.addr

    # ---- public orchestration ----

    def launch_with_startup(self, extra_packets=None) -> HandlerLaunchState:
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
        # build entry stub to force A0 = fssm_bptr<<2 before jumping to handler entry
        stub_pc = build_entry_stub(
            self.mem, self.alloc, self.boot["fssm_addr"] >> 2, self.handler_entry_addr
        )
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

    def run_burst(self, state: HandlerLaunchState, max_cycles=200000):
        """Run the handler from its current PC/SP for a limited number of cycles."""
        cpu = self.vh.machine.cpu
        mem = self.vh.alloc.get_mem()
        set_regs = None
        if not state.started:
            set_regs = {REG_A6: self.exec_base_addr}
            state.started = True
        run_state = self.vh.machine.run(
            state.pc,
            sp=state.sp,
            set_regs=set_regs,
            max_cycles=max_cycles,
            cycles_per_run=max_cycles,
            name="handler_burst",
        )
        state.run_state = run_state
        new_pc = cpu.r_pc()
        new_sp = cpu.r_reg(REG_A7)
        # Check if WaitPort blocked (saved state in ExecLibrary class variable)
        from amitools.vamos.lib.ExecLibrary import ExecLibrary
        waitport_sp = ExecLibrary._waitport_blocked_sp
        # Capture updated PC/SP for the next burst, but only if the run was
        # interrupted mid-execution (cycle limit). If run_state.done is True,
        # the handler returned via the exit trap (PC would be 0x402 = hw_exc_addr,
        # which is invalid for re-entry). If there's an error, the PC might also
        # be at an invalid location.
        # Check error first since _terminate_run sets both error and done
        if run_state.error:
            # WaitPort or other blocking call failed - handler is waiting for a message.
            if state.initialized and state.main_loop_pc != 0:
                # Restart from saved main loop PC
                state.pc = state.main_loop_pc
                state.sp = state.main_loop_sp
            # else: keep current PC (error happened during initialization)
        elif run_state.done:
            # Handler exited via exit trap.
            # Check if WaitPort blocked - if so, we can get the return address from the saved SP
            if waitport_sp is not None:
                ret_addr = mem.r32(waitport_sp)
                # Save as main loop PC if not yet initialized
                if not state.initialized or state.main_loop_pc == 0:
                    state.main_loop_pc = ret_addr
                    state.main_loop_sp = waitport_sp + 4  # Pop return address
                    state.initialized = True
                # Only restart immediately if there's a message pending.
                # Otherwise, leave state pointing to WaitPort return - caller
                # will queue a message before calling run_burst again.
                has_pending = self.exec_impl.port_mgr.has_msg(state.port_addr)
                if has_pending:
                    # Restart from return address (where WaitPort was called)
                    state.pc = ret_addr
                    state.sp = waitport_sp + 4  # Pop the return address
                    # Clear the blocked state
                    ExecLibrary._waitport_blocked_port = None
                    ExecLibrary._waitport_blocked_sp = None
                    state.exit_count = 0  # Reset exit counter
                else:
                    # No message pending - save restart point but don't spin.
                    # Next run_burst will restart from here when a message is queued.
                    state.pc = ret_addr
                    state.sp = waitport_sp + 4
                    # Keep blocked state so we know handler is waiting
                    # Don't clear: ExecLibrary._waitport_blocked_port/sp
            else:
                # Normal exit trap (RTS to run_exit_addr) without WaitPort block.
                # PFS3 handler processes ONE message and exits. To process another,
                # we need to restart. Check if there are pending messages.
                has_pending = self.exec_impl.port_mgr.has_msg(state.port_addr)
                if has_pending:
                    # Messages waiting - restart handler to process them.
                    # IMPORTANT: Use main_loop_pc to preserve handler state, not entry_stub_pc
                    # which would cause full reinitialization and lose mounted volume state.
                    if state.initialized and state.main_loop_pc != 0:
                        state.pc = state.main_loop_pc
                        state.sp = state.main_loop_sp
                    else:
                        # Fall back to entry_stub_pc if no main loop saved
                        state.pc = state.entry_stub_pc
                        state.sp = state.stack.get_initial_sp()
                        state.started = False
                # else: No messages pending - keep old PC (caller will send message and retry)
        else:
            state.pc = new_pc
            state.sp = new_sp
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

    def poll_replies(self, port_addr: int):
        """Return a list of (msg_addr, pkt_addr, res1, res2) for all queued replies."""
        results = []
        pmgr = self.exec_impl.port_mgr
        while pmgr.has_port(port_addr) and pmgr.has_msg(port_addr):
            msg_addr = pmgr.get_msg(port_addr)
            msg = AccessStruct(self.mem, MessageStruct, msg_addr)
            pkt_addr = msg.r_s("mn_Node.ln_Name")
            pkt = AccessStruct(self.mem, DosPacketStruct, pkt_addr)
            res1 = pkt.r_s("dp_Res1")
            res2 = pkt.r_s("dp_Res2")
            results.append((msg_addr, pkt_addr, res1, res2))
        return results

    def send_packet(self, state: HandlerLaunchState, pkt_type: int, args):
        """Queue a packet to the handler and return the msg/pkt addresses."""
        pkt_addr, msg_addr = self._build_std_packet(
            state.port_addr, state.reply_port_addr, pkt_type, args
        )
        return pkt_addr, msg_addr

    def send_locate(self, state: HandlerLaunchState, lock_bptr: int, name_bptr: int):
        """ACTION_LOCATE_OBJECT: returns a new lock BPTR in dp_Res1."""
        return self.send_packet(state, 8, [lock_bptr, name_bptr])

    def send_examine(self, state: HandlerLaunchState, lock_bptr: int, fib_addr: int):
        """ACTION_EXAMINE_OBJECT"""
        return self.send_packet(state, 23, [lock_bptr, fib_addr >> 2])

    def send_examine_next(self, state: HandlerLaunchState, lock_bptr: int, fib_addr: int):
        """ACTION_EXAMINE_NEXT"""
        return self.send_packet(state, 24, [lock_bptr, fib_addr >> 2])

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
            1005,  # ACTION_FINDINPUT
            [
                fh_addr >> 2,  # BPTR to FileHandle
                dir_lock_bptr,
                name_bptr,
            ],
        )
        return fh_addr, pkt_addr, msg_addr

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

    def send_seek_handle(self, state: HandlerLaunchState, fh_addr: int, offset: int, mode: int = 0):
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

    def _alloc_signal_bit(self) -> int:
        """Reserve a signal bit from exec's bitmap."""
        # mirror ExecLibrary.AllocSignal logic
        for idx, used in enumerate(self.exec_impl._signals):
            if not used:
                self.exec_impl._signals[idx] = True
                return idx
        return 0  # fallback; should not happen
