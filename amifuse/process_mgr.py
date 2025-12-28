"""
ProcessManager for multi-process filesystem handler support.

This module manages multiple Amiga processes (parent handler + children)
for filesystems like SFS that spawn child processes via CreateNewProc().
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any

from amitools.vamos.astructs import AccessStruct
from amitools.vamos.libstructs import ProcessStruct
from amitools.vamos.machine.regs import REG_D0, REG_A6


@dataclass
class ProcessState:
    """Track execution state for one Amiga process."""
    proc_addr: int              # Amiga Process structure address
    entry_pc: int               # Entry point / current PC
    sp: int                     # Current stack pointer
    name: str = ""              # Process name
    port_addr: int = 0          # Process's pr_MsgPort address
    blocked: bool = False       # True if waiting on signals
    wait_mask: int = 0          # Signal mask being waited on
    pending_signals: int = 0    # Signals received while blocked
    is_child: bool = False      # True if spawned by CreateNewProc
    started: bool = False       # True if process has started executing
    exited: bool = False        # True if process has exited
    stack: Any = None           # Stack object (for cleanup)


class ProcessManager:
    """Manage multiple Amiga processes for amifuse.

    This class tracks parent and child processes created via CreateNewProc(),
    allowing amifuse to execute multiple processes in a round-robin fashion.
    """

    def __init__(self, vh, machine, exec_impl, parent_proc_addr: int):
        """Initialize ProcessManager.

        Args:
            vh: VamosHandlerRuntime instance
            machine: m68k Machine instance
            exec_impl: ExecLibrary implementation
            parent_proc_addr: Address of parent handler's Process structure
        """
        self.vh = vh
        self.machine = machine
        self.exec_impl = exec_impl
        self.mem = machine.get_mem()
        self.cpu = machine.get_cpu()

        # Process tracking
        self.processes: Dict[int, ProcessState] = {}
        self.parent_addr = parent_proc_addr
        self.ready_queue: List[ProcessState] = []

        # Register parent process
        parent_state = ProcessState(
            proc_addr=parent_proc_addr,
            entry_pc=0,  # Set by caller
            sp=0,        # Set by caller
            name="parent_handler",
            is_child=False,
            started=True,
        )
        self.processes[parent_proc_addr] = parent_state

    def check_for_new_children(self) -> List[ProcessState]:
        """Check DosLibrary._child_processes for newly created children.

        Returns list of new ProcessState objects that were added.
        """
        from amitools.vamos.lib.DosLibrary import DosLibrary

        new_children = []
        for proc_addr, info in list(DosLibrary._child_processes.items()):
            if proc_addr not in self.processes:
                # Create ProcessState for this child
                state = ProcessState(
                    proc_addr=proc_addr,
                    entry_pc=info["entry_pc"],
                    sp=info["stack"].get_initial_sp(),
                    name=info.get("name", "child"),
                    port_addr=info.get("port_addr", 0),
                    is_child=True,
                    started=False,
                    stack=info.get("stack"),
                )
                self.processes[proc_addr] = state
                self.ready_queue.append(state)
                new_children.append(state)

        return new_children

    def get_ready_children(self) -> List[ProcessState]:
        """Get list of child processes ready to execute."""
        return [p for p in self.ready_queue
                if not p.blocked and not p.exited]

    def run_child_burst(self, child: ProcessState, max_cycles: int = 50000) -> bool:
        """Run a child process for a burst of cycles.

        Args:
            child: ProcessState for the child to run
            max_cycles: Maximum cycles to execute

        Returns:
            True if child is still running, False if exited/blocked
        """
        if child.exited:
            return False

        # Save full current CPU state (all 16 registers)
        saved_pc = self.cpu.r_pc()
        saved_sp = self.cpu.r_sp()
        saved_regs = [self.cpu.r_reg(i) for i in range(16)]  # D0-D7, A0-A7

        # Set up child's context
        if not child.started:
            # First run - set entry point and initial SP
            pc = child.entry_pc
            sp = child.sp
            child.started = True
        else:
            # Resume from where we left off
            pc = child.entry_pc  # Updated after each burst
            sp = child.sp

        # Switch ThisTask to child process
        self._set_this_task(child.proc_addr)

        # Run child
        try:
            run_state = self.machine.run(
                pc=pc,
                sp=sp,
                max_cycles=max_cycles,
                name=f"child_{child.name}"
            )

            # Update child state
            child.entry_pc = run_state.pc
            child.sp = run_state.sp

            if run_state.done:
                child.exited = True
                return False
            elif run_state.error:
                # Child blocked (WaitPort/Wait)
                child.blocked = True
                return False
            else:
                # Still running, just hit cycle limit
                return True

        finally:
            # Restore full parent context
            self._set_this_task(self.parent_addr)
            self.cpu.w_pc(saved_pc)
            self.cpu.w_sp(saved_sp)
            for i in range(16):
                self.cpu.w_reg(i, saved_regs[i])

    def _set_this_task(self, proc_addr: int):
        """Set ExecBase.ThisTask to the given process."""
        from amitools.vamos.libstructs import ExecLibraryStruct
        exec_base = self.exec_impl.exec_lib.get_addr()
        exec_struct = AccessStruct(self.mem, ExecLibraryStruct, exec_base)
        exec_struct.w_s("ThisTask", proc_addr)
        # Also update exec_impl's internal pointer
        self.exec_impl.exec_lib.this_task.aptr = proc_addr

    def run_all_ready_children(self, cycles_per_child: int = 50000) -> int:
        """Run all ready child processes for one burst each.

        Returns number of children that ran.
        """
        # Check for newly created children first
        self.check_for_new_children()

        ran = 0
        for child in self.get_ready_children():
            self.run_child_burst(child, cycles_per_child)
            ran += 1

        return ran
