"""
Sets up a minimal vamos environment for loading a filesystem handler.
This stops short of actually running the handler, but it constructs the
machine, memory map, path manager, scheduler, and lib manager, and loads
the handler into memory so we can later jump into it.
"""

import logging
import tempfile
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Optional

from amitools.vamos.cfg import VamosMainParser
from amitools.vamos.error import UnsupportedFeatureError
from amitools.vamos.log import log_machine, log_setup
from amitools.vamos.machine import Machine, MemoryMap, Runtime
from amitools.vamos.trace import TraceManager


@dataclass
class SimpleRunState:
    """Simple run state for tracking machine execution results."""
    pc: int
    sp: int
    done: bool = False
    error: bool = False
    cycles: int = 0


from amitools.vamos.path import VamosPathManager
from amitools.vamos.schedule import Scheduler
from amitools.vamos.libmgr import SetupLibManager
# local fake scsi.device
from amifuse.scsi_device import ScsiDevice

_LOG_SETUP_DONE = False


def _ensure_vamos_logging(levels):
    global _LOG_SETUP_DONE
    if _LOG_SETUP_DONE or log_machine.handlers:
        _LOG_SETUP_DONE = True
        return
    cfg = SimpleNamespace(
        file=None,
        timestamps=False,
        quiet=False,
        verbose=False,
        levels=levels,
    )
    log_setup(cfg)
    _LOG_SETUP_DONE = True


class VamosHandlerRuntime:
    def __init__(self):
        self.machine = None
        self.mem_map = None
        self.alloc = None
        self.trace_mgr = None
        self.path_mgr = None
        self.scheduler = None
        self.runtime = None
        self.slm: Optional[SetupLibManager] = None
        self.seglist_baddr: Optional[int] = None
        self._temp_dir: Optional[tempfile.TemporaryDirectory] = None

    def setup(self, cpu: Optional[str] = None):
        # Use default vamos configs (bin argument required, so pass a dummy).
        mp = VamosMainParser()
        mp.parse(paths=None, args=["dummy"], cfg_dict=None)

        # machine + mem map
        machine_cfg = mp.get_machine_dict().machine
        mem_map_cfg = mp.get_machine_dict().memmap
        # give us enough RAM headroom for relocated handlers at 0x100000+
        if hasattr(machine_cfg, "ram_size") and machine_cfg.ram_size < 8192:
            machine_cfg.ram_size = 8192
        if cpu:
            machine_cfg.cpu = cpu
        trace_cfg = mp.get_trace_dict().trace
        path_cfg = mp.get_path_dict()
        # Create a temp directory for vamos path manager to avoid permission
        # issues when amitools tries to create volume directories in vols_base_dir.
        # Using "/" would try to create /system etc. which requires root.
        self._temp_dir = tempfile.TemporaryDirectory(prefix="amifuse_")
        temp_path = self._temp_dir.name
        # Avoid touching ~/.vamos/volumes; keep auto volumes/assigns disabled here.
        path_cfg["path"]["auto_volumes"] = []
        path_cfg["path"]["auto_assigns"] = []
        path_cfg["path"]["vols_base_dir"] = temp_path
        path_cfg["path"]["cwd"] = f"root:{temp_path}"
        path_cfg["path"]["command"] = [f"root:{temp_path}"]
        # Provide a minimal root: volume pointing to temp dir so cwd resolves.
        path_cfg["volumes"] = [f"root:{temp_path}"]
        path_cfg["assigns"] = {}
        libs_cfg = mp.get_libs_dict()

        try:
            self.machine = Machine.from_cfg(machine_cfg, use_labels=False)
        except ImportError as e:
            raise RuntimeError(
                "machine68k dependency missing. Install amitools extras "
                "(pip install .[full]) or add machine68k to your environment."
            ) from e
        self.mem_map = MemoryMap(self.machine)
        if not self.mem_map.parse_config(mem_map_cfg):
            raise RuntimeError("Failed to parse memory map config")
        self.alloc = self.mem_map.get_alloc()

        # trace manager (mostly disabled by default)
        self.trace_mgr = TraceManager(self.machine)
        self.trace_mgr.parse_config(trace_cfg)

        # path manager
        self.path_mgr = VamosPathManager()
        if not self.path_mgr.parse_config(path_cfg):
            raise RuntimeError("Failed to parse path manager config")
        if not self.path_mgr.setup():
            raise RuntimeError("Failed to setup path manager")

        # scheduler
        self.scheduler = Scheduler(self.machine)

        # Create a runtime for m68k code execution
        self.runtime = Runtime(self.machine, self.machine.scratch_end)

        # runner function for library code execution
        def runner(code, name=None):
            task = self.scheduler.get_cur_task()
            if task:
                return task.sub_run(code, name=name)
            else:
                return self.runtime.run(code, name=name)

        # libs
        self.slm = SetupLibManager(
            self.machine, self.mem_map, runner, self.scheduler, self.path_mgr
        )
        if not self.slm.parse_config(libs_cfg):
            raise RuntimeError("Failed to parse lib manager config")
        self.slm.setup()
        self.slm.open_base_libs()
        # register fake scsi.device backed by backend placeholder
        # actual backend will be set by caller via set_scsi_backend()
        self.scsi_backend = None
        self.scsi_debug = False
        self.slm.lib_mgr.add_impl_cls("scsi.device", lambda: ScsiDevice(self.scsi_backend, self.scsi_debug))
        from amifuse.null_device import NullDevice  # lazy import to avoid cycles
        from amitools.vamos.lib.TimerDevice import TimerDevice
        self.slm.lib_mgr.add_impl_cls("keyboard.device", NullDevice)
        self.slm.lib_mgr.add_impl_cls("timer.device", TimerDevice)
        self.slm.lib_mgr.add_impl_cls("console.device", NullDevice)

        # Add run() method to machine for backwards compatibility
        self._add_machine_run_method()

    def _add_machine_run_method(self):
        """Add a run() method to machine that provides the expected interface."""
        machine = self.machine

        def machine_run(pc, sp=None, set_regs=None, max_cycles=1000,
                        cycles_per_run=None, name=None):
            """Run m68k code and return a SimpleRunState.

            If sp is provided and differs from current SP, prepare() is called
            which pushes an exit trap. If sp matches current SP, we resume
            without modifying the stack (for resuming from blocked state).
            """
            from amitools.vamos.machine.regs import REG_A7

            cpu = machine.cpu
            mem = machine.get_mem()

            # Set up registers
            if set_regs:
                for reg, val in set_regs.items():
                    cpu.w_reg(reg, val)

            current_sp = cpu.r_reg(REG_A7)

            # Check if we should use prepare() or just set PC directly
            if sp is None:
                sp = current_sp

            # Only call prepare() for initial runs (when SP changes significantly)
            # For resumption from blocked state, just set PC and SP directly
            needs_prepare = abs(sp - current_sp) > 16  # Heuristic: significant SP change

            if needs_prepare:
                machine.prepare(pc, sp)
            else:
                # Resume without pushing exit trap - just set PC and SP
                cpu.w_pc(pc)
                cpu.w_sp(sp)

            total_cycles = 0
            done = False
            error = False

            try:
                er = machine.execute(max_cycles)
                total_cycles = er.cycles if hasattr(er, 'cycles') else max_cycles
                done = machine.was_exit(er)
            except UnsupportedFeatureError:
                # WaitPort/WaitPkt blocked - this is expected behavior
                error = True
            except Exception:
                # Unexpected error during execution
                error = True

            return SimpleRunState(
                pc=cpu.r_pc(),
                sp=cpu.r_sp(),
                done=done,
                error=error,
                cycles=total_cycles,
            )

        machine.run = machine_run

    def set_scsi_backend(self, backend, debug=False):
        self.scsi_backend = backend
        self.scsi_debug = debug

    def enable_trace(self, show_regs: bool = False):
        if not self.machine:
            return
        _ensure_vamos_logging({"machine": "info"})
        log_machine.setLevel(logging.INFO)
        self.machine.show_instr(show_regs=show_regs)

    def load_handler(self, handler_path: Path) -> int:
        if not self.slm:
            raise RuntimeError("setup() not called")
        seg_baddr = self.slm.seg_loader.load_sys_seglist(str(handler_path))
        if seg_baddr == 0:
            raise RuntimeError(f"Failed to load handler {handler_path}")
        self.seglist_baddr = seg_baddr
        return seg_baddr

    def shutdown(self):
        if self.slm:
            self.slm.close_base_libs()
            self.slm.cleanup()
        if self.path_mgr:
            self.path_mgr.shutdown()
        if self.mem_map:
            self.mem_map.cleanup()
        if self.machine:
            self.machine.cleanup()
        if self._temp_dir:
            self._temp_dir.cleanup()
            self._temp_dir = None
