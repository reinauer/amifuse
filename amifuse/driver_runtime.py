"""
Minimal runtime skeleton for executing an Amiga filesystem handler against a host
block device. This does **not** yet execute the driver; it wires together
loading/relocation and a block-device backend so we can incrementally add a
packet loop + FUSE bridge.
"""

import argparse
import sys
from pathlib import Path
from typing import Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
AMITOOLS_PATH = REPO_ROOT / "amitools"

# Prefer local checkout of amitools if it is not installed
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(AMITOOLS_PATH) not in sys.path:
    sys.path.insert(0, str(AMITOOLS_PATH))

from amitools.binfmt.BinFmt import BinFmt  # type: ignore  # noqa: E402
from amitools.binfmt.Relocate import Relocate  # type: ignore  # noqa: E402
from amitools.fs.blkdev.RawBlockDevice import RawBlockDevice  # type: ignore  # noqa: E402
from amitools.fs.rdb.RDisk import RDisk  # type: ignore  # noqa: E402
from .packet_loop import HandlerPacketLoop  # noqa: E402
from .vamos_runner import VamosHandlerRuntime  # noqa: E402
from .bootstrap import BootstrapAllocator  # noqa: E402
from .startup_runner import HandlerLauncher  # noqa: E402


class BlockDeviceBackend:
    """Thin wrapper around a host file to provide block reads/writes."""

    def __init__(self, image: Path, block_size: Optional[int] = None, read_only=True):
        self.image = image
        self.block_size = block_size or 512
        self.read_only = read_only
        self.blkdev: Optional[RawBlockDevice] = None
        self.rdb: Optional[RDisk] = None

    def open(self):
        self.blkdev = RawBlockDevice(
            str(self.image), read_only=self.read_only, block_bytes=self.block_size
        )
        self.blkdev.open()
        self.rdb = RDisk(self.blkdev)
        # Try to auto-adjust block size if the RDB disagrees.
        peeked = self.rdb.peek_block_size()
        if peeked and peeked != self.blkdev.block_bytes:
            self.close()
            self.blkdev = RawBlockDevice(
                str(self.image), read_only=self.read_only, block_bytes=peeked
            )
            self.blkdev.open()
            self.rdb = RDisk(self.blkdev)
        if not self.rdb.open():
            raise IOError(f"Failed to parse RDB on {self.image}")
        # cache geometry for devices like scsi.device
        pd = self.rdb.rdb.phy_drv
        self.block_size = self.blkdev.block_bytes
        self.cyls = pd.cyls
        self.heads = pd.heads
        self.secs = pd.secs
        self.total_blocks = pd.cyls * pd.heads * pd.secs

    def close(self):
        if self.rdb:
            self.rdb.close()
        if self.blkdev:
            self.blkdev.close()

    def read_blocks(self, blk_num: int, num_blks: int = 1) -> bytes:
        if not self.blkdev:
            raise RuntimeError("Block device not open")
        return self.blkdev.read_block(blk_num, num_blks)

    def write_blocks(self, blk_num: int, data: bytes, num_blks: int = 1):
        if not self.blkdev:
            raise RuntimeError("Block device not open")
        if self.read_only:
            raise PermissionError("Backend opened read-only")
        self.blkdev.write_block(blk_num, data, num_blks)

    def describe(self) -> str:
        assert self.rdb is not None
        pd = self.rdb.rdb.phy_drv
        return (
            f"{self.image} cyls={pd.cyls} heads={pd.heads} secs={pd.secs} "
            f"block={self.blkdev.block_bytes if self.blkdev else self.block_size}"
        )


class DriverRuntimeSkeleton:
    """
    Loads a filesystem handler binary, relocates it, and pairs it with a block
    device backend. Execution is not implemented yet; this is the staging area
    for a vamos-backed packet loop.
    """

    def __init__(self, driver_path: Path, base_addr: int = 0x100000, padding: int = 0):
        self.driver_path = driver_path
        self.base_addr = base_addr
        self.padding = padding
        self.bin_img = None
        self.relocated = None
        self.addrs = None

    def load(self):
        self.bin_img = BinFmt().load_image(str(self.driver_path))
        if self.bin_img is None:
            raise FileNotFoundError(f"Cannot load driver binary: {self.driver_path}")
        reloc = Relocate(self.bin_img)
        self.addrs = reloc.get_seq_addrs(self.base_addr, padding=self.padding)
        self.relocated = reloc.relocate(self.addrs)

    def summary(self) -> str:
        assert self.bin_img is not None and self.addrs is not None
        segs = self.bin_img.get_segments()
        total = (self.addrs[-1] + segs[-1].size) - self.addrs[0]
        return (
            f"{self.driver_path} segments={len(segs)} "
            f"base=0x{self.addrs[0]:x} footprint={total}"
        )

    def start_packet_loop(self, backend: BlockDeviceBackend):
        """
        Placeholder for the real execution path:
        - spin up a minimal vamos machine
        - expose Exec/DOS vectors the handler needs
        - pump ACTION_* packets to the handler entry
        """
        loop = HandlerPacketLoop(backend)
        return loop.start()


def probe(driver: Path, image: Path, block_size: Optional[int], base: int, padding: int):
    backend = BlockDeviceBackend(image, block_size=block_size)
    backend.open()
    runtime = DriverRuntimeSkeleton(driver, base_addr=base, padding=padding)
    runtime.load()
    print("Driver :", runtime.summary())
    print("Backend:", backend.describe())
    print(f"Partitions: {backend.rdb.get_num_partitions() if backend.rdb else 0}")
    for part in backend.rdb.parts if backend.rdb else []:
        print(
            f"  #{part.num} {part.part_blk.drv_name} "
            f"dostype={hex(part.part_blk.dos_env.dos_type)} "
            f"blocks={part.get_num_blocks()}"
        )
    backend.close()


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Skeleton runtime for Amiga filesystem handlers (probe only)."
    )
    parser.add_argument("--driver", required=True, type=Path, help="Filesystem binary")
    parser.add_argument("--image", required=True, type=Path, help="Disk image file")
    parser.add_argument(
        "--block-size",
        type=int,
        help="Force a block size (defaults to auto/512).",
    )
    parser.add_argument(
        "--base", type=lambda x: int(x, 0), default=0x100000, help="Relocation base"
    )
    parser.add_argument(
        "--padding", type=int, default=0, help="Padding between segments when relocating"
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="Attempt to start the (stub) packet loop after probing.",
    )
    parser.add_argument(
        "--load-handler",
        action="store_true",
        help="Load the handler into a minimal vamos environment after probing.",
    )
    parser.add_argument(
        "--runner-info",
        action="store_true",
        help="Show handler runner setup (entry, ports, packet) without executing.",
    )
    parser.add_argument(
        "--start-handler",
        action="store_true",
        help="Start the handler task with FSSM (no packet loop yet).",
    )
    parser.add_argument(
        "--run-startup",
        action="store_true",
        help="Queue ACTION_STARTUP to the handler's port and report the reply.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable verbose debug output (signature scans, extra dumps).",
    )
    args = parser.parse_args(argv)
    backend, runtime = None, None
    try:
        backend = BlockDeviceBackend(args.image, block_size=args.block_size)
        backend.open()
        runtime = DriverRuntimeSkeleton(args.driver, base_addr=args.base, padding=args.padding)
        runtime.load()
        print("Driver :", runtime.summary())
        print("Backend:", backend.describe())
        print(f"Partitions: {backend.rdb.get_num_partitions() if backend.rdb else 0}")
        for part in backend.rdb.parts if backend.rdb else []:
            print(
                f"  #{part.num} {part.part_blk.drv_name} "
                f"dostype={hex(part.part_blk.dos_env.dos_type)} "
                f"blocks={part.get_num_blocks()}"
            )
        vh = None
        need_vamos = args.load_handler or args.runner_info or args.start_handler or args.run_startup
        if need_vamos:
            vh = VamosHandlerRuntime()
            vh.setup()
            vh.set_scsi_backend(backend)
            seg = vh.load_handler(args.driver)
            print(f"Loaded handler into vamos memory, seglist baddr=0x{seg:x}")
            exec_base_val = vh.alloc.get_mem().r32(4)
            print(f"ExecBase pointer: 0x{exec_base_val:x}")
            # Allocate DeviceNode/FSSM/DosEnvec using real seglist BPTR
            ba = BootstrapAllocator(vh, Path(args.image))
            boot = ba.alloc_all(handler_seglist_baddr=seg, handler_seglist_bptr=seg, handler_name="PFS0:")
            print(
                f"Bootstrap: dn@0x{boot['dn_addr']:x} fssm@0x{boot['fssm_addr']:x} env@0x{boot['env_addr']:x} "
                f"name_bstr@0x{boot['dn_name_addr']:x} dev_bstr@0x{boot['device_bstr']:x}"
            )
            if args.run_startup:
                from .amiga_structs import FileSysStartupMsgStruct  # type: ignore
                from amitools.vamos.astructs.access import AccessStruct  # type: ignore
                from amitools.vamos.libstructs.dos import DosPacketStruct, InfoDataStruct, FileInfoBlockStruct  # type: ignore
                from amitools.vamos.libstructs.dos import FileHandleStruct  # type: ignore
                from amitools.vamos.machine.regs import (  # type: ignore
                    REG_A0,
                    REG_A1,
                    REG_A2,
                    REG_A3,
                    REG_A4,
                    REG_A6,
                    REG_PC,
                )

                mem = vh.alloc.get_mem()
                fssm = AccessStruct(mem, FileSysStartupMsgStruct, boot["fssm_addr"])
                dev_bptr = fssm.r_s("fssm_Device")
                env_bptr = fssm.r_s("fssm_Environ")
                dev_addr = dev_bptr << 2
                env_addr = env_bptr << 2
                dev_bytes = mem.r_block(dev_addr, 16)
                print(
                    f"FSSM: unit={fssm.r_s('fssm_Unit')} dev_bptr=0x{dev_bptr:x} "
                    f"env_bptr=0x{env_bptr:x} dev_bytes={dev_bytes.hex()} env_addr=0x{env_addr:x}"
                )
                try:
                    env_words = [mem.r32(env_addr + i * 4) for i in range(20)]
                    print("Env (first 10 longs):", [hex(x) for x in env_words[:10]])
                    print("Env (next 10 longs):", [hex(x) for x in env_words[10:20]])
                    from .amiga_structs import DosEnvecStruct  # type: ignore

                    env_struct = AccessStruct(mem, DosEnvecStruct, env_addr)
                    print(
                        "Env fields:",
                        {
                            k: env_struct.r_s(k)
                            for k in [
                                "de_TableSize",
                                "de_SizeBlock",
                                "de_Surfaces",
                                "de_SectorPerBlock",
                                "de_BlocksPerTrack",
                                "de_LowCyl",
                                "de_HighCyl",
                                "de_NumBuffers",
                                "de_MaxTransfer",
                                "de_Mask",
                                "de_BootPri",
                                "de_DosType",
                            ]
                        },
                    )
                except Exception as e:
                    print("Env dump failed:", e)
                print(
                    f"FSSM raw: {mem.r_block(boot['fssm_addr'], FileSysStartupMsgStruct.get_size()).hex()}"
                )
                # buffers for post-startup requests
                info_buf = vh.alloc.alloc_memory(InfoDataStruct.get_size(), label="InfoData")
                read_buf = vh.alloc.alloc_memory(512, label="ReadBuf")
                entry_addr = vh.slm.seg_loader.infos[seg].seglist.get_segment().get_addr()
                print(f"Entry bytes: {mem.r_block(entry_addr, 16).hex()}")
                launcher = HandlerLauncher(vh, boot, entry_addr)
                state = launcher.launch_with_startup()
                print(f"Stub PC=0x{state.pc:x} initial SP=0x{state.sp:x}")
                print(f"Stub bytes: {mem.r_block(state.pc, 16).hex()}")
                # dump startup message/packet prior to run
                from amitools.vamos.libstructs.dos import MessageStruct  # type: ignore
                msg = AccessStruct(mem, MessageStruct, state.msg_addr)
                pkt = AccessStruct(mem, DosPacketStruct, state.stdpkt_addr)
                print(
                    f"Msg before run: ln_Name=0x{msg.r_s('mn_Node.ln_Name'):x} reply=0x{msg.r_s('mn_ReplyPort'):x} len={msg.r_s('mn_Length')}"
                )
                print(
                    f"Pkt before run: type={pkt.r_s('dp_Type')} arg1=0x{pkt.r_s('dp_Arg1'):x} arg2=0x{pkt.r_s('dp_Arg2'):x} arg3=0x{pkt.r_s('dp_Arg3'):x} port=0x{pkt.r_s('dp_Port'):x} link=0x{pkt.r_s('dp_Link'):x}"
                )
                # run startup once
                startup_state = launcher.run_burst(state, max_cycles=5000000)
                mem = vh.alloc.get_mem()
                print(
                    f"Process=0x{state.process_addr:x} port=0x{state.port_addr:x} "
                    f"msg=0x{state.msg_addr:x} pkt=0x{state.stdpkt_addr:x}"
                )
                print(
                    f"Handler PC after startup burst: 0x{state.pc:x} SP=0x{state.sp:x} cycles={getattr(state.run_state, 'cycles', None)}"
                )
                # peek startup packet fields
                spkt = AccessStruct(mem, DosPacketStruct, state.stdpkt_addr)
                print(
                    f"Startup packet: type={spkt.r_s('dp_Type')} arg1=0x{spkt.r_s('dp_Arg1'):x} "
                    f"arg2=0x{spkt.r_s('dp_Arg2'):x} arg3=0x{spkt.r_s('dp_Arg3'):x}"
                )
                pending = vh.slm.exec_impl.port_mgr.has_msg(state.reply_port_addr)
                replies = launcher.poll_replies(state.reply_port_addr)
                print(
                    f"Queued packets -> port 0x{state.reply_port_addr:x}, pending_before={pending} replies={len(replies)}"
                )
                print(
                    f"Port queue after run: has_msg={vh.slm.exec_impl.port_mgr.has_msg(state.reply_port_addr)}"
                )
                spkt_after = AccessStruct(mem, DosPacketStruct, state.stdpkt_addr)
                print(
                    f"Packet after run: type={spkt_after.r_s('dp_Type')} arg1=0x{spkt_after.r_s('dp_Arg1'):x} "
                    f"arg2=0x{spkt_after.r_s('dp_Arg2'):x} arg3=0x{spkt_after.r_s('dp_Arg3'):x} "
                    f"res1={spkt_after.r_s('dp_Res1')} res2={spkt_after.r_s('dp_Res2')}"
                )
                for idx, (_, pkt_addr, res1, res2) in enumerate(replies):
                    print(f"  reply[{idx}] packet=0x{pkt_addr:x} res1={res1} res2={res2}")
                print("InfoData first 32 bytes:", mem.r_block(info_buf.addr, 32).hex())
                print("ReadBuf first 32 bytes:", mem.r_block(read_buf.addr, 32).hex())
                if state.run_state is not None:
                    if isinstance(state.run_state, dict):
                        print(f"RunState regs={state.run_state}")
                    else:
                        print(f"RunState done={state.run_state.done} pc=0x{state.run_state.pc:x} error={state.run_state.error}")
                # If startup returns success, issue DI/READ post-startup; optionally force success to probe IO
                force_startup = False
                if force_startup or spkt_after.r_s("dp_Res1") != 0:
                    if spkt_after.r_s("dp_Res1") == 0 and force_startup:
                        spkt_after.w_s("dp_Res1", -1)
                        print("Forcing startup dp_Res1 to -1 to probe IO path")
                    print(
                        f"Sending DI (arg1=0x{info_buf.addr:x}) and READ (buf=0x{read_buf.addr:x}, off=0 len=512)"
                    )
                    launcher.send_disk_info(state, info_buf.addr)
                    launcher.send_read(state, read_buf.addr, 0, 512)
                    burst_state = launcher.run_burst(state, max_cycles=2000000)
                    print("Burst run_state:", burst_state)
                    di_replies = launcher.poll_replies(state.reply_port_addr)
                    rd_replies = launcher.poll_replies(state.reply_port_addr)
                    print(f"Post-startup DI replies: {di_replies}")
                    print(f"Post-startup READ replies: {rd_replies}")
                    print("InfoData after DI:", mem.r_block(info_buf.addr, 32).hex())
                    # Try a locate/examine/findinput/read sequence
                    fib_mem = vh.alloc.alloc_struct(FileInfoBlockStruct, label="FIB")
                    mem.w_block(fib_mem.addr, b"\x00" * FileInfoBlockStruct.get_size())
                    _, empty_bptr = launcher.alloc_bstr("", "root_name")
                    launcher.send_locate(state, 0, empty_bptr)
                    loc_state = launcher.run_burst(state, max_cycles=1000000)
                    if getattr(loc_state, "error", None):
                        print("Locate run error:", loc_state)
                        return
                    loc_replies = launcher.poll_replies(state.reply_port_addr)
                    print(f"Locate(root) replies: {loc_replies}")
                    lock_bptr = loc_replies[-1][2] if loc_replies else 0
                    chosen_name = None
                    if lock_bptr:
                        lock_addr = lock_bptr << 2
                        lock_bytes = mem.r_block(lock_addr, 20)
                        print(
                            f"Lock@0x{lock_addr:x} raw={lock_bytes.hex()} fl_Key={int.from_bytes(lock_bytes[4:8],'big',signed=True)} fl_Task=0x{int.from_bytes(lock_bytes[12:16],'big'):x}"
                        )
                        launcher.send_examine(state, lock_bptr, fib_mem.addr)
                        ex_state = launcher.run_burst(state, max_cycles=1000000)
                        if getattr(ex_state, "error", None):
                            print("Examine run error:", ex_state)
                            return
                        ex_replies = launcher.poll_replies(state.reply_port_addr)
                        print(f"Examine replies: {ex_replies}")
                        fib_bytes = mem.r_block(fib_mem.addr, FileInfoBlockStruct.get_size())
                        fname_len = fib_bytes[8]
                        fname_bytes = fib_bytes[9 : 9 + fname_len]
                        fname = bytes(fname_bytes).decode("latin-1", errors="ignore")
                        dir_type = int.from_bytes(fib_bytes[4:8], "big", signed=True)
                        print(
                            f"Fib[0] len={fname_len} name='{fname}' dir_type={dir_type} raw64={fib_bytes[:64].hex()}"
                        )
                        # iterate over a few entries to find a file
                        for i in range(1, 5):
                            launcher.send_examine_next(state, lock_bptr, fib_mem.addr)
                            nex_state = launcher.run_burst(state, max_cycles=800000)
                            if getattr(nex_state, "error", None):
                                print(f"ExamineNext run error at iter {i}:", nex_state)
                                break
                            _ = launcher.poll_replies(state.reply_port_addr)
                            fib_bytes = mem.r_block(fib_mem.addr, FileInfoBlockStruct.get_size())
                            fname_len = fib_bytes[8]
                            fname_bytes = fib_bytes[9 : 9 + fname_len]
                            fname = bytes(fname_bytes).decode("latin-1", errors="ignore")
                            dir_type = int.from_bytes(fib_bytes[4:8], "big", signed=True)
                            print(
                                f"Fib[{i}] len={fname_len} name='{fname}' dir_type={dir_type} raw32={fib_bytes[:32].hex()}"
                            )
                            if dir_type < 0 and fname:
                                chosen_name = fname
                                break
                    if chosen_name:
                        print(f"Opening file '{chosen_name}' from root...")
                        _, name_bptr = launcher.alloc_bstr(chosen_name, "find_name")
                        fh_addr, _, _ = launcher.send_findinput(state, name_bptr, lock_bptr)
                        fi_state = launcher.run_burst(state, max_cycles=1500000)
                        if getattr(fi_state, "error", None):
                            print("FindInput run error:", fi_state)
                        fi_replies = launcher.poll_replies(state.reply_port_addr)
                        print(f"FindInput replies: {fi_replies}")
                        if fi_replies and fi_replies[-1][2] != 0:
                            fh = AccessStruct(mem, FileHandleStruct, fh_addr)
                            fileentry_ptr = fh.r_s("fh_Args")
                            print(f"FileHandle args ptr=0x{fileentry_ptr:x}")
                            launcher.send_read_handle(state, fh_addr, read_buf.addr, 512)
                            rh_state = launcher.run_burst(state, max_cycles=2000000)
                            if getattr(rh_state, "error", None):
                                print("READ(handle) run error:", rh_state)
                            fr_replies = launcher.poll_replies(state.reply_port_addr)
                            print(f"READ(handle) replies: {fr_replies}")
                            print("ReadBuf after FH READ:", mem.r_block(read_buf.addr, 32).hex())
                print("ReadBuf after READ:", mem.r_block(read_buf.addr, 32).hex())
                # Inspect fssm/device string after the run
                fssm_after = AccessStruct(vh.alloc.get_mem(), FileSysStartupMsgStruct, boot["fssm_addr"])
                dev_bptr_after = fssm_after.r_s("fssm_Device")
                dev_after = vh.alloc.get_mem().r_block(dev_bptr_after << 2, 16)
                print(
                    f"FSSM after run: dev_bptr=0x{dev_bptr_after:x} dev_bytes={dev_after.hex()}"
                )
                # Inspect any large alloc capture from exec
                exec_impl = vh.slm.exec_impl
                la = getattr(exec_impl, "_last_large_alloc", None)
                if la:
                    print(
                        "Last large AllocVec:",
                        {
                            **{k: (v.hex() if hasattr(v, 'hex') else v) for k, v in la.items()},
                        },
                    )
                if args.debug:
                    # Scan memory for pfs3 debug signature 'PFS3' written by OpenDiskDevice
                    try:
                        blob = mem.r_block(0, 0x800000)
                        for sig_label, sig in (
                            ("PFSB", b"PFSB"),
                            ("PFS3", b"PFS3"),
                            ("AVEC", b"AVEC"),
                            ("MNTN", b"MNTN"),
                            ("MKLE", b"MKLE"),
                            ("EXAM", b"EXAM"),
                            ("LRUL", b"LRUL"),
                            ("LRUR", b"LRUR"),
                            ("LRUI", b"LRUI"),
                            ("DSIO", b"DSIO"),
                            ("RRIN", b"RRIN"),
                            ("RQQ1", b"RQQ1"),
                            ("RQQ2", b"RQQ2"),
                            ("GRRD", b"GRRD"),
                            ("RRCA", b"RRCA"),
                            ("APOL", b"APOL"),
                            ("FPOL", b"FPOL"),
                            ("INIT", b"INIT"),
                            ("GCR0", b"GCR0"),
                            ("BOOT", b"BOOT"),
                            ("CBSZ", b"CBSZ"),
                            ("PART", b"PART"),
                        ):
                            hits = []
                            idx = blob.find(sig)
                            while idx != -1 and len(hits) < 10:
                                if sig_label == "PFSB":
                                    vals = [
                                        int.from_bytes(blob[idx + 4 + off : idx + 8 + off], "big")
                                        for off in (0, 4, 8, 12, 16, 20)
                                    ]
                                    hits.append(
                                        {
                                            "addr": idx,
                                            "dp_arg1": vals[0],
                                            "dp_arg2": vals[1],
                                            "dp_arg3": vals[2],
                                            "fssm_ptr": vals[3],
                                            "dev_bptr": vals[4],
                                            "dev_aptr": vals[5],
                                        }
                                    )
                                elif sig_label == "AVEC":
                                    offsets = (
                                        0,
                                        4,
                                        8,
                                        12,
                                        16,
                                        20,
                                        24,
                                        28,
                                        32,
                                        36,
                                        40,
                                        44,
                                        48,
                                        52,
                                        56,
                                        60,
                                        64,
                                        68,
                                    )
                                    vals = [
                                        int.from_bytes(blob[idx + 4 + off : idx + 8 + off], "big")
                                        for off in offsets
                                    ]
                                    hits.append(
                                        {
                                            "addr": idx,
                                            "size": vals[0],
                                            "flags": vals[1],
                                            "ret": vals[2],
                                            "gptr": vals[3],
                                            "sp": vals[4],
                                            "stk": vals[5:9],
                                            "regs": vals[9:],
                                        }
                                    )
                                elif sig_label == "MNTN":
                                    vals = [
                                        int.from_bytes(blob[idx + 4 + off : idx + 8 + off], "big")
                                        for off in (0, 4, 8, 12, 16, 20, 24)
                                    ]
                                    hits.append(
                                        {
                                            "addr": idx,
                                            "ptr": vals[0],
                                            "len": vals[1],
                                            "b": vals[2:6],
                                        }
                                    )
                                elif sig_label == "MKLE":
                                    vals = [
                                        int.from_bytes(blob[idx + 4 + off : idx + 8 + off], "big")
                                        for off in (0, 4, 8, 12, 16, 20)
                                    ]
                                    hits.append(
                                        {
                                            "addr": idx,
                                            "type": vals[0],
                                            "anodenr": vals[1],
                                            "fl_key": vals[2],
                                            "fl_task": vals[3],
                                            "vol_devlist": vals[4],
                                        }
                                    )
                                elif sig_label == "EXAM":
                                    vals = [
                                        int.from_bytes(blob[idx + 4 + off : idx + 8 + off], "big", signed=True)
                                        for off in (0, 4, 8, 12, 16, 20, 24)
                                    ]
                                    fib = blob[idx + 28 : idx + 28 + 64]
                                    hits.append(
                                        {
                                            "addr": idx,
                                            "arg1": vals[0],
                                            "arg2": vals[1],
                                            "res1": vals[2],
                                            "res2": vals[3],
                                            "lockptr": vals[4],
                                            "fl_key": vals[5],
                                            "fib": fib.hex(),
                                            "fib_raw": fib,
                                        }
                                    )
                                elif sig_label == "LRUL":
                                    vals = [
                                        int.from_bytes(blob[idx + 4 + off : idx + 8 + off], "big")
                                        for off in (0, 4, 8, 12, 16, 20)
                                    ]
                                    hits.append(
                                        {
                                            "addr": idx,
                                            "poolsize": vals[0],
                                            "res_blksize": vals[1],
                                            "bufmem": vals[2],
                                            "lru_ptr": vals[3],
                                            "retry": vals[5],
                                        }
                                    )
                                elif sig_label == "LRUR":
                                    vals = [
                                        int.from_bytes(blob[idx + 4 + off : idx + 8 + off], "big")
                                        for off in (0, 4, 8, 12)
                                    ]
                                    hits.append(
                                        {
                                            "addr": idx,
                                            "rb_reserved": vals[0],
                                            "rb_cluster": vals[1],
                                            "env_sizeblock": vals[2],
                                            "env_num_buffers": vals[3],
                                        }
                                    )
                                elif sig_label == "LRUI":
                                    vals = [
                                        int.from_bytes(blob[idx + 4 + off : idx + 8 + off], "big")
                                        for off in (0, 4, 8, 12, 16)
                                    ]
                                    hits.append(
                                        {
                                            "addr": idx,
                                            "numbuf": vals[0],
                                            "poolsize": vals[1],
                                            "res_blksize": vals[2],
                                            "bufmem": vals[3],
                                            "lru_ptr": vals[4],
                                        }
                                    )
                                elif sig_label == "DSIO":
                                    def u32(off: int) -> int:
                                        return int.from_bytes(
                                            blob[idx + off : idx + off + 4], "big"
                                        )
    
                                    call = u32(4)
                                    entries = []
                                    base_off = 8
                                    slot_size = 6 * 4
                                    for slot in range(4):
                                        o = base_off + slot * slot_size
                                        entries.append(
                                            {
                                                "slot": u32(o),
                                                "orig_blk": u32(o + 4),
                                                "added_blk": u32(o + 8),
                                                "blk": u32(o + 12),
                                                "xfer": u32(o + 16),
                                                "blocks": u32(o + 20),
                                            }
                                        )
                                    hits.append({"addr": idx, "call": call, "entries": entries})
                                elif sig_label == "RRIN":
                                    def u32(off: int) -> int:
                                        return int.from_bytes(
                                            blob[idx + off : idx + off + 4], "big"
                                        )
    
                                    hits.append(
                                        {
                                            "addr": idx,
                                            "count": u32(4),
                                            "d0": u32(8),
                                            "d1": u32(12),
                                            "d2": u32(16),
                                            "a0": u32(20),
                                            "a1": u32(24),
                                        }
                                    )
                                elif sig_label in ("RQQ1", "RQQ2"):
                                    vals = [
                                        int.from_bytes(blob[idx + 4 + off : idx + 8 + off], "big")
                                        for off in (0, 4, 8, 12)
                                    ]
                                    hits.append(
                                        {
                                            "addr": idx,
                                            "iter": vals[1],
                                            "blk": vals[2],
                                            "blocks": vals[3],
                                        }
                                    )
                                elif sig_label == "GRRD":
                                    vals = [
                                        int.from_bytes(blob[idx + 4 + off : idx + 8 + off], "big")
                                        for off in (0, 4, 8, 12)
                                    ]
                                    hits.append(
                                        {
                                            "addr": idx,
                                            "iter": vals[1],
                                            "boot_blk": vals[2],
                                            "root_blk": vals[3],
                                        }
                                    )
                                elif sig_label == "APOL":
                                    vals = [
                                        int.from_bytes(blob[idx + 4 + off : idx + 8 + off], "big")
                                        for off in (0, 4, 8, 12, 16)
                                    ]
                                    hits.append(
                                        {
                                            "addr": idx,
                                            "pc": vals[1],
                                            "pool": vals[2],
                                            "size": vals[3],
                                            "ret": vals[4],
                                        }
                                    )
                                elif sig_label == "FPOL":
                                    vals = [
                                        int.from_bytes(blob[idx + 4 + off : idx + 8 + off], "big")
                                        for off in (0, 4, 8, 12, 16)
                                    ]
                                    hits.append(
                                        {
                                            "addr": idx,
                                            "pc": vals[1],
                                            "pool": vals[2],
                                            "ptr": vals[3],
                                            "size": vals[4],
                                        }
                                    )
                                elif sig_label == "RRCA":
                                    vals = [
                                        int.from_bytes(blob[idx + 4 + off : idx + 8 + off], "big")
                                        for off in (0, 4, 8, 12, 16)
                                    ]
                                    hits.append(
                                        {
                                            "addr": idx,
                                            "call": vals[1],
                                            "d0": vals[2],
                                            "d1": vals[3],
                                            "d3": vals[4],
                                        }
                                    )
                                elif sig_label == "INIT":
                                    vals = [
                                        int.from_bytes(blob[idx + 4 + off : idx + 8 + off], "big")
                                        for off in (0, 4, 8)
                                    ]
                                    hits.append(
                                        {
                                            "addr": idx,
                                            "step": vals[0],
                                            "ret": vals[1],
                                            "extra": vals[2],
                                        }
                                    )
                                elif sig_label == "GCR0":
                                    vals = [
                                        int.from_bytes(blob[idx + 4 + off : idx + 8 + off], "big")
                                        for off in (0, 4, 8, 12, 16)
                                    ]
                                    hits.append(
                                        {
                                            "addr": idx,
                                            "step": vals[0],
                                            "err": vals[1],
                                            "req": vals[2],
                                            "extra": vals[3],
                                            "extra2": vals[4],
                                        }
                                    )
                                elif sig_label == "BOOT":
                                    vals = [
                                        int.from_bytes(blob[idx + 4 + off : idx + 8 + off], "big")
                                        for off in (0, 4, 8, 12)
                                    ]
                                    hits.append(
                                        {
                                            "addr": idx,
                                            "iter": vals[0],
                                            "last_signal": vals[1],
                                            "last_nv_call": vals[2],
                                            "pad": vals[3],
                                        }
                                    )
                                elif sig_label == "CBSZ":
                                    vals = [
                                        int.from_bytes(blob[idx + 4 + off : idx + 8 + off], "big")
                                        for off in (0, 4, 8, 12, 16, 20)
                                    ]
                                    hits.append(
                                        {
                                            "addr": idx,
                                            "in_spb": vals[0],
                                            "in_blocksize": vals[1],
                                            "env_sizeblock": vals[2],
                                            "env_spb": vals[3],
                                            "result": vals[5],
                                        }
                                    )
                                elif sig_label == "PART":
                                    vals = [
                                        int.from_bytes(blob[idx + 4 + off : idx + 8 + off], "big")
                                        for off in (0, 4, 8, 12, 16)
                                    ]
                                    hits.append(
                                        {
                                            "addr": idx,
                                            "lowcyl": vals[0],
                                            "cylsecs": vals[1],
                                            "first": vals[2],
                                            "last": vals[3],
                                        }
                                    )
                                else:
                                    vals = [
                                        int.from_bytes(blob[idx + 4 + off : idx + 8 + off], "big")
                                        for off in (0, 4, 8)
                                    ]
                                    hits.append(
                                        {
                                            "addr": idx,
                                            "startup": vals[0],
                                            "dev_bptr": vals[1],
                                            "dev_aptr": vals[2],
                                        }
                                    )
                                idx = blob.find(sig, idx + 1)
                            if hits:
                                if sig_label == "PFSB":
                                    formatted = [
                                        f"0x{h['addr']:x}: arg1=0x{h['dp_arg1']:x} arg2=0x{h['dp_arg2']:x} arg3=0x{h['dp_arg3']:x} "
                                        f"fssm=0x{h['fssm_ptr']:x} dev_bptr=0x{h['dev_bptr']:x} dev_aptr=0x{h['dev_aptr']:x}"
                                        for h in hits
                                    ]
                                elif sig_label == "AVEC":
                                    formatted = [
                                        f"0x{h['addr']:x}: size=0x{h['size']:x} flags=0x{h['flags']:x} ret=0x{h['ret']:x} g=0x{h['gptr']:x} sp=0x{h['sp']:x} stk={[hex(x) for x in h['stk']]} regs={[hex(x) for x in h['regs']]}"
                                        for h in hits
                                    ]
                                elif sig_label == "MNTN":
                                    formatted = [
                                        f"0x{h['addr']:x}: ptr=0x{h['ptr']:x} len=0x{h['len']:x} bytes={[hex(x) for x in h['b']]}"
                                        for h in hits
                                    ]
                                elif sig_label == "MKLE":
                                    formatted = [
                                        f"0x{h['addr']:x}: type=0x{h['type']:x} anode=0x{h['anodenr']:x} fl_Key=0x{h['fl_key']:x} fl_Task=0x{h['fl_task']:x} vol_devlist=0x{h['vol_devlist']:x}"
                                        for h in hits
                                    ]
                                elif sig_label == "EXAM":
                                    formatted = [
                                        f"0x{h['addr']:x}: arg1=0x{h['arg1']:x} arg2=0x{h['arg2']:x} res1={h['res1']} res2={h['res2']} lockptr=0x{h['lockptr']:x} fl_Key=0x{h['fl_key']:x} fib={h['fib']}"
                                        for h in hits
                                    ]
                                elif sig_label == "LRUL":
                                    formatted = [
                                        f"0x{h['addr']:x}: poolsize=0x{h['poolsize']:x} res_blksize=0x{h['res_blksize']:x} bufmem=0x{h['bufmem']:x} lru_ptr=0x{h['lru_ptr']:x} retry=0x{h['retry']:x}"
                                        for h in hits
                                    ]
                                elif sig_label == "LRUR":
                                    formatted = [
                                        f"0x{h['addr']:x}: rb_reserved=0x{h['rb_reserved']:x} rb_cluster=0x{h['rb_cluster']:x} env_sizeblock=0x{h['env_sizeblock']:x} env_num_buffers=0x{h['env_num_buffers']:x}"
                                        for h in hits
                                    ]
                                elif sig_label == "LRUI":
                                    formatted = [
                                        f"0x{h['addr']:x}: numbuf=0x{h['numbuf']:x} poolsize=0x{h['poolsize']:x} res_blksize=0x{h['res_blksize']:x} bufmem=0x{h['bufmem']:x} lru_ptr=0x{h['lru_ptr']:x}"
                                        for h in hits
                                    ]
                                elif sig_label == "DSIO":
                                    formatted = []
                                    for h in hits:
                                        ent_str = "; ".join(
                                            f"slot=0x{e['slot']:x} orig=0x{e['orig_blk']:x} added=0x{e['added_blk']:x} blk=0x{e['blk']:x} xfer=0x{e['xfer']:x} blocks=0x{e['blocks']:x}"
                                            for e in h["entries"]
                                            if any(e.values())
                                        )
                                        formatted.append(
                                            f"0x{h['addr']:x}: call=0x{h['call']:x} {ent_str}"
                                        )
                                elif sig_label == "RRIN":
                                    formatted = [
                                        f"0x{h['addr']:x}: count=0x{h['count']:x} d0=0x{h['d0']:x} d1=0x{h['d1']:x} d2=0x{h['d2']:x} a0=0x{h['a0']:x} a1=0x{h['a1']:x}"
                                        for h in hits
                                    ]
                                elif sig_label in ("RQQ1", "RQQ2"):
                                    formatted = [
                                        f"0x{h['addr']:x}: iter=0x{h['iter']:x} blk=0x{h['blk']:x} blocks=0x{h['blocks']:x}"
                                        for h in hits
                                    ]
                                elif sig_label == "GRRD":
                                    formatted = [
                                        f"0x{h['addr']:x}: iter=0x{h['iter']:x} boot=0x{h['boot_blk']:x} root=0x{h['root_blk']:x}"
                                        for h in hits
                                    ]
                                elif sig_label == "RRCA":
                                    formatted = [
                                        f"0x{h['addr']:x}: call=0x{h['call']:x} d0=0x{h['d0']:x} d1=0x{h['d1']:x} d3=0x{h['d3']:x}"
                                        for h in hits
                                    ]
                                elif sig_label == "APOL":
                                    formatted = [
                                        f"0x{h['addr']:x}: pc=0x{h['pc']:x} pool=0x{h['pool']:x} size=0x{h['size']:x} ret=0x{h['ret']:x}"
                                        for h in hits
                                    ]
                                elif sig_label == "FPOL":
                                    formatted = [
                                        f"0x{h['addr']:x}: pc=0x{h['pc']:x} pool=0x{h['pool']:x} ptr=0x{h['ptr']:x} size=0x{h['size']:x}"
                                        for h in hits
                                    ]
                                elif sig_label == "INIT":
                                    formatted = [
                                        f"0x{h['addr']:x}: step=0x{h['step']:x} ret=0x{h['ret']:x} extra=0x{h['extra']:x}"
                                        for h in hits
                                    ]
                                elif sig_label == "GCR0":
                                    formatted = [
                                        f"0x{h['addr']:x}: step=0x{h['step']:x} err=0x{h['err']:x} req=0x{h['req']:x} extra=0x{h['extra']:x} extra2=0x{h['extra2']:x}"
                                        for h in hits
                                    ]
                                elif sig_label == "BOOT":
                                    formatted = [
                                        f"0x{h['addr']:x}: iter=0x{h['iter']:x} last_signal=0x{h['last_signal']:x} last_nv_call=0x{h['last_nv_call']:x}"
                                        for h in hits
                                    ]
                                elif sig_label == "CBSZ":
                                    formatted = [
                                        f"0x{h['addr']:x}: in_spb=0x{h['in_spb']:x} in_blocksize=0x{h['in_blocksize']:x} env_sizeblock=0x{h['env_sizeblock']:x} env_spb=0x{h['env_spb']:x} result=0x{h['result']:x}"
                                        for h in hits
                                    ]
                                elif sig_label == "PART":
                                    formatted = [
                                        f"0x{h['addr']:x}: lowcyl={h['lowcyl']} cylsecs={h['cylsecs']} first={h['first']} last={h['last']}"
                                        for h in hits
                                    ]
                                else:
                                    formatted = [
                                        f"0x{h['addr']:x}: startup=0x{h['startup']:x} dev_bptr=0x{h['dev_bptr']:x} dev_aptr=0x{h['dev_aptr']:x}"
                                        for h in hits
                                    ]
                                print(f"Debug signature hits ({sig_label}):", formatted)
                            else:
                                print(f"Debug signature hits ({sig_label}): none")
                    except Exception as e:
                        print("Debug signature scan failed:", e)
                if state.run_state and not isinstance(state.run_state, dict) and getattr(state.run_state, "error", None):
                    cpu = vh.machine.cpu
                    mem = vh.alloc.get_mem()
                    print(
                        "Handler run error:",
                        state.run_state.error,
                        f"PC=0x{cpu.r_reg(REG_PC):x} A0=0x{cpu.r_reg(REG_A0):x} "
                        f"A1=0x{cpu.r_reg(REG_A1):x} A2=0x{cpu.r_reg(REG_A2):x} "
                        f"A3=0x{cpu.r_reg(REG_A3):x} A4=0x{cpu.r_reg(REG_A4):x} "
                        f"A6=0x{cpu.r_reg(REG_A6):x}",
                    )
                    for lbl, addr in [
                        ("name_ptr", cpu.r_reg(REG_A0)),
                        ("a1", cpu.r_reg(REG_A1)),
                        ("a3", cpu.r_reg(REG_A3)),
                    ]:
                        data = mem.r_block(addr, 32)
                        print(f"  {lbl}@0x{addr:x}: {data.hex()}")
                    # quick scan for our BPTR values to see where handler stored them
                    try:
                        blob = mem.r_block(0, 0x100000)
                        for label, val in [
                            ("fssm_bptr", boot["fssm_addr"] >> 2),
                            ("dev_bptr", dev_bptr_after),
                        ]:
                            pat = val.to_bytes(4, "big", signed=False)
                            hits = [
                                i for i in range(0, len(blob), 4) if blob[i : i + 4] == pat
                            ][:5]
                            print(f"  hits for {label} (0x{val:x}): {[hex(h) for h in hits]}")
                    except Exception as e:
                        print("  mem scan failed:", e)
        if args.run:
            print("Starting stub packet loop (not yet wired to handler execution)...")
            runtime.start_packet_loop(backend)
    finally:
        if backend:
            backend.close()
        if vh:
            vh.shutdown()


if __name__ == "__main__":
    main()
