"""
Helper to allocate minimal AmigaDOS structs (DosEnvec, FileSysStartupMsg,
DeviceNode) in vamos memory using partition info.
"""

from pathlib import Path
from typing import Optional

from amitools.vamos.astructs.access import AccessStruct  # type: ignore
from amitools.fs.blkdev.RawBlockDevice import RawBlockDevice  # type: ignore

from .amiga_structs import DosEnvecStruct, FileSysStartupMsgStruct, DeviceNodeStruct
from amitools.vamos.libstructs.exec_ import MsgPortStruct, ListStruct, NodeType  # type: ignore
from .rdb_inspect import detect_adf, ADFInfo


class SyntheticDosEnv:
    """Synthetic DosEnvec-like object for ADF (floppy) images."""
    def __init__(self, adf_info: ADFInfo):
        self.size = 16  # de_TableSize
        self.block_size = 128  # de_SizeBlock in longwords (512 bytes / 4)
        self.sec_org = 0
        self.surfaces = adf_info.heads
        self.sec_per_blk = 1
        self.blk_per_trk = adf_info.sectors_per_track
        self.reserved = 2  # Boot blocks
        self.pre_alloc = 0
        self.interleave = 0
        self.low_cyl = 0
        self.high_cyl = adf_info.cylinders - 1
        self.num_buffer = 5
        self.buf_mem_type = 0
        self.max_transfer = 0x7FFFFFFF
        self.mask = 0xFFFFFFFF
        self.boot_pri = 0
        self.dos_type = adf_info.dos_type
        self.baud = 0
        self.control = 0
        self.boot_blocks = 2


class SyntheticPartition:
    """Synthetic partition info for ADF images."""
    def __init__(self, adf_info: ADFInfo):
        self.num = 0
        self.adf_info = adf_info

    def get_num_blocks(self):
        return self.adf_info.total_blocks


class BootstrapAllocator:
    def __init__(self, vh, image_path: Path, block_size=512, partition=None, adf_info: Optional[ADFInfo] = None):
        self.vh = vh
        self.alloc = vh.alloc
        self.mem = vh.alloc.get_mem()
        self.image_path = image_path
        self.block_size = block_size
        self.partition = partition  # name, index, or None for first
        self.adf_info = adf_info  # Pre-detected ADF info, if any

    def _read_partition_env(self):
        from .rdb_inspect import open_rdisk

        blk, rd, mbr_ctx = open_rdisk(self.image_path, block_size=self.block_size)
        if self.partition is None:
            part = rd.get_partition(0)
        else:
            part = rd.find_partition_by_string(str(self.partition))
            if part is None:
                rd.close()
                blk.close()
                raise ValueError(f"Partition '{self.partition}' not found")
        de = part.part_blk.dos_env
        return de, blk, rd, part

    def _read_adf_env(self):
        """Create synthetic partition info for ADF images."""
        blk = RawBlockDevice(str(self.image_path), read_only=True, block_bytes=self.block_size)
        blk.open()
        de = SyntheticDosEnv(self.adf_info)
        part = SyntheticPartition(self.adf_info)
        return de, blk, None, part  # rd is None for ADF

    def alloc_all(self, handler_seglist_baddr, handler_seglist_bptr, handler_name="PFS0:"):
        # Use ADF synthetic partition if adf_info is set, otherwise read from RDB
        if self.adf_info is not None:
            de, blk, rd, part = self._read_adf_env()
        else:
            de, blk, rd, part = self._read_partition_env()
        # DosEnvec
        env_mem = self.alloc.alloc_memory(DosEnvecStruct.get_size(), label="DosEnvec")
        env = AccessStruct(self.mem, DosEnvecStruct, env_mem.addr)
        env.w_s("de_TableSize", de.size if getattr(de, "size", 0) else 16)
        env.w_s("de_SizeBlock", de.block_size)
        env.w_s("de_SecOrg", de.sec_org)
        env.w_s("de_Surfaces", de.surfaces)
        env.w_s("de_SectorPerBlock", de.sec_per_blk)
        env.w_s("de_BlocksPerTrack", de.blk_per_trk)
        env.w_s("de_Reserved", de.reserved)
        env.w_s("de_PreAlloc", de.pre_alloc)
        env.w_s("de_Interleave", de.interleave)
        env.w_s("de_LowCyl", de.low_cyl)
        env.w_s("de_HighCyl", de.high_cyl)
        env.w_s("de_NumBuffers", de.num_buffer)
        env.w_s("de_BufMemType", de.buf_mem_type)
        env.w_s("de_MaxTransfer", de.max_transfer)
        # Relax mask: allow any address to avoid handler memorymask complaints
        env.w_s("de_Mask", 0xFFFFFFFF)
        env.w_s("de_BootPri", de.boot_pri)
        env.w_s("de_DosType", de.dos_type)
        env.w_s("de_Baud", de.baud)
        env.w_s("de_Control", de.control)
        env.w_s("de_BootBlocks", de.boot_blocks)

        # FSSM
        fssm_mem = self.alloc.alloc_memory(FileSysStartupMsgStruct.get_size(), label="FSSM")
        fssm = AccessStruct(self.mem, FileSysStartupMsgStruct, fssm_mem.addr)
        dev_bstr = b"\x0b" + b"scsi.device"
        dev_mem = self.alloc.alloc_memory(len(dev_bstr), label="dev_bstr")
        self.mem.w_block(dev_mem.addr, dev_bstr)
        fssm.w_s("fssm_Unit", 0)
        fssm.w_s("fssm_Device", dev_mem.addr >> 2)
        fssm.w_s("fssm_Environ", env_mem.addr >> 2)
        fssm.w_s("fssm_Flags", 0)

        # DeviceNode
        dn_mem = self.alloc.alloc_memory(DeviceNodeStruct.get_size(), label="DeviceNode")
        dn = AccessStruct(self.mem, DeviceNodeStruct, dn_mem.addr)
        name_bstr = bytes([len(handler_name)]) + handler_name.encode("ascii")
        name_mem = self.alloc.alloc_memory(len(name_bstr), label="dn_name")
        self.mem.w_block(name_mem.addr, name_bstr)
        dn.w_s("dn_Next", 0)
        dn.w_s("dn_Type", 0)
        dn.w_s("dn_Task", 0)  # to be set by caller (APTR)
        dn.w_s("dn_Lock", 0)
        dn.w_s("dn_Handler", handler_seglist_bptr)
        dn.w_s("dn_StackSize", 0)
        dn.w_s("dn_Priority", 0)
        dn.w_s("dn_Startup", fssm_mem.addr >> 2)
        dn.w_s("dn_SegList", handler_seglist_bptr)
        dn.w_s("dn_GlobalVec", -1)
        dn.w_s("dn_Name", name_mem.addr >> 2)

        return {
            "env_addr": env_mem.addr,
            "fssm_addr": fssm_mem.addr,
            "device_bstr": dev_mem.addr,
            "dn_addr": dn_mem.addr,
            "dn_name_addr": name_mem.addr,
            "blk": blk,
            "rd": rd,
            "part": part,
        }

    def alloc_msgport(self):
        """Allocate and minimally init a MsgPort."""
        mp_mem = self.alloc.alloc_memory(MsgPortStruct.get_size(), label="MsgPort")
        mp = AccessStruct(self.mem, MsgPortStruct, mp_mem.addr)
        mp.w_s("mp_Node.type", NodeType.NT_MSGPORT)
        # Init message list to empty
        lst = AccessStruct(self.mem, ListStruct, mp_mem.addr + MsgPortStruct.sdef.find_field_def_by_name("mp_MsgList").offset)
        lst.w_s("lh_Head", 0)
        lst.w_s("lh_Tail", 0)
        lst.w_s("lh_TailPred", 0)
        lst.w_s("lh_Type", NodeType.NT_MESSAGE)
        mp.w_s("mp_Flags", 0)
        mp.w_s("mp_SigBit", 0)
        mp.w_s("mp_SigTask", 0)
        return mp_mem.addr
