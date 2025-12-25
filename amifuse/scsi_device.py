"""
Minimal scsi.device shim that maps TD_READ/TD_WRITE/TD_GETGEOMETRY to a
BlockDeviceBackend. This is enough to satisfy filesystem handlers during
startup (OpenDevice + BeginIO/DoIO).
"""

from amitools.vamos.libcore import LibImpl  # type: ignore
from amitools.vamos.machine.regs import REG_A1, REG_D0, REG_D1  # type: ignore
from amitools.vamos.astructs.access import AccessStruct  # type: ignore
from amitools.vamos.astructs import AmigaStructDef, AmigaStruct  # type: ignore
from amitools.vamos.astructs.scalar import UBYTE, UWORD, ULONG  # type: ignore
from amitools.vamos.libstructs.exec_ import IORequestStruct, UnitStruct  # type: ignore


CMD_READ = 2  # TD_READ
CMD_WRITE = 3  # TD_WRITE
TD_GETGEOMETRY = 22
# HD_SCSICMD = 28


@AmigaStructDef
class SCSICmdStruct(AmigaStruct):
    _format = [
        (ULONG, "scsi_Data"),
        (ULONG, "scsi_Length"),
        (ULONG, "scsi_Actual"),
        (ULONG, "scsi_Command"),
        (UWORD, "scsi_CmdLength"),
        (UWORD, "scsi_CmdActual"),
        (UBYTE, "scsi_Flags"),
        (UBYTE, "scsi_Status"),
        (ULONG, "scsi_SenseData"),
        (UWORD, "scsi_SenseLength"),
        (UWORD, "scsi_SenseActual"),
    ]
# fallbacks: TD_MOTOR/TD_REMCHANGE/TD_ADDCHANGEINT and friends default to success


class ScsiDevice(LibImpl):
    def __init__(self, backend, debug=False):
        super().__init__()
        self.backend = backend
        self.debug = debug

    def get_version(self):
        return 40

    def open_lib(self, ctx, open_cnt):
        # no-op
        return 0

    def close_lib(self, ctx, open_cnt):
        return 0

    def BeginIO(self, ctx):
        # A1 points to IORequest
        req_ptr = ctx.cpu.r_reg(REG_A1)
        mem = ctx.mem
        ior = AccessStruct(mem, IORequestStruct, req_ptr)
        cmd = ior.r_s("io_Command")
        length = ior.r_s("io_Length")
        offset = ior.r_s("io_Offset")
        buf_ptr = ior.r_s("io_Data")
        # SCSI command dispatch
        cmd_names = {
            2: "CMD_READ", 3: "CMD_WRITE", 9: "TD_MOTOR", 11: "TD_FORMAT",
            14: "TD_CHANGESTATE", 15: "TD_PROTSTATUS", 18: "TD_GETDRIVETYPE",
            22: "TD_GETGEOMETRY", 28: "HD_SCSICMD",
        }
        if self.debug:
            cmd_name = cmd_names.get(cmd, f"CMD_{cmd}")
            print(f"[SCSI] {cmd_name} offset={offset} len={length} buf=0x{buf_ptr:08x}")

        # Clear error
        ior.w_s("io_Error", 0)

        if cmd == 28:  # HD_SCSICMD
            scsi = AccessStruct(mem, SCSICmdStruct, buf_ptr)
            cdb_ptr = scsi.r_s("scsi_Command")
            cdb_len = scsi.r_s("scsi_CmdLength")
            data_ptr = scsi.r_s("scsi_Data")
            data_len = scsi.r_s("scsi_Length")
            sense_ptr = scsi.r_s("scsi_SenseData")
            sense_len = scsi.r_s("scsi_SenseLength")
            flags = scsi.r_s("scsi_Flags")
            opcode = mem.r8(cdb_ptr) if cdb_len > 0 else 0
            cdb_bytes = mem.r_block(cdb_ptr, cdb_len) if cdb_ptr and cdb_len else b""
            actual = 0
            status = 0
            # default: no sense data
            if sense_ptr:
                mem.w_block(sense_ptr, b"\x00" * min(sense_len, 18))
            if opcode == 0x00:  # TEST UNIT READY
                actual = 0
            elif opcode == 0x03:  # REQUEST SENSE
                length_req = min(data_len, sense_len if sense_len else 18)
                mem.w_block(data_ptr, b"\x00" * length_req)
                actual = length_req
            elif opcode == 0x12:  # INQUIRY
                alloc_len = mem.r8(cdb_ptr + 4) if cdb_len > 4 else data_len
                alloc_len = min(alloc_len, data_len)
                resp = bytearray(max(alloc_len, 36))
                resp[0] = 0x00  # direct-access block
                resp[2] = 0x05  # SPC-3
                resp[3] = 0x02  # response data format
                resp[4] = len(resp) - 5
                mem.w_block(data_ptr, bytes(resp[:alloc_len]))
                actual = alloc_len
            elif opcode == 0x1A:  # MODE SENSE(6)
                alloc_len = mem.r8(cdb_ptr + 4) if cdb_len > 4 else data_len
                resp = bytearray([0x00, 0x00, 0x00, 0x00])
                alloc_len = min(alloc_len, data_len, len(resp))
                mem.w_block(data_ptr, bytes(resp[:alloc_len]))
                actual = alloc_len
            elif opcode == 0x25:  # READ CAPACITY(10)
                resp = bytearray(8)
                last_lba = self.backend.total_blocks - 1
                resp[0:4] = last_lba.to_bytes(4, "big")
                resp[4:8] = self.backend.block_size.to_bytes(4, "big")
                mem.w_block(data_ptr, bytes(resp[: data_len if data_len < 8 else 8]))
                actual = min(data_len, 8)
            elif opcode in (0x08, 0x28):  # READ(6)/READ(10)
                if opcode == 0x08:
                    lba = ((mem.r8(cdb_ptr + 1) & 0x1F) << 16) | (mem.r8(cdb_ptr + 2) << 8) | mem.r8(cdb_ptr + 3)
                    xfer_blocks = mem.r8(cdb_ptr + 4) or 256
                else:
                    lba = mem.r32(cdb_ptr + 2)
                    xfer_blocks = mem.r16(cdb_ptr + 7)
                data = self.backend.read_blocks(lba, xfer_blocks)
                mem.w_block(data_ptr, data[: data_len])
                actual = min(len(data), data_len)
            elif opcode == 0x2A:  # WRITE(10)
                lba = mem.r32(cdb_ptr + 2)
                xfer_blocks = mem.r16(cdb_ptr + 7)
                data = mem.r_block(data_ptr, min(data_len, xfer_blocks * self.backend.block_size))
                self.backend.write_blocks(lba, data, xfer_blocks)
                actual = len(data)
            else:
                # Unsupported command: report check condition
                status = 2  # check condition
            scsi.w_s("scsi_CmdActual", cdb_len)
            scsi.w_s("scsi_Status", status)
            scsi.w_s("scsi_Actual", actual)
            scsi.w_s("scsi_SenseActual", 0)
            ior.w_s("io_Actual", actual)
        elif cmd == CMD_READ:
            block_num = offset // self.backend.block_size
            data = self.backend.read_blocks(block_num, length // self.backend.block_size)
            mem.w_block(buf_ptr, data)
            ior.w_s("io_Actual", len(data))
        elif cmd == CMD_WRITE:
            data = mem.r_block(buf_ptr, length)
            block_num = offset // self.backend.block_size
            self.backend.write_blocks(block_num, data, length // self.backend.block_size)
            ior.w_s("io_Actual", length)
        elif cmd == TD_GETGEOMETRY:
            # Geometry layout matches Amiga DriveGeometry:
            # UWORD dg_SectorSize, ULONG dg_TotalSectors, UWORD dg_Cylinders,
            # UWORD dg_CylSects, UWORD dg_Heads, UWORD dg_TrackSects,
            # UWORD dg_BufMemType, UBYTE dg_DeviceType, UBYTE dg_Flags, UWORD dg_Reserved
            geo_ptr = buf_ptr
            total_secs = self.backend.total_blocks
            cyls = self.backend.cyls
            cyl_secs = self.backend.secs * self.backend.heads
            mem.w16(geo_ptr + 0, self.backend.block_size)
            mem.w32(geo_ptr + 2, total_secs)
            mem.w16(geo_ptr + 6, cyls)
            mem.w16(geo_ptr + 8, cyl_secs)
            mem.w16(geo_ptr + 10, self.backend.heads)
            mem.w16(geo_ptr + 12, self.backend.secs)
            mem.w16(geo_ptr + 14, 1)  # BufMemType
            mem.w8(geo_ptr + 16, 0)  # DeviceType
            mem.w8(geo_ptr + 17, 0)  # Flags
            mem.w16(geo_ptr + 18, 0)  # Reserved
            ior.w_s("io_Actual", 0)
        elif cmd == 9:  # TD_MOTOR
            # Turn motor on/off, return old motor state (always 0 for us)
            ior.w_s("io_Actual", 0)
        elif cmd == 11:  # TD_FORMAT
            # Format tracks - no-op for existing disk images
            ior.w_s("io_Actual", length)
        elif cmd == 14:  # TD_CHANGESTATE
            # Check if disk is present: io_Actual=0 means disk present
            ior.w_s("io_Actual", 0)
        elif cmd == 15:  # TD_PROTSTATUS
            # Check write protect: io_Actual=0 means not protected
            ior.w_s("io_Actual", 0 if not self.backend.read_only else 1)
        elif cmd == 18:  # TD_GETDRIVETYPE
            # Return drive type: 0 = 3.5" drive
            ior.w_s("io_Actual", 0)
        else:
            # For unhandled commands (e.g., TD_ADDCHANGEINT), report success.
            ior.w_s("io_Error", 0)
            ior.w_s("io_Actual", 0)
        return 0

    def AbortIO(self, ctx):
        return 0
