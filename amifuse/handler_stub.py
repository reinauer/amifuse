"""
Build entry stub for filesystem handlers.

AmigaOS starts C/assembler handlers (GlobVec -1 or -2) at the first byte
of the first segment. The startup packet is queued to pr_MsgPort, and the
handler retrieves it via WaitPort/GetMsg.

The handler's own startup code sets up registers (A4, A6, etc.) as needed.
We just need to jump to the segment start.
"""


def build_entry_stub(mem, alloc, segment_start_addr):
    """
    Build a minimal stub that jumps to the handler's segment start.

    The handler's own startup code will:
    1. Set up any needed registers (A4 for small data, A6 for ExecBase, etc.)
    2. Call WaitPort/GetMsg on pr_MsgPort to get the startup packet

    Returns stub PC.
    """
    # 68000 code: jmp segment_start_addr
    code = bytearray()
    code += b"\x4e\xf9"  # jmp absolute long
    code += (segment_start_addr >> 16).to_bytes(2, "big")
    code += (segment_start_addr & 0xFFFF).to_bytes(2, "big")
    mem_obj = alloc.alloc_memory(len(code), label="handler_stub")
    mem.w_block(mem_obj.addr, bytes(code))
    return mem_obj.addr
