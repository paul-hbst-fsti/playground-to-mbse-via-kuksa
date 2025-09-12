#!/usr/bin/env python3
import argparse, asyncio, struct, logging

def parse_args():
    p = argparse.ArgumentParser("UDP placeholder that always sends 70 to a fixed dst")
    p.add_argument("--listen", default="0.0.0.0")
    p.add_argument("--port", type=int, default=50001,
                   help="local port to listen for any trigger packet")
    p.add_argument("--format", choices=["float32le", "text", "uint8", "uint32"],
                   default="float32le", help="payload format for 70")
    p.add_argument("--send-host", default="127.0.0.1",
                   help="destination host (provider’s UDP listen)")
    p.add_argument("--send-port", type=int, default=50000,
                   help="destination port (provider’s UDP listen)")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()

def make_payload(fmt: str) -> bytes:
    if fmt == "float32le":
        return struct.pack("<f", 70.0)
    if fmt == "text":
        return b"70.0"
    if fmt == "uint8":
        return (70).to_bytes(1, "little", signed=False)
    if fmt == "uint32":
        return struct.pack("<I", 70)
    raise ValueError("unknown format")

class Placeholder(asyncio.DatagramProtocol):
    def __init__(self, payload: bytes, dst):
        self.payload = payload
        self.dst = dst
        self.transport = None
    def connection_made(self, transport):
        self.transport = transport
        logging.info("Listening on %s, sending to %s", transport.get_extra_info("sockname"), self.dst)
    def datagram_received(self, data, addr):
        logging.info("Trigger %d bytes from %s — sending 70 to %s", len(data), addr, self.dst)
        self.transport.sendto(self.payload, self.dst)

async def main():
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(levelname)s %(message)s")
    payload = make_payload(args.format)
    dst = (args.send_host, args.send_port)

    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(
        lambda: Placeholder(payload, dst),
        local_addr=(args.listen, args.port)
    )
    try:
        await asyncio.Future()  # run forever
    finally:
        transport.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
