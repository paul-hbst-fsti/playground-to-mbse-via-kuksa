import argparse, asyncio, logging

def parse_args():
    p = argparse.ArgumentParser("UDP relay: echo received payload to a fixed dst")
    p.add_argument("--listen", default="0.0.0.0")
    p.add_argument("--port", type=int, default=50001, help="local listen port")
    p.add_argument("--send-host", default="127.0.0.1", help="fixed destination host")
    p.add_argument("--send-port", type=int, default=50000, help="fixed destination port")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()

class Relay(asyncio.DatagramProtocol):
    def __init__(self, dst):
        self.dst = dst
        self.transport = None
    def connection_made(self, transport):
        self.transport = transport
        logging.info("Listening on %s, relaying to %s", transport.get_extra_info("sockname"), self.dst)
    def datagram_received(self, data, addr):
        logging.info("Got %d bytes from %s — forwarding to %s", len(data), addr, self.dst)
        self.transport.sendto(data, self.dst)

async def main():
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(levelname)s %(message)s")
    dst = (args.send_host, args.send_port)

    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(
        lambda: Relay(dst),
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
