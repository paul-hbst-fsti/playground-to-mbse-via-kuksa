"""
UDP ⇄ KUKSA Databroker Provider (door demo)

- Receives the door angle from Modelica over UDP and writes it to Databroker as a CURRENT value
- Subscribes to a target actuator signal (e.g., Vehicle.Body.Door.Row1.Left.IsOpen)
  and forwards open/close commands to Modelica over UDP (1 byte: 0x01 open, 0x00 close)

USAGE
-----
python udp_databroker_provider.py \
  --broker-host 127.0.0.1 --broker-port 55555 \
  --angle-path Vehicle.Custom.Door.Row1.Left.Angle \
  --command-path Vehicle.Body.Door.Row1.Left.IsOpen \
  --udp-angle-listen 0.0.0.0:50000 \
  --udp-command-send 127.0.0.1:50001 \
  --angle-format float32le  # or 'text'

Notes
-----
* Use the standard VSS path for your command; for the angle you can use a custom path or map
  a normalized percentage to an existing signal if your catalog lacks an angle.
* By default this runs without TLS/auth. For TLS and tokens, set the corresponding flags.
* Requires: `pip install kuksa-client` (Python SDK) and Python ≥3.9.
"""
from __future__ import annotations
import argparse
import grpc
import asyncio
import logging
import os
import struct
from typing import Tuple
from kuksa_client.grpc.aio import Datapoint

# KUKSA Python SDK (async)
from kuksa_client.grpc.aio import VSSClient
from kuksa.val.v2 import val_pb2_grpc as vgrpc, val_pb2 as vpb, types_pb2 as tpb

async def send_udp_u8(loop: asyncio.AbstractEventLoop, dst: Tuple[str, int], value: int):
    """Send one unsigned byte (0..255) via UDP."""
    b = max(0, min(255, int(value))).to_bytes(1, "little", signed=False)
    transport, _ = await loop.create_datagram_endpoint(lambda: asyncio.DatagramProtocol(), remote_addr=dst)
    try:
        transport.sendto(b)
    finally:
        transport.close()
        
async def start_v2_provider_registration(broker_addr: str, path: str, udp_dst: Tuple[str, int]):
    """
    Claims the actuator via VAL v2 OpenProviderStream.
    Your existing V2 Subscribe(TARGET_VALUE) continues to handle UDP forwarding.
    """
    channel = grpc.aio.insecure_channel(broker_addr)
    stub = vgrpc.VALStub(channel)
    stream = stub.OpenProviderStream()

    async def sender():
        # claim the actuator
        req = vpb.OpenProviderStreamRequest(
            provide_actuation_request=vpb.ProvideActuationRequest(
                actuator_identifiers=[tpb.SignalID(path=path)]
            )
        )
        await stream.write(req)
        logging.info("VAL v2: provided actuation for %s", path)
        while True:
            await asyncio.sleep(3600)

    async def receiver():
        async for resp in stream:
            action = resp.WhichOneof("action")
            if action == "provide_actuation_response":
                logging.info("Broker accepted provide_actuation for %s", path)
            elif action == "batch_actuate_stream_request":
                req = resp.batch_actuate_stream_request
                for ar in req.actuate_requests:
                    v = ar.value
                    if   v.HasField("uint32"): target = int(v.uint32)
                    elif v.HasField("int32"):  target = int(v.int32)
                    else:
                        logging.warning("Unsupported actuation type: %s", v)
                        continue
                    target = max(0, min(100, target))  # window position 0..100

                    # send one byte over UDP
                    await send_udp_u8(asyncio.get_running_loop(), udp_dst, target)
                    logging.info("Actuation → UDP %s = %d", path, target)

                    # ACK back to broker
                    ack = vpb.OpenProviderStreamRequest(
                        batch_actuate_stream_response=vpb.BatchActuateStreamResponse(signal_id=ar.signal_id)
                    )
                    await stream.write(ack)

    asyncio.create_task(sender())
    asyncio.create_task(receiver())
    return channel  # keep reference alive
# ----------------------
# Argument parsing
# ----------------------

def parse_hostport(value: str) -> Tuple[str, int]:
    if ":" not in value:
        raise argparse.ArgumentTypeError("expected HOST:PORT")
    host, port_s = value.rsplit(":", 1)
    try:
        port = int(port_s)
    except ValueError:
        raise argparse.ArgumentTypeError("PORT must be an integer")
    return host, port


def build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="UDP ⇄ KUKSA Databroker provider (door demo)")

    # Databroker
    p.add_argument("--broker-host", default=os.getenv("BROKER_HOST", "127.0.0.1"))
    p.add_argument("--broker-port", type=int, default=int(os.getenv("BROKER_PORT", "55555")))
    p.add_argument("--insecure", action="store_true", default=True,
                   help="Use insecure channel (no TLS). For TLS, supply --tls and certificates.")
    p.add_argument("--tls", action="store_true", help="Enable TLS (provide --root-ca and optionally --token)")
    p.add_argument("--root-ca", default=os.getenv("KUKSA_CA", ""), help="Path to CA cert (PEM) if --tls")
    p.add_argument("--tls-server-name", default=os.getenv("KUKSA_TLS_SERVER_NAME", "localhost"))
    p.add_argument("--token", default=os.getenv("KUKSA_TOKEN", ""), help="JWT for Databroker if configured")

    # VSS paths
    p.add_argument("--angle-path", default=os.getenv("ANGLE_PATH", "Vehicle.Custom.Door.Row1.Left.Angle"))
    p.add_argument("--command-path", default=os.getenv("COMMAND_PATH", "Vehicle.Body.Door.Row1.Left.IsOpen"))

    # UDP endpoints
    p.add_argument("--udp-angle-listen", type=parse_hostport,
                   default=parse_hostport(os.getenv("UDP_ANGLE_LISTEN", "0.0.0.0:50000")),
                   help="Where to listen for angle from Modelica (host:port)")
    p.add_argument("--udp-command-send", type=parse_hostport,
                   default=parse_hostport(os.getenv("UDP_COMMAND_SEND", "127.0.0.1:50001")),
                   help="Where to send door open/close commands for Modelica (host:port)")

    
    p.add_argument("--angle-format", choices=["float32le", "uint32", "uint8", "text"],
               default="float32le")


    # Misc
    p.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO"))

    return p.parse_args()


# ----------------------
# UDP protocol handlers
# ----------------------

class AngleReceiver(asyncio.DatagramProtocol):
    def __init__(self, client: VSSClient, angle_path: str, fmt: str):
        self.client = client
        self.angle_path = angle_path
        self.fmt = fmt
        self.transport: asyncio.DatagramTransport | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]
        sock = self.transport.get_extra_info("sockname")
        logging.info("Angle UDP listening on %s", sock)

    def datagram_received(self, data: bytes, addr):
        print("received")
        try:
            if self.fmt == "float32le":
                if len(data) < 4: return
                value = struct.unpack("<f", data[:4])[0]
            elif self.fmt == "uint32":
                if len(data) < 4: return
                value = struct.unpack("<I", data[:4])[0]
            elif self.fmt == "uint8":
                if len(data) < 1: return
                value = data[0]
            else:  # text
                value = float(data.decode("utf-8").strip())
        except Exception as e:
            logging.exception("Failed to parse angle from %s: %s", addr, e)
            return
        print("value")
        print(value)

        asyncio.create_task(self._publish_angle(value))

    async def _publish_angle(self, angle: float):
        try:
            await self.client.set_current_values({self.angle_path: Datapoint(angle)})
            logging.debug("Published %s = %.3f", self.angle_path, angle)
        except Exception as e:
            logging.exception("Failed to publish to Databroker: %s", e)


async def send_udp_command(loop: asyncio.AbstractEventLoop, dst: Tuple[str, int], value: bool):
    # 0x01 for open (True), 0x00 for close (False)
    payload = b"\x01" if value else b"\x00"
    transport, protocol = await loop.create_datagram_endpoint(lambda: asyncio.DatagramProtocol(), remote_addr=dst)
    try:
        transport.sendto(payload)
    finally:
        transport.close()


# ----------------------
# Main tasks
# ----------------------

def _iter_updates(updates):
    # Supports both v1 (dict) and v2 (list of entries)
    if isinstance(updates, dict):
        for path, dp in updates.items():
            val = getattr(dp, "value", dp)
            yield path, val
    else:
        for u in updates:
            path = getattr(u, "path", None) or (u.get("path") if isinstance(u, dict) else None)
            dp = getattr(u, "datapoint", None) or (u.get("datapoint") if isinstance(u, dict) else None)
            # some SDKs put value directly
            val = getattr(dp, "value", dp)
            yield path, val

async def subscribe_and_forward_commands(client, command_path, udp_dst):
    import asyncio, logging
    from kuksa_client.grpc import SubscribeEntry, View, Field

    entries = [SubscribeEntry(path=command_path, view=View.TARGET_VALUE, fields=[Field.VALUE])]
    logging.info("Subscribing (V2) to TARGET for %s", command_path)

    # works with kuksa-client ≥ 0.5; falls back to .subscribe(...)
    subscribe_v2 = getattr(client, "subscribe_v2", None)
    aiter = subscribe_v2(entries=entries) if callable(subscribe_v2) else client.subscribe(entries=entries)

    async for updates in aiter:
        for path, val in _iter_updates(updates):
            if path != command_path or val is None:
                continue
            try:
                pos = int(val)  # expect 0..100 (uint8)
            except Exception:
                logging.warning("Ignoring non-integer target %r for %s", val, path)
                continue
            # send single uint8 to Modelica
            b = max(0, min(100, pos)).to_bytes(1, "little", signed=False)
            transport, _ = await asyncio.get_running_loop().create_datagram_endpoint(
                lambda: asyncio.DatagramProtocol(), remote_addr=udp_dst
            )
            try:
                transport.sendto(b)
            finally:
                transport.close()
            logging.info("Forwarded TARGET %s=%d to UDP %s", path, pos, udp_dst)
async def main():
    args = build_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="[%(asctime)s] %(levelname)s %(message)s")

    # Connect KUKSA Databroker (async client)
    tls_kwargs = {}
    if args.tls:
        from pathlib import Path
        if not args.root_ca:
            raise SystemExit("--tls requires --root-ca <CA.pem>")
        tls_kwargs = {
            "root_certificates": Path(args.root_ca),
            "tls_server_name": args.tls_server_name,
        }
    token = args.token if args.token else None

    client = VSSClient(args.broker_host, args.broker_port, token=token, **tls_kwargs)
    await client.connect()
    await start_v2_provider_registration(f"{args.broker_host}:{args.broker_port}",
                                     args.command_path, args.udp_command_send)
    logging.info("Connected to Databroker at %s:%d", args.broker_host, args.broker_port)

    # Start UDP listener for angle
    loop = asyncio.get_running_loop()
    angle_host, angle_port = args.udp_angle_listen
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: AngleReceiver(client, args.angle_path, args.angle_format),
        local_addr=(angle_host, angle_port),
    )

    # Start subscription task for actuator target commands
    udp_cmd_dst = args.udp_command_send
    sub_task = asyncio.create_task(subscribe_and_forward_commands(client, args.command_path, udp_cmd_dst))

    try:
        await sub_task
    finally:
        transport.close()
        await client.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
