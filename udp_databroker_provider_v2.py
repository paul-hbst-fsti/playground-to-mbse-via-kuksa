
from __future__ import annotations
import argparse, asyncio, json, logging, os, struct
from pathlib import Path
from typing import Dict, Tuple, Any, Optional, List

import grpc
from kuksa_client.grpc.aio import VSSClient, Datapoint
from kuksa.val.v2 import val_pb2_grpc as vgrpc, val_pb2 as vpb, types_pb2 as tpb

# ----------------------
# Config
# ----------------------

def load_config(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise SystemExit(f"Config file not found: {p}")
    if p.suffix.lower() in {".yaml", ".yml"}:
        try:
            import yaml  # pip install pyyaml
        except ImportError:
            raise SystemExit("YAML config requested but PyYAML not installed. `pip install pyyaml`")
        with p.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def parse_hostport(value: str) -> Tuple[str, int]:
    host, port = value.rsplit(":", 1)
    return host, int(port)

# ----------------------
# UDP utils
# ----------------------

def encode_value(dtype: str, value: Any) -> bytes:
    if dtype == "float32le":
        return struct.pack("<f", float(value))
    if dtype == "uint32":
        return struct.pack("<I", int(value) & 0xFFFFFFFF)
    if dtype == "uint8":
        return bytes([max(0, min(255, int(value)))])
    if dtype == "bool":
        return b"\x01" if bool(value) else b"\x00"
    if dtype == "text":
        return (str(value) + "\n").encode("utf-8")
    raise ValueError(f"Unsupported dtype for encode: {dtype}")

def decode_value(dtype: str, data: bytes) -> Any:
    if dtype == "float32le":
        if len(data) < 4: raise ValueError("need 4 bytes")
        return struct.unpack("<f", data[:4])[0]
    if dtype == "uint32":
        if len(data) < 4: raise ValueError("need 4 bytes")
        return struct.unpack("<I", data[:4])[0]
    if dtype == "uint8":
        if len(data) < 1: raise ValueError("need 1 byte")
        return data[0]
    if dtype == "bool":
        if len(data) < 1: raise ValueError("need 1 byte")
        return bool(data[0])
    if dtype == "text":
        return float(data.decode("utf-8").strip())
    raise ValueError(f"Unsupported dtype for decode: {dtype}")

async def udp_send(dst: Tuple[str, int], payload: bytes):
    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(
        lambda: asyncio.DatagramProtocol(), remote_addr=dst
    )
    try:
        transport.sendto(payload)
    finally:
        transport.close()

# ----------------------
# VAL v2 Provider (actuation) with ID<->path map from ListMetadata
# ----------------------

class Provider:
    def __init__(self, broker_addr: str, token: Optional[str]):
        self.broker_addr = broker_addr
        self.token = token
        self.channel: Optional[grpc.aio.Channel] = None
        self.stub: Optional[vgrpc.VALStub] = None
        self.stream = None
        self.paths: Dict[str, Dict[str, Any]] = {}  # path -> signal config
        self._path_to_id: dict[str, int] = {}
        self._id_to_path: dict[int, str] = {}

    async def open(self):
        self.channel = grpc.aio.insecure_channel(self.broker_addr)
        self.stub = vgrpc.VALStub(self.channel)
        metadata = (("authorization", f"Bearer {self.token}"),) if self.token else None
        # Preload metadata map (ID <-> path) once
        await self._load_metadata_map()
        self.stream = self.stub.OpenProviderStream(metadata=metadata)
        asyncio.create_task(self._receiver())

    async def close(self):
        if self.channel:
            await self.channel.close()

    async def _load_metadata_map(self):
        # Fetch all metadata and build maps; simple and robust
        try:
            resp = await self.stub.ListMetadata(vpb.ListMetadataRequest())
        except Exception as e:
            logging.warning("ListMetadata failed; ID<->path resolution may be limited: %s", e)
            return
        count = 0
        for md in getattr(resp, "metadatas", []):
            path = getattr(md, "path", None)
            sid = getattr(md, "id", None)  # md.id is int in v2 schema
            if isinstance(sid, int) and path:
                self._path_to_id[path] = sid
                self._id_to_path[sid] = path
                count += 1
        logging.info("Loaded %d metadata entries for ID<->path map", count)

    async def provide(self, path: str):
        req = vpb.OpenProviderStreamRequest(
            provide_actuation_request=vpb.ProvideActuationRequest(
                actuator_identifiers=[tpb.SignalID(path=path)]
            )
        )
        await self.stream.write(req)
        logging.info("Provided actuation for %s (id=%s)", path, self._path_to_id.get(path))

    async def _receiver(self):
        async for resp in self.stream:
            action = resp.WhichOneof("action")
            if action == "provide_actuation_response":
                logging.info("Broker accepted provider registration.")
            elif action == "batch_actuate_stream_request":
                batch = resp.batch_actuate_stream_request
                for ar in batch.actuate_requests:
                    sid = ar.signal_id
                    which = sid.WhichOneof("signal")
                    if which == "path":
                        path = sid.path
                    elif which == "id":
                        path = self._id_to_path.get(sid.id)
                        if not path:
                            logging.warning("Actuation with unknown id=%s; update metadata map if needed", sid.id)
                            continue
                    else:
                        logging.warning("Actuation with missing signal id: %s", sid)
                        continue

                    if path not in self.paths:
                        logging.warning("Actuation for unmanaged path %s — ignoring", path)
                        continue

                    scfg = self.paths[path]
                    dtype = scfg["dtype"]
                    dst = parse_hostport(scfg["udp_send"])

                    tv = ar.value
                    if   tv.HasField("uint32"): val = int(tv.uint32)
                    elif tv.HasField("int32"):  val = int(tv.int32)
                    elif tv.HasField("float"):  val = float(tv.float)
                    elif tv.HasField("bool"):   val = bool(tv.bool)
                    else:
                        logging.warning("Unsupported TypedValue for %s: %s", path, tv)
                        continue

                    await udp_send(dst, encode_value(dtype, val))
                    logging.info("Actuation → UDP %s (id=%s) = %r -> %s",
                                path, self._path_to_id.get(path), val, scfg["udp_send"])

                    ack = vpb.OpenProviderStreamRequest(
                        batch_actuate_stream_response=vpb.BatchActuateStreamResponse(signal_id=ar.signal_id)
                    )
                    await self.stream.write(ack)

# ----------------------
# UDP → CURRENT publisher
# ----------------------

class ListenAndPublish(asyncio.DatagramProtocol):
    def __init__(self, client: VSSClient, path: str, dtype: str):
        self.client = client
        self.path = path
        self.dtype = dtype
        self.transport: Optional[asyncio.DatagramTransport] = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]
        sock = self.transport.get_extra_info("sockname")
        logging.info("Listening for %s on UDP %s (%s)", self.path, sock, self.dtype)

    def datagram_received(self, data: bytes, addr):
        asyncio.create_task(self._handle(data, addr))

    async def _handle(self, data: bytes, addr):
        try:
            value = decode_value(self.dtype, data)
        except Exception as e:
            logging.warning("Decode failed for %s from %s: %s", self.path, addr, e)
            return
        try:
            await self.client.set_current_values({self.path: Datapoint(value)})
            logging.debug("Published CURRENT %s = %r", self.path, value)
        except Exception as e:
            logging.exception("Publish failed for %s: %s", self.path, e)

# ----------------------
# TARGET subscription → UDP
# ----------------------

async def subscribe_targets(client: VSSClient, signals: list[dict]):
    from kuksa_client.grpc import SubscribeEntry, View, Field

    entries = []
    by_path: dict[str, dict] = {}
    for s in signals:
        if s.get("udp_send"):
            entries.append(SubscribeEntry(path=s["path"], view=View.TARGET_VALUE, fields=[Field.VALUE]))
            by_path[s["path"]] = s

    if not entries:
        return

    logging.info("Subscribing to %d TARGET signals", len(entries))
    subscribe_v2 = getattr(client, "subscribe_v2", None)
    aiter = subscribe_v2(entries=entries) if callable(subscribe_v2) else client.subscribe(entries=entries)

    async for batch in aiter:
        updates_iter = batch.updates if hasattr(batch, "updates") else (batch if isinstance(batch, (list, tuple)) else [batch])
        for u in updates_iter:
            path = getattr(u, "path", None) or getattr(getattr(u, "entry", None), "path", None)
            dp = getattr(u, "datapoint", None) or getattr(getattr(u, "entry", None), "datapoint", None)
            val = getattr(dp, "value", None) if dp is not None else None
            if path not in by_path or val is None:
                continue

            s = by_path[path]
            try:
                payload = encode_value(s["dtype"], val)
            except Exception as e:
                logging.warning("Encode failed for %s: %s", path, e)
                continue

            dst = parse_hostport(s["udp_send"])
            await udp_send(dst, payload)
            logging.info("TARGET → UDP %s = %r -> %s", path, val, s["udp_send"])

# ----------------------
# Main
# ----------------------

async def run(config_path: str):
    cfg = load_config(config_path)

    broker = cfg["broker"]
    host, port = broker["host"], int(broker["port"])
    token = broker.get("token") or None

    tls_kwargs = {}
    tls = broker.get("tls", {})
    if tls.get("enabled"):
        ca = tls.get("root_ca")
        if not ca:
            raise SystemExit("TLS enabled but no root_ca provided")
        tls_kwargs = {"root_certificates": Path(ca), "tls_server_name": tls.get("server_name", "localhost")}

    client = VSSClient(host, port, token=token, **tls_kwargs)
    await client.connect()
    logging.info("Connected to Databroker at %s:%d", host, port)

    signals = cfg.get("signals", [])

    # Start UDP listeners (ingress)
    loop = asyncio.get_running_loop()
    for s in signals:
        if s.get("udp_listen"):
            h, p = parse_hostport(s["udp_listen"])
            await loop.create_datagram_endpoint(
                lambda s=s: ListenAndPublish(client, s["path"], s["dtype"]),
                local_addr=(h, p),
            )

    # Provider (actuation)
    provide_signals = [s for s in signals if s.get("provide_actuation") or s.get("udp_send")]
    provider: Optional[Provider] = None
    if provide_signals:
        provider = Provider(f"{host}:{port}", token)
        provider.paths = {s["path"]: s for s in provide_signals}
        await provider.open()
        for s in provide_signals:
            await provider.provide(s["path"])

    # TARGET subscriptions (egress)
    sub_task = asyncio.create_task(subscribe_targets(client, signals))

    try:
        await sub_task
    finally:
        if provider:
            await provider.close()
        await client.disconnect()

def main():
    ap = argparse.ArgumentParser(description="UDP ⇄ KUKSA Databroker Bridge (config-driven)")
    ap.add_argument("config", help="Path to config (YAML or JSON)")
    ap.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO"))
    args = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="[%(asctime)s] %(levelname)s %(message)s",
    )
    try:
        asyncio.run(run(args.config))
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()

