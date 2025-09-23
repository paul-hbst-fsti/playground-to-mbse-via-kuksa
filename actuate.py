
import argparse
import grpc
from kuksa.val.v2 import val_pb2_grpc as vgrpc, val_pb2 as vpb, types_pb2 as tpb

HOST_DEFAULT = "c804wg0wsso8kokgwss0w0ks.116.203.177.174.sslip.io:55555"

TYPE_FIELD_MAP = {
    "bool": "bool",
    "string": "string",
    "bytes": "bytes",
    "int32": "int32",
    "int64": "int64",
    "uint32": "uint32",
    "uint64": "uint64",
    "float": "float",
    "double": "double",
}

def build_value(kind: str, raw: str) -> tpb.Value:
    field = TYPE_FIELD_MAP[kind]
    if kind in {"int32", "int64", "uint32", "uint64"}:
        val = int(raw)
    elif kind in {"float", "double"}:
        val = float(raw)
    elif kind == "bool":
        val = raw.lower() in {"1", "true", "t", "yes", "y"}
    elif kind == "bytes":
        val = raw.encode("utf-8")
    else:  # string
        val = raw
    return tpb.Value(**{field: val})

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=HOST_DEFAULT)
    ap.add_argument("--path", required=True,
                    help="Signal path, e.g. Vehicle.Cabin.Door.Row1.PassengerSide.Window.Position")
    ap.add_argument("--type", required=True, choices=TYPE_FIELD_MAP.keys(),
                    help="Value type for the 'value' oneof")
    ap.add_argument("--value", required=True, help="Value payload")
    ap.add_argument("--timeout", type=float, default=3.0)
    args = ap.parse_args()

    channel = grpc.insecure_channel(args.host)
    stub = vgrpc.VALStub(channel)

    req = vpb.ActuateRequest(
        signal_id=tpb.SignalID(path=args.path),
        value=build_value(args.type, args.value),
    )

    try:
        resp = stub.Actuate(req, timeout=args.timeout)
        print("Actuate response:", resp)
    except grpc.RpcError as e:
        print(f"RPC failed: {e.code().name} - {e.details()}")

if __name__ == "__main__":
    main()
