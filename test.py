
import grpc
from kuksa.val.v2 import val_pb2_grpc as vgrpc, val_pb2 as vpb, types_pb2 as tpb

HOST = "c804wg0wsso8kokgwss0w0ks.116.203.177.174.sslip.io:55555"
PATH = "Vehicle.Cabin.Door.Row1.PassengerSide.Window.Position"

def main():
    channel = grpc.insecure_channel(HOST)  # like grpcurl -plaintext
    stub = vgrpc.VALStub(channel)

    req = vpb.GetValueRequest(
        signal_id=tpb.SignalID(path=PATH)   # <-- SignalID + snake_case field
    )

    resp = stub.GetValue(req, timeout=3.0)
    print("Full:", resp)

    # Best-effort value extraction if resp.value is a oneof
    if hasattr(resp, "value"):
        dp = resp.value
        which = dp.WhichOneof("value")
        if which:
            print("Value:", getattr(dp, which))

if __name__ == "__main__":
    main()

