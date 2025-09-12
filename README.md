start command:

python udp_databroker_provider.py \
  --broker-host c804wg0wsso8kokgwss0w0ks.116.203.177.174.sslip.io --broker-port 55555 \
  --angle-path Vehicle.Cabin.Door.Row1.PassengerSide.Window.Position \
  --command-path Vehicle.Cabin.Door.Row1.PassengerSide.Window.Position \
  --udp-angle-listen 0.0.0.0:50000 \
  --udp-command-send 127.0.0.1:50001 \
  --angle-format float32le


test command:

grpcurl -plaintext -d '{"signalId":{"path":"Vehicle.Cabin.Door.Row1.PassengerSide.Window.Position"}}' \
c804wg0wsso8kokgwss0w0ks.116.203.177.174.sslip.io:55555 kuksa.val.v2.VAL/GetValue




