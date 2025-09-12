from kuksa_client.grpc import Field
from kuksa_client.grpc import SubscribeEntry
from kuksa_client.grpc import View
from kuksa_client.grpc import VSSClient

with VSSClient('c804wg0wsso8kokgwss0w0ks.116.203.177.174.sslip.io', 55555) as client:
    for updates in client.subscribe(entries=[
        SubscribeEntry('Vehicle.Cabin.Door.Row1.PassengerSide.Window.Position', View.FIELDS, (Field.VALUE, Field.ACTUATOR_TARGET)),
    ]):
        for update in updates:
            if update.entry.value is not None:
                print(f"Current value for {update.entry.path} is now: {update.entry.value}")
            if update.entry.actuator_target is not None:
                print(f"Target value for {update.entry.path} is now: {update.entry.actuator_target}")
