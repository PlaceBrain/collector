import json
import logging

import grpc
from placebrain_contracts import devices_pb2 as devices_pb
from placebrain_contracts.devices_pb2 import DEVICE_STATUS_OFFLINE, DEVICE_STATUS_ONLINE
from placebrain_contracts.devices_pb2_grpc import DevicesServiceStub

logger = logging.getLogger(__name__)

_STATUS_MAP = {"online": DEVICE_STATUS_ONLINE, "offline": DEVICE_STATUS_OFFLINE}


class StatusHandler:
    def __init__(self, devices_stub: DevicesServiceStub) -> None:
        self._stub = devices_stub

    async def handle(self, topic: str, payload: str) -> None:
        # Topic: placebrain/{place_id}/devices/{device_id}/status
        parts = topic.split("/")
        if len(parts) < 5:
            logger.warning("Invalid status topic: %s", topic)
            return

        device_id = parts[3]

        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            logger.warning("Invalid JSON in status message: %s", payload[:100])
            return

        status_str = data.get("status", "offline")
        proto_status = _STATUS_MAP.get(status_str, DEVICE_STATUS_OFFLINE)

        try:
            await self._stub.UpdateDeviceStatus(
                devices_pb.UpdateDeviceStatusRequest(device_id=device_id, status=proto_status)
            )
            logger.info("Updated device %s status to %s", device_id, status_str)
        except grpc.aio.AioRpcError:
            logger.exception("Failed to update device status for %s", device_id)
