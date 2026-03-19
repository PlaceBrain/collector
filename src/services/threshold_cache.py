import asyncio
import logging
from dataclasses import dataclass

import grpc
from placebrain_contracts.devices_pb2 import GetAllThresholdsRequest
from placebrain_contracts.devices_pb2_grpc import DevicesServiceStub

logger = logging.getLogger(__name__)


@dataclass
class ThresholdInfo:
    threshold_id: str
    sensor_id: str
    threshold_type: str  # "min" or "max"
    value: float
    severity: str  # "warning" or "critical"


@dataclass
class SensorMapping:
    sensor_id: str
    device_id: str
    place_id: str
    key: str
    thresholds: list[ThresholdInfo]


_THRESHOLD_TYPE_MAP = {1: "min", 2: "max"}
_SEVERITY_MAP = {1: "warning", 2: "critical"}


class ThresholdCache:
    def __init__(self, devices_stub: DevicesServiceStub) -> None:
        self._stub = devices_stub
        self._sensor_map: dict[tuple[str, str], SensorMapping] = {}
        self._refresh_interval = 300  # 5 minutes

    def lookup(self, device_id: str, key: str) -> SensorMapping | None:
        return self._sensor_map.get((device_id, key))

    async def refresh(self) -> None:
        try:
            response = await self._stub.GetAllThresholds(GetAllThresholdsRequest())
            new_map: dict[tuple[str, str], SensorMapping] = {}
            for sensor in response.sensors:
                thresholds = [
                    ThresholdInfo(
                        threshold_id=t.threshold_id,
                        sensor_id=t.sensor_id,
                        threshold_type=_THRESHOLD_TYPE_MAP.get(t.type, "max"),
                        value=t.value,
                        severity=_SEVERITY_MAP.get(t.severity, "warning"),
                    )
                    for t in sensor.thresholds
                ]
                mapping = SensorMapping(
                    sensor_id=sensor.sensor_id,
                    device_id=sensor.device_id,
                    place_id=sensor.place_id,
                    key=sensor.key,
                    thresholds=thresholds,
                )
                new_map[(sensor.device_id, sensor.key)] = mapping
            self._sensor_map = new_map
            logger.info("Threshold cache refreshed: %d sensors", len(new_map))
        except grpc.aio.AioRpcError:
            logger.exception("Failed to refresh threshold cache")

    async def run_refresh_loop(self) -> None:
        while True:
            await self.refresh()
            await asyncio.sleep(self._refresh_interval)
