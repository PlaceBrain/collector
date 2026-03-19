import logging

from dishka import FromDishka
from dishka.integrations.grpcio import inject
from google.protobuf.timestamp_pb2 import Timestamp
from placebrain_contracts.collector_pb2 import GetLatestReadingsResponse
from placebrain_contracts.collector_pb2 import SensorReading as SensorReadingProto
from placebrain_contracts.collector_pb2_grpc import CollectorServiceServicer

from src.services.readings import ReadingsService

logger = logging.getLogger(__name__)


class CollectorHandler(CollectorServiceServicer):
    @inject
    async def GetLatestReadings(
        self, request, context, readings_service: FromDishka[ReadingsService]
    ):
        readings = await readings_service.get_latest(request.device_id)
        proto_readings = []
        for r in readings:
            ts = Timestamp()
            ts.FromDatetime(r.time)
            proto_readings.append(SensorReadingProto(key=r.key, value=r.value, time=ts))
        return GetLatestReadingsResponse(readings=proto_readings)
