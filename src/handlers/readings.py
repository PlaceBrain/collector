import logging
from datetime import UTC, timedelta

import grpc
from dishka import FromDishka
from dishka.integrations.grpcio import inject
from google.protobuf.timestamp_pb2 import Timestamp
from placebrain_contracts import collector_pb2 as collector_pb
from placebrain_contracts.collector_pb2_grpc import CollectorServiceServicer

from src.services.readings import MAX_RAW_RANGE_HOURS, ReadingsService

logger = logging.getLogger(__name__)


class CollectorHandler(CollectorServiceServicer):
    @inject
    async def GetLatestReadings(  # type: ignore[override]
        self,
        request: collector_pb.GetLatestReadingsRequest,
        context: grpc.aio.ServicerContext,
        readings_service: FromDishka[ReadingsService],
    ) -> collector_pb.GetLatestReadingsResponse:
        try:
            readings = await readings_service.get_latest(request.device_id)
        except ValueError as e:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"Invalid device_id: {e}")
            raise
        proto_readings = []
        for r in readings:
            ts = Timestamp()
            ts.FromDatetime(r.time)
            proto_readings.append(collector_pb.SensorReading(key=r.key, value=r.value, time=ts))
        return collector_pb.GetLatestReadingsResponse(readings=proto_readings)

    @inject
    async def GetReadings(  # type: ignore[override]
        self,
        request: collector_pb.GetReadingsRequest,
        context: grpc.aio.ServicerContext,
        readings_service: FromDishka[ReadingsService],
    ) -> collector_pb.GetReadingsResponse:
        try:
            time_from = getattr(request, "from").ToDatetime(tzinfo=UTC)
            time_to = request.to.ToDatetime(tzinfo=UTC)
        except ValueError as e:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"Invalid timestamp: {e}")
            raise

        if time_from >= time_to:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "'from' must be before 'to'")
            raise ValueError("'from' must be before 'to'")

        interval = request.interval_seconds
        if interval < 0:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "interval_seconds must be >= 0")
            raise ValueError("interval_seconds must be >= 0")

        if interval == 0 and (time_to - time_from) > timedelta(hours=MAX_RAW_RANGE_HOURS):
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"Raw mode limited to {MAX_RAW_RANGE_HOURS}h. Use interval_seconds > 0.",
            )
            raise ValueError("Raw mode range exceeded")

        keys = list(request.keys)
        series: list[collector_pb.KeyReadings] = []

        if interval == 0:
            raw_data = await readings_service.get_readings_raw(
                request.device_id, keys, time_from, time_to
            )
            for key, raw_readings in raw_data.items():
                raw_points = []
                for r in raw_readings:
                    ts = Timestamp()
                    ts.FromDatetime(r.time)
                    raw_points.append(collector_pb.SensorReading(key=r.key, value=r.value, time=ts))
                series.append(collector_pb.KeyReadings(key=key, raw_points=raw_points))
        else:
            agg_data = await readings_service.get_readings_aggregated(
                request.device_id, keys, time_from, time_to, interval
            )
            for key, agg_readings in agg_data.items():
                points = []
                for ar in agg_readings:
                    ts = Timestamp()
                    ts.FromDatetime(ar.time)
                    point = collector_pb.AggregatedReading(time=ts)
                    if ar.avg is not None:
                        point.avg = ar.avg
                    if ar.min is not None:
                        point.min = ar.min
                    if ar.max is not None:
                        point.max = ar.max
                    points.append(point)
                series.append(collector_pb.KeyReadings(key=key, points=points))

        return collector_pb.GetReadingsResponse(series=series)
