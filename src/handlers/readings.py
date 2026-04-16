import logging
from datetime import UTC, timedelta

import grpc
from dishka import FromDishka
from dishka.integrations.grpcio import inject
from google.protobuf.timestamp_pb2 import Timestamp
from placebrain_contracts import collector_pb2 as collector_pb
from placebrain_contracts.collector_pb2_grpc import CollectorServiceServicer

from src.services.alerts import AlertService
from src.services.alerts_query import AlertRow, AlertsService
from src.services.readings import MAX_RAW_RANGE_HOURS, ReadingsService

logger = logging.getLogger(__name__)


_THRESHOLD_TYPE_TO_PROTO = {
    "min": collector_pb.ALERT_THRESHOLD_TYPE_MIN,
    "max": collector_pb.ALERT_THRESHOLD_TYPE_MAX,
}
_SEVERITY_TO_PROTO = {
    "warning": collector_pb.ALERT_SEVERITY_WARNING,
    "critical": collector_pb.ALERT_SEVERITY_CRITICAL,
}
_STATUS_TO_PROTO = {
    "active": collector_pb.ALERT_STATUS_ACTIVE,
    "resolved": collector_pb.ALERT_STATUS_RESOLVED,
}
_PROTO_TO_STATUS = {v: k for k, v in _STATUS_TO_PROTO.items()}
_PROTO_TO_SEVERITY = {v: k for k, v in _SEVERITY_TO_PROTO.items()}


def _alert_to_proto(row: AlertRow) -> collector_pb.Alert:
    created_ts = Timestamp()
    created_ts.FromDatetime(row.created_at)
    msg = collector_pb.Alert(
        id=row.id,
        sensor_id=row.sensor_id,
        threshold_id=row.threshold_id,
        device_id=row.device_id,
        place_id=row.place_id,
        key=row.key,
        value=row.value,
        threshold_value=row.threshold_value,
        threshold_type=_THRESHOLD_TYPE_TO_PROTO.get(
            row.threshold_type, collector_pb.ALERT_THRESHOLD_TYPE_UNSPECIFIED
        ),
        severity=_SEVERITY_TO_PROTO.get(row.severity, collector_pb.ALERT_SEVERITY_UNSPECIFIED),
        status=_STATUS_TO_PROTO.get(row.status, collector_pb.ALERT_STATUS_UNSPECIFIED),
        created_at=created_ts,
    )
    if row.resolved_at is not None:
        resolved_ts = Timestamp()
        resolved_ts.FromDatetime(row.resolved_at)
        msg.resolved_at.CopyFrom(resolved_ts)
    return msg


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

    @inject
    async def GetAlerts(  # type: ignore[override]
        self,
        request: collector_pb.GetAlertsRequest,
        context: grpc.aio.ServicerContext,
        alerts_service: FromDishka[AlertsService],
    ) -> collector_pb.GetAlertsResponse:
        try:
            status = _PROTO_TO_STATUS.get(request.status) if request.HasField("status") else None
            severity = (
                _PROTO_TO_SEVERITY.get(request.severity) if request.HasField("severity") else None
            )
            sensor_id = request.sensor_id if request.HasField("sensor_id") else None
            device_id = request.device_id if request.HasField("device_id") else None
            time_from = (
                getattr(request, "from").ToDatetime(tzinfo=UTC)
                if request.HasField("from")
                else None
            )
            time_to = request.to.ToDatetime(tzinfo=UTC) if request.HasField("to") else None

            items, total = await alerts_service.list_alerts(
                place_id=request.place_id,
                status=status,
                severity=severity,
                sensor_id=sensor_id,
                device_id=device_id,
                time_from=time_from,
                time_to=time_to,
                page=request.page,
                per_page=request.per_page,
            )
        except ValueError as e:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"Invalid argument: {e}")
            raise

        return collector_pb.GetAlertsResponse(
            items=[_alert_to_proto(it) for it in items],
            total=total,
        )

    @inject
    async def GetAlertCounts(  # type: ignore[override]
        self,
        request: collector_pb.GetAlertCountsRequest,
        context: grpc.aio.ServicerContext,
        alerts_service: FromDishka[AlertsService],
    ) -> collector_pb.GetAlertCountsResponse:
        try:
            per_place, total = await alerts_service.count_unresolved(list(request.place_id))
        except ValueError as e:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"Invalid place_id: {e}")
            raise

        response = collector_pb.GetAlertCountsResponse(total_unresolved=total)
        for place_id, cnt in per_place.items():
            response.unresolved_by_place[place_id] = cnt
        return response

    @inject
    async def ResolveAlert(  # type: ignore[override]
        self,
        request: collector_pb.ResolveAlertRequest,
        context: grpc.aio.ServicerContext,
        alerts_service: FromDishka[AlertsService],
        alert_service: FromDishka[AlertService],
    ) -> collector_pb.ResolveAlertResponse:
        try:
            row = await alerts_service.resolve(request.alert_id)
        except ValueError as e:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"Invalid alert_id: {e}")
            raise

        if row is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, "Alert not found")
            raise RuntimeError("unreachable")

        await alert_service.publish_resolved(
            alert_id=row.id,
            sensor_id=row.sensor_id,
            threshold_id=row.threshold_id,
            device_id=row.device_id,
            place_id=row.place_id,
            key=row.key,
            value=row.value,
            threshold_value=row.threshold_value,
            threshold_type=row.threshold_type,
            severity=row.severity,
            resolved_at=row.resolved_at,
        )
        return collector_pb.ResolveAlertResponse(alert=_alert_to_proto(row))
