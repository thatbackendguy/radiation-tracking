"""GET /health — liveness plus per-component status.

Always returns HTTP 200 while the app is serving: the Dockerfile HEALTHCHECK
and the compose `service_healthy` gate on flink-job depend on that contract
(degraded mode must not restart-loop the stack). Degradation is reported in the
body instead, so operators and the droplet smoke test can still see it.
"""

from typing import Literal

from fastapi import APIRouter
from pydantic import BaseModel

from app.core import kafka_consumer
from app.core.broadcaster import _broadcaster
from app.core.kafka_producer import _producer

router = APIRouter()

# Lifecycle states of the Kafka components. "degraded" means the component
# wanted to start but the broker was unreachable; "disabled" is deliberate
# (env toggle) and therefore still healthy.
ComponentStatus = Literal["running", "available", "degraded", "disabled", "stopped"]


class HealthResponse(BaseModel):
    status: Literal["ok", "degraded"]
    service: str
    kafka_consumer: ComponentStatus
    config_producer: ComponentStatus
    ws_clients: int


@router.get(
    "/health",
    response_model=HealthResponse,
    tags=["health"],
    summary="Liveness + per-component status",
    description=(
        "Always HTTP 200 while the app is serving (the image HEALTHCHECK and the "
        "compose `service_healthy` gate depend on it); a Kafka component that "
        "could not reach the broker is reported as `degraded` in the body."
    ),
)
async def health_check() -> HealthResponse:
    consumer_status = kafka_consumer.status()
    producer_status = _producer.status
    degraded = "degraded" in (consumer_status, producer_status)
    return HealthResponse(
        status="degraded" if degraded else "ok",
        service="backend",
        kafka_consumer=consumer_status,
        config_producer=producer_status,
        ws_clients=_broadcaster.subscriber_count,
    )
