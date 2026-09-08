import asyncio
import secrets
from datetime import datetime
from typing import Annotated, Optional

from fastapi import APIRouter, Body, Depends, Header, HTTPException, status
from pydantic import BaseModel, Field, field_serializer, model_validator

from app.core.kafka_producer import ConfigPublishError, _producer
from app.core.settings import settings

router = APIRouter(prefix="/config", tags=["config"])


def require_write_auth(
    x_config_token: Annotated[Optional[str], Header()] = None,
) -> None:
    """Gate mutating config writes behind an optional shared token (finding (e)).

    POST /config rewrites the global thresholds/area/timespan that drive the whole
    pipeline. When ``settings.config_write_token`` is unset (default) this is a no-op,
    so local dev is unchanged. On the public droplet the ``.env`` sets a token and every
    write must carry a matching ``X-Config-Token`` header, so an unauthenticated caller
    can no longer rewrite the active config. Compared in constant time to avoid leaking
    the token via response timing. Defence-in-depth alongside the reverse-proxy gate.
    """
    expected = settings.config_write_token
    if not expected:
        return
    if x_config_token is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="X-Config-Token header required",
        )
    if not secrets.compare_digest(x_config_token, expected):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="invalid config write token",
        )


class AreaFilter(BaseModel):
    min_lat: Annotated[float, Field(ge=-90.0, le=90.0, examples=[34.0])]
    max_lat: Annotated[float, Field(ge=-90.0, le=90.0, examples=[38.5])]
    min_lon: Annotated[float, Field(ge=-180.0, le=180.0, examples=[138.0])]
    max_lon: Annotated[float, Field(ge=-180.0, le=180.0, examples=[142.0])]

    @model_validator(mode="after")
    def lat_lon_order(self) -> "AreaFilter":
        if self.min_lat >= self.max_lat:
            raise ValueError("min_lat must be less than max_lat")
        if self.min_lon >= self.max_lon:
            raise ValueError("min_lon must be less than max_lon")
        return self


class TimespanFilter(BaseModel):
    start: Annotated[datetime, Field(examples=["2011-03-11T00:00:00+00:00"])]
    end: Annotated[datetime, Field(examples=["2011-04-30T23:59:59+00:00"])]

    @model_validator(mode="after")
    def time_order(self) -> "TimespanFilter":
        if self.start >= self.end:
            raise ValueError("start must be before end")
        return self

    @field_serializer("start", "end")
    def _serialize_dt(self, value: datetime) -> str:
        # Emit an offset form (...+00:00) rather than a trailing 'Z': the Flink
        # consumer's ConfigUpdate.from_dict uses datetime.fromisoformat, which
        # rejects 'Z' on Python < 3.11.
        return value.isoformat()


class ConfigPayload(BaseModel):
    """Partial update — omitted fields keep their current value."""

    cpm_warn_threshold: Optional[float] = Field(default=None, gt=0, examples=[100.0])
    cpm_danger_threshold: Optional[float] = Field(default=None, gt=0, examples=[1000.0])
    area: Optional[AreaFilter] = None
    timespan: Optional[TimespanFilter] = None

    @model_validator(mode="after")
    def threshold_order(self) -> "ConfigPayload":
        warn = self.cpm_warn_threshold
        danger = self.cpm_danger_threshold
        if warn is not None and danger is not None and warn >= danger:
            raise ValueError("cpm_warn_threshold must be less than cpm_danger_threshold")
        return self


class ConfigResponse(BaseModel):
    cpm_warn_threshold: float
    cpm_danger_threshold: float
    area: Optional[AreaFilter] = None
    timespan: Optional[TimespanFilter] = None


class ConfigUpdateResponse(BaseModel):
    status: str
    config: ConfigResponse


# In-memory active config — replaced by Kafka broadcast state in Week 3+
_active_config = ConfigResponse(
    cpm_warn_threshold=settings.cpm_warn_threshold,
    cpm_danger_threshold=settings.cpm_danger_threshold,
    area=None,
    timespan=None,
)

# Serializes the read-modify-write cycle on _active_config so concurrent partial
# updates can't interleave between merge, publish and commit (see update_config).
_config_lock = asyncio.Lock()


@router.get(
    "",
    response_model=ConfigResponse,
    summary="Read the active filter settings",
)
async def get_config() -> ConfigResponse:
    return _active_config


@router.post(
    "",
    response_model=ConfigUpdateResponse,
    status_code=202,
    dependencies=[Depends(require_write_auth)],
    summary="Update filter settings and publish them to Flink",
    description=(
        "Merges the partial payload onto the active config, validates the merged "
        "state, and publishes the **full merged** settings to the `config.updates` "
        "Kafka topic (Flink broadcast state). The change is visible on the map "
        "within seconds. Publish-before-commit: on broker failure nothing changes. "
        "On the droplet this write is gated by an `X-Config-Token` header when "
        "`CONFIG_WRITE_TOKEN` is set (no-op locally)."
    ),
    responses={
        401: {"description": "`X-Config-Token` header required (droplet: token configured)."},
        403: {"description": "`X-Config-Token` did not match the configured write token."},
        422: {
            "description": (
                "Validation failed — thresholds must satisfy warn < danger (also "
                "re-checked on the merged state), lat/lon ranges need min < max, "
                "and a timespan needs start < end."
            )
        },
        503: {
            "description": (
                "`config.updates` topic unavailable — the settings were NOT "
                "committed; backend and Flink still agree on the previous state."
            )
        },
    },
)
async def update_config(
    payload: Annotated[ConfigPayload, Body()],
) -> ConfigUpdateResponse:
    global _active_config

    # Hold the lock across the whole merge → validate → publish → commit cycle. Without it,
    # two concurrent partial updates could each model_copy the same base and interleave their
    # publish/commit, leaving the backend and Flink with mismatched state (last-write-wins).
    async with _config_lock:
        # Build update dict from model instances, not dicts, so model_copy receives the right types.
        update_fields = {
            k: v
            for k, v in {
                "cpm_warn_threshold": payload.cpm_warn_threshold,
                "cpm_danger_threshold": payload.cpm_danger_threshold,
                "area": payload.area,
                "timespan": payload.timespan,
            }.items()
            if v is not None
        }
        updated = _active_config.model_copy(update=update_fields)

        # Validate merged state — payload validator only fires when both fields are present in the
        # same request, so a two-step partial update can still break the invariant without this check.
        if updated.cpm_warn_threshold >= updated.cpm_danger_threshold:
            raise HTTPException(
                status_code=422,
                detail="cpm_warn_threshold must be less than cpm_danger_threshold",
            )

        # Publish the FULL merged state (not just the changed fields) — the Flink consumer's
        # ConfigUpdate.from_dict() requires both cpm_warn_threshold and cpm_danger_threshold
        # on every message. area/timespan are omitted when unset (optional in the schema).
        wire = updated.model_dump(mode="json", exclude_none=True)
        try:
            await _producer.publish(wire)
        except ConfigPublishError as exc:
            # Strict, atomic failure: do not commit locally if the publish fails, so the backend
            # and Flink never silently disagree about the active settings.
            raise HTTPException(
                status_code=503,
                detail="config.updates topic unavailable; settings unchanged",
            ) from exc

        # Commit only after a successful publish.
        _active_config = updated
        return ConfigUpdateResponse(status="accepted", config=_active_config)
