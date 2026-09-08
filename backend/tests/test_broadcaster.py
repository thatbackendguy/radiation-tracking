from app.core.broadcaster import Broadcaster, Subscriber


def _clean(sensor_id: str) -> dict:
    return {"type": "clean", "data": {"sensor_id": sensor_id}}


def _alert(alert_id: str) -> dict:
    return {"type": "alert", "data": {"id": alert_id}}


def test_publish_fans_out_to_all_subscribers():
    broadcaster = Broadcaster(queue_maxsize=10, flush_interval_ms=0)
    s1 = broadcaster.subscribe()
    s2 = broadcaster.subscribe()

    broadcaster.publish(_clean("s-1"))

    assert broadcaster.subscriber_count == 2
    assert s1.drain() == [_clean("s-1")]
    assert s2.drain() == [_clean("s-1")]


def test_unsubscribe_stops_delivery():
    broadcaster = Broadcaster(queue_maxsize=10, flush_interval_ms=0)
    subscriber = broadcaster.subscribe()

    broadcaster.unsubscribe(subscriber)
    broadcaster.publish(_clean("s-1"))

    assert broadcaster.subscriber_count == 0
    assert subscriber.drain() == []


def test_unsubscribe_is_idempotent():
    broadcaster = Broadcaster(queue_maxsize=10, flush_interval_ms=0)
    subscriber = broadcaster.subscribe()

    broadcaster.unsubscribe(subscriber)
    broadcaster.unsubscribe(subscriber)  # second call must not raise

    assert broadcaster.subscriber_count == 0


def test_clean_events_coalesce_latest_wins_per_sensor():
    subscriber = Subscriber(max_pending=10)

    subscriber.offer({"type": "clean", "data": {"sensor_id": "s-1", "cpm": 10}})
    subscriber.offer({"type": "clean", "data": {"sensor_id": "s-1", "cpm": 20}})
    subscriber.offer({"type": "clean", "data": {"sensor_id": "s-2", "cpm": 30}})

    batch = subscriber.drain()
    by_sensor = {m["data"]["sensor_id"]: m["data"]["cpm"] for m in batch}
    assert by_sensor == {"s-1": 20, "s-2": 30}  # s-1 superseded by the newer value
    assert subscriber.coalesced_count == 1


def test_aggregated_events_coalesce_per_bucket():
    subscriber = Subscriber(max_pending=10)

    subscriber.offer({"type": "aggregated", "data": {"geohash": "xn7", "v": 1}})
    subscriber.offer({"type": "aggregated", "data": {"geohash": "xn7", "v": 2}})

    batch = subscriber.drain()
    assert batch == [{"type": "aggregated", "data": {"geohash": "xn7", "v": 2}}]


def test_alerts_are_never_coalesced():
    subscriber = Subscriber(max_pending=10)

    subscriber.offer(_alert("a-1"))
    subscriber.offer(_alert("a-2"))
    subscriber.offer(_alert("a-3"))

    batch = subscriber.drain()
    assert batch == [_alert("a-1"), _alert("a-2"), _alert("a-3")]
    assert subscriber.coalesced_count == 0


def test_clean_backlog_is_bounded_by_sensor_count_not_volume():
    subscriber = Subscriber(max_pending=2)

    # 100 updates across 3 sensors must collapse to 3 pending messages — the
    # coalescing lane is bounded by distinct keys, not the FIFO cap.
    for i in range(100):
        subscriber.offer(_clean(f"s-{i % 3}"))

    assert len(subscriber.drain()) == 3


def test_alert_fifo_cap_sheds_oldest_as_last_resort():
    subscriber = Subscriber(max_pending=2)

    subscriber.offer(_alert("a-1"))
    subscriber.offer(_alert("a-2"))
    subscriber.offer(_alert("a-3"))  # exceeds cap → oldest shed

    batch = subscriber.drain()
    assert batch == [_alert("a-2"), _alert("a-3")]
    assert subscriber.dropped_count == 1


def test_coalesced_lane_is_capped_by_key_count():
    subscriber = Subscriber(max_pending=10, coalesced_max=3)

    # Five distinct sensors, cap of 3 → the two oldest-updated keys are evicted,
    # so the lane never holds more than one entry per key up to the ceiling.
    for i in range(5):
        subscriber.offer(_clean(f"s-{i}"))

    batch = subscriber.drain()
    assert {m["data"]["sensor_id"] for m in batch} == {"s-2", "s-3", "s-4"}
    assert subscriber.dropped_count == 2  # s-0 and s-1 evicted


def test_coalesced_update_refreshes_recency_before_eviction():
    subscriber = Subscriber(max_pending=10, coalesced_max=3)

    subscriber.offer(_clean("s-0"))
    subscriber.offer(_clean("s-1"))
    subscriber.offer(_clean("s-2"))
    subscriber.offer(_clean("s-0"))  # updates s-0 → now most-recently-updated
    subscriber.offer(_clean("s-3"))  # new key over cap → evicts oldest (s-1)

    sensors = {m["data"]["sensor_id"] for m in subscriber.drain()}
    assert sensors == {"s-0", "s-2", "s-3"}  # s-1 evicted, s-0 survived


def test_fifo_alerts_lead_coalesced_state_in_batch():
    subscriber = Subscriber(max_pending=10)

    subscriber.offer(_clean("s-1"))
    subscriber.offer(_alert("a-1"))

    # Alerts (FIFO) are delivered before bulk coalesced state within a flush.
    assert subscriber.drain() == [_alert("a-1"), _clean("s-1")]


def test_drain_clears_pending_state():
    subscriber = Subscriber(max_pending=10)
    subscriber.offer(_clean("s-1"))

    assert subscriber.drain() == [_clean("s-1")]
    assert subscriber.drain() == []  # nothing left after draining
