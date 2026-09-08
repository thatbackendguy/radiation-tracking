import asyncio

from app.core.ring_buffer import RingBuffer


def test_append_and_recent_preserve_insertion_order(make_event):
    buffer = RingBuffer(maxsize=10)

    async def scenario():
        for i in range(3):
            await buffer.append(make_event(sensor_id=f"sensor-{i}"))
        return await buffer.recent(3)

    events = asyncio.run(scenario())
    assert [e.sensor_id for e in events] == ["sensor-0", "sensor-1", "sensor-2"]


def test_overflow_evicts_oldest(make_event):
    buffer = RingBuffer(maxsize=3)

    async def scenario():
        for i in range(5):
            await buffer.append(make_event(sensor_id=f"sensor-{i}"))
        return await buffer.recent(10)

    events = asyncio.run(scenario())
    assert [e.sensor_id for e in events] == ["sensor-2", "sensor-3", "sensor-4"]


def test_recent_limit_smaller_than_occupancy_returns_newest(make_event):
    buffer = RingBuffer(maxsize=10)

    async def scenario():
        for i in range(5):
            await buffer.append(make_event(sensor_id=f"sensor-{i}"))
        return await buffer.recent(2)

    events = asyncio.run(scenario())
    assert [e.sensor_id for e in events] == ["sensor-3", "sensor-4"]


def test_recent_limit_larger_than_occupancy_returns_all(make_event):
    buffer = RingBuffer(maxsize=10)

    async def scenario():
        await buffer.append(make_event())
        return await buffer.recent(100)

    assert len(asyncio.run(scenario())) == 1


def test_recent_with_nonpositive_limit_returns_empty(make_event):
    buffer = RingBuffer(maxsize=10)

    async def scenario():
        await buffer.append(make_event())
        return await buffer.recent(0)

    assert asyncio.run(scenario()) == []


def test_len_reports_occupancy(make_event):
    buffer = RingBuffer(maxsize=3)
    assert len(buffer) == 0

    async def scenario():
        for _ in range(5):
            await buffer.append(make_event())

    asyncio.run(scenario())
    assert len(buffer) == 3


def test_page_without_cursor_returns_newest_page(make_event):
    buffer = RingBuffer(maxsize=10)

    async def scenario():
        for i in range(5):
            await buffer.append(make_event(sensor_id=f"sensor-{i}"))
        return await buffer.page(cursor=None, limit=2)

    page = asyncio.run(scenario())
    assert [e.sensor_id for e in page.events] == ["sensor-3", "sensor-4"]
    assert page.has_more is True
    assert page.next_cursor is not None


def test_page_cursor_walks_backwards_through_window(make_event):
    buffer = RingBuffer(maxsize=10)

    async def scenario():
        for i in range(5):
            await buffer.append(make_event(sensor_id=f"sensor-{i}"))
        first = await buffer.page(cursor=None, limit=2)
        second = await buffer.page(cursor=first.next_cursor, limit=2)
        third = await buffer.page(cursor=second.next_cursor, limit=2)
        return first, second, third

    first, second, third = asyncio.run(scenario())
    assert [e.sensor_id for e in first.events] == ["sensor-3", "sensor-4"]
    assert [e.sensor_id for e in second.events] == ["sensor-1", "sensor-2"]
    assert [e.sensor_id for e in third.events] == ["sensor-0"]
    # The oldest page exhausts the window.
    assert third.has_more is False
    assert third.next_cursor is None


def test_page_last_page_reports_no_more(make_event):
    buffer = RingBuffer(maxsize=10)

    async def scenario():
        for i in range(3):
            await buffer.append(make_event(sensor_id=f"sensor-{i}"))
        return await buffer.page(cursor=None, limit=10)

    page = asyncio.run(scenario())
    assert [e.sensor_id for e in page.events] == ["sensor-0", "sensor-1", "sensor-2"]
    assert page.has_more is False
    assert page.next_cursor is None


def test_page_empty_buffer_returns_empty_page(make_event):
    buffer = RingBuffer(maxsize=10)
    page = asyncio.run(buffer.page(cursor=None, limit=10))
    assert page.events == []
    assert page.has_more is False
    assert page.next_cursor is None


def test_page_nonpositive_limit_returns_empty_page(make_event):
    buffer = RingBuffer(maxsize=10)

    async def scenario():
        await buffer.append(make_event())
        return await buffer.page(cursor=None, limit=0)

    page = asyncio.run(scenario())
    assert page.events == []
    assert page.has_more is False


def test_page_cursor_below_window_returns_empty(make_event):
    buffer = RingBuffer(maxsize=10)

    async def scenario():
        for i in range(3):
            await buffer.append(make_event(sensor_id=f"sensor-{i}"))
        # seq 0 is the oldest entry; nothing is strictly older than it.
        return await buffer.page(cursor=0, limit=10)

    page = asyncio.run(scenario())
    assert page.events == []
    assert page.has_more is False
    assert page.next_cursor is None
