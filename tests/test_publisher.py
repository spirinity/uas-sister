from publisher_app.publisher import build_events, chunks


def test_publisher_generates_expected_total_events():
    events = build_events(total_events=100, duplicate_rate=0.30)
    assert len(events) == 100


def test_publisher_generates_duplicates():
    events = build_events(total_events=100, duplicate_rate=0.30)
    keys = [(event["topic"], event["event_id"]) for event in events]
    assert len(set(keys)) < len(keys)


def test_chunks_splits_items_by_size():
    assert list(chunks([1, 2, 3, 4, 5], 2)) == [[1, 2], [3, 4], [5]]
