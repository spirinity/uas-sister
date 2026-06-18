from aggregator_app.database import SCHEMA_SQL


def test_schema_has_processed_events_unique_constraint():
    assert "UNIQUE (topic, event_id)" in SCHEMA_SQL
