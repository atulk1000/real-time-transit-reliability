from transit_reliability.streaming.topics import snapshot_key, topic_for_source, topic_partitions, train_position_key


def test_topic_config_matches_v2_partition_plan() -> None:
    assert topic_for_source("train_positions") == "wmata.train_positions.raw"
    assert topic_partitions("train_positions") == 3

    assert topic_for_source("lines") == "wmata.lines.raw"
    assert topic_partitions("lines") == 1

    assert topic_for_source("stations") == "wmata.stations.raw"
    assert topic_partitions("stations") == 1

    assert topic_for_source("standard_routes") == "wmata.standard_routes.raw"
    assert topic_partitions("standard_routes") == 1


def test_train_position_key_uses_train_id_when_available() -> None:
    assert train_position_key({"TrainId": "336"}, "batch-1") == "336"


def test_train_position_key_falls_back_to_batch_id() -> None:
    assert train_position_key({"TrainId": None}, "batch-1") == "batch-1"
    assert train_position_key({}, "batch-1") == "batch-1"


def test_snapshot_key_namespaces_reference_batches() -> None:
    assert snapshot_key("stations", "batch-1") == "stations:batch-1"

