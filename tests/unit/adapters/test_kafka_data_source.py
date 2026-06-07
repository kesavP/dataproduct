from typing import Optional, Type
from unittest.mock import MagicMock

import pytest

from retail_data_product.adapters import KafkaDataSource, PandasDataFrameEngine
from retail_data_product.application.ports import DataFrameEngine, DataSource
from retail_data_product.domain.data_governance import Schema
from tests.templates.ports.test_data_sources import DataSourceTestTemplate


def _make_consumer_factory(messages: list[dict]) -> MagicMock:
    """Return a factory that yields a mock KafkaConsumer pre-loaded with messages."""

    def _msg(value: dict) -> MagicMock:
        m = MagicMock()
        m.value = value
        return m

    consumer = MagicMock()
    consumer.poll.return_value = {"partition-0": [_msg(v) for v in messages]}
    consumer.close.return_value = None

    factory = MagicMock(return_value=consumer)
    return factory


class TestKafkaDataSource(DataSourceTestTemplate):
    """Unit tests for KafkaDataSource using a mock Kafka consumer."""

    def create_data_source_with_data(
        self, data: list[dict], schema: Optional[Schema] = None
    ) -> DataSource:
        # Bypass real Kafka: inject a consumer factory that returns `data`
        # as pre-deserialized dicts so the default deserializer is skipped.
        factory = _make_consumer_factory(data)
        return KafkaDataSource(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            group_id="test-group",
            # value_deserializer receives whatever msg.value is — since the
            # mock already stores dicts, pass through without JSON decoding.
            value_deserializer=lambda v: v,
            _consumer_factory=factory,
        )

    def get_dataframe_engine(self) -> Type[DataFrameEngine]:
        return PandasDataFrameEngine

    def test_consumer_is_closed_after_fetch(self):
        """Consumer must be closed even when poll returns no messages."""
        factory = _make_consumer_factory([])
        source = KafkaDataSource(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            group_id="test-group",
            value_deserializer=lambda v: v,
            _consumer_factory=factory,
        )
        source.fetch(PandasDataFrameEngine)

        factory.return_value.close.assert_called_once()

    def test_consumer_is_closed_on_poll_error(self):
        """Consumer must be closed even if poll raises."""
        consumer = MagicMock()
        consumer.poll.side_effect = RuntimeError("broker unavailable")
        factory = MagicMock(return_value=consumer)

        source = KafkaDataSource(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            group_id="test-group",
            value_deserializer=lambda v: v,
            _consumer_factory=factory,
        )
        with pytest.raises(RuntimeError):
            source.fetch(PandasDataFrameEngine)

        consumer.close.assert_called_once()

    def test_custom_value_deserializer(self):
        """A custom deserializer is applied to each raw message value."""
        raw_messages = [b'{"id": 1}', b'{"id": 2}']
        consumer = MagicMock()
        consumer.poll.return_value = {
            "partition-0": [MagicMock(value=v) for v in raw_messages]
        }
        factory = MagicMock(return_value=consumer)

        import json

        source = KafkaDataSource(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            group_id="test-group",
            value_deserializer=lambda v: json.loads(v),
            _consumer_factory=factory,
        )
        result = source.fetch(PandasDataFrameEngine)
        collected = result.collect()

        assert len(collected) == 2
        assert collected[0]["id"] == 1
        assert collected[1]["id"] == 2

    def test_consumer_created_with_correct_config(self):
        """KafkaConsumer is instantiated with the topic and config passed in."""
        factory = _make_consumer_factory([])
        source = KafkaDataSource(
            bootstrap_servers="broker:9092",
            topic="orders",
            group_id="orders-group",
            auto_offset_reset="latest",
            value_deserializer=lambda v: v,
            _consumer_factory=factory,
        )
        source.fetch(PandasDataFrameEngine)

        factory.assert_called_once_with(
            "orders",
            bootstrap_servers="broker:9092",
            group_id="orders-group",
            auto_offset_reset="latest",
            enable_auto_commit=True,
        )
