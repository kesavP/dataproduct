from __future__ import annotations
import json
from typing import Callable, Optional, Type, Union

from kafka import KafkaConsumer

from ..application.ports import DataFrameEngine, DataSource
from ..domain.data_governance import Dataframe
from .pandas_dataframe_engine import PandasDataFrameEngine


class KafkaDataSource(DataSource):
    """
    Adapter: Read a batch of messages from a Kafka topic.

    Connects to Kafka, polls up to `batch_size` messages, deserializes each
    message value (JSON by default), and returns them as a Dataframe.

    Example:
        source = KafkaDataSource(
            bootstrap_servers="localhost:9092",
            topic="orders",
            group_id="data-product-consumer",
        )
        df = source.fetch(PandasDataFrameEngine)
    """

    def __init__(
        self,
        bootstrap_servers: Union[str, list[str]],
        topic: str,
        group_id: str,
        batch_size: int = 100,
        timeout_ms: int = 5000,
        auto_offset_reset: str = "earliest",
        value_deserializer: Optional[Callable[[bytes], dict]] = None,
        _consumer_factory: Optional[Callable] = None,
    ):
        """
        Args:
            bootstrap_servers: Kafka broker address(es), e.g. "localhost:9092"
            topic: Topic to consume from
            group_id: Consumer group id
            batch_size: Maximum number of messages to fetch per call
            timeout_ms: How long to wait for messages before returning (ms)
            auto_offset_reset: Where to start if no committed offset exists
                ("earliest" or "latest")
            value_deserializer: Function to deserialize raw message bytes into a
                dict. Defaults to JSON UTF-8 decoding.
            _consumer_factory: Injectable factory for the Kafka consumer; used
                in tests to avoid a real broker dependency.
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.batch_size = batch_size
        self.timeout_ms = timeout_ms
        self.auto_offset_reset = auto_offset_reset
        self.value_deserializer = value_deserializer or (
            lambda raw: json.loads(raw.decode("utf-8"))
        )
        self._consumer_factory = _consumer_factory or KafkaConsumer

    def fetch(self, dataframe_engine: Type[DataFrameEngine]) -> Dataframe:
        """
        Poll Kafka for up to `batch_size` messages and return them as a Dataframe.

        Args:
            dataframe_engine: Engine to use (PandasDataFrameEngine,
                SparkDataFrameEngine, …)

        Returns:
            Dataframe containing one row per Kafka message
        """
        consumer = self._consumer_factory(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=self.auto_offset_reset,
            enable_auto_commit=True,
        )

        try:
            raw_poll = consumer.poll(
                timeout_ms=self.timeout_ms,
                max_records=self.batch_size,
            )
        finally:
            consumer.close()

        records: list[dict] = []
        for partition_messages in raw_poll.values():
            for msg in partition_messages:
                records.append(self.value_deserializer(msg.value))

        engine = dataframe_engine.create_from_list_of_dict(records)
        return Dataframe(backend_dataframe=engine)
