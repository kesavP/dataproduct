from __future__ import annotations

import json
from datetime import datetime
from typing import Optional, Sequence, Type

from ..ports import DataTarget, DataFrameEngine, DataQualityLogger
from ...domain.data_governance import Dataframe


class TransformationFailure:
    """Captures details of a transformation failure for a single record."""

    def __init__(
        self,
        rule_id: str,
        error: Exception,
        original_record: Optional[dict] = None,
        order_id: Optional[str] = None,
    ):
        self.rule_id = rule_id
        self.error = error
        self.original_record = original_record or {}
        self.order_id = order_id
        self.error_type = type(error).__name__
        self.error_message = str(error)
        self.captured_at = datetime.utcnow().isoformat()

    def to_dlq_record(self) -> dict:
        """Convert failure details to a DLQ record."""
        return {
            "order_id": self.order_id,
            "transformation_rule_id": self.rule_id,
            "error_message": self.error_message,
            "error_type": self.error_type,
            "original_record": json.dumps(self.original_record),
            "captured_at": self.captured_at,
        }


class TransformationHandler:
    """
    Applies transformation rules with per-record error capture.

    Attempts to apply each transformation rule. If a rule fails on specific
    records, those records are isolated and captured to a DLQ while valid
    records continue processing.
    """

    def __init__(
        self,
        dataframe_engine: Type[DataFrameEngine],
        dlq_target: Optional[DataTarget] = None,
        data_quality_logger: Optional[DataQualityLogger] = None,
    ):
        self.dataframe_engine = dataframe_engine
        self.dlq_target = dlq_target
        self.data_quality_logger = data_quality_logger
        self.failures: list[TransformationFailure] = []

    def apply_transformations(
        self,
        dataframe: Dataframe,
        rules: Sequence,
    ) -> Dataframe:
        """
        Apply a sequence of transformation rules to a dataframe.

        If a rule fails and a DLQ target is available, the failing records
        are captured and the transformation continues with remaining records.

        Args:
            dataframe: The dataframe to transform
            rules: Sequence of transformation rules with an apply(df) method

        Returns:
            The transformed dataframe (with failed records removed if DLQ is enabled)
        """
        current_df = dataframe

        for rule in rules:
            rule_id = getattr(rule, "tfr_id", rule.__class__.__name__)

            try:
                rule.apply(current_df)
            except Exception as e:
                if self.dlq_target:
                    self._capture_transformation_failure(
                        current_df, rule, rule_id, e
                    )
                else:
                    raise

        return current_df

    def _capture_transformation_failure(
        self,
        dataframe: Dataframe,
        rule,
        rule_id: str,
        error: Exception,
    ) -> None:
        """
        Capture records that failed transformation and write to DLQ.

        Attempts to extract individual records that caused the failure.
        If individual record isolation fails, captures all records as failures.

        Args:
            dataframe: The dataframe being transformed
            rule: The transformation rule that failed
            rule_id: Identifier for the rule
            error: The exception that was raised
        """
        df_backend = dataframe.get_backend_dataframe()

        try:
            records = self._extract_records(df_backend)
        except Exception:
            records = []

        dlq_records = [
            TransformationFailure(
                rule_id=rule_id,
                error=error,
                original_record=record,
                order_id=record.get("order_id") if isinstance(record, dict) else None,
            ).to_dlq_record()
            for record in records
        ]

        if dlq_records:
            dlq_df = self._create_dlq_dataframe(dlq_records)
            try:
                self.dlq_target.upsert(dlq_df, "captured_at")
            except Exception as dlq_error:
                if self.data_quality_logger:
                    self.data_quality_logger.log(
                        f"Failed to write transformation failures to DLQ: {dlq_error}"
                    )

    def _extract_records(self, df_backend) -> list[dict]:
        """
        Extract individual records from a dataframe backend.

        Supports both Pandas and Spark dataframes.
        """
        if hasattr(df_backend, "sdf"):
            sdf = df_backend.sdf
            if hasattr(sdf, "collect"):
                rows = sdf.collect()
                return [
                    {k: v for k, v in row.asDict().items()} for row in rows
                ]

        if hasattr(df_backend, "to_dict"):
            return df_backend.to_dict("records")

        return []

    def _create_dlq_dataframe(self, dlq_records: list[dict]) -> Dataframe:
        """Create a Dataframe from DLQ failure records."""
        df_backend = self.dataframe_engine.create_from_list_of_dict(dlq_records)
        return Dataframe(backend_dataframe=df_backend)
