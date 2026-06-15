from abc import abstractmethod
from dataclasses import dataclass, field
from enum import Enum
import datetime

from ..entities.dataframe import Dataframe


# =============================================================================
# Enums
# =============================================================================


class DataQualityStatus(Enum):
    """Status of a data quality evaluation."""

    FAILED = 0
    PASSED = 1


class DQCriticity(Enum):
    """Criticality level of a data quality rule."""

    WARNING = "warning"
    CRITICAL = "critical"


class DQActionOnFail(Enum):
    """Action to take when a data quality rule fails."""

    ABORT_WORKFLOW = "abort_worklow"
    REMOVE_ROW = "remove_row"
    FLAG = "flag"
    LOG = "log"


# =============================================================================
# Data Quality Result
# =============================================================================


@dataclass(frozen=True)
class DataQualityResult:
    """
    Result of a data quality rule evaluation.

    This is a value object that captures the outcome of applying
    a data quality rule to a dataframe at a specific point in time.

    Attributes:
        dqr_name (str): Name of the data quality rule that was evaluated
        status (DataQualityStatus): Result status of the data quality
        action_fail (DQActionOnFail): Instructs what to do if the status is failing
        dq_criticiy (DQCriticity): Informs on the data quality criticity that was evaluated
        message (str): Detailed explanation of the data quality evaluation
        evaluation_datetime (datetime.datetime): Timestamp corresponding on when the DQ was evaluated
    """

    dqr_name: str
    status: DataQualityStatus
    action_fail: DQActionOnFail
    dq_criticiy: DQCriticity
    message: str
    evaluation_datetime: datetime.datetime = field(
        default_factory=datetime.datetime.now
    )


# =============================================================================
# Data Quality Rules
# =============================================================================


@dataclass(frozen=True)
class DataQualityRule:
    """
    Value Object representing a data quality rule definition.

    Rules are immutable definitions of quality checks. They define:
    - What to check (via evaluate_dataframe)
    - How critical the check is
    - What action to take on failure

    Attributes:
        dqr_id (str): Unique identifier for the data quality rule
        criticity (DQCriticity): Criticity level of this rule
        action_on_fail (DQActionOnFail): Action to take if rule fails
    """

    dqr_id: str
    criticity: DQCriticity
    action_on_fail: DQActionOnFail

    @abstractmethod
    def evaluate_dataframe(self, dataframe: Dataframe) -> DataQualityResult:
        """
        Evaluate the dataframe and return the associated data quality result.

        Args:
            dataframe: The dataframe to evaluate

        Returns:
            DataQualityResult: The result of the evaluation
        """


@dataclass(frozen=True)
class FieldIsNotNullRule(DataQualityRule):
    """
    Ensures that every field element has a non-null value.

    Attributes:
        field_name (str): Field name to inspect
    """

    field_name: str

    def evaluate_dataframe(self, dataframe: Dataframe) -> DataQualityResult:
        if self.field_name not in dataframe.get_column_names():
            return DataQualityResult(
                dqr_name=self.dqr_id,
                status=DataQualityStatus.FAILED,
                action_fail=self.action_on_fail,
                dq_criticiy=self.criticity,
                message=f"{self.field_name} was not an existing field",
            )
        elif not dataframe.colums_contains(self.field_name, None):
            return DataQualityResult(
                dqr_name=self.dqr_id,
                status=DataQualityStatus.PASSED,
                action_fail=self.action_on_fail,
                dq_criticiy=self.criticity,
                message=f"{self.field_name} has no NULL values",
            )
        else:
            return DataQualityResult(
                dqr_name=self.dqr_id,
                status=DataQualityStatus.FAILED,
                action_fail=self.action_on_fail,
                dq_criticiy=self.criticity,
                message=f"{self.field_name} has at least one NULL value",
            )
