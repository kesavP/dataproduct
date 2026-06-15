import pytest

from retail_data_product.adapters import PandasDataFrameEngine
from retail_data_product.domain.data_governance import (
    Dataframe,
    DataQualityStatus,
    DataQualityChecker,
)
from retail_data_product.domain.retail.orders import OrderIdNotNull, ORDERS_RAW


class TestOrderIdNotNull:
    def test_order_id_not_null_rule_passes_when_valid(self):
        # Given
        df = Dataframe(
            PandasDataFrameEngine.create_from_list_of_dict(
                [{"order_id": 1}, {"order_id": 2}]
            )
        )

        # When/Then
        assert (
            OrderIdNotNull().evaluate_dataframe(df).status == DataQualityStatus.PASSED
        )

    def test_order_id_not_null_rule_fails_on_missing_column(self):
        # Given
        df = Dataframe(
            PandasDataFrameEngine.create_from_list_of_dict([{"other_column": 1}])
        )

        # When/Then
        assert (
            OrderIdNotNull().evaluate_dataframe(df).status == DataQualityStatus.FAILED
        )

    def test_order_id_not_null_rule_fails_on_null_value(self):
        # Given
        df = Dataframe(
            PandasDataFrameEngine.create_from_list_of_dict(
                [{"order_id": 1}, {"order_id": None}]
            )
        )

        # When/Then
        assert (
            OrderIdNotNull().evaluate_dataframe(df).status == DataQualityStatus.FAILED
        )


class TestOrderTableEvaluation:
    def test_orders_raw_passes_valid_data(self):
        # Given
        df = Dataframe(
            PandasDataFrameEngine.create_from_list_of_dict(
                [{"order_id": 123}, {"order_id": 456}]
            )
        )

        # When/Then
        report = DataQualityChecker.check(df, [OrderIdNotNull()], ORDERS_RAW.name)
        report.assert_no_critical_dq_are_failed()

    def test_orders_raw_fails_on_invalid_data(self):
        # Given
        df = Dataframe(
            PandasDataFrameEngine.create_from_list_of_dict(
                [{"order_id": 123}, {"order_id": None}]
            )
        )

        # When/Then
        report = DataQualityChecker.check(df, [OrderIdNotNull()], ORDERS_RAW.name)
        with pytest.raises(AssertionError):
            report.assert_no_critical_dq_are_failed()
