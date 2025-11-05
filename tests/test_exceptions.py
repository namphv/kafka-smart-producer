"""Tests for exception hierarchy."""

from kafka_smart_producer.exceptions import (
    CacheError,
    ConfigurationError,
    HealthError,
    LagDataUnavailableError,
    PartitionSelectionError,
    SmartProducerError,
)


class TestExceptionHierarchy:
    def test_all_inherit_from_base(self):
        for exc_cls in [
            ConfigurationError,
            LagDataUnavailableError,
            HealthError,
            CacheError,
            PartitionSelectionError,
        ]:
            assert issubclass(exc_cls, SmartProducerError)

    def test_base_with_context(self):
        err = SmartProducerError("msg", cause=ValueError("inner"), context={"k": "v"})
        assert str(err) == "msg"
        assert isinstance(err.cause, ValueError)
        assert err.context == {"k": "v"}

    def test_base_defaults(self):
        err = SmartProducerError("msg")
        assert err.cause is None
        assert err.context == {}

    def test_catchable_as_exception(self):
        try:
            raise LagDataUnavailableError("no data")
        except SmartProducerError as e:
            assert "no data" in str(e)
