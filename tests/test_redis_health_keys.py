"""redis_health_keys helpers."""

from bifrost_core.core.redis_health_keys import redis_hash_field_truthy


def test_redis_hash_field_truthy_connected() -> None:
    assert redis_hash_field_truthy({"connected": "1"}) is True
    assert redis_hash_field_truthy({"connected": "0"}) is False
    assert redis_hash_field_truthy({"connected": 1}) is True
    assert redis_hash_field_truthy({"connected": True}) is True
    assert redis_hash_field_truthy({}) is False
    assert redis_hash_field_truthy({"connected": " true "}) is True
