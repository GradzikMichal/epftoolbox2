from src.data.transformations.BaseTransformation import BaseTransformation
from dataclasses import dataclass


def test_fetch_data():
    BaseTransformation.__abstractmethods__ = set()

    @dataclass
    class DummySource(BaseTransformation):
        pass

    base = DummySource()
    assert base.transform(None) is None
