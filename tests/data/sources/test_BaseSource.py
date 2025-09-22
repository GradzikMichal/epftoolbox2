from src.data.sources.BaseSource import BaseSource
from dataclasses import dataclass


def test_fetch_data():
    BaseSource.__abstractmethods__ = set()

    @dataclass
    class DummySource(BaseSource):
        pass

    base = DummySource()
    assert base.fetch_data() is None
    assert base.fetch_data_within_date_range("", "") is None
