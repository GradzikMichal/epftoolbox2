import pandas as pd
import pydantic
import pytest
from src.data.DataPipeline import DataPipeline
from unittest import mock
from src.data.sources.CsvSource import CsvSource
from src.data.sources.EntsoeSource import EntsoeSource


def test_data_pipeline_creation_without_args():
    with pytest.raises(TypeError):
        DataPipeline()


def test_data_pipeline_creation_with_correct_args():
    pipeline = DataPipeline(start_date="2020-01-01", end_date="2020-02-01")
    assert pipeline


def test_data_pipeline_creation_with_wrong_start_date():
    with pytest.raises(pydantic.ValidationError):
        pipeline = DataPipeline(start_date=True, end_date="2020-02-01")


def test_data_pipeline_creation_with_wrong_end_date():
    with pytest.raises(pydantic.ValidationError):
        pipeline = DataPipeline(start_date="2020-02-01", end_date=False)


def test_data_pipeline_creation_with_wrong_start_date_and_end_date():
    with pytest.raises(pydantic.ValidationError):
        pipeline = DataPipeline(start_date=False, end_date=False)


def test_data_pipeline_creation_with_wrong_sources():
    with pytest.raises(pydantic.ValidationError):
        pipeline = DataPipeline(start_date="2020-01-01", end_date="2020-02-01", sources="abc")


def test_data_pipeline_creation_with_wrong_transformations():
    with pytest.raises(pydantic.ValidationError):
        pipeline = DataPipeline(start_date="2020-01-01", end_date="2020-02-01", transformations="abc")


def test_data_pipeline_get_data_no_file(capfd):
    pipeline = DataPipeline(start_date="2020-01-01", end_date="2020-02-01")
    with pytest.raises(SystemExit):
        pipeline.get_data(use_saved=True, file_path="missing")
    out, err = capfd.readouterr()
    assert out == "[Errno 2] No such file or directory: 'missing'\n[red]Error while reading file! File not found[/red]\n"

@pytest.fixture(scope="module")
@mock.patch("src.data.sources.EntsoeSource.EntsoeSource.fetch_data_within_date_range")
@mock.patch("src.data.sources.CsvSource.CsvSource.fetch_data_within_date_range")
@mock.patch("pandas.merge")
def test_data_pipeline_get_data_from_sources(entsoe_mock, csv_mock, merge_mock):
    entsoe_source = EntsoeSource(country_code="PL", api_key="test_api")
    csv_source = CsvSource(file_path="path/to/file.csv", index_col="index", date_col="datetime")
    pipeline = DataPipeline(start_date="2020-01-01", end_date="2020-02-01", sources=[entsoe_source, csv_source])
    result = pipeline.get_data()
    entsoe_mock.assert_called_once()
    csv_mock.assert_called_once()
    assert (merge_mock.call_count == 2)
    assert isinstance(result, pd.DataFrame)



def test_data_pipeline_get_data_from_source_entsoe():
    pipeline = DataPipeline(start_date="2020-01-01", end_date="2020-02-01", sources=[])
    result = pipeline.get_data()
    assert isinstance(result, pd.DataFrame)
    assert pd.DataFrame.empty
