import pandas as pd
import pydantic
import pytest
from src.data.sources.CsvSource import CsvSource

file_path_good_file = "tests/files/test_data.csv"
file_path_bad_file = "tests/files/test.csv"


def test_csv_source_without_args():
    with pytest.raises(TypeError):
        CsvSource()


def test_csv_source_with_correct_required_args():
    csvSource = CsvSource(path="path/to/file.csv", index_col="index")
    assert csvSource


def test_csv_source_with_correct_all_args():
    csvSource = CsvSource(path="path/to/file.csv", index_col="index", date_col="datetime")
    assert csvSource


def test_csv_source_immutable_fields():
    csvSource = CsvSource(path="path/to/file.csv", index_col="index")
    with pytest.raises(pydantic.ValidationError):
        csvSource.file_path = "new/path/to/file.csv"
    with pytest.raises(pydantic.ValidationError):
        csvSource.index_col = "new_index"
    with pytest.raises(pydantic.ValidationError):
        csvSource.date_col = "new_date_col"


def test_csv_source_with_incorrect_required_args():
    with pytest.raises(pydantic.ValidationError):
        CsvSource(path=True, index_col=False)


def test_csv_source_fetch_existing_data():
    csvSource = CsvSource(path=file_path_good_file, index_col="datetime")
    data = csvSource.fetch_data()
    assert pd.DataFrame == type(data)


def test_csv_source_fetch_non_existing_data(capfd):
    csvSource = CsvSource(path=file_path_bad_file, index_col="datetime")
    with pytest.raises(SystemExit):
        csvSource.fetch_data()
    out, err = capfd.readouterr()
    assert out == f"[Errno 2] No such file or directory: '{file_path_bad_file}'\n[red]Error while reading file! File not found[/red]\n"


def test_csv_source_fetch_data_within_date_range_and_date_and_index_col_are_the_same():
    csvSource = CsvSource(path=file_path_good_file, index_col="datetime", date_col="datetime")
    data = csvSource.fetch_data_within_date_range(start_date="2023-01-01 01:00:00", end_date="2023-01-01 03:00:00")
    assert pd.DataFrame == type(data)
    assert data.index[0] == pd.Timestamp("2023-01-01 01:00:00")
    assert data.index[-1] == pd.Timestamp("2023-01-01 03:00:00")


def test_csv_source_fetch_data_within_date_range_and_date_and_index_col_are_different():
    csvSource = CsvSource(path=file_path_good_file, index_col=None, date_col="datetime")
    data = csvSource.fetch_data_within_date_range(start_date="2023-01-01 01:00:00", end_date="2023-01-01 03:00:00")
    assert pd.DataFrame == type(data)
    assert data["datetime"][data.index[0]] == "2023-01-01 01:00:00"
    assert data["datetime"][data.index[-1]] == "2023-01-01 03:00:00"


def test_csv_source_fetch_data_within_date_range_wrong_date_col():
    csvSource = CsvSource(path=file_path_good_file, index_col="datetime")
    with pytest.raises(KeyError):
        csvSource.fetch_data_within_date_range(start_date="2023-01-01 01:00:00", end_date="2023-01-01 03:00:00")
