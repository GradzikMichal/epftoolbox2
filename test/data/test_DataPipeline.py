import pytest

from src.data.DataPipeline import DataPipeline


def test_data_pipeline_creation_without_args():
    with pytest.raises(TypeError):
        pipeline = DataPipeline()


def test_data_pipeline_creation_with_correct_args():
    pipeline = DataPipeline(start_date="2020-01-01", end_date="2020-02-01")
    assert pipeline


def test_data_pipeline_get_data_no_file(capfd):
    pipeline = DataPipeline(start_date="2020-01-01", end_date="2020-02-01")
    with pytest.raises(SystemExit):
        pipeline.get_data(use_saved=True, file_name="missing")
    out, err = capfd.readouterr()
    assert out == "[Errno 2] No such file or directory: 'missing'\nError while reading cached file! File not found\n"
