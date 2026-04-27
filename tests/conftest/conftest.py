import shutil
import tempfile

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("mlops-pipeline-tests")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def temp_dir():
    directory = tempfile.mkdtemp()
    yield directory
    shutil.rmtree(directory, ignore_errors=True)