import pytest

from src.Infrastructure.Credentials.environment_credentials_provider import EnvironmentCredentialProvider


def test_reads_username_and_password_from_environment(monkeypatch):
    monkeypatch.setenv("TEST_DB_USER", "alice")
    monkeypatch.setenv("TEST_DB_PASSWORD", "s3cret")

    credentials = EnvironmentCredentialProvider("TEST_DB_USER", "TEST_DB_PASSWORD").get_credentials()

    assert credentials == {"username": "alice", "password": "s3cret"}


def test_missing_variable_raises(monkeypatch):
    monkeypatch.delenv("TEST_DB_USER", raising=False)
    monkeypatch.setenv("TEST_DB_PASSWORD", "s3cret")

    with pytest.raises(ValueError):
        EnvironmentCredentialProvider("TEST_DB_USER", "TEST_DB_PASSWORD").get_credentials()
