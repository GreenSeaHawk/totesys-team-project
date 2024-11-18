import pytest
import os
import io
import boto3
import pandas as pd
from moto import mock_aws
from src.dbconnection import get_db_credentials, connect_to_db

@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def rds(aws_credentials):
    """
    Return a mocked RDS client
    """
    with mock_aws():
        yield boto3.client("rds", region_name="eu-west-2")


@pytest.fixture(scope="function")
def mocked_aws(aws_credentials):
    """
    Mock all AWS interactions
    Requires you to create your own boto3 clients
    """
    with mock_aws():
        yield

@pytest.fixture(scope="function")
def create_rds(rds):
    rds.create_db_instance(
        DBName='test-database',
        MasterUsername='username',
        MasterUserPassword='password',
        Engine='postgres',
        DBInstanceIdentifier='test-database',
        DBInstanceClass='db.m5d.large',
        EngineVersion='16.3',
        Port=5432
    )

# class TestMockFixtures:



