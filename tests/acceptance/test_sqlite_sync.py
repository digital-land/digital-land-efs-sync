import pytest
import boto3
from moto import mock_aws
import os

from pathlib import Path

from task.sqlite_sync import CollectionSync

@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"

@pytest.fixture
def s3_client(aws_credentials):
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        yield conn

@pytest.fixture
def testing_bucket(s3_client):
    s3_client.create_bucket(Bucket='testing_bucket')
    return 'testing_bucket'


# @pytest.fixture
# def mnt_dir(tmp_path):
#     os.makedirs(tmp_path / 'mnt')
#     return 'testing_bucket'



def test_run_process_where_hash_json_does_not_exist(s3_client,testing_bucket,tmp_path):
    # upload a file to the bucket (let's just use one that exists for ease)
    test_sqlite_data = Path('tests/data/article-4-direction-area.sqlite3')
    test_sqlite_data_json = Path('tests/data/article-4-direction-area.sqlite3.json')
    key = test_sqlite_data.name
    json_key = test_sqlite_data_json.name
    s3_client.upload_file(test_sqlite_data,testing_bucket,key)
    s3_client.upload_file(test_sqlite_data_json,testing_bucket,json_key)
    

    mnt_dir = tmp_path / 'mnt'
    mnt_dir.mkdir(parents=True,exist_ok=True)
    temp_dir = tmp_path / 'temp'
    temp_dir.mkdir(parents=True,exist_ok=True)
    
    # 
    collection_sync = CollectionSync(mnt_dir=mnt_dir,temp_dir=temp_dir)
    collection_sync.process_object(key, testing_bucket)

    expected_output_paths = [
        mnt_dir / 'datasets' / 'article-4-direction-area.sqlite3',
        # mnt_dir / 'datasets' / 'article-4-direction-area.sqlite3.json',
        mnt_dir / 'datasets' / 'inspect-data-all.json'
    ]

    for expected_output_path in expected_output_paths:
        assert expected_output_path.exists()


# def test_run_process_where_hash_matches(testing_bucket):



# def test_run_process_where_hash_does_not_match(testing_bucket):
