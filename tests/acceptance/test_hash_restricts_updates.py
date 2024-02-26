import pytest
import boto3
import json
import os
import shutil

from moto import mock_aws
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

def test_run_process_where_hash_json_does_not_exist(s3_client,testing_bucket,tmp_path):
    # upload a file to the bucket (let's just use one that exists for ease)
    test_sqlite_data = Path('tests/data/central-activities-zone.sqlite3')
    test_sqlite_data_json = Path('tests/data/central-activities-zone.sqlite3.json')
    key = test_sqlite_data.name
    json_key = test_sqlite_data_json.name
    s3_client.upload_file(test_sqlite_data,testing_bucket,key)
    s3_client.upload_file(test_sqlite_data_json,testing_bucket,json_key)
    

    mnt_dir = tmp_path / 'mnt'
    mnt_dir.mkdir(parents=True,exist_ok=True)
    temp_dir = tmp_path / 'temp'
    temp_dir.mkdir(parents=True,exist_ok=True)
    hash_dir = mnt_dir/ 'datasets' / 'hashes'
    
    # 
    collection_sync = CollectionSync(mnt_dir=mnt_dir,temp_dir=temp_dir)
    collection_sync.process_object(key, testing_bucket)

    expected_output_paths = [
        mnt_dir / 'datasets' / test_sqlite_data.name,
        mnt_dir / 'datasets' / 'inspect-data-all.json',
        hash_dir / f'{test_sqlite_data.stem}.json'
    ]

    for expected_output_path in expected_output_paths:
        assert expected_output_path.exists()


def test_run_process_where_hash_matches(s3_client,testing_bucket,tmp_path):
    test_sqlite_data = Path('tests/data/central-activities-zone.sqlite3')
    test_sqlite_data_json = Path('tests/data/central-activities-zone.sqlite3.json')
    key = test_sqlite_data.name
    json_key = test_sqlite_data_json.name
    s3_client.upload_file(test_sqlite_data,testing_bucket,key)
    s3_client.upload_file(test_sqlite_data_json,testing_bucket,json_key)
    

    mnt_dir = tmp_path / 'mnt'
    mnt_dir.mkdir(parents=True,exist_ok=True)
    temp_dir = tmp_path / 'temp'
    temp_dir.mkdir(parents=True,exist_ok=True)
    hash_dir = mnt_dir/ 'datasets' / 'hashes'
    hash_dir.mkdir(parents=True,exist_ok=True)
    dataset_dir = mnt_dir / 'datasets'
    dataset_dir.mkdir(parents=True,exist_ok=True)

    # add sqlite to mounted drive
    shutil.copy(test_sqlite_data,dataset_dir / test_sqlite_data.name)
    current_sqlite_mtime = os.stat(dataset_dir / test_sqlite_data.name).st_mtime

    # add hash to file
    hash_path = hash_dir / f'{test_sqlite_data.stem}.json'
    hash_dict = {'hash':'25f3a5bcc31bf2cf991d636fe2fe36ea8f9fe162'}
    with open(hash_path, 'w') as file:
            file.write(json.dumps(hash_dict))

    # run process
    collection_sync = CollectionSync(mnt_dir=mnt_dir,temp_dir=temp_dir)
    collection_sync.process_object(key, testing_bucket)

    expected_output_paths = [
        mnt_dir / 'datasets' / test_sqlite_data.name,
        hash_dir / f'{test_sqlite_data.stem}.json'
    ]

    for expected_output_path in expected_output_paths:
        assert expected_output_path.exists(), f'expected output file {expected_output_path.name} is missing'

    # check modification date hasn't changed betweent he sqlites
    assert os.stat(mnt_dir / 'datasets' / test_sqlite_data.name).st_mtime == current_sqlite_mtime, 'sqlite file was modified implying the hash didnt stop it' 


def test_run_process_where_hash_does_not_match(testing_bucket,tmp_path,s3_client):
    test_sqlite_data = Path('tests/data/central-activities-zone.sqlite3')
    test_sqlite_data_json = Path('tests/data/central-activities-zone.sqlite3.json')
    key = test_sqlite_data.name
    json_key = test_sqlite_data_json.name
    s3_client.upload_file(test_sqlite_data,testing_bucket,key)
    s3_client.upload_file(test_sqlite_data_json,testing_bucket,json_key)
    

    mnt_dir = tmp_path / 'mnt'
    mnt_dir.mkdir(parents=True,exist_ok=True)
    temp_dir = tmp_path / 'temp'
    temp_dir.mkdir(parents=True,exist_ok=True)
    hash_dir = mnt_dir / 'datasets' /'hashes'
    hash_dir.mkdir(parents=True,exist_ok=True)
    dataset_dir = mnt_dir / 'datasets'
    dataset_dir.mkdir(parents=True,exist_ok=True)

    # add sqlite to mounted drive
    shutil.copy(test_sqlite_data,dataset_dir / test_sqlite_data.name)
    current_sqlite_mtime = os.stat(dataset_dir / test_sqlite_data.name).st_mtime

    # add hash to file
    hash_path = hash_dir / f'{test_sqlite_data.stem}.json'
    hash_dict = {'hash':'notwhattheactualhashis'}
    with open(hash_path, 'w') as file:
            file.write(json.dumps(hash_dict))

    # run process
    collection_sync = CollectionSync(mnt_dir=mnt_dir,temp_dir=temp_dir)
    collection_sync.process_object(key, testing_bucket)

    expected_output_paths = [
        mnt_dir / 'datasets' / test_sqlite_data.name,
        mnt_dir / 'datasets' / 'inspect-data-all.json',
        hash_dir / f'{test_sqlite_data.stem}.json'
    ]

    for expected_output_path in expected_output_paths:
        assert expected_output_path.exists(), f'expected output file {expected_output_path.name} is missing'

    # check modification date hasn't changed betweent he sqlites
    assert os.stat(mnt_dir / 'datasets' / test_sqlite_data.name).st_mtime > current_sqlite_mtime, 'sqlite file wasnt modified implying the hash stopped it' 

    # check hash has been updated
    new_hash = collection_sync.get_current_sqlite_hash(test_sqlite_data.stem)
    assert new_hash == '25f3a5bcc31bf2cf991d636fe2fe36ea8f9fe162'