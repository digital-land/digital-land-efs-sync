from pathlib import Path

from task.utils import generate_sqlite_hash

def test_generate_sqlite_hash_returns_hash():

    sqlite_path = Path('tests/data/central-activities-zone.sqlite3')
    hash = generate_sqlite_hash(sqlite_path)
    assert hash == '25f3a5bcc31bf2cf991d636fe2fe36ea8f9fe162'

def test_generate_sqlite_hash_returns_same_hash():
    sqlite_path_1 = Path('tests/data/central-activities-zone.sqlite3')
    hash_1 = generate_sqlite_hash(sqlite_path_1)

    sqlite_path_2 = Path('tests/data/central-activities-zone-same-but-latest.sqlite3')
    hash_2 = generate_sqlite_hash(sqlite_path_1)

    assert hash_1 == hash_2