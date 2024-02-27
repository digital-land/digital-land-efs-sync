from pathlib import Path

from task.utils import generate_sqlite_hash

def test_generate_sqlite_hash_returns_hash():

    sqlite_path = Path('tests/data/central-activities-zone.sqlite3')
    hash = generate_sqlite_hash(sqlite_path)
    assert hash == '641e7b7c812b21281cc343659742f3c2d9c1e475'

def test_generate_sqlite_hash_returns_same_hash():
    sqlite_path_1 = Path('tests/data/central-activities-zone.sqlite3')
    hash_1 = generate_sqlite_hash(sqlite_path_1)

    sqlite_path_2 = Path('tests/data/central-activities-zone-new.sqlite3')
    hash_2 = generate_sqlite_hash(sqlite_path_2)

    assert hash_1 == hash_2
