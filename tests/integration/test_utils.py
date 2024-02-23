from pathlib import Path

from task.utils import generate_sqlite_hash

def test_generate_sqlite_hash_returns_hash():

    sqlite_path = Path('tests/data/article-4-direction-area.sqlite3')
    hash = generate_sqlite_hash(sqlite_path)
    assert hash == 'd464759f9ad3b1d098ea5e067175a96df57b9ca0'