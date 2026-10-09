import json

import pytest

import task.retire
from task.retire import is_dry_run, retire_dataset


@pytest.fixture
def mnt_dir(tmp_path):
    """A datasette volume serving two datasets, as the sync leaves it."""
    dataset_dir = tmp_path / "datasets"
    (dataset_dir / "hashes").mkdir(parents=True)
    for dataset in ("company", "conservation-area"):
        (dataset_dir / f"{dataset}.sqlite3").write_bytes(b"sqlite")
        (dataset_dir / f"{dataset}.sqlite3.json").write_text(
            json.dumps({dataset: {"hash": "x"}})
        )
        (dataset_dir / "hashes" / f"{dataset}.json").write_text(
            json.dumps({"hash": "x"})
        )
    (dataset_dir / "inspect-data-all.json").write_text(
        json.dumps({"company": {"hash": "x"}, "conservation-area": {"hash": "x"}})
    )
    return tmp_path


@pytest.fixture
def no_wait(monkeypatch):
    """Record the waits instead of sleeping."""
    waits = []
    monkeypatch.setattr(
        task.retire.time, "sleep", lambda seconds: waits.append(seconds)
    )
    return waits


def served(mnt_dir):
    return set(json.loads((mnt_dir / "datasets" / "inspect-data-all.json").read_text()))


def test_dry_run_lists_the_files_and_removes_nothing(mnt_dir, no_wait):
    files = retire_dataset(mnt_dir, "company", dry_run=True)

    assert sorted(path.name for path in files) == [
        "company.json",
        "company.sqlite3",
        "company.sqlite3.json",
    ]
    assert all(path.exists() for path in files)
    assert served(mnt_dir) == {"company", "conservation-area"}
    assert no_wait == []


def test_removes_the_dataset_and_leaves_the_others(mnt_dir, no_wait):
    retire_dataset(mnt_dir, "company", dry_run=False, wait_seconds=30)

    dataset_dir = mnt_dir / "datasets"
    assert not (dataset_dir / "company.sqlite3").exists()
    assert not (dataset_dir / "company.sqlite3.json").exists()
    assert not (dataset_dir / "hashes" / "company.json").exists()
    assert served(mnt_dir) == {"conservation-area"}
    assert (dataset_dir / "conservation-area.sqlite3").exists()
    assert (dataset_dir / "hashes" / "conservation-area.json").exists()


def test_database_is_kept_until_datasette_has_restarted_without_it(
    mnt_dir, monkeypatch
):
    # datasette will not start if inspect-data-all.json lists a database whose file is missing
    seen_during_wait = {}

    def sleep(seconds):
        seen_during_wait["served"] = served(mnt_dir)
        seen_during_wait["database exists"] = (
            mnt_dir / "datasets" / "company.sqlite3"
        ).exists()

    monkeypatch.setattr(task.retire.time, "sleep", sleep)

    retire_dataset(mnt_dir, "company", dry_run=False)

    assert seen_during_wait == {
        "served": {"conservation-area"},
        "database exists": True,
    }


def test_no_wait_when_datasette_was_not_serving_the_dataset(mnt_dir, no_wait):
    # a database with no inspection file is not listed, so datasette has nothing to restart for
    (mnt_dir / "datasets" / "company.sqlite3.json").unlink()
    (mnt_dir / "datasets" / "inspect-data-all.json").write_text(
        json.dumps({"conservation-area": {"hash": "x"}})
    )

    retire_dataset(mnt_dir, "company", dry_run=False)

    assert no_wait == []
    assert not (mnt_dir / "datasets" / "company.sqlite3").exists()


def test_nothing_to_remove_is_not_an_error(mnt_dir, no_wait):
    assert retire_dataset(mnt_dir, "no-such-dataset", dry_run=False) == []
    assert served(mnt_dir) == {"company", "conservation-area"}


@pytest.mark.parametrize("dataset", ["digital-land", "performance", "entity"])
def test_builder_databases_cannot_be_retired(mnt_dir, dataset):
    with pytest.raises(ValueError, match="cannot be retired"):
        retire_dataset(mnt_dir, dataset, dry_run=False)


@pytest.mark.parametrize(
    "value,expected",
    [("false", False), ("true", True), ("", True), ("False", True), ("no", True)],
)
def test_only_an_explicit_false_removes_anything(value, expected):
    assert is_dry_run(value) is expected
