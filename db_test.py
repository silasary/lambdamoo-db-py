from lambdamoo_db import exporter
from lambdamoo_db.reader import load
import cattrs


def test_lambda() -> None:
    db = load("LambdaCore-latest.db")
    assert db is not None
    assert db.version == 4
    assert db.total_objects == 97
    assert db.total_verbs == 1727

    with open("LambdaCore-latest.json", "w") as f:
        exporter.to_json_file(cattrs.unstructure(db), f, indent=2)


def test_toast() -> None:
    db = load("toastcore.db")
    assert db is not None
    assert db.version == 17

    with open("toastcore.json", "w") as f:
        exporter.to_json_file(cattrs.unstructure(db), f, indent=2)


def test_toast2() -> None:
    db = load("toast2.db")
    assert db is not None
    assert len(db.suspendedTasks) > 0
    assert len(db.waifs) > 0
