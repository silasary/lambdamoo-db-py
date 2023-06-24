from io import TextIOWrapper
import json
import os
import re
import shutil
from typing import Any, Optional
import cattrs
from lambdamoo_db.database import ObjNum, WaifReference, MooDatabase


ILLEGAL_NAMES = [
    "CON",
    "PRN",
    "AUX",
    "NUL",
    "COM0",
    "COM1",
    "COM2",
    "COM3",
    "COM4",
    "COM5",
    "COM6",
    "COM7",
    "COM8",
    "COM9",
    "LPT0",
    "LPT1",
    "LPT2",
    "LPT3",
    "LPT4",
    "LPT5",
    "LPT6",
    "LPT7",
    "LPT8",
    "LPT9",
]


def converter(x: Any) -> Any:
    if isinstance(x, WaifReference):
        return f"WAIF({x.index})"


def sanitize(filename: str) -> str:
    name = re.sub(r"[\*\?\|:;\/\\<>]", "", filename)
    if name.upper() in ILLEGAL_NAMES:
        return ""
    return name


def to_json(db: MooDatabase) -> str:
    return json.dumps(cattrs.unstructure(db), indent=2, default=converter)


def to_json_file(db: MooDatabase, f: TextIOWrapper, indent: Optional[int] = None) -> None:
    json.dump(cattrs.unstructure(db), f, indent=indent, default=converter)


def to_moo_files(db: MooDatabase, path: str, corrify: bool) -> None:
    if os.path.exists(path):
        shutil.rmtree(path)

    os.mkdir(path)
    names = {}
    if corrify:
        for p in db.objects[0].properties:
            if p.propertyName and isinstance(p.value, ObjNum) and not p.value in names:
                names[p.value] = "$" + p.propertyName

    def name(i: int | ObjNum) -> str:
        id = str(i)
        if corrify and i in names:
            id = names[i]
        return id

    for i, o in db.objects.items():
        id = name(i)
        os.mkdir(os.path.join(path, id))
        with open(os.path.join(path, id, "info.json"), "w") as f:

            info = {
                "name": o.name,
                "parent": None,
                "parents": [name(p) for p in o.parents],
                "owner": o.owner,
                "location": o.location,
                "verbs": [v.name for v in o.verbs],
            }
            if len(o.parents) < 2:
                info["parent"] = o.parent

            json.dump(info, f, indent=2)
        with open(os.path.join(path, id, "props.json"), "w") as f:
            json.dump(cattrs.unstructure(o.properties), f, indent=2, default=converter)

        for i, v in enumerate(o.verbs):
            filename = (sanitize(v.name) or str(i)).split(" ", 1)[0] + ".moo"
            with open(os.path.join(path, id, filename), "w") as f:
                f.write("\n".join(v.code))
