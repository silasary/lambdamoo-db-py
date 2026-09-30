"""Read-only inspection of a textdump: resolve objects, verbs and properties.

The ``moodb`` CLI (``lambdamoo_db.cli:moodb``) is a thin layer over these
functions. Lookups follow ToastStunt semantics where it matters: ``$name``
means ``#0.name``, verb names match with ``*`` abbreviations, and verbs and
property values are inherited through parents.
"""

from __future__ import annotations

import hashlib
import os
import pickle
import re
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, Iterator

import attrs

from .database import CLEAR, Anon, MooCatch, MooDatabase, MooError, MooFinally, MooObject, ObjNum, Property, Verb, Waif, WaifReference
from .enums import ObjectFlags, PropertyFlags
from .reader import load

ERROR_NAMES = [
    "E_NONE", "E_TYPE", "E_DIV", "E_PERM", "E_PROPNF", "E_VERBNF", "E_VARNF",
    "E_INVIND", "E_RECMOVE", "E_MAXREC", "E_RANGE", "E_ARGS", "E_NACC",
    "E_INVARG", "E_QUOTA", "E_FLOAT", "E_FILE", "E_EXEC", "E_INTRPT",
]

# db_verbs.cc prep_list; the index is the stored prep value.
PREPOSITIONS = [
    "with/using", "at/to", "in front of", "in/inside/into",
    "on top of/on/onto/upon", "out of/from inside/from", "over", "through",
    "under/underneath/beneath", "behind", "beside", "for/about", "is", "as",
    "off/off of",
]
PREP_ANY = -2
PREP_NONE = -1
ARG_NAMES = ["none", "any", "this"]

VERB_READ, VERB_WRITE, VERB_EXEC, VERB_DEBUG = 1, 2, 4, 8


class LookupFailed(Exception):
    pass


# --------------------------------------------------------------------------
# Loading with a pickle cache (parsing a 100 MB dump takes ~30 s)


def _package_version() -> str:
    try:
        return version("lambdamoo-db")
    except PackageNotFoundError:
        return "dev"


def default_cache_dir() -> Path:
    base = os.environ.get("XDG_CACHE_HOME") or os.environ.get("LOCALAPPDATA")
    return (Path(base) if base else Path.home() / ".cache") / "lambdamoo-db"


def cache_path_for(db_path: Path, cache_dir: Path) -> Path:
    st = db_path.stat()
    # The reader's own source is part of the key, so a parser change invalidates old pickles.
    reader_src = Path(__file__).with_name("reader.py").read_bytes()
    key = "|".join([
        str(db_path.resolve()), str(st.st_size), str(st.st_mtime_ns),
        _package_version(), hashlib.sha256(reader_src).hexdigest(),
    ])
    return cache_dir / (hashlib.sha256(key.encode()).hexdigest()[:32] + ".pickle")


def load_cached(db_path: str | Path, cache_dir: Path | None = None) -> MooDatabase:
    """Load a textdump, reusing a pickle keyed on path, size, mtime and parser version."""
    db_path = Path(db_path)
    if cache_dir is None:
        return load(str(db_path))
    cached = cache_path_for(db_path, cache_dir)
    if cached.exists():
        return pickle.loads(cached.read_bytes())
    db = load(str(db_path))
    cache_dir.mkdir(parents=True, exist_ok=True)
    tmp = cached.with_suffix(".tmp")
    tmp.write_bytes(pickle.dumps(db, protocol=pickle.HIGHEST_PROTOCOL))
    tmp.replace(cached)
    return db


# --------------------------------------------------------------------------
# Object references


_REF_RE = re.compile(r"^(\$[A-Za-z_][A-Za-z0-9_]*|#?-?\d+)")


def split_ref(spec: str) -> tuple[str, str]:
    """Split ``$httpd:GET`` / ``#852.prop`` into (``$httpd``, ``:GET``)."""
    m = _REF_RE.match(spec)
    if not m:
        raise LookupFailed(f"not an object reference: {spec!r} (use #N or $name)")
    return m.group(1), spec[m.end():]


def resolve_object(db: MooDatabase, ref: str) -> MooObject:
    if ref.startswith("$"):
        value = property_value(db, db.objects[0], ref[1:])
        if not isinstance(value, ObjNum):
            raise LookupFailed(f"#0.{ref[1:]} is {format_value(value)}, not an object")
        num = int(value)
    else:
        num = int(ref.lstrip("#"))
    obj = db.objects.get(num)
    if obj is None:
        raise LookupFailed(f"#{num} does not exist (recycled or out of range)")
    return obj


def ancestors(db: MooDatabase, obj: MooObject) -> Iterator[MooObject]:
    """obj, then its parents depth-first in declared order, each object once."""
    seen: set[int] = set()
    stack = [obj]
    while stack:
        o = stack.pop(0)
        if o.id in seen:
            continue
        seen.add(o.id)
        yield o
        stack[0:0] = [db.objects[int(p)] for p in o.parents if int(p) in db.objects]


def dollar_names(db: MooDatabase) -> dict[int, str]:
    """Map object number to its ``$name`` from #0's own properties."""
    names: dict[int, str] = {}
    for p in db.objects[0].properties:
        if isinstance(p.propertyName, str) and isinstance(p.value, ObjNum):
            names.setdefault(int(p.value), "$" + p.propertyName)
    return names


def object_flags(obj: MooObject) -> str:
    """Flag names as `@display` shows them (player, programmer, wizard, r, w, f), obsolete bits omitted."""
    shown = [("player", ObjectFlags.USER), ("programmer", ObjectFlags.PROGRAMMER), ("wizard", ObjectFlags.WIZARD),
             ("r", ObjectFlags.READ), ("w", ObjectFlags.WRITE), ("f", ObjectFlags.FERTILE),
             ("anonymous", ObjectFlags.ANONYMOUS), ("invalid", ObjectFlags.INVALID), ("recycled", ObjectFlags.RECYCLED)]
    return " ".join(name for name, bit in shown if obj.flags & bit) or "-"


def property_perms(prop: Property) -> str:
    return "".join(c for bit, c in zip((PropertyFlags.READ, PropertyFlags.WRITE, PropertyFlags.CLEAR), "rwc") if prop.perms & bit)


def label(db: MooDatabase, num: int, names: dict[int, str] | None = None) -> str:
    num = int(num)
    obj = db.objects.get(num)
    text = f"#{num}"
    if names and num in names:
        text += f" {names[num]}"
    if obj is not None:
        text += f" {obj.name!r}"
    return text


# --------------------------------------------------------------------------
# Verbs


def verb_name_matches(pattern: str, name: str) -> bool:
    """ToastStunt verbcasecmp: ``foo*bar`` matches foo, foob, fooba, foobar; ``foo*`` matches foo..."""
    pattern, name = pattern.lower(), name.lower()
    if "*" not in pattern:
        return pattern == name
    prefix, _, rest = pattern.partition("*")
    if not name.startswith(prefix):
        return False
    if rest == "":
        return True
    return (prefix + rest).startswith(name)


def verb_matches(verb: Verb, wanted: str) -> bool:
    return any(verb_name_matches(alias, wanted) for alias in verb.name.split())


@attrs.frozen
class VerbHit:
    obj: MooObject
    index: int
    verb: Verb


def find_verb(db: MooDatabase, obj: MooObject, wanted: str, inherited: bool = True) -> VerbHit:
    """Find a verb by name (MOO matching) or by ``N`` index on obj itself."""
    if wanted.isdigit():
        idx = int(wanted)
        if idx >= len(obj.verbs):
            raise LookupFailed(f"#{obj.id} has only {len(obj.verbs)} verbs")
        return VerbHit(obj, idx, obj.verbs[idx])
    # Waif class verbs are stored as ":name"; fall back to that spelling when nothing plain matches.
    for name in (wanted, ":" + wanted) if not wanted.startswith(":") else (wanted,):
        for o in ancestors(db, obj) if inherited else [obj]:
            for idx, v in enumerate(o.verbs):
                if verb_matches(v, name):
                    return VerbHit(o, idx, v)
    raise LookupFailed(f"verb {wanted!r} not found on #{obj.id}{' or its ancestors' if inherited else ''}")


def verb_perms(verb: Verb) -> str:
    return "".join(c for bit, c in zip((VERB_READ, VERB_WRITE, VERB_EXEC, VERB_DEBUG), "rwxd") if verb.perms & bit)


def verb_args(verb: Verb) -> str:
    dobj = ARG_NAMES[(verb.perms >> 4) & 3]
    iobj = ARG_NAMES[(verb.perms >> 6) & 3]
    if verb.preps == PREP_ANY:
        prep = "any"
    elif verb.preps == PREP_NONE:
        prep = "none"
    elif 0 <= verb.preps < len(PREPOSITIONS):
        prep = PREPOSITIONS[verb.preps]
    else:
        prep = f"prep{verb.preps}"
    return f"{dobj} {prep} {iobj}"


def grep_verbs(db: MooDatabase, pattern: re.Pattern[str], objs: list[MooObject] | None = None) -> Iterator[tuple[MooObject, int, Verb, int, str]]:
    """Yield (obj, verb index, verb, 1-based line number, line) for matching code lines."""
    for o in objs if objs is not None else (db.objects[k] for k in sorted(db.objects)):
        for idx, v in enumerate(o.verbs):
            for n, line in enumerate(v.code or [], 1):
                if pattern.search(line):
                    yield o, idx, v, n, line


# --------------------------------------------------------------------------
# Properties


def own_properties(obj: MooObject) -> list[Property]:
    return obj.properties[: obj.propdefs_count]


def find_property(obj: MooObject, name: str) -> Property | None:
    for p in obj.properties:
        if p.propertyName == name:
            return p
    return None


BUILTIN_PROPS = ("name", "owner", "location", "parents", "children", "contents", "flags")


def property_value(db: MooDatabase, obj: MooObject, name: str) -> Any:
    """The effective value, following ``clear`` slots up the parent chain."""
    if name in BUILTIN_PROPS:
        return {
            "name": obj.name, "owner": ObjNum(obj.owner), "location": ObjNum(obj.location),
            "parents": [ObjNum(p) for p in obj.parents], "children": [ObjNum(c) for c in obj.children],
            "contents": [ObjNum(c) for c in obj.contents], "flags": int(obj.flags),
        }[name]
    for o in ancestors(db, obj):
        p = find_property(o, name)
        if p is None:
            continue
        if p.value is not CLEAR:
            return p.value
    if find_property(obj, name) is None:
        raise LookupFailed(f"property {name!r} not found on #{obj.id}")
    return CLEAR


def property_definer(db: MooDatabase, obj: MooObject, name: str) -> MooObject | None:
    for o in ancestors(db, obj):
        if any(p.propertyName == name for p in own_properties(o)):
            return o
    return None


# --------------------------------------------------------------------------
# Values


def _moo_string(s: str) -> str:
    return '"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"'


def format_value(value: Any, limit: int | None = None) -> str:
    """Render a value as a MOO literal; ``limit`` truncates the result."""
    text = _format(value)
    if limit is not None and len(text) > limit:
        text = text[: max(limit - 3, 0)] + "..."
    return text


def _format(value: Any) -> str:
    if value is CLEAR:
        return "clear"
    if value is None:
        return "none"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, ObjNum):
        return f"#{int(value)}"
    if isinstance(value, Anon):
        return f"*anonymous #{int(value)}*"
    if isinstance(value, MooError):
        n = int(value)
        return ERROR_NAMES[n] if 0 <= n < len(ERROR_NAMES) else f"E_{n}"
    if isinstance(value, (MooCatch, MooFinally)):
        return repr(value)
    if isinstance(value, str):
        return _moo_string(value)
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, list):
        return "{" + ", ".join(_format(v) for v in value) + "}"
    if isinstance(value, dict):
        return "[" + ", ".join(f"{_format(k)} -> {_format(v)}" for k, v in value.items()) + "]"
    if isinstance(value, Waif):
        return f"<waif of #{value.waif_class}>"
    if isinstance(value, WaifReference):
        return f"<waif ref {value.index}>"
    return repr(value)
