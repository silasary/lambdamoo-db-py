from pathlib import Path

import pytest
from click.testing import CliRunner

from lambdamoo_db.cli import moodb
from lambdamoo_db.database import CLEAR, MooError, ObjNum, Verb
from lambdamoo_db.inspection import (
    LookupFailed,
    ancestors,
    find_verb,
    format_value,
    load_cached,
    property_value,
    resolve_object,
    split_ref,
    verb_args,
    verb_name_matches,
    verb_perms,
)
from lambdamoo_db.reader import load

TOASTCORE = Path(__file__).parent.parent / "toastcore.db"


@pytest.fixture(scope="module")
def db():
    return load(str(TOASTCORE))


@pytest.mark.parametrize(
    "pattern,name,expected",
    [
        ("GET", "get", True),
        ("status_redirect*ion", "status_redirect", True),
        ("status_redirect*ion", "status_redirecti", True),
        ("status_redirect*ion", "status_redirection", True),
        ("status_redirect*ion", "status_redirec", False),
        ("status_redirect*ion", "status_redirectionx", False),
        ("foo*", "foobar", True),
        ("foo*", "fo", False),
        ("html_robots.txt", "html_robots.txt", True),
    ],
)
def test_verb_name_matches(pattern, name, expected):
    assert verb_name_matches(pattern, name) is expected


def test_verb_args_and_perms():
    v = Verb("GET HEAD", ObjNum(2), 173, -1, 852)
    assert verb_args(v) == "this none this"
    assert verb_perms(v) == "rxd"
    assert verb_args(Verb("look", ObjNum(2), 1 | (1 << 4), -2, 1)) == "any any none"
    assert verb_args(Verb("put", ObjNum(2), 0, 3, 1)) == "none in/inside/into none"


def test_format_value():
    assert format_value(["a\"b\\", ObjNum(5), MooError(3), {"k": 1.5}, True, CLEAR]) == '{"a\\"b\\\\", #5, E_PERM, ["k" -> 1.5], true, clear}'
    assert format_value("x" * 50, 10) == '"xxxxxx...'


def test_split_ref():
    assert split_ref("$httpd:GET") == ("$httpd", ":GET")
    assert split_ref("#852.port") == ("#852", ".port")
    assert split_ref("12:3") == ("12", ":3")
    assert split_ref("#-1") == ("#-1", "")
    with pytest.raises(LookupFailed):
        split_ref("httpd:GET")


def test_resolve_dollar_name(db):
    assert resolve_object(db, "$string_utils").id == 20
    assert resolve_object(db, "#20").id == 20
    with pytest.raises(LookupFailed):
        resolve_object(db, "#999999")
    with pytest.raises(LookupFailed):
        resolve_object(db, "$no_such_name_here")


def test_find_verb_by_name_index_and_inheritance(db):
    su = resolve_object(db, "$string_utils")
    hit = find_verb(db, su, "from_list")
    assert (hit.obj.id, hit.verb.name) == (20, "from_list")
    assert find_verb(db, su, str(hit.index)).verb is hit.verb

    # A verb defined only on an ancestor is found from the child, and reported on the ancestor.
    sysobj = db.objects[0]
    own = {n for v in sysobj.verbs for n in v.name.split()}
    root = db.objects[1]
    inherited = next(v for v in root.verbs if not any(verb_name_matches(n, a) for n in v.name.split() for a in own))
    wanted = inherited.name.split()[0].replace("*", "")
    hit = find_verb(db, sysobj, wanted)
    assert hit.obj.id == 1
    with pytest.raises(LookupFailed):
        find_verb(db, sysobj, wanted, inherited=False)


def test_property_value_follows_clear(db):
    for obj in db.objects.values():
        cleared = [p for p in obj.properties[obj.propdefs_count:] if p.value is CLEAR and isinstance(p.propertyName, str)]
        if obj.parents and cleared:
            break
    else:
        pytest.skip("no inherited clear property in fixture")
    name = cleared[0].propertyName
    expected = next(
        p.value for a in list(ancestors(db, obj))[1:] for p in a.properties if p.propertyName == name and p.value is not CLEAR
    )
    assert property_value(db, obj, name) == expected
    assert property_value(db, obj, "name") == obj.name


def test_load_cached_writes_and_reuses_pickle(tmp_path):
    first = load_cached(TOASTCORE, tmp_path)
    pickles = list(tmp_path.glob("*.pickle"))
    assert len(pickles) == 1
    second = load_cached(TOASTCORE, tmp_path)
    assert len(second.objects) == len(first.objects)
    assert second.objects[20].name == first.objects[20].name


def run(*args):
    return CliRunner().invoke(moodb, ["--db", str(TOASTCORE), "--no-cache", *args])


def test_cli_code_prop_grep_find_obj():
    r = run("code", "-n", "$string_utils:from_list")
    assert r.exit_code == 0, r.output
    assert r.output.startswith("@program #20 $string_utils 'string utilities':[")
    assert "   1  " in r.output and r.output.rstrip().endswith(".")

    r = run("prop", "$string_utils.name", "0.string_utils")
    assert r.output.splitlines() == ['#20.name = "string utilities"', "#0.string_utils = #20"]

    r = run("grep", "-l", "-F", "tostr(@thelist)", "-o", "$string_utils")
    assert r.exit_code == 0 and "'from_list'" in r.output

    r = run("find", "string_utils")
    assert "#20 $string_utils 'string utilities'" in r.output

    r = run("obj", "0")
    assert r.exit_code == 0
    assert "##" not in r.output
    assert "  parents   #1 $root_class" in r.output


def test_cli_lookup_errors_exit_nonzero():
    r = run("obj", "#999999")
    assert r.exit_code == 1 and "does not exist" in r.output
    r = run("code", "$string_utils")
    assert r.exit_code == 1 and "expected REF:VERB" in r.output
