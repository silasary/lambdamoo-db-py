import os
import re
from pathlib import Path

import click
from .exporter import to_moo_files
from .inspection import (
    LookupFailed,
    ancestors,
    default_cache_dir,
    dollar_names,
    find_verb,
    format_value,
    grep_verbs,
    label,
    load_cached,
    object_flags,
    own_properties,
    property_perms,
    property_definer,
    property_value,
    resolve_object,
    split_ref,
    verb_args,
    verb_perms,
)
from .reader import load
from .split import DEFAULT_MAX_PIECE_BYTES, SplitError, first_difference, join_dir, write_split


@click.command()
@click.argument("dbfile")
@click.argument("dir")
def moodb2flat(dbfile: str, dir: str) -> None:
    db = load(dbfile)
    to_moo_files(db, dir, True)


@click.command()
@click.argument("dbfile", type=click.Path(exists=True, dir_okay=False))
@click.argument("outdir", type=click.Path(file_okay=False))
@click.option("--max-piece-bytes", default=DEFAULT_MAX_PIECE_BYTES, show_default=True, help="Fail if any piece is larger than this.")
def moodb_split(dbfile: str, outdir: str, max_piece_bytes: int) -> None:
    """Split DBFILE into per-object pieces in OUTDIR and verify they rejoin byte for byte."""
    data = Path(dbfile).read_bytes()
    try:
        stats = write_split(data, outdir, max_piece_bytes=max_piece_bytes)
    except SplitError as e:
        where = first_difference(outdir, data) if (Path(outdir) / "MANIFEST").exists() else None
        raise click.ClickException(f"{e}" + (f"; first difference at {where}" if where else ""))
    click.echo(
        f"split ok: {stats['pieces']} pieces, {stats['written']} written, " f"{stats['unchanged']} unchanged, {stats['removed']} removed"
    )


@click.command()
@click.argument("indir", type=click.Path(exists=True, file_okay=False))
@click.argument("outfile", type=click.Path(dir_okay=False), required=False)
@click.option("--compare", type=click.Path(exists=True, dir_okay=False), help="Report where INDIR first differs from this db file.")
def moodb_join(indir: str, outfile: str | None, compare: str | None) -> None:
    """Rebuild a db file from INDIR, verifying it against the MANIFEST. Without OUTFILE, only verify."""
    if compare:
        where = first_difference(indir, Path(compare).read_bytes())
        if where:
            raise click.ClickException(f"differs from {compare} at {where}")
        click.echo(f"identical to {compare}")
    try:
        data = join_dir(indir)
    except SplitError as e:
        raise click.ClickException(str(e))
    if outfile:
        tmp = Path(outfile + ".tmp")
        tmp.write_bytes(data)
        os.replace(tmp, outfile)
    click.echo(f"join ok: {len(data)} bytes")


@click.group()
@click.option("--db", "db_path", envvar="MOODB", required=True, type=click.Path(exists=True, dir_okay=False),
              help="Textdump to read (or set MOODB).")
@click.option("--no-cache", is_flag=True, help="Parse the dump without reading or writing the pickle cache.")
@click.option("--cache-dir", type=click.Path(file_okay=False), default=None,
              help="Pickle cache directory (default: lambdamoo-db under %LOCALAPPDATA%, $XDG_CACHE_HOME or ~/.cache).")
@click.pass_context
def moodb(ctx: click.Context, db_path: str, no_cache: bool, cache_dir: str | None) -> None:
    """Read-only inspection of a LambdaMOO/ToastStunt textdump.

    Object references are #N, N or $name (#0.name). Verbs are REF:NAME with MOO
    name matching, or REF:N for the Nth verb. Properties are REF.NAME.
    """
    cache = None if no_cache else Path(cache_dir) if cache_dir else default_cache_dir()
    ctx.obj = {"path": db_path, "cache": cache}


def _db(ctx: click.Context):
    if "db" not in ctx.obj:
        ctx.obj["db"] = load_cached(ctx.obj["path"], ctx.obj["cache"])
        ctx.obj["names"] = dollar_names(ctx.obj["db"])
    return ctx.obj["db"], ctx.obj["names"]


@moodb.command()
@click.argument("ref")
@click.pass_context
def obj(ctx: click.Context, ref: str) -> None:
    """Show an object: header, parents, verbs and its own properties."""
    db, names = _db(ctx)
    try:
        o = resolve_object(db, ref)
    except LookupFailed as e:
        raise click.ClickException(str(e))
    click.echo(label(db, o.id, names))
    click.echo(f"  owner     {label(db, o.owner, names)}")
    click.echo(f"  location  {label(db, o.location, names)}")
    click.echo(f"  flags     {object_flags(o)}")
    click.echo("  parents   " + ", ".join(label(db, int(p), names) for p in o.parents))
    click.echo("  ancestry  " + " > ".join(f"#{a.id}" for a in ancestors(db, o)))
    click.echo(f"  children  {len(o.children)}  contents {len(o.contents)}")
    click.echo(f"verbs ({len(o.verbs)}):")
    for idx, v in enumerate(o.verbs):
        click.echo(f"  [{idx}] {v.name!r}  {verb_args(v)}  {verb_perms(v)}  owner #{int(v.owner)}  {len(v.code or [])} lines")
    props = own_properties(o)
    click.echo(f"own properties ({len(props)}):")
    for p in props:
        click.echo(f"  .{p.propertyName}  {property_perms(p)}  owner #{int(p.owner)}  = {format_value(p.value, 100)}")


@moodb.command()
@click.argument("ref")
@click.option("-i", "--inherited", is_flag=True, help="Also list verbs of every ancestor.")
@click.pass_context
def verbs(ctx: click.Context, ref: str, inherited: bool) -> None:
    """List verbs as #OBJ:[index] 'names' args perms owner."""
    db, names = _db(ctx)
    try:
        o = resolve_object(db, ref)
    except LookupFailed as e:
        raise click.ClickException(str(e))
    for a in ancestors(db, o) if inherited else [o]:
        if inherited:
            click.echo(f"== {label(db, a.id, names)}")
        for idx, v in enumerate(a.verbs):
            click.echo(f"#{a.id}:[{idx}] {v.name!r}  {verb_args(v)}  {verb_perms(v)}  owner #{int(v.owner)}")


@moodb.command()
@click.argument("spec", nargs=-1, required=True)
@click.option("-n", "--line-numbers", is_flag=True, help="Number the code lines.")
@click.pass_context
def code(ctx: click.Context, spec: tuple[str, ...], line_numbers: bool) -> None:
    """Print verb code, e.g. `code '$httpd:GET' 852:12`. Inherited verbs are found on ancestors."""
    db, names = _db(ctx)
    for s in spec:
        try:
            ref, rest = split_ref(s)
            if not rest.startswith(":") or len(rest) < 2:
                raise LookupFailed(f"expected REF:VERB, got {s!r}")
            hit = find_verb(db, resolve_object(db, ref), rest[1:])
        except LookupFailed as e:
            raise click.ClickException(str(e))
        v = hit.verb
        click.echo(f"@program {label(db, hit.obj.id, names)}:[{hit.index}] {v.name!r}  {verb_args(v)}  {verb_perms(v)}  owner #{int(v.owner)}")
        if v.code is None:
            click.echo("  (no program)")
        for n, line in enumerate(v.code or [], 1):
            click.echo(f"{n:4}  {line}" if line_numbers else line)
        click.echo(".")


@moodb.command()
@click.argument("spec", nargs=-1, required=True)
@click.option("--full", is_flag=True, help="Do not truncate long values.")
@click.pass_context
def prop(ctx: click.Context, spec: tuple[str, ...], full: bool) -> None:
    """Print effective property values, e.g. `prop '$httpd.port' 0.httpd`."""
    db, _ = _db(ctx)
    for s in spec:
        try:
            ref, rest = split_ref(s)
            if not rest.startswith(".") or len(rest) < 2:
                raise LookupFailed(f"expected REF.PROP, got {s!r}")
            o = resolve_object(db, ref)
            value = property_value(db, o, rest[1:])
        except LookupFailed as e:
            raise click.ClickException(str(e))
        definer = property_definer(db, o, rest[1:])
        where = f" (defined on #{definer.id})" if definer and definer.id != o.id else ""
        click.echo(f"#{o.id}.{rest[1:]}{where} = {format_value(value, None if full else 2000)}")


@moodb.command()
@click.argument("pattern")
@click.option("-o", "--obj", "objs", multiple=True, help="Only search these objects (repeatable).")
@click.option("-i", "--ignore-case", is_flag=True)
@click.option("-F", "--fixed-strings", is_flag=True, help="PATTERN is a literal string.")
@click.option("-l", "--verbs-only", is_flag=True, help="Print each matching verb once.")
@click.pass_context
def grep(ctx: click.Context, pattern: str, objs: tuple[str, ...], ignore_case: bool, fixed_strings: bool, verbs_only: bool) -> None:
    """Search verb code: #OBJ:[index] name:LINE: text."""
    db, names = _db(ctx)
    try:
        selected = [resolve_object(db, r) for r in objs] or None
    except LookupFailed as e:
        raise click.ClickException(str(e))
    rx = re.compile(re.escape(pattern) if fixed_strings else pattern, re.IGNORECASE if ignore_case else 0)
    seen: set[tuple[int, int]] = set()
    for o, idx, v, n, line in grep_verbs(db, rx, selected):
        if verbs_only:
            if (o.id, idx) not in seen:
                seen.add((o.id, idx))
                click.echo(f"{label(db, o.id, names)}:[{idx}] {v.name!r}")
            continue
        click.echo(f"#{o.id}:[{idx}] {v.name.split()[0]}:{n}: {line.strip()}")


@moodb.command()
@click.argument("text")
@click.pass_context
def find(ctx: click.Context, text: str) -> None:
    """Find objects whose name or $name contains TEXT (case-insensitive)."""
    db, names = _db(ctx)
    needle = text.lower().lstrip("$")
    for num in sorted(db.objects):
        o = db.objects[num]
        if needle in o.name.lower() or needle in names.get(num, "").lower():
            click.echo(label(db, num, names))
