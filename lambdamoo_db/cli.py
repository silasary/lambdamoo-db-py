import click
from .exporter import to_moo_files
from .reader import load


@click.command()
@click.argument("dbfile")
@click.argument("dir")
def moodb2flat(dbfile: str, dir: str) -> None:
    db = load(dbfile)
    to_moo_files(db, dir, True)
