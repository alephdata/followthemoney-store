import sys
import json
import click
import logging
from uuid import uuid4
from itertools import count

from followthemoney.cli.cli import cli as main
from ftmstore import get_dataset
from ftmstore.settings import DATABASE_URI
from ftmstore.store import Store
from ftmstore.utils import NULL_ORIGIN, write_stream, iterate_stream

log = logging.getLogger("ftmstore")


@click.group(help="Store FollowTheMoney object data")
@click.option("-v", "--verbose", default=False, is_flag=True)
def cli(verbose):
    fmt = "%(name)s [%(levelname)s] %(message)s"
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(stream=sys.stderr, level=level, format=fmt)


@cli.command("write", help="Store entities")
@click.option("--db", metavar="URI", default=DATABASE_URI, show_default=True)
@click.option("-d", "--dataset", required=True)
@click.option("-i", "--infile", type=click.File("r"), default="-")
@click.option("-o", "--origin", default=NULL_ORIGIN)
def write(db, dataset, infile, origin):
    dataset = get_dataset(dataset, origin=origin, database_uri=db)
    try:
        write_stream(dataset, infile, origin=origin)
    except BrokenPipeError:
        raise click.Abort() from BrokenPipeError


@cli.command("iterate", help="Iterate entities")
@click.option("--db", metavar="URI", default=DATABASE_URI, show_default=True)
@click.option("-d", "--dataset", required=True)
@click.option("-o", "--outfile", type=click.File("w+b"), default="-")
def iterate(db, dataset, outfile):
    dataset = get_dataset(dataset, database_uri=db)
    try:
        iterate_stream(dataset, outfile)
    finally:
        outfile.flush()


@cli.command("aggregate", help="Combination of write and iterate.")
@click.option("-i", "--infile", type=click.File("r"), default="-")
@click.option("-o", "--outfile", type=click.File("w+b"), default="-")
def aggregate(infile, outfile):
    dataset = get_dataset("aggregate_%s" % uuid4().hex)
    try:
        write_stream(dataset, infile)
        iterate_stream(dataset, outfile)
    except BrokenPipeError:
        raise click.Abort() from BrokenPipeError
    finally:
        outfile.flush()
        dataset.drop()


@cli.command("list", help="List datasets in a store")
@click.option("--db", metavar="URI", default=DATABASE_URI, show_default=True)
def list_datasets(db):
    store = Store(database_uri=db)
    for dataset in store.all():
        log.info("%s", dataset.name)


@cli.command("delete", help="Delete entities")
@click.option("--db", metavar="URI", default=DATABASE_URI, show_default=True)
@click.option("-d", "--dataset", required=True)
@click.option("-o", "--origin", default=None)
@click.option("-e", "--entity", default=None)
def delete(db, dataset, origin, entity):
    dataset = get_dataset(dataset, origin=origin, database_uri=db)
    if origin is None and entity is None:
        dataset.drop()
    else:
        dataset.delete(origin=origin, entity_id=entity)


# Register with main FtM command-line tool.
main.add_command(cli, name="store")
