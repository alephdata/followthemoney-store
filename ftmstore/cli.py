import sys
import json
import click
import logging
from uuid import uuid4
from itertools import count

from followthemoney.cli.cli import cli as main
from ftmstore.settings import DATABASE_URI
from ftmstore.store import Store
from ftmstore.utils import NULL_ORIGIN

log = logging.getLogger("ftmstore")


def write_stream(dataset, file, origin=NULL_ORIGIN):
    bulk = dataset.bulk()
    for idx in count(1):
        line = file.readline()
        if not line:
            break
        entity = json.loads(line)
        bulk.put(entity, fragment=str(idx), origin=origin)
        if idx % 10000 == 0:
            log.info("Write [%s]: %s entities", dataset.name, idx)
    bulk.flush()


def iterate_stream(dataset, file, entity_id=None):
    from followthemoney.cli.util import write_object

    for entity in dataset.iterate(entity_id=entity_id):
        log.debug("[%s]: %s", entity.id, entity.caption)
        write_object(file, entity)


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
    store = Store(database_uri=db)
    dataset = store.get(dataset, origin=origin)
    try:
        write_stream(dataset, infile, origin=origin)
    except BrokenPipeError:
        raise click.Abort() from BrokenPipeError
    finally:
        store.close()


@cli.command("iterate", help="Iterate entities")
@click.option("--db", metavar="URI", default=DATABASE_URI, show_default=True)
@click.option("-d", "--dataset", required=True)
@click.option("-o", "--outfile", type=click.File("w"), default="-")
def iterate(db, dataset, outfile):
    store = Store(database_uri=db)
    dataset = store.get(dataset)
    try:
        iterate_stream(dataset, outfile)
    finally:
        outfile.flush()
        store.close()


@cli.command("aggregate", help="Combination of write and iterate.")
@click.option("-i", "--infile", type=click.File("r"), default="-")
@click.option("-o", "--outfile", type=click.File("w"), default="-")
def aggregate(infile, outfile):
    store = Store()
    dataset = store.get("aggregate_%s" % uuid4().hex)
    try:
        write_stream(dataset, infile)
        iterate_stream(dataset, outfile)
    except BrokenPipeError:
        raise click.Abort() from BrokenPipeError
    finally:
        dataset.delete()
        outfile.flush()
        store.close()


@cli.command("list", help="List datasets in a store")
@click.option("--db", metavar="URI", default=DATABASE_URI, show_default=True)
def list_datasets(db):
    store = Store(database_uri=db)
    try:
        for dataset in store.all():
            log.info("%s", dataset.name)
    finally:
        store.close()


@cli.command("delete", help="Delete entities")
@click.option("--db", metavar="URI", default=DATABASE_URI, show_default=True)
@click.option("-d", "--dataset", required=True)
@click.option("-o", "--origin", default=None)
@click.option("-e", "--entity", default=None)
def delete(db, dataset, origin, entity):
    store = Store(database_uri=db)
    dataset = store.get(dataset, origin=origin)
    if origin is None and entity is None:
        dataset.drop()
    else:
        dataset.delete(origin=origin, entity_id=entity)
    store.close()


# Register with main FtM command-line tool.
main.add_command(cli, name="store")
