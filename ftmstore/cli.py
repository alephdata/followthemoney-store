import sys
import json
import click
import logging
from uuid import uuid4
from itertools import count

from followthemoney.cli.cli import cli as main
from ftmstore.dataset import Dataset, NULL_ORIGIN

log = logging.getLogger('ftmstore')


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
@click.option('-v', '--verbose', default=False, is_flag=True)
def cli(verbose):
    fmt = '%(name)s [%(levelname)s] %(message)s'
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(stream=sys.stderr, level=level, format=fmt)


@cli.command('write', help="Store entities")
@click.option('-d', '--dataset', required=True)
@click.option('-i', '--infile', type=click.File('r'), default='-')  # noqa
@click.option('-o', '--origin', default=NULL_ORIGIN)  # noqa
def write(dataset, infile, origin):
    try:
        dataset = Dataset(dataset)
        write_stream(dataset, infile, origin=origin)
    except BrokenPipeError:
        raise click.Abort()


@cli.command('iterate', help="Iterate entities")
@click.option('-d', '--dataset', required=True)
@click.option('-o', '--outfile', type=click.File('w'), default='-')  # noqa
def iterate(dataset, outfile):
    dataset = Dataset(dataset)
    iterate_stream(dataset, outfile)
    outfile.flush()


@cli.command('aggregate', help="Combination of write and iterate.")
@click.option('-i', '--infile', type=click.File('r'), default='-')  # noqa
@click.option('-o', '--outfile', type=click.File('w'), default='-')  # noqa
def aggregate(infile, outfile):
    dataset = Dataset('aggregate_%s' % uuid4().hex)
    try:
        write_stream(dataset, infile)
        iterate_stream(dataset, outfile)
    except BrokenPipeError:
        raise click.Abort()
    finally:
        dataset.delete()
        outfile.flush()


@cli.command('delete', help="Delete entities")
@click.option('-d', '--dataset', required=True)
@click.option('-o', '--origin', default=None)
@click.option('-e', '--entity', default=None)
def delete(dataset, origin, entity):
    dataset = Dataset(dataset)
    dataset.delete(origin=origin, entity_id=entity)


# Register with main FtM command-line tool.
main.add_command(cli, name='store')
