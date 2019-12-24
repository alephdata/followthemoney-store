import sys
import json
import click
import logging
from uuid import uuid4
from itertools import count

from balkhash import settings

log = logging.getLogger('balkhash')


def get_dataset(name):
    from balkhash import init
    return init(name)


def write_stream(dataset, file):
    bulk = dataset.bulk()
    for idx in count(1):
        line = file.readline()
        if not line:
            break
        entity = json.loads(line)
        bulk.put(entity, fragment=str(idx))
        if idx % 10000 == 0:
            log.info("Write [%s]: %s entities", dataset.name, idx)
    bulk.flush()


def iterate_stream(dataset, file, entity_id=None):
    for entity in dataset.iterate(entity_id=entity_id):
        if settings.VERBOSE:
            log.debug("[%s]: %s", entity.id, entity.caption)
        entity = json.dumps(entity.to_dict())
        file.write(entity + '\n')


@click.group(help="Store FollowTheMoney object data")
@click.option('-v', '--verbose', default=False, is_flag=True)
def cli(verbose):
    fmt = '%(name)s [%(levelname)s] %(message)s'
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(stream=sys.stderr, level=level, format=fmt)
    settings.VERBOSE = verbose


@cli.command('write', help="Store entities")
@click.option('-d', '--dataset', required=True)
@click.option('-f', '--file', type=click.File('r'), default='-')  # noqa
def write(dataset, file):
    dataset = get_dataset(dataset)
    try:
        write_stream(dataset, file)
    except BrokenPipeError:
        raise click.Abort()
    dataset.close()


@cli.command('iterate', help="Iterate entities")
@click.option('-d', '--dataset', required=True)
@click.option('-f', '--file', type=click.File('w'), default='-')  # noqa
@click.option('-e', '--entity', default=None)
def iterate(dataset, file, entity):
    dataset = get_dataset(dataset)
    iterate_stream(dataset, file, entity_id=entity)
    dataset.close()
    file.flush()


@cli.command('aggregate', help="Combination of write and iterate.")
@click.option('-i', '--infile', type=click.File('r'), default='-')  # noqa
@click.option('-o', '--outfile', type=click.File('w'), default='-')  # noqa
def aggregate(infile, outfile):
    dataset = 'aggregate_%s' % uuid4().hex
    dataset = get_dataset(dataset)
    try:
        write_stream(dataset, infile)
        iterate_stream(dataset, outfile)
    except BrokenPipeError:
        raise click.Abort()
    finally:
        dataset.delete()
        dataset.close()
        outfile.flush()


@cli.command('delete', help="Delete entities")
@click.option('-d', '--dataset', required=True)
@click.option('-e', '--entity', default=None)
def delete(dataset, entity):
    dataset = get_dataset(dataset)
    dataset.delete(entity_id=entity)
    dataset.close()
