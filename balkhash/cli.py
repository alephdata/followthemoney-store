import sys
import json
import click
import logging
from itertools import count

log = logging.getLogger('balkhash')


def get_dataset(name):
    from balkhash import init
    return init(name)


@click.group(help="Store FollowTheMoney object data")
def cli():
    fmt = '%(name)s [%(levelname)s] %(message)s'
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format=fmt)


@cli.command('write', help="Store entities")
@click.option('-d', '--dataset', required=True)
@click.option('-f', '--file', type=click.File('r'), default='-')  # noqa
def write(dataset, file):
    dataset = get_dataset(dataset)
    bulk = dataset.bulk()
    try:
        for idx in count(1):
            line = file.readline()
            if not line:
                break
            entity = json.loads(line)
            bulk.put(entity, fragment=str(idx))
            if idx % 1000 == 0:
                log.info("Write [%s]: %s entities", dataset.name, idx)
        bulk.flush()
    except BrokenPipeError:
        raise click.Abort()
    dataset.close()


@cli.command('iterate', help="Iterate entities")
@click.option('-d', '--dataset', required=True)
@click.option('-f', '--file', type=click.File('w'), default='-')  # noqa
@click.option('-e', '--entity', default=None)
def iterate(dataset, file, entity):
    dataset = get_dataset(dataset)
    for entity in dataset.iterate(entity_id=entity):
        entity = json.dumps(entity.to_dict())
        file.write(entity)
        file.write('\n')
        file.flush()
    dataset.close()


@cli.command('delete', help="Delete entities")
@click.option('-d', '--dataset', required=True)
@click.option('-e', '--entity', default=None)
def delete(dataset, entity):
    dataset = get_dataset(dataset)
    dataset.delete(entity_id=entity)
    dataset.close()
