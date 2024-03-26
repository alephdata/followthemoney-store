import json
import logging
from hashlib import sha1
from normality import stringify
from itertools import count


NULL_ORIGIN = "null"

log = logging.getLogger("ftmstore")


class StoreException(Exception):
    pass


def safe_fragment(fragment):
    """Make a hashed fragement."""
    fragment = stringify(fragment)
    if fragment is not None:
        fragment = fragment.encode("utf-8", errors="replace")
        return sha1(fragment).hexdigest()


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
    from followthemoney.cli.util import write_entity

    for entity in dataset.iterate(entity_id=entity_id):
        log.debug("[%s]: %s", entity.id, entity.caption)
        write_entity(file, entity)
