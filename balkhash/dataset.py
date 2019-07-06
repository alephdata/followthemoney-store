from followthemoney import model
from abc import ABC, abstractmethod

# We have to cast null fragment values to "" to make the
# UniqueConstraint work
DEFAULT = 'default'


class Dataset(ABC):

    def __init__(self, config):
        self.config = config
        self.name = config.get('name')

    @abstractmethod
    def delete(self, entity_id=None, fragment=None):
        pass

    @abstractmethod
    def put(self, entity, fragment=DEFAULT):
        pass

    @abstractmethod
    def bulk(self, size=1000):
        pass

    @abstractmethod
    def fragments(self, entity_id=None, fragment=None):
        pass

    def close(self):
        pass

    def _entity_dict(self, entity):
        if hasattr(entity, 'to_dict'):
            entity = entity.to_dict()
        return entity

    def get(self, entity_id):
        for entity in self.iterate(entity_id=entity_id):
            return entity

    def iterate(self, entity_id=None):
        entity = None
        for fragment in self.fragments(entity_id=entity_id):
            partial = model.get_proxy(fragment)
            partial.context = {}
            if entity is not None:
                if entity.id == partial.id:
                    entity.merge(partial)
                    continue
                yield entity
            entity = partial
        if entity is not None:
            yield entity

    def __iter__(self):
        return self.iterate()


class Bulk(ABC):

    def __init__(self, dataset, size):
        self.dataset = dataset
        self.size = size
        self.buffer = {}

    def put(self, entity, fragment=DEFAULT):
        entity = self.dataset._entity_dict(entity)
        self.buffer[(entity['id'], fragment or DEFAULT)] = entity
        if len(self.buffer) >= self.size:
            self.flush()
            self.buffer = {}

    @abstractmethod
    def flush(self):
        pass
