from followthemoney import model
from abc import ABC, abstractmethod


class Dataset(ABC):

    def __init__(self, name):
        self.name = name

    @abstractmethod
    def delete(self, entity_id=None, fragment=None):
        pass

    @abstractmethod
    def put(self, entity, fragment=None):
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
            if entity is not None:
                if entity.id == partial.id:
                    entity.merge(partial)
                    continue
                yield entity
            entity = partial
        if entity is not None:
            yield entity


class Bulk(ABC):

    def __init__(self, dataset, size):
        self.dataset = dataset
        self.size = size
        self.buffer = []

    def put(self, entity, fragment=None):
        entity = self.dataset._entity_dict(entity)
        self.buffer.append((entity, fragment))
        if len(self.buffer) >= self.size:
            self.flush()
            self.buffer = []

    @abstractmethod
    def flush(self):
        pass
