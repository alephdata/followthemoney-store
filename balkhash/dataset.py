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
    def fragments(self, entity_id=None, fragment=None):
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
            fragment = model.get_proxy(fragment)
            if entity is not None:
                if entity.id == fragment.id:
                    entity.merge(fragment)
                    continue
                yield entity
            entity = fragment
        if entity is not None:
            yield entity
