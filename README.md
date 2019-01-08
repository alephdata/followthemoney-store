# Balkhash

This library provides methods to store, fetch and list entities from datasets stored in local or remote key-value
stores.

## Usage Example

```python
import balkhash

storage = balkhash.init(...)
dataset = storage.create_dataset(name="US-OFAC")
dataset.put(key=entity["id"], val=json.dumps(entity))

```

## ToDo

- Proper serialization and deserialization based on backend
- Use entity hierarchy while storing fragments on Google Datastore

## License
MIT