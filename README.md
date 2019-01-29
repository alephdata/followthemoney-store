# Balkhash

This library provides methods to store, fetch and list entities from datasets stored in local or remote key-value
stores.

## Usage Example

```python
import balkhash

dataset = balkhash.init("US-OFAC")
dataset.put(entity, fragment='1')
```

## ToDo

- Proper serialization and deserialization based on backend
- Use entity hierarchy while storing fragments on Google Datastore
