# Balkhash

This library provides methods to store, fetch and list raw data and entities from datasets in cloud storage buckets.

## Usage Example

```python
import balkhash

storage = balkhash.init(...)
dataset = storage.create_dataset(name="US-OFAC", public=True)
dataset.put(key=entity["id"], val=entity, context="entities")

```

## ToDo

- Signed URL for blobs
- Retry with back-off when rate limit exceeded for creating buckets