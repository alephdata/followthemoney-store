# Balkhash

This library provides methods to store, fetch and list raw data and entities from datasets in cloud storage buckets.

## Usage Example

```python
import balkhash

storage = balkhash.init(...)
dataset = storage.create_dataset(name="US-OFAC", public=True)
dataset.put(key=entity["id"], val=json.dumps(entity), context="entities")

```

## ToDo

- Signed URL for blobs
- Upload files and content-type other than plain text
- Retry with back-off when rate limit exceeded for creating buckets

## License
MIT