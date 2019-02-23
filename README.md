# Balkhash

This library provides methods to store, fetch and list entities formatted as
`followthemoney` data as datasets stored in local or remote key-value stores.

## Usage

### Command-line usage

```bash
# Insert a bunch of FtM entities into a store:
$ cat ftm-entities.ijson | balkhash write -f my_dataset
# Re-create the entities in aggregated form:
$ balkhash iterate -f my_dataset | alephclient write-entities -f my_dataset
```

### Python Library

```python
import balkhash

dataset = balkhash.init("US-OFAC")
dataset.put(entity, fragment='1')
```

## ToDo

- Proper serialization and deserialization based on backend
- Use entity hierarchy while storing fragments on Google Datastore
