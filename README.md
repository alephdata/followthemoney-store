# Balkhash

This library provides methods to store, fetch and list entities formatted as
`followthemoney` data as datasets stored in local or remote key-value stores.

## Usage

### Command-line usage

```bash
# Insert a bunch of FtM entities into a store:
$ cat ftm-entities.ijson | balkhash write -d my_dataset
# Re-create the entities in aggregated form:
$ balkhash iterate -d my_dataset | alephclient write-entities -f my_dataset
```

If you don't want to keep the balkhash dataset generated above, there's a
shortcut that combines the write and iterate functions:

```bash
$ cat ftm-entities.ijson | balkhash aggregate | alephclient write-entities -f my_dataset
```

### Python Library

```python
import balkhash

dataset = balkhash.init("US-OFAC")
dataset.put(entity, fragment='1')
```
