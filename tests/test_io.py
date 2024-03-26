from pathlib import Path
import tempfile

from ftmstore import init
from ftmstore.utils import write_stream, iterate_stream


def test_input_output():
    uri = "sqlite://"
    dataset = init("IO-TEST", database_uri=uri)
    assert dataset.name == "IO-TEST"
    assert len(dataset.store) == 0

    input_file = Path("./tests/fixtures/entities")

    with open(input_file, "r") as f:
        data = f.readlines()

    number_of_entities = len(data)

    # test writing FTM entities to FTM Store
    write_stream(dataset, open(input_file, "r"))
    assert len(dataset) == number_of_entities

    # test reading FTM entities from FTM Store
    output_file = Path("./tests/fixtures/temp_output")

    fh = open(output_file, "w+b")
    iterate_stream(dataset, fh)
    fh.close()

    with open(output_file, "r") as f:
        assert len(f.readlines()) == number_of_entities

    output_file.unlink()
