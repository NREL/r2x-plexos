import inspect
import sys

import pytest

from r2x_plexos.models.component import PLEXOSObject


def get_subclasses_of_base_model():
    module = sys.modules["r2x_plexos.models"]
    classes = []
    for _, obj in inspect.getmembers(module, inspect.isclass):
        if issubclass(obj, PLEXOSObject) and obj is not PLEXOSObject:
            classes.append(obj)
    return classes


models = get_subclasses_of_base_model()


@pytest.mark.parametrize(
    "model",
    models,
)
def test_model_exmaples(model):
    test = model.example()
    assert isinstance(test, model)
    assert isinstance(test, PLEXOSObject)
