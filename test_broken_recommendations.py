import pytest

from broken_recommendations import Matrix


@pytest.mark.parametrize(
    'items, expected_repr',
    [
        pytest.param([], "None"),
        pytest.param([1], "1 None\nNone None"),
        pytest.param([1, 2], "1 2\nNone None"),
        pytest.param([1, 2, 3], "1 2 3\nNone None None\nNone None None"),
        pytest.param([1, 2, 3, 4, 5, 6], "1 2 3\n4 5 6\nNone None None"),
        pytest.param([1, 2, 3, 4, 5, 6, 7], "1 2 3 4\n5 6 7 None\nNone None None None\nNone None None None"),
    ]
)
def test_add_items_and_resize(items, expected_repr):
    matrix = Matrix()
    for item in items:
        matrix.add_item(item)
    assert str(matrix) == expected_repr


def test_add_none():
    with pytest.raises(ValueError):
        Matrix().add_item(None)
    with pytest.raises(ValueError):
        Matrix().add_item()


@pytest.mark.parametrize(
    'items, popped_item, expected_repr',
    [
        pytest.param([1, 2], 2, "1 None\nNone None"),
        pytest.param([1], 1, "None"),
    ]
)
def test_pop(items, popped_item, expected_repr):
    matrix = Matrix()
    for item in items:
        matrix.add_item(item)
    assert matrix.pop() is popped_item
    assert str(matrix) == expected_repr


def test_pop_empty():
    with pytest.raises(IndexError):
        Matrix().pop()
