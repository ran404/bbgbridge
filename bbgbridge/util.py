import json
import numpy as np
import pandas as pd


def is_string(arg):
    return isinstance(arg, str)


def as_list(arg):
    if is_string(arg):
        return [arg]
    return arg


def to_timestamp(timestamp_obj):
    """
    Creates a pandas pd.Timestamp using its constructor if not already
    a pd.Timestamp object, otherwise just returns it
    """
    if isinstance(timestamp_obj, pd.Timestamp):
        return timestamp_obj
    else:
        return pd.Timestamp(timestamp_obj)


def datetime_string(dt, date_format):
    return to_timestamp(dt).strftime(date_format)


def date_bloomberg_string(dt):
    if is_string(dt) and '/' in dt:
        raise ValueError('ambiguous date string', dt)
    return datetime_string(dt, "%Y%m%d")


def dedupe(items, key=None):
    seen = set()
    for item in items:
        val = item if key is None else key(item)
        if val not in seen:
            yield item
            seen.add(val)


def merge_dicts(*dict_args):
    """
    Given any number of dicts, shallow copy and merge into a new dict,
    precedence goes to key value pairs in latter dicts.
    """
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result


def numpy_obj_to_python(obj):
    if isinstance(obj, np.ndarray) and obj.ndim == 0:
        return obj.item()
    else:
        return obj


class CustomJSONEncoder(json.JSONEncoder):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def default(self, obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        if isinstance(obj, pd.DataFrame) or isinstance(obj, pd.Series):
            return self.pandas_obj_to_json(obj)
        if isinstance(obj, np.ndarray) and obj.ndim <= 1:
            return obj.tolist() if obj.ndim == 1 else obj.item()
        if isinstance(obj, np.bool_):
            return obj.item()
        if isinstance(obj, np.signedinteger):
            return obj.item()
        return json.JSONEncoder.default(self, obj)

    @staticmethod
    def pandas_obj_to_json(obj):
        if isinstance(obj, pd.DataFrame):
            new_obj = obj.applymap(numpy_obj_to_python)
        elif isinstance(obj, pd.Series):
            new_obj = obj.apply(numpy_obj_to_python)
        else:
            raise ValueError('Something very bad has happened, programming error?!')
        return json.loads(new_obj.to_json(orient='split', date_format='iso'))
