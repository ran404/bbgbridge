from datetime import date, datetime

from blpapi import DataType
from collections import OrderedDict

from bbgbridge.util import to_timestamp

bb_container_types = frozenset([
    DataType.SEQUENCE,
    DataType.CHOICE,
    DataType.ENUMERATION
])

bb_plain_types = frozenset([
    DataType.INT32,
    DataType.STRING,
    DataType.FLOAT64,
    DataType.DATE,
    DataType.CHAR
])


def parse_message(msg):
    return parse_element(msg.asElement())


def parse_element(element):
    if element.isArray():
        return parse_array(element)
    else:
        data_type = element.datatype()
        if data_type == DataType.SEQUENCE:
            name = str(element.elementDefinition().name())
            return {name: parse_sequence(element)}
        elif data_type == DataType.CHOICE:
            return parse_element(element.getChoice())
        elif data_type == DataType.ENUMERATION:
            return str(element.getValue())
        else:
            return safe_element_value(element)


def parse_sequence(element):
    children = OrderedDict()
    for child in element.elements():
        child_definition = child.elementDefinition().name()
        children[str(child_definition)] = parse_element(child)
    return children


def parse_array(element):
    data_type = element.datatype()
    if data_type in bb_plain_types:
        return [try_convert_datetime(x) for x in element.values()]
    else:
        return [parse_element(element_value(x)) for x in element.values()]


def element_value(element):
    data_type = element.datatype()
    if data_type in bb_container_types:
        return element
    elif data_type in bb_plain_types:
        return safe_element_value(element)
    else:
        raise ValueError("I don't know how to handle the data type: " + data_type)


def safe_element_value(element):
    return None if element.isNull() else try_convert_datetime(element.getValue())


def try_convert_datetime(value):
    """ Convert value to Timestamp if it is date like. """
    if isinstance(value, (date, datetime)):
        return to_timestamp(value)
    else:
        return value
