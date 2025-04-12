"""Which ``lw_search_attributes`` column holds each attribute type.

Kept free of imports from the store package so both the store and the visibility modules
can use it without import cycles.
"""

from __future__ import annotations

#: Maps each :class:`~loomwork.visibility.schema.AttributeType` to the column that holds its
#: value in ``lw_search_attributes``, and to the ``value_type`` discriminator stored
#: alongside it. Keyword lists store one row per element, each with ``value_type ==
#: "keyword"`` and the element in ``str_value``.
VALUE_COLUMNS: dict[str, str] = {
    "string": "str_value",
    "int": "int_value",
    "float": "float_value",
    "bool": "bool_value",
    "datetime": "time_value",
    "keyword_list": "str_value",
}

#: The ``value_type`` discriminator stored for each attribute type. Keyword list elements
#: are stored as ``"keyword"`` rather than ``"keyword_list"`` since each row is one element.
VALUE_TYPES: dict[str, str] = {
    "string": "string",
    "int": "int",
    "float": "float",
    "bool": "bool",
    "datetime": "datetime",
    "keyword_list": "keyword",
}

__all__ = ["VALUE_COLUMNS", "VALUE_TYPES"]
