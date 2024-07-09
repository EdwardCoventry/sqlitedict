# !/usr/bin/env python
# -*- coding: utf-8 -*-
#
# This code is distributed under the terms and conditions
# from the Apache License, Version 2.0
#
# http://opensource.org/licenses/apache2.0.php

"""
A lightweight wrapper around Python's sqlite3 database, with a dict-like interface
and multi-thread access support::

>>> mydict = SqliteDict('some.db', autocommit=True) # the mapping will be persisted to file `some.db`
>>> mydict['some_key'] = any_picklable_object
>>> print(mydict['some_key'])
>>> print(len(mydict)) # etc... all dict functions work

Pickle is used internally to serialize the values. Keys are strings.

If you don't use autocommit (default is no autocommit for performance), then
don't forget to call `mydict.commit()` when done with a transaction.
"""

import os
import sqlite3
import tempfile
import logging
from base64 import b64decode, b64encode
from pickle import dumps, loads, HIGHEST_PROTOCOL as PICKLE_PROTOCOL
from collections import UserDict as DictClass
from typing import Dict, List
from .sqlitedb import SqliteDb  # Relative import from sqlitedb

logger = logging.getLogger(__name__)


def encode(obj):
    """Serialize an object using pickle to a binary format accepted by SQLite."""
    return sqlite3.Binary(dumps(obj, protocol=PICKLE_PROTOCOL))


def decode(obj):
    """Deserialize objects retrieved from SQLite."""
    return loads(bytes(obj))


def encode_key(key):
    """Serialize a key using pickle + base64 encoding to text accepted by SQLite."""
    return b64encode(dumps(key, protocol=PICKLE_PROTOCOL)).decode("ascii")


def decode_key(key):
    """Deserialize a key retrieved from SQLite."""
    return loads(b64decode(key.encode("ascii")))


def identity(obj):
    """Identity function f(x) = x for encoding/decoding."""
    return obj


class SqliteDict(SqliteDb, DictClass):
    standard_columns: Dict[str, str] = {'key': 'TEXT PRIMARY KEY', 'value': 'BLOB'}
    additional_columns: Dict[str, str] = {}
    index_standard_columns: List[str] = ['key']
    index_additional_columns: List[str] = []
    table_specific_additional_columns: Dict[str, Dict[str, str]] = {}
    table_specific_index_additional_columns: Dict[str, List[str]] = {}

    @property
    def table_columns(self):
        return self.table_specific_additional_columns.get(self.tablename, {})

    @property
    def index_table_columns(self):
        return self.table_specific_index_additional_columns.get(self.tablename, [])

    def __init__(self, filename=None, tablename='unnamed', flag='c',
                 autocommit=False, journal_mode="DELETE", encode=encode,
                 decode=decode, encode_key=identity, decode_key=identity,
                 timeout=5, outer_stack=True):
        self.encode = encode
        self.decode = decode
        self.encode_key = encode_key
        self.decode_key = decode_key
        super().__init__(filename, tablename, flag, autocommit, journal_mode, timeout, outer_stack)

    def __str__(self):
        return f"SqliteDict {self.filename}"

    def create_table(self):
        all_columns = {**self.standard_columns, **self.additional_columns, **self.table_columns}
        column_defs = ', '.join(f'"{col_name}" {col_type}' for col_name, col_type in all_columns.items())
        CREATE_TABLE_SQL = f'''CREATE TABLE IF NOT EXISTS "{self.tablename}" (
                               {column_defs}
                               )'''
        self.conn.execute(CREATE_TABLE_SQL)

    def create_indexes(self):
        for idx in (*self.index_standard_columns, *self.index_additional_columns, *self.index_table_columns):
            INDEX_CMD = f'CREATE INDEX IF NOT EXISTS idx_{idx} ON "{self.tablename}"({idx})'
            self.conn.execute(INDEX_CMD)

    def __len__(self):
        GET_LEN = 'SELECT COUNT(*) FROM "%s"' % self.tablename
        rows = self.conn.select_one(GET_LEN)[0]
        return rows if rows is not None else 0

    def __bool__(self):
        GET_MAX = 'SELECT MAX(ROWID) FROM "%s"' % self.tablename
        m = self.conn.select_one(GET_MAX)[0]
        return True if m is not None else False

    def iterkeys(self):
        GET_KEYS = 'SELECT key FROM "%s" ORDER BY rowid' % self.tablename
        for key in self.conn.select(GET_KEYS):
            yield self.decode_key(key[0])

    def itervalues(self):
        GET_VALUES = 'SELECT value FROM "%s" ORDER BY rowid' % self.tablename
        for value in self.conn.select(GET_VALUES):
            yield self.decode(value[0])

    def iteritems(self):
        GET_ITEMS = 'SELECT key, value FROM "%s" ORDER BY rowid' % self.tablename
        for key, value in self.conn.select(GET_ITEMS):
            yield self.decode_key(key), self.decode(value)

    def keys(self):
        return list(self.iterkeys())

    def values(self):
        return list(self.itervalues())

    def items(self):
        return list(self.iteritems())

    def __contains__(self, key):
        HAS_ITEM = 'SELECT 1 FROM "%s" WHERE key = ?' % self.tablename
        return self.conn.select_one(HAS_ITEM, (self.encode_key(key),)) is not None

    def __getitem__(self, key):
        GET_ITEM = 'SELECT value FROM "%s" WHERE key = ?' % self.tablename
        item = self.conn.select_one(GET_ITEM, (self.encode_key(key),))
        if item is None:
            raise KeyError(key)
        return self.decode(item[0])

    def __setitem__(self, key, value):
        if self.flag == 'r':
            raise RuntimeError('Refusing to write to read-only SqliteDict')
        ADD_ITEM = 'REPLACE INTO "%s" (key, value) VALUES (?,?)' % self.tablename
        self.conn.execute(ADD_ITEM, (self.encode_key(key), self.encode(value)))
        if self.autocommit:
            self.commit()

    def __delitem__(self, key):
        if self.flag == 'r':
            raise RuntimeError('Refusing to delete from read-only SqliteDict')
        if key not in self:
            raise KeyError(key)
        DEL_ITEM = 'DELETE FROM "%s" WHERE key = ?' % self.tablename
        self.conn.execute(DEL_ITEM, (self.encode_key(key),))
        if self.autocommit:
            self.commit()

    def update(self, items=(), **kwds):
        if self.flag == 'r':
            raise RuntimeError('Refusing to update read-only SqliteDict')
        try:
            items = items.items()
        except AttributeError:
            pass
        items = [(self.encode_key(k), self.encode(v)) for k, v in items]
        UPDATE_ITEMS = 'REPLACE INTO "%s" (key, value) VALUES (?, ?)' % self.tablename
        self.conn.executemany(UPDATE_ITEMS, items)
        if kwds:
            self.update(kwds)
        if self.autocommit:
            self.commit()

    def __iter__(self):
        return self.iterkeys()

    def clear(self):
        if self.flag == 'r':
            raise RuntimeError('Refusing to clear read-only SqliteDict')
        CLEAR_ALL = 'DELETE FROM "%s";' % self.tablename
        self.conn.commit()
        self.conn.execute(CLEAR_ALL)
        self.conn.commit()
