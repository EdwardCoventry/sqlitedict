#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# This code is distributed under the terms and conditions
# from the Apache License, Version 2.0
#
# http://opensource.org/licenses/apache2.0.php

import os
import sqlite3
import tempfile
import logging
from queue import Queue
import threading
import traceback
import weakref


class SqliteDb:
    VALID_FLAGS = ['c', 'r', 'w', 'n']

    def __init__(self, filename=None, tablename='unnamed', flag='c',
                 autocommit=False, journal_mode="DELETE", timeout=5, outer_stack=True):
        self.in_temp = filename is None
        if self.in_temp:
            fd, filename = tempfile.mkstemp(prefix='sqldict')
            os.close(fd)

        if flag not in BaseSqlite.VALID_FLAGS:
            raise RuntimeError("Unrecognized flag: %s" % flag)
        self.flag = flag

        if flag == 'n':
            if os.path.exists(filename):
                os.remove(filename)

        dirname = os.path.dirname(filename)
        if dirname:
            if not os.path.exists(dirname):
                raise RuntimeError('Error! The directory does not exist, %s' % dirname)

        self.filename = filename
        self.tablename = tablename.replace('"', '""')
        self.autocommit = autocommit
        self.journal_mode = journal_mode
        self.timeout = timeout
        self._outer_stack = outer_stack

        self.conn = self._new_conn()
        if self.flag == 'r':
            if self.tablename not in self.get_tablenames(self.filename):
                msg = 'Refusing to create a new table "%s" in read-only DB mode' % tablename
                raise RuntimeError(msg)
        else:
            self.create_table()
            self.create_indexes()
            if self.autocommit:
                self.conn.commit()
        if flag == 'w':
            self.clear()

    def _new_conn(self):
        return SqliteMultithread(
            self.filename,
            autocommit=self.autocommit,
            journal_mode=self.journal_mode,
            outer_stack=self._outer_stack,
        )

    def create_table(self):
        raise NotImplementedError

    def create_indexes(self):
        raise NotImplementedError

    def get_tablenames(self, filename):
        if not os.path.isfile(filename):
            raise IOError('file %s does not exist' % (filename))
        GET_TABLENAMES = 'SELECT name FROM sqlite_master WHERE type="table"'
        with sqlite3.connect(filename) as conn:
            cursor = conn.execute(GET_TABLENAMES)
            res = cursor.fetchall()
        return [name[0] for name in res]

    def commit(self, blocking=True):
        if self.conn is not None:
            self.conn.commit()

    sync = commit

    def close(self, do_log=True, force=False):
        if hasattr(self, 'conn') and self.conn is not None:
            if self.conn.autocommit and not force:
                self.conn.commit(blocking=True)
            self.conn.close()
            self.conn = None
        if self.in_temp:
            try:
                os.remove(self.filename)
            except Exception:
                pass

    def terminate(self):
        if self.flag == 'r':
            raise RuntimeError('Refusing to terminate read-only SqliteDict')
        self.close()
        if self.filename == ':memory:':
            return
        try:
            if os.path.isfile(self.filename):
                os.remove(self.filename)
        except (OSError, IOError):
            pass

    def __del__(self):
        try:
            self.close(do_log=False, force=True)
        except Exception:
            pass
