import logging
import sqlite3
import threading
import traceback
import weakref
from queue import Queue

logger = logging.getLogger(__name__)

_REQUEST_CLOSE = '--close--'
_REQUEST_COMMIT = '--commit--'
_RESPONSE_NO_MORE = '--no more--'


def reraise(tp, value, tb=None):
    if value is None:
        value = tp()
    if value.__traceback__ is not tb:
        raise value.with_traceback(tb)
    raise value


def _put(queue_reference, item):
    if queue_reference is not None:
        queue = queue_reference()
        if queue is None:
            return 1
        queue.put(item)
        return 0
    return 2


class SqliteMultithread(threading.Thread):
    """
    Wrap sqlite connection in a way that allows concurrent requests from multiple threads.
    This is done by internally queueing the requests and processing them sequentially
    in a separate thread (in the same order they arrived).
    """

    def __init__(self, filename, autocommit, journal_mode, outer_stack=True):
        super().__init__()
        self.filename = filename
        self.autocommit = autocommit
        self.journal_mode = journal_mode
        self.reqs = Queue()
        self.daemon = True
        self._outer_stack = outer_stack
        self.log = logging.getLogger('sqlitedict.SqliteMultithread')
        self._lock = threading.Lock()
        self._lock.acquire()
        self.exception = None
        self.start()

    def _connect(self):
        try:
            if self.autocommit:
                conn = sqlite3.connect(self.filename, isolation_level=None, check_same_thread=False)
            else:
                conn = sqlite3.connect(self.filename, check_same_thread=False)
        except Exception:
            self.log.exception("Failed to initialize connection for filename: %s" % self.filename)
            self.exception = sys.exc_info()
            raise

        try:
            conn.execute('PRAGMA journal_mode = %s' % self.journal_mode)
            conn.text_factory = str
            cursor = conn.cursor()
            conn.commit()
            cursor.execute('PRAGMA synchronous=OFF')
        except Exception:
            self.log.exception("Failed to execute PRAGMA statements.")
            self.exception = sys.exc_info()
            raise

        return conn, cursor

    def run(self):
        try:
            conn, cursor = self._connect()
        finally:
            self._lock.release()

        res_ref = None
        while True:
            req, arg, res_ref, outer_stack = self.reqs.get()

            if req == _REQUEST_CLOSE:
                assert res_ref, ('--close-- without return queue', res_ref)
                break
            elif req == _REQUEST_COMMIT:
                conn.commit()
                _put(res_ref, _RESPONSE_NO_MORE)
            else:
                try:
                    cursor.execute(req, arg)
                except Exception:
                    with self._lock:
                        self.exception = (e_type, e_value, e_tb) = sys.exc_info()

                    inner_stack = traceback.extract_stack()
                    self.log.error('Inner exception:')
                    for item in traceback.format_list(inner_stack):
                        self.log.error(item)
                    self.log.error('')
                    for item in traceback.format_exception_only(e_type, e_value):
                        self.log.error(item)
                    self.log.error('')
                    if self._outer_stack:
                        self.log.error('Outer stack:')
                        for item in traceback.format_list(outer_stack):
                            self.log.error(item)
                        self.log.error('Exception will be re-raised at next call.')
                    else:
                        self.log.error(
                            'Unable to show the outer stack. Pass outer_stack=True when initializing to show the outer stack.')

                if res_ref:
                    for rec in cursor:
                        if _put(res_ref, rec) == 1:
                            break
                    _put(res_ref, _RESPONSE_NO_MORE)

                if self.autocommit:
                    conn.commit()

        self.log.debug('received: %s, send: --no more--', req)
        conn.close()
        _put(res_ref, _RESPONSE_NO_MORE)

    def check_raise_error(self):
        with self._lock:
            if self.exception:
                e_type, e_value, e_tb = self.exception
                self.exception = None
                self.log.error(
                    'An exception occurred from a previous statement, view the logging namespace "sqlitedict" for outer stack.')
                reraise(e_type, e_value, e_tb)

    def execute(self, req, arg=None, res=None):
        self.check_raise_error()
        stack = None

        if self._outer_stack:
            stack = traceback.extract_stack()[:-1]

        res_ref = None
        if res:
            res_ref = weakref.ref(res)

        self.reqs.put((req, arg or tuple(), res_ref, stack))

    def executemany(self, req, items):
        for item in items:
            self.execute(req, item)
        self.check_raise_error()

    def select(self, req, arg=None):
        res = Queue()
        self.execute(req, arg, res)
        while True:
            rec = res.get()
            self.check_raise_error()
            if rec == _RESPONSE_NO_MORE:
                break
            yield rec

    def select_one(self, req, arg=None):
        try:
            return next(iter(self.select(req, arg)))
        except StopIteration:
            return None

    def commit(self, blocking=True):
        if blocking:
            self.select_one(_REQUEST_COMMIT)
        else:
            self.execute(_REQUEST_COMMIT)

    def close(self, force=False):
        if force:
            self.reqs.put((_REQUEST_CLOSE, None, weakref.ref(Queue()), None))
        else:
            self.select_one(_REQUEST_CLOSE)
            self.join()
