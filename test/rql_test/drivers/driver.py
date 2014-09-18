from __future__ import print_function

import atexit, collections, os, re, sys
from datetime import datetime, tzinfo, timedelta

try:
    unicode
except NameError:
    unicode = str
try:
	xrange
except NameError:
	xrange = range

# -- timezone objects

class UTCTimeZone(tzinfo):
    '''UTC'''
    
    def utcoffset(self, dt):
        return timedelta(0)
    
    def tzname(self, dt):
        return "UTC"
    
    def dst(self, dt):
        return timedelta(0)

class PacificTimeZone(tzinfo):
    '''Pacific timezone emulator for timestamp: 1375147296.68'''
    
    def utcoffset(self, dt):
        return timedelta(-1, 61200)
    
    def tzname(self, dt):
        return 'PDT'
    
    def dst(self, dt):
        return timedelta(0, 3600)

# -- import test resources - NOTE: these are path dependent

stashedPath = sys.path

# - test_util - TODO: replace with methods from common

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import test_util

# - common

sys.path.insert(0, os.path.realpath(os.path.join(os.path.dirname(__file__), os.path.pardir, os.path.pardir, 'common')))
import utils
r = utils.import_python_driver()

# -

sys.path = stashedPath

# --

# JSPORT = int(sys.argv[1])
CPPPORT = int(sys.argv[2])
DB_AND_TABLE_NAME = sys.argv[3]
CLUSTER_PORT = int(sys.argv[4])
BUILD = sys.argv[5]

# -- utilities --

failure_count = 0

def print_test_failure(test_name, test_src, message):
    global failure_count
    failure_count = failure_count + 1
    print('')
    print("TEST FAILURE: %s" % test_name.encode('utf-8'))
    print("TEST BODY:    %s" % test_src.encode('utf-8'))
    print(message)
    print('')

def check_pp(src, query):
    # This isn't a good indicator because of lambdas, whitespace differences, etc
    # But it will at least make sure that we don't crash when trying to print a query
    printer = r.errors.QueryPrinter(query)
    composed = printer.print_query()
    #if composed != src:
    #    print('Warning, pretty printing inconsistency:')
    #    print("Source code: %s", src)
    #    print("Printed query: %s", composed)

class Lst:
    def __init__(self, lst):
        self.lst = lst

    def __eq__(self, other):
        if not hasattr(other, '__iter__'):
            return False

        i = 0
        for row in other:
            if i >= len(self.lst) or (self.lst[i] != row):
                return False
            i += 1

        if i != len(self.lst):
            return False

        return True

    def __repr__(self):
        return repr(self.lst)

class Bag(Lst):
    def __init__(self, lst):
        self.lst = sorted(lst, key=lambda x: repr(x))

    def __eq__(self, other):
        if not hasattr(other, '__iter__'):
            return False

        other = sorted(other, key=lambda x: repr(x))

        if len(self.lst) != len(other):
            return False

        for a, b in zip(self.lst, other):
            if a != b:
                return False

        return True

class Dct:
    def __init__(self, dct):
        assert isinstance(dct, dict)
        self.dct = dct

    def __eq__(self, other):
        if not isinstance(other, dict):
            return False
        
        if not set(self.keys()) == set(other.keys()):
            return False
        
        for key in self.dct:
            if not key in other:
                return False
            val = other[key]
            if isinstance(val, (str, unicode)):
                # Remove additional error info that creeps in in debug mode
                val = re.sub("(?ms)\nFailed assertion:.*", "", val)
            other[key] = val
            if not self.dct[key] == other[key]:
                return False
        return True
    
    def keys(self):
        return self.dct.keys()
    
    def __repr__(self):
        return repr(self.dct)

class Err:
    def __init__(self, err_type=None, err_msg=None, err_frames=None, regex=False):
        self.etyp = err_type
        self.emsg = err_msg
        self.frames = None # err_frames # TODO: test frames
        self.regex = regex

    def __eq__(self, other):
        if not isinstance(other, Exception):
            return False

        if self.etyp and self.etyp != other.__class__.__name__:
            return False

        if self.regex:
            return re.match(self.emsg, str(other))

        else:
            otherMessage = str(other)
            if isinstance(other, (r.errors.RqlError, r.errors.RqlDriverError)):
                otherMessage = other.message
                            
                # Strip "offending object" from the error message
                otherMessage = re.sub("(?ms)(\.)?( in)?:\n.*", ".", otherMessage)
                otherMessage = re.sub("(?ms)\nFailed assertion:.*", "", otherMessage)

            if self.emsg and self.emsg != otherMessage:
                return False

            if self.frames and (not hasattr(other, 'frames') or self.frames != other.frames):
                return False

            return True

    def __repr__(self):
        return "%s(%s\"%s\")" % (self.etyp, self.regex and '~' or '', repr(self.emsg) or '')


class Arr:
    def __init__(self, length, thing=None):
        self.length = length
        self.thing = thing

    def __eq__(self, arr):
        if not isinstance(arr, list):
            return False

        if not self.length == len(arr):
            return False

        if self.thing is None:
            return True

        return all([v == self.thing for v in arr])

    def __repr__(self):
        return "arr(%d, %s)" % (self.length, repr(self.thing))

class Uuid:
    def __eq__(self, thing):
        if not isinstance(thing, (str, unicode)):
            return False
        return re.match("[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}", thing) != None

    def __repr__(self):
        return "uuid()"

class Int:
    def __init__(self, i):
        self.i = i

    def __eq__(self, thing):
        return isinstance(thing, int) and (self.i == thing)

class Float:
    def __init__(self, f):
        self.f = f

    def __eq__(self, thing):
        return isinstance(thing, float) and (self.f == thing)

# -- Curried output test functions --

def eq(exp):
    if exp == ():
        return lambda x: True

    if isinstance(exp, list):
        exp = Lst(exp)
    elif isinstance(exp, dict):
        exp = Dct(exp)

    def sub(val):
        if not (val == exp):
            return False
        else:
            return True
    return sub

class PyTestDriver:
    
    cpp_conn = None
    
    def __init__(self):
        print('Creating default connection to CPP server on port %s\n' % str(CPPPORT))
        self.cpp_conn = self.connect()
        self.scope = {}
        
        if 'test' not in r.db_list().run(self.cpp_conn):
            r.db_create('test').run(self.cpp_conn)
    
    def connect(self):
        return r.connect(host='localhost', port=CPPPORT)

    def define(self, expr):
        try:
            exec(expr, globals(), self.scope)
        except Exception as e:
            print_test_failure('Exception while processing define', expr, str(e))

    def run(self, src, expected, name, runopts, testopts):
        if runopts:
            runopts["profile"] = True
        else:
            runopts = {"profile": True}
        
        conn = None
        if 'new-connection' in testopts and testopts['new-connection'] is True:
            conn = self.connect()
        else:
            conn = self.cpp_conn
        
        # Try to build the expected result
        if expected:
            exp_val = eval(expected, dict(list(globals().items()) + list(self.scope.items())))
        else:
            # This test might not have come with an expected result, we'll just ensure it doesn't fail
            exp_val = ()
        
        # Run the test
        if 'reql-query' in testopts and str(testopts['reql-query']).lower() == 'false':
            try:
                result = eval(src, globals(), self.scope)
            except Exception as err:
                result = err
        else:
            # Try to build the test
            try:
                query = eval(src, dict(list(globals().items()) + list(self.scope.items())))
            except Exception as err:
                if not isinstance(exp_val, Err):
                    print_test_failure(name, src, "Error eval'ing test src:\n\t%s" % repr(err))
                elif not eq(exp_val)(err):
                    print_test_failure(name, src, "Error eval'ing test src not equal to expected err:\n\tERROR: %s\n\tEXPECTED: %s" % (repr(err), repr(exp_val)))
    
                return # Can't continue with this test if there is no test query
    
            # Check pretty-printing
            check_pp(src, query)
    
            # Run the test
            result = None
            try:
                result = query.run(conn, **runopts)
                if result and "profile" in runopts and runopts["profile"] and "value" in result:
                    result = result["value"]
            except Exception as err:
                result = err
        
        # Save variable if requested
        
        if 'variable' in testopts:
            self.scope[testopts['variable']] = result
        
        # Compare to the expected result
        
        if isinstance(result, Exception):
            if not isinstance(exp_val, Err):
                print_test_failure(name, src, "Error running test on CPP server:\n\t%s %s" % (repr(result), str(result)))
            elif not eq(exp_val)(result):
                print_test_failure(name, src, "Error running test on CPP server not equal to expected err:\n\tERROR: %s\n\tEXPECTED: %s" % (repr(result), repr(exp_val)))
        elif not eq(exp_val)(result):
            print_test_failure(name, src, "CPP result is not equal to expected result:\n\tVALUE: %s\n\tEXPECTED: %s" % (repr(result), repr(exp_val)))

driver = PyTestDriver()

# Emitted test code will consist of calls to these functions

def test(query, expected, name, runopts=None, testopts=None):
    if runopts is None:
        runopts = {}
    else:
        for k, v in runopts.items():
            if isinstance(v, str):
                runopts[k] = eval(v)
    if testopts is None:
        testopts = {}
    
    if 'max_batch_rows' not in runopts:
        runopts['max_batch_rows'] = 3
    if expected == '':
        expected = None
    driver.run(query, expected, name, runopts, testopts)

# Generated code must call either `setup_table()` or `check_no_table_specified()`
def setup_table(table_variable_name, table_name):
    def _teardown_table():
        if DB_AND_TABLE_NAME == "no_table_specified":
            res = r.db("test").table_drop(table_name).run(driver.cpp_conn)
            assert res == {"dropped": 1}
        else:
            db, table = DB_AND_TABLE_NAME.split(".")
            res = r.db(db).table(table).delete().run(driver.cpp_conn)
            assert res["errors"] == 0
            res = r.db(db).table(table).index_list().for_each(
                r.db(db).table(table).index_drop(r.row)).run(driver.cpp_conn)
            assert "errors" not in res or res["errors"] == 0
    atexit.register(_teardown_table)
    if DB_AND_TABLE_NAME == "no_table_specified":
        res = r.db("test").table_create(table_name).run(driver.cpp_conn)
        assert res == {"created": 1}
        globals()[table_variable_name] = r.db("test").table(table_name)
    else:
        db, table = DB_AND_TABLE_NAME.split(".")
        globals()[table_variable_name] = r.db(db).table(table)

def check_no_table_specified():
    if DB_AND_TABLE_NAME != "no_table_specified":
        raise ValueError("This test isn't meant to be run against a specific table")

def define(expr):
    driver.define(expr)

def bag(lst):
    return Bag(lst)

def err(err_type, err_msg=None, frames=None):
    return Err(err_type, err_msg, frames)

def err_regex(err_type, err_msg=None, frames=None):
    return Err(err_type, err_msg, frames, True)

def arrlen(length, thing=None):
    return Arr(length, thing)

def uuid():
    return Uuid()

def shard(table_name):
    test_util.shard_table(CLUSTER_PORT, BUILD, table_name)

def int_cmp(i):
    return Int(i)

def float_cmp(f):
    return Float(f)

def the_end():
    global failure_count
    if failure_count > 0:
        sys.exit("Failed %d tests" % failure_count)

false = False
true = True
