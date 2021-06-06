import os
import psutil
import time
import itertools
import re
from collections import deque
from contextlib import contextmanager

SPLIT_REGEX = re.compile(r"""
(
 (?:                     # Start of non-capturing group
  (?:\r\n|\r|\n)      |  # Match any single newline, or
  [^\r\n'"]+          |  # Match any character series without quotes or
                         # newlines, or
  "(?:[^"\\]|\\.)*"   |  # Match double-quoted strings, or
  '(?:[^'\\]|\\.)*'      # Match single quoted strings
 )
)
""", re.VERBOSE)

LINE_MATCH = re.compile(r'(\r\n|\r|\n)')


def split_unquoted_newlines(stmt):
    """Split a string on all unquoted newlines.

    Unlike str.splitlines(), this will ignore CR/LF/CR+LF if the requisite
    character is inside of a string."""
    text = str(stmt)
    lines = SPLIT_REGEX.split(text)
    outputlines = ['']
    for line in lines:
        if not line:
            continue
        elif LINE_MATCH.match(line):
            outputlines.append('')
        else:
            outputlines[-1] += line
    return outputlines


def remove_quotes(val):
    """Helper that removes surrounding quotes from strings."""
    if val is None:
        return
    if val[0] in ('"', "'") and val[0] == val[-1]:
        val = val[1:-1]
    return val


def recurse(*cls):
    """Function decorator to help with recursion

    :param cls: Classes to not recurse over
    :return: function
    """
    def wrap(f):
        def wrapped_f(tlist):
            for sgroup in tlist.get_sublists():
                if not isinstance(sgroup, cls):
                    wrapped_f(sgroup)
            f(tlist)

        return wrapped_f

    return wrap


def imt(token, i=None, m=None, t=None):
    """Helper function to simplify comparisons Instance, Match and TokenType
    :param token:
    :param i: Class or Tuple/List of Classes
    :param m: Tuple of TokenType & Value. Can be list of Tuple for multiple
    :param t: TokenType or Tuple/List of TokenTypes
    :return:  bool
    """
    clss = i
    types = [t, ] if t and not isinstance(t, list) else t
    mpatterns = [m, ] if m and not isinstance(m, list) else m

    if token is None:
        return False
    elif clss and isinstance(token, clss):
        return True
    elif mpatterns and any(token.match(*pattern) for pattern in mpatterns):
        return True
    elif types and any(token.ttype in ttype for ttype in types):
        return True
    else:
        return False


def consume(iterator, n):
    """Advance the iterator n-steps ahead. If n is none, consume entirely."""
    deque(itertools.islice(iterator, n), maxlen=0)


@contextmanager
def offset(filter_, n=0):
    filter_.offset += n
    yield
    filter_.offset -= n


@contextmanager
def indent(filter_, n=1):
    filter_.indent += n
    yield
    filter_.indent -= n



def memLog(func):
    """
    内存检视装饰器
    在函数定义时在上方加上@memLog，执行函数时可以检视python运行内存
    """
    def wrapper(*args, **kw):
        print(u'运行前的的内存使用：%.4f MB' % (psutil.Process(os.getpid()).memory_info().rss / 1024 /1024) )
        result = func(*args, **kw)
        print(u'运行后的内存使用：%.4f MB' % (psutil.Process(os.getpid()).memory_info().rss / 1024 /1024) )
        return result
    return wrapper

def timeLog(func):
    """
    运行时间检视装饰器
    在函数定义时在上方加上@timeLog，执行函数时可以记录函数运行时间
    """
    def wrapper(*args, **kw):
        t0 = time.time()
        result = func(*args, **kw)
        t1 = time.time()
        print("当前函数运行时间：%.5f s"%(t1-t0))
        return result
    return wrapper
