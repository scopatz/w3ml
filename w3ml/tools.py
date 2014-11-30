"""Some generic helper tools for warcraft 3 machine learning.
"""
from __future__ import unicode_literals, print_function
import re
from binascii import hexlify, unhexlify

import numpy as np

def u2i(x, size=None):
    """Converts a unicode string to an array of integers. If size is 
    not None, then the result will have a null-padded size.
    """
    v = np.array(bytearray(x, 'utf8'), np.uint8)
    if size is not None:
        lenv = len(v)
        if lenv > size:
            v = v[:size]
        elif lenv < size:
            vv = np.empty(size, np.uint8)
            vv[:lenv] = v
            vv[lenv:] = 0
            v = vv
    return v

def i2u(x):
    """Converts an integer array to a unicode string."""
    return bytearray(x).decode('utf-8')

def ensure_slice(s=None):
    """Ensures a valide slice is returned."""
    if isinstance(s, str):
        s = s.split(':')
        if len(s) == 1:
            s = int(s[0])
            s = slice(s, s + 1)
        else:
            s = [x or 'None' for x in s]
            s = eval('slice({0})'.format(', '.join(s)))
    elif isinstance(s, int):
        s = slice(s, s+1)
    elif s is None:
        s = slice(s)
    else:
        assert isinstance(s, slice)
    return s

noop = lambda x: x

shortsha1 = lambda x: hexlify(x[:4])

isdigit = lambda x: x.isdigit()

def isnumeric(x):
    """Determines if a str or bytes is a number."""
    if hasattr(x, 'isnumeric'):
        return x.isnumeric()
    return all(map(isdigit, x))

def ms_to_time(x):
    """Converts and integer number of miliseconds to a time string."""
    secs = x / 1000.0
    s = secs % 60
    m = int(secs / 60) % 60
    h = int(secs / 3600)
    rtn = []
    if h > 0: 
        rtn.append("{0:02}".format(h))
    if m > 0: 
        rtn.append("{0:02}".format(m))
    rtn.append("{0:06.3f}".format(s))
    return ":".join(rtn)

def stramp(x):
    """String formats actions per minute"""
    return '{0:.5}'.format(x)

HEX_RE = re.compile('[0-9A-Fa-f]*')

def ishex(s):
    """Checks if a string is in hexdecimal form."""
    return HEX_RE.match(s) is not None

