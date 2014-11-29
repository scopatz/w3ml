"""Some generic helper tools for warcraft 3 machine learning.
"""
from binascii import hexlify, unhexlify

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
