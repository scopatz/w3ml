"""Some generic helper tools for warcraft 3 machine learning.
"""

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

isdigit = lambda x: x.isdigit()

def isnumeric(x):
    if hasattr(x, 'isnumeric'):
        return x.isnumeric()
    return all(map(isdigit, x))