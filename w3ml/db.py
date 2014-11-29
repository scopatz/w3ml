"""Tools for handling the w3ml database.
"""
from __future__ import print_function, unicode_literals
import os
import sys
from io import BytesIO
from hashlib import sha1
from binascii import hexlify, unhexlify

import numpy as np
import tables as tb
from prettytable import PrettyTable

import w3g

from w3ml.tools import ensure_slice, isnumeric, noop, shortsha1, ms_to_time, \
    u2i, i2u

if sys.version_info[0] < 3:
    # to print unicode
    import codecs
    UTF8Writer = codecs.getwriter('utf8')
    utf8writer = UTF8Writer(sys.stdout)
    def umake(f):
        def uprint(*objects, **kw):
            uo = map(unicode, objects)
            if 'file' not in kw:
                kw['file'] = utf8writer
            f(*uo, **kw)
        return uprint
    print = umake(print)

METADATA_DESC = np.dtype([(b'sha1', b'S20'), (b'speed', b'S6'), (b'map', b'S100'),
    (b'winner', np.uint8), (b'player1_name', np.uint8, 50), (b'player2_name', np.uint8, 50), (b'build_num', b'i2'), (b'duration', b'i4')])

class Database(object):
    """Represents a database of Warcraft 3 replay files."""

    def __init__(self, db):
        """Parameters
        ----------
        db : str or tables.File
            An HDF5 file.
        """
        self._opened_here = False
        if isinstance(db, str):
            db = tb.open_file(db, 'a')
            self._opened_here = True
        self.db = db
        self.root = db.root
        self._ensure_heirarchy()
        self.replays = db.root.replays
        self.metadata = db.root.metadata
        self._load_metadata()

    def __del__(self):
        if self._opened_here and not self.closed:
            self.db.close()

    def __enter__(self):
        return self

    def __exit__(self ,type, value, traceback):
        if self._opened_here and not self.closed:
            self.db.close()

    @property
    def closed(self):
        return not self.db.isopen

    def __len__(self):
        return len(self.replay_idx)

    def _ensure_heirarchy(self):
        r = self.root
        db = self.db
        filters = tb.Filters(complib=b'zlib', complevel=1)
        if 'replays' not in r:
            db.create_vlarray(r, 'replays', atom=tb.VLStringAtom())
        if 'metadata' not in r:
            db.create_table(r, 'metadata', filters=filters, description=METADATA_DESC)

    def _load_metadata(self):
        m = self.metadata[:]
        self.replay_idx = dict(zip(m['sha1'], range(len(m))))

    def sha1(self, x):
        """Get the SHA1 hash from part or all of a hash."""
        ris = self.replay_idx
        if x in ris:
            return ris[x]
        if isinstance(x, str):
            x = unhexlify(x)
        hashes = sorted(ris.keys())
        n = len(hashes)
        i = n // 2
        h = hashes[i]
        while i != -1 and i != n and not h.startswith(x):
            if i == 0:
                i = -1
                continue
            elif i+1 == n:
                i = n
                continue
            i = (i + n)//2 if x > h else i//2
            h = hashes[i]
        if i == -1 or i == n:
            raise ValueError('SHA1 could not be found that matches {0}'.format(x))
        return h

    def idx(self, x):
        """Get the index for a sha1 hash, the first part of a sha1 hash, or a number.
        """
        ris = self.replay_idx
        if isinstance(x, int):
            return x
        elif x in ris:
            return ris[x]
        elif isnumeric(x):
            return int(x)
        else:
            return ris[self.sha1(x)] 

    def add_replay(self, path):
        """Adds a replay from path to the database."""
        with open(path, 'rb') as f:
            b = f.read()
        h = sha1(b).digest()
        if h in self.replay_idx:
            msg = '{0} (SHA1 {1}) already in database at index {2}, skipping.'
            print(msg.format(path, hexlify(h), self.replay_idx[h]))
            return
        w3f = w3g.File(BytesIO(b))
        self.replays.append(b)
        self.metadata.append([(h, w3f.game_speed, w3f.map_name, w3f.winner(),
            u2i(w3f.player_name(1), 50), u2i(w3f.player_name(2), 50), 
            w3f.build_num, w3f.replay_length)])
        self.replay_idx[h] = len(self)

    def dump(self, i):
        """Dumps a replay to the screen."""
        i = self.idx(i)
        b = bytes(self.replays[i])
        sys.stdout.write(b)

    def pprint(self, s=None):
        s = ensure_slice(s)
        transformers = [shortsha1, noop, noop, noop, i2u, i2u, noop, ms_to_time]
        cols = ['idx'] + list(map(lambda x: x.replace('_', ' '), METADATA_DESC.names))
        pt = PrettyTable(cols)
        data = self.metadata[s]
        for i, row in enumerate(data):
            r = [i] + [f(x) for f, x in zip(transformers, row)]
            pt.add_row(r)
        ptstr = pt.get_string()
        print(ptstr)

def act(db, ns):
    """Performs command line actions."""
    if ns.add is not None:
        db.add_replay(ns.add)
    if ns.dump is not None:
        db.dump(ns.dump)
    if ns.list != '<not-given>':
        db.pprint(ns.list)

def main():
    import argparse
    parser = argparse.ArgumentParser('w3ml-db')
    parser.add_argument('file', help='Path to the database')
    parser.add_argument('-a', '--add', dest='add', default=None, 
                        help='adds a replay to the database from a file or url')
    parser.add_argument('-l', '--list', dest='list', nargs='?', const=None,
                        default='<not-given>', 
                        help='lists metadata in the database')
    parser.add_argument('--dump', dest='dump', default=None, 
                        help='dumps a replay to the screen')
    ns = parser.parse_args()

    with Database(ns.file) as db:
        act(db, ns)

if __name__ == "__main__":
    main()
