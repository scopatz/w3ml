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

import w3g

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

METADATA_DESC = np.dtype([(b'sha1', u'S20'), (b'build_num', b'i2'), 
                          (b'duration', b'i4')])

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
            db.create_vlarray(r, 'replays', atom=tb.VLStringAtom)
        if 'metadata' not in r:
            db.create_table(r, 'metadata', filters=filters, description=METADATA_DESC)

    def _load_metadata(self):
        m = self.metadata[:]
        self.replay_idx = dict(zip(m['sha1'], range(len(m))))

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
        self.metadata.append([(h, w3f.build_num, w3f.replay_length)])
        self.replay_idx[h] = len(self)

    def dump(self, i):
        """Dumps a replay to the screen."""
        if isinstance(i, bytes) and len(i) == 20:
            i = self.replay_idx[i]
        b = bytes(self.replays[i])
        sys.stdout.write(b)

def act(db, ns):
    """Performs command line actions."""
    if ns.add is not None:
        db.add_replay(ns.add)
    if ns.dump is not None:
        i = unhexlify(ns.dump) if len(ns.dump) == 40 else int(ns.dump)
        db.dump(i)

def main():
    import argparse
    parser = argparse.ArgumentParser('w3ml-db')
    parser.add_argument('file', help='Path to the database')
    parser.add_argument('-a', '--add', dest='add', default=None, 
                        help='adds a replay to the database from a file or url')
    parser.add_argument('--dump', dest='dump', default=None, 
                        help='dumps a replay to the screen')
    ns = parser.parse_args()

    with Database(ns.file) as db:
        act(db, ns)

if __name__ == "__main__":
    main()
