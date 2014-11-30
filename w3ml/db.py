"""Tools for handling the w3ml database.
"""
from __future__ import print_function, unicode_literals
import os
import sys
from io import BytesIO
from hashlib import sha1
from warnings import warn
from binascii import hexlify, unhexlify

import numpy as np
import tables as tb
from prettytable import PrettyTable

import w3g

from w3ml.tools import ensure_slice, isnumeric, noop, shortsha1, ms_to_time, \
    u2i, i2u, stramp

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
            utf8writer.flush()
            sys.stdout.flush()
        return uprint
    print = umake(print)

    from urllib import urlopen
else:
    from urllib.request import urlopen

METADATA_DESC = np.dtype([(b'sha1', b'S20'), (b'map', b'S100'),
    (b'src', b'S250'), (b'winner', np.uint8), 
    (b'player1', np.uint8, 50), (b'race1', b'S16'), (b'pid1', np.int8), 
    (b'color1', b'S10'), (b'actions1', np.int64), (b'apm1', np.float64), 
    (b'player2', np.uint8, 50), (b'race2', b'S16'), (b'pid2', np.int8),
    (b'color2', b'S10'), (b'actions2', np.int64), (b'apm2', np.float64),
    (b'build', b'i2'), (b'speed', b'S6'), (b'duration', b'i4')])

DEFAULT_COLS = ('idx', 'sha1', 'map', 'winner', 'player1', 'race1', 'pid1', 'apm1', 
                'player2', 'race2', 'pid2', 'apm2', 'duration')

NSTEPS = (120*60) + 1

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
        self.actions = db.root.actions
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
        if 'actions' not in r:
            db.create_earray(r, 'actions', atom=tb.Int64Atom(), shape=(0, 2, NSTEPS), 
                             filters=filters)

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
        """Adds a replay to the database, path may be a file or url."""
        if path.startswith('http://'):
            # must be a url
            response = urlopen(path)
            b = response.read()
        else:
            # must be a normal file
            with open(path, 'rb') as f:
                b = f.read()
        self.add_raw_replay(b, path)

    def add_raw_replay(self, b, src='<unknown>'):
        """Adds a replay from path to the database."""
        h = sha1(b).digest()
        if h in self.replay_idx:
            msg = '{0} (SHA1 {1}) already in database at index {2}, skipping.'
            print(msg.format(src, hexlify(h), self.replay_idx[h]))
            return
        w3f = w3g.File(BytesIO(b))
        actions = w3f.timegrid_actions()
        mins = w3f.replay_length / (1000.0 * 60)
        if len(actions) != 2:
            warn('Replay does not appear to be 1v1 game...skipping', RuntimeWarning)
            return 
        pid1, pid2 = sorted(actions.keys())
        self.replays.append(b)
        self.metadata.append([(h, w3f.map_name, src, w3f.winner(),
            u2i(w3f.player_name(pid1), 50), w3f.player_race(pid1), pid1, 
            w3f.slot_record(pid1).color, actions[pid1][-1], actions[pid1][-1] / mins, 
            u2i(w3f.player_name(pid2), 50), w3f.player_race(pid2), pid2,
            w3f.slot_record(pid2).color, actions[pid2][-1], actions[pid2][-1] / mins,
            w3f.build_num, w3f.game_speed, w3f.replay_length)])
        self.actions.append([[actions[pid1], actions[pid2]]])
        self.replay_idx[h] = len(self)

    def dump(self, i):
        """Dumps a replay to the screen."""
        i = self.idx(i)
        b = bytes(self.replays[i])
        sys.stdout.write(b)

    def pprint(self, s=None, cols=DEFAULT_COLS):
        """Pretty printer for metadata table."""
        s = ensure_slice(s)
        transformers = [shortsha1, noop, noop, noop, 
                        i2u, noop, noop, noop, noop, stramp,
                        i2u, noop, noop, noop, noop, stramp,
                        noop, noop, ms_to_time]
        colnames = ['idx'] + list(map(lambda x: x.replace('_', ' '), METADATA_DESC.names))
        pt = PrettyTable(colnames)
        data = self.metadata[s]
        for i, row in enumerate(data):
            r = [i] + [f(x) for f, x in zip(transformers, row)]
            pt.add_row(r)
        ptstr = pt.get_string(fields=cols)
        print(ptstr)

    def events(self, i):
        """Returns the events from a replay."""
        i = self.idx(i)
        b = bytes(self.replays[i])
        with w3g.File(BytesIO(b)) as f:
            events = f.events
        return events

    def print_events(self, i):
        """Prints the events from a replay to the screen."""
        for event in self.events(i):
            print(event)

def act(db, ns):
    """Performs command line actions."""
    if ns.add is not None:
        db.add_replay(ns.add)
    if ns.dump is not None:
        db.dump(ns.dump)
    if ns.events is not None:
        db.print_events(ns.events)
    if ns.list != '<not-given>':
        db.pprint(ns.list, cols=ns.cols)

def main():
    import argparse
    parser = argparse.ArgumentParser('w3ml-db')
    parser.add_argument('file', help='Path to the database')
    parser.add_argument('-a', '--add', dest='add', default=None, 
                        help='adds a replay to the database from a file or url')
    parser.add_argument('-l', '--list', dest='list', nargs='?', const=None,
                        default='<not-given>', 
                        help='lists metadata in the database')
    parser.add_argument('--cols', dest='cols', nargs='+', 
                        default=DEFAULT_COLS, 
                        help='lists only the given columns. available columns are: '
                             'idx, ' + ', '.join(METADATA_DESC.names))
    parser.add_argument('-e', '--events', dest='events', default=None, 
                        help='prints the events in a replay to the screen')
    parser.add_argument('--dump', dest='dump', default=None, 
                        help='dumps a replay to the screen')
    ns = parser.parse_args()

    with Database(ns.file) as db:
        act(db, ns)

    if sys.version_info[0] < 2:
        utf8writer.flush()
        utf8writer.close()

if __name__ == "__main__":
    main()
