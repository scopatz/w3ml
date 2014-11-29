"""Tools for handling the w3ml database.
"""
from __future__ import print_function, unicode_literals
import os
import sys
from io import BytesIO

import numpy as np
import tables as tb

import w3g

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

def main():
    import argparse
    parser = argparse.ArgumentParser('w3ml-db')
    parser.add_argument('file', help='Path to the database')
    ns = parser.parse_args()

    with Database(ns.file) as db:
        pass

if __name__ == "__main__":
    main()