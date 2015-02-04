#!/usr/bin/env python

import unittest
from scgpm_lims.connection import Connection
from scgpm_lims.models import RunInfo


class TestRunInfo(unittest.TestCase):

    def setUp(self):
        conn = Connection(local_only=True)
        run = '141117_MONK_0387_AC4JCDACXX'
        self.runinfo = RunInfo(conn=conn, run=run)

    def testsomethign(self):
        self.assertTrue(False)

if __name__=='__main__':
    unittest.main()
