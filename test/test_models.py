#!/usr/bin/env python

import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import unittest
from components.connection import Connection
from components.models import RunInfo


class TestRunInfo(unittest.TestCase):

    def setUp(self):
        conn = Connection(local_only=True)
        run = '141117_MONK_0387_AC4JCDACXX'
        self.runinfo = RunInfo(conn=conn, run=run)

    def testGetRunStatus(self):
        self.assertEqual(self.runinfo.get_run_status(), RunInfo.SEQUENCING_RUN_STATUS_DONE)

    


if __name__=='__main__':
    unittest.main()
