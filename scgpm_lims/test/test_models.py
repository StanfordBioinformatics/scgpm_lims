#!/usr/bin/env python

import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import unittest
from scgpm_lims.components.connection import Connection
from scgpm_lims.components.models import RunInfo, SolexaRun


class TestRunInfo(unittest.TestCase):

    def setUp(self):
        conn = Connection(local_only=True)
        run = '141117_MONK_0387_AC4JCDACXX'
        self.runinfo = RunInfo(conn=conn, run=run)

    def testGetSolexaRunStatus(self):
        self.assertEqual(self.runinfo.get_solexa_run_status(), SolexaRun.STATUS_SEQUENCING_DONE)

if __name__=='__main__':
    unittest.main()
