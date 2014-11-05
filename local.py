from datetime import datetime
import json
import os
import random
from warnings import warn


class LocalDataManager:

    _runinfofile = 'runinfo.json'
    _samplesheetsfile = 'samplesheets.json'
    _pipelinerunsfile = 'pipelineruns.json'
    _laneresultsfile = 'laneresults.json'
    _mapperresultsfile = 'mapperresults.json'

    _testdatadir = 'testdata'

    def __init__(self, loadtestdata=False, disable=False):

        self._runinfo = {}
        self._samplesheets = {}
        self._pipelineruns = {}
        self._laneresults = {}
        self._mapperresults = {}

        if loadtestdata:
            self._loadall()
            self._loadedtestdata = True
        else:
            self._loadedtestdata = False

        self.disable = disable

    def getruninfo(self, run=None):
        if self.disable:
            return None

        return self._runinfo.get(run)

    def getsamplesheet(self, run=None, lane=None):
        if lane:
            lane = str(lane)
        if self.disable:
            return None

        run = self._samplesheets.get(run)
        if not lane:
            return run
        else:
            return run.get(lane)

    def showpipelinerun(self, idd=None):
        if self.disable:
            return None

        return self._pipelineruns.get(idd)

    def showlaneresult(self, idd=None):
        if self.disable:
            return None

        return self._laneresults.get(idd)

    def showmapperresult(self, idd=None):
        if self.disable:
            return None

        return self._mapperresults.get(idd)

    def indexpipelineruns(self, run=None):
        if self.disable: 
            return {}

        run_idd =self.getrunid(run)
        found_pipelineruns = {}
        for idd, pipelinerun in self._pipelineruns.iteritems():
            if str(pipelinerun.get('solexa_run_id')) == str(run_id):
                found_pipelineruns[str(pipelinerun.get('id'))] = pipelinerun
        return found_pipelineruns

    def indexlaneresults(self, run=None, lane=None, barcode=None, readnumber=None):
        if self.disable:
            return {}

        laneids = self._getlaneids(run)
        found_laneresults = {}
        for idd, laneresult in self._laneresults.iteritems():
            if str(laneresult.get('solexa_run_id')) in laneids:
                found_laneresults[str(laneresult.get('id'))] = laneresult
                #TODO add other filters for lane, barcode, readnumber
        return found_laneresults

    def indexmapperresults(self, run=None):
        if self.disable: 
            return None

        laneresultids = self._getlaneresultids(run)
        found_mapperresults = {}
        for idd, mapperresult in self._mapperresults.iteritems():
            if str(mapperresult.get('dataset_id')) in laneresultids:
                found_mapperresults[idd] = mapperresult
        return found_mapperresults

    def createpipelinerun(self, run_id, lane, paramdict=None):
        if self.disable:
            return None

        idd =self._getrandomid()
        pipelinerun = {
            'id': idd,
            'solexa_run_id': run_id,
            'started': True,
            'active': True,
            'finished': None,
            'start_time':str(datetime.now()),
            'created_at':str(datetime.now()),
            'pass_read_count': None,
            }

        if paramdict:
            pipelinerun.update(paramdict)
        
        self.addpipelinerun(id, pipelinerun)
        return pipelinerun

    def createlaneresult(self, lane_id, paramdict):
        if self.disable:
            return None

        idd =self._getrandomid()
        laneresult = {'id': idd,
                      'solexa_lane_id': lane_id,
                      'solexa_pipeline_run_id': None,
                      'created_at': str(datetime.now()),
                      'active': True,
                      'codepoint': None,
                      }
        laneresult.update(paramdict)

        self.addlaneresult(idd, laneresult)
        
        return laneresult

    def createmapperresult(self, paramdict):
        if self.disable:
            return None
        
        idd =self._getrandomid()
        mapperresult = { 'id': idd,
                         'created_at': str(datetime.now()),
                         'active': True
                         }
        mapperresult.update(paramdict)
        self.addmapperresult(idd, mapperresult)

        return mapperresult

    def updatepipelinerun(self, idd, paramdict):
        if self.disable:
            return None

        idd =str(idd)
        try:
            self._pipelineruns.get(idd).update(paramdict)
        except:
            return None
        return self.showpipelinerun(idd)

    def updatelaneresult(self, idd, paramdict):
        if self.disable:
            return None

        idd =str(idd)
        try:
            self._laneresults.get(idd).update(paramdict)
        except:
            return None
        return self.showlaneresult(idd)

    def updatemapperresult(self, idd, paramdict):
        if self.disable:
            return None

        idd =str(idd)
        try:
            self._mapperresults.get(idd).update(paramdict)
        except:
            return None
        return self.showmapperresult(idd)

    def _getrandomid(self):
        # High enough min to exclude valid ids in LIMS
        # Large enough range to make repetition vanishingly improbable
        return random.randint(1e12,2e12)

    def resetlane(self, run, lane):
        # TODO
        pass

    def addruninfo(self, run, runinfo):
        self._runinfo[run] = runinfo

    def addsamplesheet(self, run, samplesheet, lane=None):
        # lane = None means samplesheet for all lanes.
        run = self._samplesheets.setdefault(run, {}) 
        run[lane] = samplesheet

    def addpipelinerun(self, idd, pipelinerun):
        self._pipelineruns[str(idd)] = pipelinerun

    def addlaneresult(self, idd, laneresult):
        self._laneresults[str(idd)] = laneresult

    def addmapperresult(self, idd, mapperresult):
        self._mapperresults[str(idd)] = mapperresult

    def addpipelineruns(self, pipelineruns):
        for idd, pipelinerun in pipelineruns.iteritems():
            self.addpipelinerun(idd, pipelinerun)

    def addlaneresults(self, laneresults):
        for idd, laneresult in laneresults.iteritems():
            self.addlaneresult(idd, laneresult)

    def addmapperresults(self, mapperresults):
        for idd, mapperresult in mapperresults.iteritems():
            self.addmapperresult(idd, mapperresult)

    def getrunid(self, run):
        try:
            return str(self.getruninfo(run).get('id'))
        except:
            return None

    def getlaneid(self, run, lane):
        runinfo = self.getruninfo(run)
        try:
            idd =runinfo.get('run_info').get('lanes').get(str(lane)).get('id')
        except:
            return None
        
        return idd

    def _getlaneresultids(self, run):
        laneresultids = []
        for laneresult in self.indexlaneresults(run).values():
            laneresultids.append(str(laneresult.get('id')))
        return laneresultids

    def _getlaneids(self, run_name):
        runinfo = self.getruninfo(run_name)
        try:
            lanes = runinfo.get('run_info').get('lanes')
        except:
            return None
        laneids = []
        for lane in lanes.values():
            laneids.append(str(lane.get('id')))
        return laneids

    def writeruninfotodisk(self):
        self._writetodisk(self._runinfo, self._runinfofile)

    def writesamplesheetstodisk(self):
        self._writetodisk(self._samplesheets, self._samplesheetsfile)

    def writepipelinerunstodisk(self):
        self._writetodisk(self._pipelineruns, self._pipelinerunsfile)

    def writelaneresultstodisk(self):
        self._writetodisk(self._laneresults, self._laneresultsfile)

    def writemapperresultstodisk(self):
        self._writetodisk(self._mapperresults, self._mapperresultsfile)

    def _writetodisk(self, info, datafile):
        if not self._loadedtestdata:
            raise Exception("Since you didn't load local testdata, writing to disk will overwrite the current testdata. Stopping without save.")
        fullfilename = self._fullpath(datafile)
        if os.path.exists(fullfilename):
            os.remove(fullfilename)
        with open(fullfilename,'w') as fp:
            fp.write(json.dumps(info, sort_keys=True, indent=4, separators=(',', ': ')))

    def _loadall(self):
        self._loadruninfo()
        self._loadsamplesheets()
        self._loadpipelineruns()
        self._loadlaneresults()
        self._loadmapperresults()
        
    def _loadruninfo(self):
        self._runinfo = self._load(self._runinfofile)

    def _loadsamplesheets(self):
        self._samplesheets = self._load(self._samplesheetsfile)

    def _loadpipelineruns(self):
        self._pipelineruns = self._load(self._pipelinerunsfile)

    def _loadlaneresults(self):
        self._laneresults = self._load(self._laneresultsfile)

    def _loadmapperresults(self):
        self._mapperresults = self._load(self._mapperresultsfile)

    def _fullpath(self, infile):
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), self._testdatadir, infile)

    def _load(self, datafile):
        try:
            with open(self._fullpath(datafile)) as fp:
                data = json.load(fp)
        except (ValueError, IOError):
            warn("Could not load testdata from %s" % datafile)
            data = {}
        return data
