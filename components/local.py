from datetime import datetime
import json
import os
import random
from warnings import warn


class LocalDataManager:

    _runinfofile = 'runinfo.json'
    _samplesheetsfile = 'samplesheets.json'
    _solexarunsfile = 'solexaruns.json'
    _solexaflowcellsfile = 'solexaflowcells.json'
    _pipelinerunsfile = 'pipelineruns.json'
    _laneresultsfile = 'laneresults.json'
    _mapperresultsfile = 'mapperresults.json'

    _testdatadir = '../testdata'

    def __init__(self):

        self._runinfo = {}
        self._samplesheets = {}
        self._solexaruns = {}
        self._solexaflowcells = {}
        self._pipelineruns = {}
        self._laneresults = {}
        self._mapperresults = {}

        self._loadall()

    def getruninfo(self, run=None):
        return self._runinfo.get(run)

    def getsamplesheet(self, run=None, lane=None):
        run = self._samplesheets.get(run)
        if lane:
            lane = str(lane)
            return run.get(lane)
        else:
            return run

    def showsolexarun(self, id=None):
        return self._solexaruns.get(id)

    def showsolexaflowcell(self, id=None):
        return self._solexaflowcells.get(id)

    def showpipelinerun(self, id=None):
        """
        Args : id - Pipeline Run ID
        """
        return self._pipelineruns.get(id)

    def showlaneresult(self, id=None):
        """
        Args : id - a Solexa Lane Result ID. For example, given the run 141117_MONK_0387_AC4JCDACXX, click on the analysis link with the date of '2014-11-30 20:22:00 -0800'.
                     Then on the resulting page, find the Analysis Results table. In the Details columns, those 'View' links take you to a page that refers to a lane result.
                     The lane result ID number is present at the end of the URL in the browser.
        """ 
        return self._laneresults.get(id)

    def showmapperresult(self, id=None):
        return self._mapperresults.get(id)

    def indexsolexaruns(self, run=None):
        run_id =self.getrunid(run)
        solexa_run = self._solexaruns.get(run_id)
        if solexa_run is None:
            return {}
        else:
            return solexa_run
        
    def indexpipelineruns(self, run=None):
        """
        Finds all pipeline runs for a given run name from the test file, and puts them into a dict keyed by the pipeline run id
        and valued by a dict being with the metadata on the pipeline run.
        """
        run_id =self.getrunid(run)
        found_pipelineruns = {}
        for id, pipelinerun in self._pipelineruns.iteritems():
            if str(pipelinerun.get('solexa_run_id')) == str(run_id):
                found_pipelineruns[str(pipelinerun.get('id'))] = pipelinerun
        return found_pipelineruns

    def indexlaneresults(self, run, lane=None, barcode=None, readnumber=None):
        """
        Function : Finds all lane results for a given run name. Doesn't yet support filtering for a particular
                   lane, barcode, and readnumber. Puts retrieved lane results into a dict keyed by the lane result ID
                   and valued by a dict being the lane results for the particular barcode and readnumber retrieved.
        """
        laneids = self._getlaneids(run)
        found_laneresults = {}
        for id, laneresult in self._laneresults.iteritems():
            if str(laneresult.get('solexa_lane_id')) in laneids:
                found_laneresults[str(laneresult.get('id'))] = laneresult
                #TODO add other filters for lane, barcode, readnumber
        return found_laneresults

    def indexmapperresults(self, run=None):
        laneresultids = self._getlaneresultids(run)
        found_mapperresults = {}
        for id, mapperresult in self._mapperresults.iteritems():
            if str(mapperresult.get('dataset_id')) in laneresultids:
                found_mapperresults[id] = mapperresult
        return found_mapperresults

    def createpipelinerun(self, run_id, lane, paramdict=None):
        id =self._getrandomid()
        pipelinerun = {
            'id': id,
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
        id =self._getrandomid()
        laneresult = {'id': id,
                      'solexa_lane_id': lane_id,
                      'solexa_pipeline_run_id': None,
                      'created_at': str(datetime.now()),
                      'active': True,
                      'codepoint': None,
                      }
        laneresult.update(paramdict)
        self.addlaneresult(id, laneresult)
        return laneresult

    def createmapperresult(self, paramdict):
        id =self._getrandomid()
        mapperresult = { 'id': id,
                         'created_at': str(datetime.now()),
                         'active': True
                         }
        mapperresult.update(paramdict)
        self.addmapperresult(id, mapperresult)
        return mapperresult

    def updatesolexarun(self, id, paramdict):
        id=str(id)
        try:
            self._solexaruns.get(id).update(paramdict)
        except:
            return None
        return self.showsolexarun(id)

    def updatesolexaflowcell(self, id, paramdict):
        id=str(id)
        try:
            self._solexaflowcells.get(id).update(paramdict)
        except:
            return None
        return self.showsolexaflowcell(id)

    def updatepipelinerun(self, id, paramdict):
        id =str(id)
        try:
            self._pipelineruns.get(id).update(paramdict)
        except:
            return None
        return self.showpipelinerun(id)

    def updatelaneresult(self, id, paramdict):
        id =str(id)
        try:
            self._laneresults.get(id).update(paramdict)
        except:
            return None
        return self.showlaneresult(id)

    def updatemapperresult(self, id, paramdict):
        id =str(id)
        try:
            self._mapperresults.get(id).update(paramdict)
        except:
            return None
        return self.showmapperresult(id)

    def _getrandomid(self):
        # High enough min to exclude valid ids in LIMS
        # Large enough range to make repetition vanishingly improbable
        return random.randint(1e12,2e12)

    def deletelaneresults(self, run, lane):
        # TODO
        raise Exception("Todo. This method hasn't been implemented for local connection yet.")

    def addruninfo(self, run, runinfo):
        self._runinfo[run] = runinfo

    def addrun(self, id, run):
        self._runs[str(id)] = run

    def addsamplesheet(self, run, samplesheet, lane=None):
        # lane = None means samplesheet for all lanes.
        run = self._samplesheets.setdefault(run, {}) 
        run[lane] = samplesheet

    def addsolexarun(self, id, solexarun):
        self._solexaruns[str(id)] = solexarun

    def addsolexaflowcell(self, id, solexaflowcell):
        self._solexaflowcells[str(id)] = solexaflowcell

    def addpipelinerun(self, id, pipelinerun):
        self._pipelineruns[str(id)] = pipelinerun

    def addlaneresult(self, id, laneresult):
        self._laneresults[str(id)] = laneresult

    def addmapperresult(self, id, mapperresult):
        self._mapperresults[str(id)] = mapperresult

    def addsolexaruns(self, solexaruns):
        for id, solexarun in solexaruns.iteritems():
            self.addsolexarun(id, solexarun)

    def addsolexaflowcells(self, solexaflowcells):
        for id, solexarun in solexaruns.iteritems():
            self.addsolexaflowcell(id, solexaflowcell)

    def addpipelineruns(self, pipelineruns):
        for id, pipelinerun in pipelineruns.iteritems():
            self.addpipelinerun(id, pipelinerun)

    def addlaneresults(self, laneresults):
        for id, laneresult in laneresults.iteritems():
            self.addlaneresult(id, laneresult)

    def addmapperresults(self, mapperresults):
        for id, mapperresult in mapperresults.iteritems():
            self.addmapperresult(id, mapperresult)

    def getrunid(self, run):
        try:
            return str(self.getruninfo(run).get('id'))
        except:
            return None

    def getlaneid(self, run, lane):
        runinfo = self.getruninfo(run)
        try:
            id =runinfo.get('run_info').get('lanes').get(str(lane)).get('id')
        except:
            return None
        
        return id

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
        for lane in lanes.values(): #each value is a dict from the lane
            laneids.append(str(lane.get('id')))
        return laneids

    def writeruninfotodisk(self):
        self._writetodisk(self._runinfo, self._runinfofile)

    def writesamplesheetstodisk(self):
        self._writetodisk(self._samplesheets, self._samplesheetsfile)

    def writesolexarunstodisk(self):
        self._writetodisk(self._solexaruns, self._solexarunsfile)

    def writesolexaflowcellstodisk(self):
        self._writetodisk(self._solexaflowcells, self._solexaflowcellsfile)

    def writepipelinerunstodisk(self):
        self._writetodisk(self._pipelineruns, self._pipelinerunsfile)

    def writelaneresultstodisk(self):
        self._writetodisk(self._laneresults, self._laneresultsfile)

    def writemapperresultstodisk(self):
        self._writetodisk(self._mapperresults, self._mapperresultsfile)

    def _writetodisk(self, info, datafile):
        fullfilename = self._fullpath(datafile)
        if os.path.exists(fullfilename):
            os.remove(fullfilename)
        with open(fullfilename,'w') as fp:
            fp.write(json.dumps(info, sort_keys=True, indent=4, separators=(',', ': ')))

    def _loadall(self):
        self._loadruninfo()
        self._loadsamplesheets()
        self._loadsolexaruns()
        self._loadsolexaflowcells()
        self._loadpipelineruns()
        self._loadlaneresults()
        self._loadmapperresults()
        
    def _loadruninfo(self):
        self._runinfo = self._load(self._runinfofile)

    def _loadsamplesheets(self):
        self._samplesheets = self._load(self._samplesheetsfile)

    def _loadsolexaruns(self):
        self._solexaruns = self._load(self._solexarunsfile)

    def _loadsolexaflowcells(self):
        self._solexaflowcells = self._load(self._solexaflowcellsfile)

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

    def testconnection(self):
        # No-op. This mirrors the same method in remote to test a valid http connection.
        pass
