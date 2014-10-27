import json
import pprint
import re

import remote
import local

class Connection:

    __version__ = '0.1'

    def __init__(self, lims_url=None, lims_token=None, apiversion='v1', verbose=False, override_owner=None, local_only=False, remote_is_read_only=False, testdata_update_mode=False):

        # turn on logs to stdout
        self.verbose = verbose

        if not local_only:
            # lims info is required
            if lims_url is None:
                raise Exception('lims_url is requred unless running in local_only mode')
            if lims_token is None:
                raise Exception('lims_token is requred unless running in local_only mode')

        if local_only:
            # No connection with LIMS. Since this is used for testing mode,
            # we load the test data.
            write_lims = False
            read_lims = False
            loadtestdata = True
            disable_local = False
            self.saveresults = False
            self.log('Running in local only mode')

        elif testdata_update_mode:
            # Testdata is updated by pulling info from LIMS, so local_only doesn't make sense.
            if local_only:
                raise Exception("You cannot use local_only with testdata_update_mode")

            # Both LIMS and local testdata are needed, so that data for new test cases can be
            # queried and saved locally.
            #
            # We don't want to write to LIMS while we're editing our tests, so we
            # override the default remote_is_read_only=False
            write_lims = False
            read_lims = True
            disable_local = False
            loadtestdata = True
            self.saveresults = True
            self.log('Running in testdata update mode')

        elif remote_is_read_only:
            # If remote_is_read_only is set, write operations go to a local in-memory cache.
            #
            # For new objects created in local cache, an object id is generated and returned.
            #
            # Read operations need not specify whether an id is for the local cache
            # or for the LIMS. For read operations, we first check the local cache
            # and query the lims only if the key is not found there
            write_lims = False
            read_lims = True
            disable_local = False
            loadtestdata = False
            self.saveresults = False
            self.log('Running in remote is read only mode. No changes will be made to the remote LIMS')

        else:
            # No flags set, normal mode where we work with the LIMS and
            # disable the local cache.
            write_lims = True
            read_lims = True
            disable_local=True
            loadtestdata = None # no effect, local disabled
            self.saveresults = False
            self.log('Running in normal mode, reading from and writing to remote LIMS')

        self.local = local.LocalDataManager(loadtestdata=loadtestdata, disable=disable_local)
        self.remote = remote.RemoteDataManager(write_lims=write_lims, read_lims=read_lims, lims_url=lims_url, lims_token=lims_token, apiversion=apiversion)

        # If override_owner is set to a valid email address,
        # emails in runinfo will be replaced by the override.
        if override_owner is None:
            self.override_owner = None
        else:
            self.override_owner = self._clean_override_owner(override_owner)

        # Initialize pretty printer for writing data structures in the log
        self.pprint = pprint.PrettyPrinter(indent=2, width=1).pprint

    def getsamplesheet(self, run, lane=None, filename='samplesheet.csv'):

        if lane is not None:
            self.log("Writing samplesheet for run %s lane %s to file %s" % (run, lane, filename))
        else:
            self.log("Writing samplesheet for run %s, all lanes, to file %s" % (run, filename))

        samplesheet = self.local.getsamplesheet(run=run, lane=lane)
        if samplesheet is None:
            samplesheet = self.remote.getsamplesheet(run=run, lane=lane)
        if not samplesheet:
            raise Exception('samplesheet for run %s could not be found.' % run)

        if self.saveresults:
            self.local.addsamplesheet(run=run, samplesheet=samplesheet, lane=lane)
            self.local.writesamplesheetstodisk()
            if lane is not None:
                self.log("Added samplesheet for run %s lane %s to testdata." % (run, lane))
            else:
                self.log("Added samplesheet for run %s all lanes to testdata." % run)

        if filename:
            with open(filename, 'w') as f:
                f.write(samplesheet)

        self.log(samplesheet)
        return samplesheet

    def getruninfo(self, run=None):

        self.log("Getting run info for run %s" % run)

        dirty_runinfo = self.local.getruninfo(run=run)
        if not dirty_runinfo:
            dirty_runinfo = self.remote.getruninfo(run=run)

        runinfo = self._processruninfo(dirty_runinfo)

        if not runinfo:
            raise Exception('runinfo for run %s could not be found.' % run)

        if self.saveresults:
            self.local.addruninfo(run=run, runinfo=runinfo)
            self.local.writeruninfotodisk()
            self.log("Added runinfo for %s to testdata." % run)

        self.log(runinfo, pretty=True)
        return runinfo

    def createpipelinerun(self, run, lane, paramdict = None):

        if self.remote.write_lims:
            self.log("Resetting any old results before creating pipeline run")
            self.remote.resetlane(run, lane)
            self.log("Creating pipeline run object for run=%s, lane=%s, paramdict=%s" % (run, lane, paramdict))
            pipelinerun = self.remote.createpipelinerun(run, lane, paramdict)
        else:
            self.log("Resetting any old results before creating pipeline run")
            self.local.resetlane(run, lane)
            # run_id may be for a run in either remote or local, so we look it up here
            # instead of in the local data manager
            run_id = self.getrunid(run)
            self.log("Creating pipeline run object for run=%s, lane=%s, paramdict=%s" % (run, lane, paramdict))
            pipelinerun = self.local.createpipelinerun(run_id, lane, paramdict)

        if not pipelinerun:
            raise Exception('Failed to create pipelinerun for run=%s lane=%s paramdict=%s' % (run, lane, paramdict))

        self.log(pipelinerun, pretty=True)
        return pipelinerun

    def createlaneresult(self, paramdict, run, lane):

        self.log("Creating lane result for run=%s, lane=%s, paramsdict=%s" % (run, lane, paramdict))
        if self.remote.write_lims:
            laneresult = self.remote.createlaneresult(paramdict, run=run, lane=lane)
        else:
            # lane_id may be for a run in either remote or local, so we look it up here
            # instead of in the local data manager
            lane_id = self.getlaneid(run=run, lane=lane)
            laneresult = self.local.createlaneresult(lane_id, paramdict)

        if not laneresult:
            raise Exception('Failed to create laneresult for run=%s lane=%s paramdict=%s' % (run, lane, paramdict))

        self.log(laneresult, pretty=True)
        return laneresult

    def createmapperresult(self, paramdict):

        self.log("Creating mapper result with paramsdict=%s" % paramdict)

        if self.remote.write_lims:
            mapperresult = self.remote.createmapperresult(paramdict)
        else:
            mapperresult = self.local.createmapperresult(paramdict)

        if not mapperresult:
            raise Exception('Failed to create mapperresult for paramdict=%s' % paramdict)
        
        self.log(mapperresult, pretty=True)
        return mapperresult

    def showpipelinerun(self, id):

        self.log("Showing pipeline run with id=%s" % id)

        pipelinerun = self.local.showpipelinerun(id)
        if pipelinerun is None:
            pipelinerun = self.remote.showpipelinerun(id)

        if not pipelinerun:
            raise Exception('pipelinerun with id %s could not be found.' % id)

        if self.saveresults:
            self.local.addpipelinerun(id=id, pipelinerun=pipelinerun)
            self.local.writepipelinerunstodisk()
            self.log("Added pipelinerun id %s to testdata." % id)

        self.log(pipelinerun, pretty=True)
        return pipelinerun

    def showlaneresult(self, id):

        self.log("Showing laneresult with id=%s" % id)

        laneresult = self.local.showlaneresult(id)
        if not laneresult:
            laneresult = self.remote.showlaneresult(id)

        if not laneresult:
            raise Exception('laneresult with id %s could not be found.' % id)

        if self.saveresults:
            self.local.addlaneresult(id=id, laneresult=laneresult)
            self.local.writelaneresultstodisk()
            self.log("Added laneresult id %s to testdata." % id)

        self.log(laneresult, pretty=True)
        return laneresult

    def showmapperresult(self, id):

        self.log("Showing mapper result with id=%s" % id)

        mapperresult = self.local.showmapperresult(id)
        if not mapperresult:
            mapperresult = self.remote.showmapperresult(id)

        if not mapperresult:
            raise Exception('mapperresult with id %s could not be found.' % id)

        if self.saveresults:
            self.local.addmapperresult(id=id, mapperresult=mapperresult)
            self.local.writemapperresultstodisk()
            self.log("Added mapperresult id %s to testdata." % id)

        self.log(mapperresult, pretty=True)
        return mapperresult

    def indexpipelineruns(self, run):

        self.log("Indexing pipeline runs where run=%s" % run)

        pipelineruns = self.remote.indexpipelineruns(run)
        # data from remote is overridden if found in local
        pipelineruns.update(self.local.indexpipelineruns(run))

        # Don't raise Exception if the list is empty.

        if self.saveresults:
            self.local.addpipelineruns(pipelineruns=pipelineruns)
            self.local.writepipelinerunstodisk()
            self.log("Added %s pipeline runs to testdata" % len(pipelineruns))

        self.log(pipelineruns, pretty=True)
        return pipelineruns

    def indexlaneresults(self, run, lane=None, barcode=None, readnumber=None):

        self.log("Indexing lane results where run=%s, lane=%s, barcode=%s" % 
                 (run, lane, barcode))

        laneresults = self.remote.indexlaneresults(run, lane=lane, barcode=barcode, readnumber=readnumber)
        # data from remote is overridden if found in local
        laneresults.update(self.local.indexlaneresults(run, lane=lane, barcode=barcode, readnumber=readnumber))

        # Don't raise Exception if the list is empty.

        if self.saveresults:
            self.local.addlaneresults(laneresults = laneresults)
            self.local.writelaneresultstodisk()
            self.log("Added %s lane results to testdata" % len(laneresults))

        self.log(laneresults, pretty=True)
        return laneresults

    def indexmapperresults(self, run):

        self.log("Indexing mapper results where run=%s" % run)

        mapperresults = self.remote.indexmapperresults(run)
        mapperresults.update(self.local.indexmapperresults(run))

        # Don't raise Exception if the list is empty.

        if self.saveresults:
            self.local.addmapperresults(mapperresults=mapperresults)
            self.local.writemapperresultstodisk()
            self.log("Added %s mapper results to testdata" % len(mapperresults))

        self.log(mapperresults, pretty=True)
        return mapperresults

    def updatepipelinerun(self, id, paramdict):

        self.log("Updating pipeline run id=%s with paramdict=%s" % (id, paramdict))

        pipelinerun = self.remote.updatepipelinerun(id, paramdict)
        if not pipelinerun:
            pipelinerun = self.local.updatepipelinerun(id, paramdict)

        if not pipelinerun:
            raise Exception("Failed to update pipelinerun id=%s paramdict=%s" % (id, paramdict))

        self.log(pipelinerun, pretty=True)
        return pipelinerun
    
    def updatelaneresult(self, id, paramdict):

        self.log("Updating lane result id=%s with paramdict=%s" % (id, paramdict))

        laneresult = self.remote.updatelaneresult(id, paramdict)
        if not laneresult:
            laneresult = self.local.updatelaneresult(id, paramdict)

        if not laneresult:
            raise Exception("Failed to update laneresult id=%s paramdict=%s" % (id, paramdict))

        self.log(laneresult, pretty=True)
        return laneresult

    def updatemapperresult(self, id, paramdict):

        self.log("Updating mapper result id=%s with paramdict=%s" % (id, paramdict))

        mapperresult = self.remote.updatemapperresult(id, paramdict)
        if not mapperresult:
            mapperresult = self.local.updatemapperresult(id, paramdict)

        if not mapperresult:
            raise Exception("Failed to update mapperresult id=%s paramdict=%s" % (id, paramdict))

        self.log(mapperresult, pretty=True)
        return mapperresult

    def getrunid(self, run):

        id = self.local.getrunid(run)
        if not id:
            id = self.remote.getrunid(run)
        if not id:
            raise Exception("Failed to find id for run %s" % run)
        return id

    def getlaneid(self, run, lane):
        id = self.local.getlaneid(run=run, lane=lane)
        if not id:
            id = self.remote.getlaneid(run=run, lane=lane)

        if not id:
            raise Exception("Failed to find lane_id for run=%s lane=%s" % (run, lane))

        return id

    def getallrunobjects(self, run):
        runinfo = self.getruninfo(run)
        self.getsamplesheet(run, filename=None)
        for lane in runinfo['run_info']['lanes'].keys():
            self.getsamplesheet(run, filename=None, lane=lane)
        self.indexpipelineruns(run)
        self.indexlaneresults(run)
        self.indexmapperresults(run)

    def _processruninfo(self, runinfo):

        # Replace emails if override_owner is set
        if self.override_owner:
            lanes = runinfo['run_info']['lanes']
            for lane in lanes.values():
                for notify in lane.get('notify'):
                    notify['email'] = self.override_owner
                    lane['submitter_email'] = self.override_owner
        return runinfo

    def _clean_override_owner(self, email):

        if re.match(r'^\S+@\S+\.\S+$', email):
            return email
        else:
            raise Exception('override_owner setting "%s" is not a valid email address.' % email)

    def log(self, message, pretty=False):

        if self.verbose:
            if pretty:
                self.pprint(message)
            else:
                print message
