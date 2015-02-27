import json
import os
import pprint
import re

import remote
import local

class Connection:

    __version__ = '0.1'

    def __init__(self, lims_url=None, lims_token=None, apiversion='v1', verbose=False, override_owner=None, local_only=False, remote_is_read_only=False, testdata_update_mode=False, verify_cert=False):

        # The Connection class is a tool for connecting to the HTTP API of the UHTS LIMS
        # for making queries or updating objects in the database.

        # INPUT ARGUMENTS
        #
        # lims_url and lims_token are required unless local_only==True.
        # They can be provided as arguments to the constructor, as env vars LIMS_URL and LIMS_TOKEN, or via manual entry
        #
        # verbose=True will cause diagnostic information to be logged (to stdout)
        #
        # override_owner='valid.email@address.edu'
        # When override_owner is set to a valid email address, the owner email address in 'runinfo' will be replaced
        # with this value. This is useful for testing purposes, so that real data sets can be run, but notification
        # emails will be set to a designated test email address instead of to Sequencing Center customers.
        #
        # local_only=True will prevent any connection to the LIMS, either read or write.
        # Instead the test database will be used. This is saved as a flat file that can be checked in with source code.
        #
        # remote_is_read_only=True will prevent write operations to LIMS, writing instead to the local database.
        # Be careful with this option. It provides data from the local database when it is available (otherwise data
        # updates recorded in the local database could not be read). So if the local database contains an object with the
        # same ID as the remote LIMS, the local object will mask the object in the remote LIMS.
        #
        # testdata_update_mode=True will write the results of every query to the local database. This is useful for
        # saving a copy of data from remote LIMS to the code base to be used later for testing without connecting to the
        # remote LIMS.
        #
        # verify_cert=True will cause an exception to be raised if the LIMS ssl certificate is not from a trusted source.


        # turn on logs to stdout
        self.verbose = verbose

        # If LIMS info not provided to constructor, get it from environment variables
        if not lims_url:
            lims_url = os.getenv('UHTS_LIMS_URL')
        if not lims_token:
            lims_token = os.getenv('UHTS_LIMS_TOKEN')

        if not local_only:
            # LIMS info is required. Give option to enter it manually.
            if lims_url is None:
                print "'lims_url' argument was not provided when creating Connection(), and LIMS_URL environment variable was not found."
                lims_url = raw_input("You can manually enter the LIMS URL now: ")
                if lims_url == None:
                    raise Exception('lims_url is requred unless running in local_only mode')
            if lims_token is None:
                print "'lims_token' argument was not provided when creating Connection(), and LIMS_TOKEN environment variable was not found."
                lims_token = raw_input("You can manually enter the LIMS token now: ")
                if lims_token == None:
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
        self.remote = remote.RemoteDataManager(write_lims=write_lims, read_lims=read_lims, lims_url=lims_url, lims_token=lims_token, apiversion=apiversion, verify=verify_cert)

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

        if self.saveresults: #true only when testdata_update_mode is true
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
            self.remote.deletelaneresults(run, lane)
            self.log("Creating pipeline run object for run=%s, lane=%s, paramdict=%s" % (run, lane, paramdict))
            pipelinerun = self.remote.createpipelinerun(run, lane, paramdict)
        else:
            self.log("Resetting any old results before creating pipeline run")
            self.local.deletelaneresults(run, lane)
            # run_id may be for a run in either remote or local, so we look it up here
            # instead of in the local data manager
            run_id = self.getrunid(run)
            self.log("Creating pipeline run object for run=%s, lane=%s, paramdict=%s" % (run, lane, paramdict))
            pipelinerun = self.local.createpipelinerun(run_id, lane, paramdict)

        if not pipelinerun:
            raise Exception('Failed to create pipelinerun for run=%s lane=%s paramdict=%s' % (run, lane, paramdict))

        self.log(pipelinerun, pretty=True)
        return pipelinerun

    def deletelaneresults(self, run, lane):
        if self.remote.write_lims:
            self.log("Reseting old results")
            self.remote.deletelaneresults(run, lane)
        else:
            self.log("Resetting old results")
            self.local.deletelaneresults(run, lane)

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

    def showsolexarun(self, idd):
        
        self.log("Getting solexarun id %s" % idd)

        solexarun = self.local.showsolexarun(idd)
        if solexarun is None:
            solexarun = self.remote.showsolexarun(idd)

        if not solexarun:
            raise Exception('solexarun with id %s could not be found.' % idd)
        
        if self.saveresults:
            self.local.addsolexarun(idd=idd, solexarun=solexarun)
        self.log("Added solexarun id %s to testdata." % run_name)

    def showpipelinerun(self, idd):

        self.log("Showing pipeline run with id=%s" % idd)

        pipelinerun = self.local.showpipelinerun(idd)
        if pipelinerun is None:
            pipelinerun = self.remote.showpipelinerun(idd)

        if not pipelinerun:
            raise Exception('pipelinerun with id %s could not be found.' % idd)

        if self.saveresults:
            self.local.addpipelinerun(idd=idd, pipelinerun=pipelinerun)
            self.local.writepipelinerunstodisk()
            self.log("Added pipelinerun id %s to testdata." % idd)

        self.log(pipelinerun, pretty=True)
        return pipelinerun

    def showlaneresult(self, idd):

        self.log("Showing laneresult with id=%s" % idd)

        laneresult = self.local.showlaneresult(idd)
        if not laneresult:
            laneresult = self.remote.showlaneresult(idd)

        if not laneresult:
            raise Exception('laneresult with id %s could not be found.' % idd)

        if self.saveresults:
            self.local.addlaneresult(idd=idd, laneresult=laneresult)
            self.local.writelaneresultstodisk()
            self.log("Added laneresult id %s to testdata." % idd)

        self.log(laneresult, pretty=True)
        return laneresult

    def showmapperresult(self, idd):

        self.log("Showing mapper result with id=%s" % idd)

        mapperresult = self.local.showmapperresult(idd)
        if not mapperresult:
            mapperresult = self.remote.showmapperresult(idd)

        if not mapperresult:
            raise Exception('mapperresult with id %s could not be found.' % idd)

        if self.saveresults:
            self.local.addmapperresult(idd=idd, mapperresult=mapperresult)
            self.local.writemapperresultstodisk()
            self.log("Added mapperresult id %s to testdata." % idd)

        self.log(mapperresult, pretty=True)
        return mapperresult

    def indexsolexaruns(self, run):

        self.log("Indexing solexa run(s) where run=%s" % run)

        solexaruns = self.remote.indexsolexaruns(run)
        # data from remote is overridden if found in local
        solexaruns.update(self.local.indexsolexaruns(run))

        # Don't raise Exception if the list is empty.

        if self.saveresults:
            self.local.addsolexaruns(solexaruns=solexaruns)
            self.local.writesolexarunstodisk()
            self.log("Added %s solexa runs to testdata" % len(solexaruns))

        self.log(solexaruns, pretty=True)
        return solexaruns

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

    def updatesolexarun(self, run_id, paramdict):
        
        self.log("Updating Solexa Run id=%s with paramdict=%s" % (run_id, paramdict))
    
        run = self.remote.updaterun(run_id, paramdict)
        if not run:
            run = self.local.updatepipelinerun(run_id, paramdict)

        if not run:
            raise Exception("Failed to update Solexa Run id=%s paramdict=%s" % (run_id, paramdict))
        
        self.log(run, pretty=True)
        return run

    def updatepipelinerun(self, idd, paramdict):

        self.log("Updating pipeline run id=%s with paramdict=%s" % (idd, paramdict))

        pipelinerun = self.remote.updatepipelinerun(idd, paramdict)
        if not pipelinerun:
            pipelinerun = self.local.updatepipelinerun(idd, paramdict)

        if not pipelinerun:
            raise Exception("Failed to update pipelinerun id=%s paramdict=%s" % (idd, paramdict))

        self.log(pipelinerun, pretty=True)
        return pipelinerun
    
    def updatelaneresult(self, idd, paramdict):

        self.log("Updating lane result id=%s with paramdict=%s" % (idd, paramdict))

        laneresult = self.remote.updatelaneresult(idd, paramdict)
        if not laneresult:
            laneresult = self.local.updatelaneresult(idd, paramdict)

        if not laneresult:
            raise Exception("Failed to update laneresult id=%s paramdict=%s" % (idd, paramdict))

        self.log(laneresult, pretty=True)
        return laneresult

    def updatemapperresult(self, idd, paramdict):

        self.log("Updating mapper result id=%s with paramdict=%s" % (idd, paramdict))

        mapperresult = self.remote.updatemapperresult(idd, paramdict)
        if not mapperresult:
            mapperresult = self.local.updatemapperresult(idd, paramdict)

        if not mapperresult:
            raise Exception("Failed to update mapperresult id=%s paramdict=%s" % (idd, paramdict))

        self.log(mapperresult, pretty=True)
        return mapperresult

    def getrunid(self, run):

        idd = self.local.getrunid(run)
        if not idd:
            idd = self.remote.getrunid(run)
        if not idd:
            raise Exception("Failed to find id for run %s" % run)
        return idd

    def getlaneid(self, run, lane):
        idd = self.local.getlaneid(run=run, lane=lane)
        if not idd:
            idd = self.remote.getlaneid(run=run, lane=lane)

        if not idd:
            raise Exception("Failed to find lane_id for run=%s lane=%s" % (run, lane))

        return idd

    def getallrunobjects(self, run):
        runinfo = self.getruninfo(run)
        self.indexsolexaruns(run)
        self.getsamplesheet(run, filename=None)
        self.indexsolexaruns(run)
        for lane in runinfo['run_info']['lanes'].keys():
            self.getsamplesheet(run, filename=None, lane=lane)
        self.indexpipelineruns(run)
        self.indexlaneresults(run)
        self.indexmapperresults(run)

    def testconnection(self):
        # Raises exception if no 200 response
        self.remote.testconnection()
        return True

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
