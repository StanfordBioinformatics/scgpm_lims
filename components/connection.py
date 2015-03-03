import json
import os
import pprint
import re

import remote
import local

class Connection:

    __version__ = '0.1'

    def __init__(self, lims_url=None, lims_token=None, apiversion='v1', verbose=False, override_owner=None, local_only=False, testdata_update_mode=False, verify_cert=False):

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

        # testdata_update_mode reads from remote, so using it with local_only doesn't make sense.
        if testdata_update_mode and local_only:
            raise Exception("You cannot use local_only with testdata_update_mode")

        if local_only:
            # No connection with LIMS. Since this is used for testing mode,
            # we load the test data.
            self.server = local.LocalDataManager()
            self.autosaveserver = None
            self.log('Running in local only mode')

        elif testdata_update_mode:
            # Remote LIMS is used, but all queries are saved to local testdata.
            self.server = remote.RemoteDataManager(lims_url=lims_url, lims_token=lims_token, apiversion=apiversion, verify=verify_cert)
            self.autosaveserver = local.LocalDataManager()
            self.log('Running in testdata update mode')

        else:
            # Normal mode where we work with the LIMS and
            # disable the local cache.
            self.remote = remote.RemoteDataManager(lims_url=lims_url, lims_token=lims_token, apiversion=apiversion, verify=verify_cert)
            self.autosaveserver = None
            self.log('Running in normal mode, reading from and writing to remote LIMS')

        # If override_owner is set to a valid email address,
        # emails in runinfo will be replaced by the override.
        # Useful for using production data but not sending
        # automated emails to users.
        if override_owner is None:
            self.override_owner = None
        else:
            self.override_owner = self._clean_override_owner(override_owner)

        # Initialize pretty printer for writing data structures in the log
        self.pprint = pprint.PrettyPrinter(indent=2, width=1).pprint

    def getsamplesheet(self, run, lane=None, filename='samplesheet.csv'):
        if lane is None:
            self.log("Writing samplesheet for run %s, all lanes, to file %s" % (run, filename))
        else:
            self.log("Writing samplesheet for run %s lane %s to file %s" % (run, lane, filename))

        samplesheet = self.server.getsamplesheet(run=run, lane=lane)

        if not samplesheet:
            raise Exception('samplesheet for run %s could not be found.' % run)

        if self.autosaveserver:
            self.autosaveserver.addsamplesheet(run=run, samplesheet=samplesheet, lane=lane)
            self.autosaveserver.writesamplesheetstodisk()
            if lane is None:
                self.log("Added samplesheet for run %s all lanes to testdata." % run)
            else:
                self.log("Added samplesheet for run %s lane %s to testdata." % (run, lane))

        if filename:
            with open(filename, 'w') as f:
                f.write(samplesheet)

        self.log(samplesheet)
        return samplesheet

    def getruninfo(self, run=None):
        self.log("Getting run info for run %s" % run)
        dirty_runinfo = self.server.getruninfo(run=run)
        runinfo = self._processruninfo(dirty_runinfo) #update emails if self.override_owner is True

        if not runinfo:
            raise Exception('runinfo for run %s could not be found.' % run)

        if self.autosaveserver:
            self.autosaveserver.addruninfo(run=run, runinfo=runinfo)
            self.autosaveserver.writeruninfotodisk()
            self.log("Added runinfo for %s to testdata." % run)

        self.log(runinfo, pretty=True)
        return runinfo

    def createpipelinerun(self, run, lane, paramdict = None):
        self.log("Resetting any old results before creating pipeline run")
        if self.autosaveserver:
            self._write_not_supported_error()

        self.server.deletelaneresults(run, lane)
        self.log("Creating pipeline run object for run=%s, lane=%s, paramdict=%s" % (run, lane, paramdict))
        pipelinerun = self.server.createpipelinerun(run, lane, paramdict)

        if not pipelinerun:
            raise Exception('Failed to create pipelinerun for run=%s lane=%s paramdict=%s' % (run, lane, paramdict))

        self.log(pipelinerun, pretty=True)
        return pipelinerun

    def deletelaneresults(self, run, lane):
        self.log("Resetting old results")
        if self.autosaveserver:
            self._delete_not_supported_error()

        self.server.deletelaneresults(run, lane)


    def createlaneresult(self, paramdict, run, lane):
        self.log("Creating lane result for run=%s, lane=%s, paramsdict=%s" % (run, lane, paramdict))
        if self.autosaveserver:
            self._write_not_supported_error()

        laneresult = self.server.createlaneresult(paramdict, run=run, lane=lane)

        if not laneresult:
            raise Exception('Failed to create laneresult for run=%s lane=%s paramdict=%s' % (run, lane, paramdict))

        if self.autosaveserver:
            self._write_not_supported_error()

        self.log(laneresult, pretty=True)
        return laneresult

    def createmapperresult(self, paramdict):
        self.log("Creating mapper result with paramsdict=%s" % paramdict)
        if self.autosaveserver:
            self._write_not_supported_error()
        
        mapperresult = self.server.createmapperresult(paramdict)

        if not mapperresult:
            raise Exception('Failed to create mapperresult for paramdict=%s' % paramdict)

        self.log(mapperresult, pretty=True)
        return mapperresult

    def showsolexarun(self, id):
        self.log("Getting solexarun id %s" % id)
        solexarun = self.server.showsolexarun(id)

        if not solexarun:
            raise Exception('solexarun with id %s could not be found.' % id)
        
        if self.autosaveserver:
            self.autosaveserver.addsolexarun(id=id, solexarun=solexarun)
            self.writesolexarunstodisk()
            self.log("Added solexarun %s to testdata." % run_name)

        self.log(solexarun, pretty=True)
        return solexarun

    def showsolexaflowcell(self, id):
        self.log("Getting solexaflowcell id %s" % id)
        solexaflowcell = self.server.showsolexaflowcell(id)

        if not solexaflowcell:
            raise Exception('solexaflowcell with id %s could not be found.' % id)

        if self.autosaveserver:
            self.autosaveserver.addsolexaflowcell(id=id, solexaflowcell=solexaflowcell)
            self.autosaveserver.writesolexaflowcellstodisk()
            self.log("added solexaflowcell id %s to testdata." % id)

        self.log(solexaflowcell, pretty=True)
        return solexaflowcell

    def showpipelinerun(self, id):
        self.log("Showing pipeline run with id=%s" % id)
        pipelinerun = self.server.showpipelinerun(id)

        if not pipelinerun:
            raise Exception('pipelinerun with id %s could not be found.' % id)

        if self.autosaveserver:
            self.autosaveserver.addpipelinerun(id=id, pipelinerun=pipelinerun)
            self.autosaveserver.writepipelinerunstodisk()
            self.log("Added pipelinerun id %s to testdata." % id)

        self.log(pipelinerun, pretty=True)
        return pipelinerun

    def showlaneresult(self, id):
        self.log("Showing laneresult with id=%s" % id)
        laneresult = self.server.showlaneresult(id)

        if not laneresult:
            raise Exception('laneresult with id %s could not be found.' % id)

        if self.autosaveserver:
            self.autosaveserver.addlaneresult(id=id, laneresult=laneresult)
            self.autosaveserver.writelaneresultstodisk()
            self.log("Added laneresult id %s to testdata." % id)

        self.log(laneresult, pretty=True)
        return laneresult

    def showmapperresult(self, id):
        self.log("Showing mapper result with id=%s" % id)
        mapperresult = self.server.showmapperresult(id)

        if not mapperresult:
            raise Exception('mapperresult with id %s could not be found.' % id)

        if self.autosaveserver:
            self.autosaveserver.addmapperresult(id=id, mapperresult=mapperresult)
            self.autosaveserver.writemapperresultstodisk()
            self.log("Added mapperresult id %s to testdata." % id)

        self.log(mapperresult, pretty=True)
        return mapperresult

    def indexsolexaruns(self, run):
        self.log("Indexing solexa run(s) where run=%s" % run)
        solexaruns = self.server.indexsolexaruns(run)

        if self.autosaveserver:
            self.autosaveserver.addsolexaruns(solexaruns=solexaruns)
            self.autosaveserver.writesolexarunstodisk()
            self.log("Added %s solexa runs to testdata" % len(solexaruns))

        self.log(solexaruns, pretty=True)
        return solexaruns

    def indexpipelineruns(self, run):
        self.log("Indexing pipeline runs where run=%s" % run)
        pipelineruns = self.server.indexpipelineruns(run)

        if self.autosaveserver:
            self.autosaveserver.addpipelineruns(pipelineruns=pipelineruns)
            self.autosaveserver.writepipelinerunstodisk()
            self.log("Added %s pipeline runs to testdata" % len(pipelineruns))

        self.log(pipelineruns, pretty=True)
        return pipelineruns

    def indexlaneresults(self, run, lane=None, barcode=None, readnumber=None):

        self.log("Indexing lane results where run=%s, lane=%s, barcode=%s" % 
                 (run, lane, barcode))

        laneresults = self.server.indexlaneresults(run, lane=lane, barcode=barcode, readnumber=readnumber)

        if self.autosaveserver:
            self.autosaveserver.addlaneresults(laneresults = laneresults)
            self.autosaveserver.writelaneresultstodisk()
            self.log("Added %s lane results to testdata" % len(laneresults))

        self.log(laneresults, pretty=True)
        return laneresults

    def indexmapperresults(self, run):
        self.log("Indexing mapper results where run=%s" % run)
        mapperresults = self.server.indexmapperresults(run)

        if self.autosaveserver:
            self.autosaveserver.addmapperresults(mapperresults=mapperresults)
            self.autosaveserver.writemapperresultstodisk()
            self.log("Added %s mapper results to testdata" % len(mapperresults))

        self.log(mapperresults, pretty=True)
        return mapperresults

    def updatesolexarun(self, run_id, paramdict):
        self.log("Updating Solexa Run id=%s with paramdict=%s" % (run_id, paramdict))
        if self.autosaveserver:
            self._write_not_supported_error()
        
        run = self.server.updatesolexarun(run_id, paramdict)
        if not run:
            raise Exception("Failed to update Solexa Run id=%s paramdict=%s" % (run_id, paramdict))

        self.log(run, pretty=True)
        return run

    def updatesolexaflowcell(self, id, paramdict):
        self.log("Updating Solexa Flow Cell id=%s with paramdict=%s" % (id, paramdict))
        if self.autosaveserver:
            self._write_not_supported_error()
    
        flowcell = self.server.updatesolexaflowcell(id, paramdict)

        if not flowcell:
            raise Exception("Failed to update Solexa Flow Cell id=%s paramdict=%s" % (id, paramdict))
        
        self.log(flowcell, pretty=True)
        return flowcell

    def updatepipelinerun(self, id, paramdict):
        self.log("Updating pipeline run id=%s with paramdict=%s" % (id, paramdict))
        if self.autosaveserver:
            self._write_not_supported_error()

        pipelinerun = self.server.updatepipelinerun(id, paramdict)

        if not pipelinerun:
            raise Exception("Failed to update pipelinerun id=%s paramdict=%s" % (id, paramdict))

        self.log(pipelinerun, pretty=True)
        return pipelinerun
    
    def updatelaneresult(self, id, paramdict):
        self.log("Updating lane result id=%s with paramdict=%s" % (id, paramdict))
        if self.autosaveserver:
            self._write_not_supported_error()

        laneresult = self.server.updatelaneresult(id, paramdict)

        if not laneresult:
            raise Exception("Failed to update laneresult id=%s paramdict=%s" % (id, paramdict))

        self.log(laneresult, pretty=True)
        return laneresult

    def updatemapperresult(self, id, paramdict):
        self.log("Updating mapper result id=%s with paramdict=%s" % (id, paramdict))
        if self.autosaveserver:
            self._write_not_supported_error()

        mapperresult = self.server.updatemapperresult(id, paramdict)

        if not mapperresult:
            raise Exception("Failed to update mapperresult id=%s paramdict=%s" % (id, paramdict))

        self.log(mapperresult, pretty=True)
        return mapperresult

    def getallrunobjects(self, run):
        runinfo = self.getruninfo(run)
        run_data = self.indexsolexaruns(run).values()[0]
        self.showsolexaflowcell(run_data['solexa_flow_cell_id'])
        self.getsamplesheet(run, filename=None)
        self.indexsolexaruns(run)
        for lane in runinfo['run_info']['lanes'].keys():
            self.getsamplesheet(run, filename=None, lane=lane)
        self.indexpipelineruns(run)
        self.indexlaneresults(run)
        self.indexmapperresults(run)

    def _write_not_supported_error(self):
        raise Exception('Write operations are not supported in test_data_update mode. '+
                        'If you want to create objects in the local cache, run in local_only '+
                        'mode and call write_to_disk')

    def _delete_not_supported_error(self):
        raise Exception('Delete operations are not supported in test_data_update mode. '+
                        'If you want to destroy objects in the local cache, run in local_only '+
                        'mode and call write_to_disk')

    def testconnection(self):
        # Raises exception if no 200 response
        self.server.testconnection()
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
