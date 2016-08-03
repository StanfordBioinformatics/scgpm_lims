import json
import requests
import warnings
import os
import sys

warnings.filterwarnings('ignore', 'Unverified HTTPS request is being made. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.org/en/latest/security.html')
import urllib3
urllib3.disable_warnings()

class RemoteDataManager:

    localorremote = 'remote'

    def __init__(self, apiversion=None, lims_url=None, lims_token=None, verify=False):

        if not apiversion:
            raise Exception('apiversion is required')
        self.apiversion = apiversion

        if (not lims_url) or not (lims_token):
            raise Exception("lims_url and lims_token are required. Current settings are lims_url=%s, lims_token=%s" % (lims_url, lims_token))
        self.token = lims_token
        self.urlprefix = self._geturlprefix(lims_url, apiversion)
        self.verify = verify


    def getrunstoanalyze(self):
        """
        Fetches the runs names from UHTS that need to have analyses started.
        Runs with the following criteria are returned:
            1) sequencing_run_status = sequencing_done
            2) analysis_done = false 
            3) There aren't any pipeline_runs
            4) The sequencing instrument isn't a HiSeq 4000 (since those aren't supported yet in the pipeline).
       """
        params = {
            'token': self.token,
            }
        response = requests.get(
            self.urlprefix+'runs_to_analyze', #runs_to_analyze_controller.rb
            params=params,
            verify=self.verify,
            )
        self._checkstatus(response)
        print(response.url)
        return response.json()

    def getsamplesheet(self, bcl2fastq_version, run, lane=None):
        """
        Creates the sample sheet contents for demultiplexing. Supports bcl2fastq 1x and 2x. For 2x, the second index (I5) is reverse-complemented
        with respect to what's stored in UHTS. As stated in the Illumina docs: For Illumina sequencing systems running RTA version 1.18.54 and 
        later, use bcl2fastq2 Conversion Software v2.17.  For Illumina sequencing systems runnings RTA versions earlier than 1.18.54, use bcl2fastq
        Conversion Software v1.8.4.

        The version of RTA used in the sequencing run can be found in the runParameters.xml file with the run directory.
       """
        params = {
            'token': self.token,
            'run': run,
            'bcl2fastq_version': bcl2fastq_version
            }
        if lane is not None:
            params['lane'] = str(lane)

        response = requests.get(
            self.urlprefix+'samplesheets',  #calls solexa_run.samplesheets in app/models in UHTS
            params=params,
            verify=self.verify,
            )
        self._checkstatus(response)
        print(response.url)
        return response.text

    def getruninfo(self, run):
        response = requests.get(
            self.urlprefix+'run_info',
            params = {
                'token': self.token,
                'run': run
                },
            verify = self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def get_dna_library_info(self, dna_library_id):
        url = self.urlprefix + 'dna_libraries' + '/' + dna_library_id
        response = requests.get(
            url,
            params = {
                'token': self.token
            },
            verify = self.verify
        )   
        self._checkstatus(response)
        return response.json()
    
    def get_runinfo_by_library_name(self,library_name):
        #run_info_by_library_name defined in config/routes.rb in RAILS app.
        # Also see the UHTS controller app/controllers/api/v1/run_info_by_library_name_controller.rb.
 
        url = self.urlprefix + "run_info_by_library_name" #run_info_by_library_name route defined in config/routes.rb in RAILS app
        print(url)
        response = requests.get(
            url,
            params = {
                'token': self.token,
                'starts_with': library_name
            },
            verify = self.verify
        )
        self._checkstatus(response)
        return response.json()

    def get_person_attributes_by_email(self,email):
        url = self.urlprefix + "get_person_by_email" #get_person_by_email route defined in config/routes.rb in RAILS app
        print(url)
        response = requests.get(
            url,
            params = {
                'token': self.token,
                 'email': email
            },
            verify = self.verify
        )
        print(response.url)
        self._checkstatus(response)
        return response.json()

    def update_person(self,personid,attributeDict={}):
        """
        Function : Updates/sets an attribute of a Person record.
        Args     : personid - The ID of a UHTS.Person record.
                   attributeDict - dict. Keys are Person attribute names.
        Returns  : A JSON hash of the person specified by personid as it exists in the database after the record update(s).
        """
        url = self.urlprefix + "people/" + str(personid)
        params = {"token": self.token}
        params.update(attributeDict)
        print(params)
        response = requests.patch(
            url,
            params = params,
            verify = self.verify
        )
        self._checkstatus(response)
        print(response.url)
        return response.json()
           

    def getrunid(self, run):
        runinfo = requests.getruninfo(run)
        try:
            id = runinfo.get('id')
        except:
            return None
        return id

    def getlaneid(self, run, lane):
        runinfo = requests.getruninfo(run)
        try:
            id = runinfo.get('run_info').get('lanes').get(str(lane)).get('id')
        except:
            return None
        return id

    def showsolexarun(self, id):
        response = requests.get(
            self.urlprefix+'solexa_runs/%s' % id,
            params = {
                'token': self.token
                },
            verify = self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def showsolexaflowcell(self, id):
        response = requests.get(
            self.urlprefix+'solexa_flow_cells/%s' % id,
            params = {
                'token': self.token
                },
            verify = self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def showpipelinerun(self, id):
        response = requests.get(
            self.urlprefix+'solexa_pipeline_runs/%s' % id,
            params = {
                'token': self.token
                },
            verify = self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def showlaneresult(id):
        response = requests.get(
            self.urlprefix+'solexa_lane_results/%s' % id,
            params = {
                'token': self.token
                },
            verify = self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def showmapperresult(self, id):
        response = requests.get(
            self.urlprefix+'mapper_results/%s' % id,
            params = {
                'token': self.token
                },
            verify=self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def indexsolexaruns(self, run):
        response = requests.get(
            self.urlprefix+'solexa_runs',
            params = {
                'token': self.token,
                'run': run
                },
            verify=self.verify,
            )

        self._checkstatus(response)
        return self._listtodict(response.json())

    def indexpipelineruns(self, run):
        response = requests.get(
            self.urlprefix+'solexa_pipeline_runs',
            params = {
                'token': self.token,
                'run': run
                },
            verify=self.verify,
            )
        self._checkstatus(response)
        return self._listtodict(response.json())

    def indexlaneresults(self, run, lane=None, barcode=None, readnumber=None):
        params = {'run': run,
                  'token': self.token}
        if lane is not None:
            params.update({'lane': lane})
            if barcode is not None:
                params.update({'barcode': barcode})
                if readnumber is not None:
                    params.update({'read_number': readnumber})
        response = requests.get(
            self.urlprefix+'solexa_lane_results',
            params = params,
            verify=self.verify,
            )

        self._checkstatus(response)
        return self._listtodict(response.json())

    def indexmapperresults(self, run):
        response = requests.get(
            self.urlprefix+'mapper_results',
            params = {
                'token': self.token,
                'run': run
                },
            verify=self.verify,
            )
        self._checkstatus(response)

        return self._listtodict(response.json())

    def createpipelinerun(self, run, paramdict = None):
        if paramdict:
            data = json.dumps(paramdict)
        else:
            data = None

        response = requests.post(
            self.urlprefix+'solexa_pipeline_runs',
            params = {
                'run': run,
                'token': self.token
                },
            data = data,
            headers = {'content-type': 'application/json'},
            verify=self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def createlaneresult(self, paramdict, run=None, lane=None):
        params = {'token': self.token}
        if run is not None:
            params.update({'run': run})
            if lane is not None:
                params.update({'lane': lane})
        response = requests.post(
            self.urlprefix+'solexa_lane_results',
            params = params,
            data = json.dumps(paramdict),
            headers = {'content-type': 'application/json'},
            verify=self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def createmapperresult(self, paramdict):
        response = requests.post(
            self.urlprefix+'mapper_results',
            params = {'token': self.token},
            data = json.dumps(paramdict),
            headers = {'content-type': 'application/json'},
            verify=self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def updatesolexarun(self, id, paramdict):
        response = requests.patch(
            self.urlprefix+'solexa_runs/%s' % id,
            params = {
                'token': self.token
                },
            data=json.dumps(paramdict),
            headers = {'content-type': 'application/json'},
            verify=self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def updatesolexaflowcell(self, id, paramdict):
        response = requests.patch(
            self.urlprefix+'solexa_flow_cells/%s' % id,
            params = {
                'token': self.token
                },
            data=json.dumps(paramdict),
            headers = {'content-type': 'application/json'},
            verify=self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def updatepipelinerun(self, id, paramdict):
        response = requests.patch(
            self.urlprefix+'solexa_pipeline_runs/%s' % id,
            params = {
                'token': self.token
                },
            data=json.dumps(paramdict),
            headers = {'content-type': 'application/json'},
            verify=self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def updatelaneresult(self, id, paramdict):
        response = requests.patch(
            self.urlprefix+'solexa_lane_results/%s' % id,
            params = {
                'token': self.token
                },
            data=json.dumps(paramdict),
            headers = {'content-type': 'application/json'},
            verify=self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def updatemapperresult(self, id, paramdict):
        response = requests.patch(
            self.urlprefix+'mapper_results/%s' % id,
            params = {
                'token': self.token
                },
            data=json.dumps(paramdict),
            headers = {'content-type': 'application/json'},
            verify=self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def deletelaneresults(self, run, lane):
        response = requests.post(
            self.urlprefix+'delete_lane_results',
            params = {
                'run': run,
                'lane': lane,
                'token': self.token
                },
            verify=self.verify,
            )
        self._checkstatus(response)

    def testconnection(self):
        response = requests.get(
            self.urlprefix+'ok',
            params = {
                'token': self.token
                },
            verify=self.verify,
            )
        self._checkstatus(response)
        return

    def _listtodict(self, resultslist):
        resultsdict = {}
        for result in resultslist:
            resultsdict[str(result.get('id'))] = result
        return resultsdict

    def _geturlprefix(self, rooturl, apiversion):
        
        return os.path.join(rooturl,"api",apiversion) + "/"

    def _checkstatus(self, response):

       #response.raise_for_status(), doesn't show the url that you attempted, so I'll that that in addition to sys.stderr
        if not response.ok: 
            sys.stderr.write("HTTPError! {status} ({reason}). Final URL location of Response: {url}. Response text: {text}\n\n".format(status=response.status_code,reason=response.reason, url=response.url,text=response.text))
            sys.stderr.write("Body sent in the request:\n")
            body = response.request.body #can be None
            if body:
                sys.stderr.write(body)
            #response.status_code - Integer Code of responded HTTP Status, e.g. 404 or 200.
            #response.reason - Textual reason of responded HTTP Status, e.g. "Not Found" or "OK".
            #response.url    - Final URL location of Response. 
            response.raise_for_status() #Raises stored HTTPError, if one occurred (and here one did occur).

