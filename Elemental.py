#! /usr/bin/env python
## For Elemental VOD encoding

import xml.dom.minidom
import xmltodict
import hashlib
import time
from urlparse import urlparse
import requests
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)


class Job():
    pass


class Elemental():
    address = ""
    user = ""
    apikey = ""
    destination_path = ""

    def __init__(self, address=None, user=None, apikey=None):
        if address != None:
            self.address = address
        if user != None:
            self.user = user
        if apikey != None:
            self.apikey = apikey

    def url(self):
        """ Returns the http url for the Elemental """
        return "http://" + self.address

    ## These functions are used to communicate with the Elemental Live appliance
    ## Auth-REST XML Key Generation
    def xauthkey(self, xurl, xauthuser, apikey, xexpire):
        purl = urlparse(xurl)[2]
        #primary
        p = hashlib.md5()
        #secondary
        s = hashlib.md5()
        p.update(purl + xauthuser + apikey + xexpire)
        s.update(apikey + p.hexdigest())
        return s.hexdigest()

    ## Auth-REST XML POST
    def postRequest(self, xurl, data, xuser, apikey):
        expire = str(time.time() + 60)
        headers = {'X-Auth-User': xuser,
            'X-Auth-Expires': expire,
            'X-Auth-Key': self.xauthkey(xurl, xuser, apikey, expire),
            'Accept': 'application/xml',
            'Content-type': 'application/xml'}
        try:
            r = requests.post(xurl, data=data, headers=headers)
            return r.text
        except (requests.ConnectionError, requests.Timeout) as error:
            # logger.error(error)
            raise requests.ConnectionError(error)

    ## Auth-REST XML GET
    def getRequest(self, xurl, xuser, apikey):
        expire = str(time.time() + 60)
        headers = {'X-Auth-User': xuser,
            'X-Auth-Expires': expire,
            'X-Auth-Key': self.xauthkey(xurl, xuser, apikey, expire),
            'Accept': 'application/xml',
            'Content-type': 'application/xml'}
        try:
            r = requests.get(xurl, headers=headers)
            return r.text
        except (requests.ConnectionError, requests.Timeout) as error:
            raise requests.ConnectionError(error)

    ## Posting a job to the Elemental
    def postJob(self, job):
        """Posts the Job Request and hopefully returns the "job_guid" of the job. """
        joburl = self.url() + "/jobs"
        respones = self.postRequest(joburl, job, self.user, self.apikey)
        returnXML = xml.dom.minidom.parseString(respones)
        # logger.debug(respones)
        if len(returnXML.getElementsByTagName("job")) > 0:
            eventElement = returnXML.getElementsByTagName("job")[0]
            if eventElement.getAttribute("href") != None:
                job_id = eventElement.getAttribute("href").rsplit('/', 1)[1]
                job = {'job_guid': job_id, 'job_url': self.url() + job_id, 'priority': 50}
                return job
        return None

    def buildStitchJob(self, inputfiles, destination, preset):
        string = """<?xml version=\"1.0\" ?>"""
        dom = xml.dom.minidom.parseString(string)
        return dom

    ## Create an encode job
    def buildJob(self, inputs, preset):
        input_string = """ """
        for input_ in inputs:
            input_string += """
            <input>
                <order>""" + str(input_['order']) + """</order>
                <file_input>
                    <uri>""" + input_['location'] + """</uri>
                </file_input>
                <name>input_""" + str(input_['order']) + """</name>
                <video_selector>
                        <color_space>follow</color_space>
                        <order>1</order>
                        <program_id nil="true"></program_id>
                        <name>input_""" + str(input_['order']) + """_audio_selector_1</name>
                    </video_selector>
                    <audio_selector>
                        <default_selection>true</default_selection>
                        <order>1</order>
                        <name>input_""" + str(input_['order']) + """_audio_selector_1</name>
                </audio_selector>
            </input>"""

        string = """<?xml version=\"1.0\" ?>
                <job>
                    """ + input_string + """
                    <priority>50</priority>
                    """

                    # <profile>5</profile>
        string += """<pre_process>
                        <copy_local>false</copy_local>
                            <script>
                                <uri>/data/mnt/material/pre-process.py</uri>
                            </script>
                    </pre_process>"""

        string += """<post_process>
                         <script>
                             <uri>/data/mnt/material/post-process.py</uri>
                         </script>
                    </post_process>"""

        string += """<output_group>
                        <order>1</order>
                        <file_group_settings>
                            <destination>
                                <uri>""" + self.destination_path + "/" + preset['destination'] + """/</uri>
                            </destination>
                        </file_group_settings>
                        <output>
                            <order>1</order>
                            <stream_assembly_name>stream_1</stream_assembly_name>
                            <preset>""" + preset['preset_name'] + """</preset>
                            <name_modifier>""" + preset['name_modifier'] + """</name_modifier>
                            <extension>""" + preset['extension'] + """</extension>
                        </output>
                    </output_group>
                    <stream_assembly>
                        <name>stream_1</name>
                        <preset>""" + preset['preset_name'] + """</preset>
                    </stream_assembly>
                </job>"""
        # logger.debug(string)
        dom = xml.dom.minidom.parseString(string)
        return dom

    def getJobStatusRaw(self, job_id):
        job_url = self.url() + '/jobs/' + job_id + '/status'
        respones = self.getRequest(job_url, self.user, self.apikey)
        return respones

    ## Get a job's status
    def getJobStatus(self, job_id):
        data = {'error': False}
        job_url = self.url() + '/jobs/' + job_id + '/status'
        respones = self.getRequest(job_url, self.user, self.apikey)
        _dict = xmltodict.parse(respones)
        # logger.debug(_dict)
        if _dict.get('errors'):
            return {'job_id': None, 'job_url': None, 'status': 'missing', 'warning': _dict['errors']['error'], 'error': _dict['errors']['error']}
        data['status'] = _dict['job']['status']
        # data['status'] = 'pending'
        data['job_id'] = job_id
        data['job_url'] = _dict['job']['@href']
        if _dict['job'].get('warning_messages'):
            data['warning'] = _dict['job']['warning_messages']['warning']['message']
        if _dict['job'].get('error_messages'):
            data['error'] = _dict['job']['error_messages']['error']['message']
        # logger.debug(respones)
        return data

    def getJob(self, job_id):
        job_url = self.url() + '/jobs/' + str(job_id)
        respones = self.getRequest(job_url, self.user, self.apikey)
        return respones

    ## List active jobs on an Elemental
    def getJobList(self):
        results = {}
        joburl = self.url() + '/jobs'
        response = self.getRequest(joburl, self.user, self.apikey)
        # logger.debug(response)
        _dict = xmltodict.parse(response)
        logger.debug(_dict['job_list'])
        if _dict['job_list'].get('empty'):
            results['results'] = "empty foo"
            results['error'] = True
        else:
            for job in _dict['job_list']['job']:
                job_result = {}
                job_result['job_guid'] = job['@href']
                # logger.debug(job['@href'])
                if job.get('warning_messages'):
                    job_result['warning_messages'] = job['warning_messages']['warning']['message']
                    # logger.debug(job['warning_messages']['warning']['message'])
                if job.get('error_messages'):
                    job_result['error_messages'] = job['error_messages']['error']['message']
                    # logger.debug(job['error_messages']['error']['message'])
                results['results'].append(job_result)
        return results
