"""Alation API Wrapper."""

import json
import logging
import requests

from src.models.alation.auth import AlationAuth
from src.models.alation.job import Job

API_LOGGER = logging.getLogger("alation_rest")


class AlationRestAPI(object):
    """Alation REST API Wrapper."""

    def __init__(self, configs: dict):
        """Create an instance of the AlationAPI Object.

        Args:
            configs (dict): Script Environment Configurations.

        """
        self.alation_host = configs['alation_host']
        self.username = configs['alation_username']
        self.password = configs['alation_password']
        self.refresh_token = configs['alation_refresh_token_name']

        if configs['alation_enable_ssl']:
            if configs.get('alation_ssl_cert'):
                self.verify_ssl = configs['alation_ssl_cert']
            else:
                self.verify_ssl = True
        else:
            self.verify_ssl = False

        self._bi_server_id = None
        self.api_v2_url = f'{self.alation_host}/integration/v2'
        self.bi_api_url = f'{self.api_v2_url}/bi/server'

        self.acceptable_objects = ['FOLDER', 'CONNECTION', 'DATASOURCE', 'DATASOURCE FIELD',
                                   'REPORT', 'REPORT FIELD']

        #self.custom_fields = eval(configs['alation_custom_fields'])
        self.custom_fields = \
            {"REPORT": [
                {'Report LPN':  {'properties': {'f_oid': '', 'property': 'self.report_lpn'}}},
                {'Business Area': {'properties': {'f_oid': '', 'property': 'self.business_area'}}},
                {'Report Business Owner': {'properties': {'f_oid': '', 'property': 'self.report_business_owner'}}},
                {'Report Path': {'properties': {'f_oid': '', 'property': 'self.report_location'}}},
                {'Report Project Name': {'properties': {'f_oid': '', 'property': 'self.project_name'}}}],
             "REPORT FIELD": [
                {'Field Project Name': {'properties': {'f_oid': '', 'property': 'self.project_name'}}}]
            }


        #if self.custom_fields in not None:
        #    self_load_custom_fields()

    def api_query_custom_field(self, api_token: str, field_singular_name: str):

        bi_url = f"{self.alation_host}/integration/v2/custom_field/?name_singular={field_singular_name}"

        api_response = requests.get(bi_url, headers={'Token': api_token},
                                    verify=self.verify_ssl)
        response_data = api_response.json()

        if api_response.status_code != 200:
            error_code, title, detail = self._format_error(response_data)
            API_LOGGER.error(
                "Error querying for a Custom field in Alation Environment.",
                extra={'API Call': 'Get Custom Field',
                       'Method': 'GET',
                       'Host': self.alation_host,
                       'Response': api_response.status_code,
                       'Error Code': error_code,
                       'Error Title': title,
                       'Error Detail': detail})

        else:
            API_LOGGER.debug(
                "Successfully queried the Custom Field in Alation Environment.",
                extra={'API Call': 'Get Custom Field',
                       'Method': 'GET',
                       'Host': self.alation_host,
                       'Response': api_response.status_code})
            if len(response_data) == 0:
                API_LOGGER.debug(
                    f'No value returned for custom field {field_singular_name}')
            else:
                return response_data[0].get('id')

    def api_query_custom_fields(self, api_token: str, object_type: str) -> dict:
        """Retrive custom field IDS from alation that correspond to the object_type, i.e. Report.

        Args:
            api_token (str): Alation REST API Authentication Token.
            object_type (str): Type of Virtual BI Server Object to be Created.

        Returns:
            custom_fields: Alatoin Custom Field IDs.

        """
        object_type = object_type.upper()
        for custom_field in self.custom_fields.get(object_type):
            #field_id = self.api_query_custom_field(api_token, custom_field)
            for key, value in custom_field.items():
                value.get('properties')['f_oid'] = self.api_query_custom_field(api_token, key)

        #field_ids = [self.api_query_custom_field(api_token=api_token, field_singular_name=custom_field)
            #             for custom_field in self.custom_fields.get(object_type)]

        return self.custom_fields

    def api_update_custom_field_values(self, api_token: str, object_type: str, bi_objects: list):
        """Update custom fields in objects using Alation REST GBMv2 APIs.

        Args:
            api_token (str): Alation REST API Authentication Token.
            object_type (str): Type of Virtual BI Server Object to be Created.
            bi_objects (list): List of Virtual BI Server JSON objects to be Created.

        Returns:
            Job: Alation Background Job Object.

        """
        if object_type.upper() not in self.acceptable_objects:
            raise ValueError(f'GBMv2 does not accept the object type: {object_type.title()}')

        api_url = f'{self.api_v2_url}/custom_field_value/'
        payload = json.dumps(bi_objects, default=str)
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Token": api_token
        }

        api_response = requests.put(api_url, data=payload,
                                    headers=headers, verify=self.verify_ssl)
        response_data = api_response.json()

        if api_response.status_code != 200:
            error_code, title, detail = self._format_error(response_data)
            API_LOGGER.error(
                f"Error submitting the request to update custom fields for {len(bi_objects)} BI {object_type.title()}s",
                extra={'API Call': f'Update custom fields for {object_type.title()}s',
                       'Method': 'PUT',
                       'Host': self.alation_host,
                       'Payload': payload,
                       'Response': api_response.status_code,
                       'Error Code': error_code,
                       'Error Title': title,
                       'Error Detail': detail})

        else:
            API_LOGGER.info(
                f"Successfully submitted the request to update {len(bi_objects)} custom fields for BI {object_type.title()}s"
                f" - Job ID: {response_data.get('job_id')}",
                extra={'API Call': f'Update custom fields {object_type.title()}s',
                       'Method': 'PUT',
                       'Host': self.alation_host,
                       'Payload': payload,
                       'Response': api_response.status_code})

            return response_data

    def api_query_job(self, api_token: str, job: Job):
        """Update the Job object with the latest Job status details.

        Args:
            api_token (str): Alation REST API Authentication Token.
            job (Job): Alation Background Job.

        Returns:
            Job: Alation Background Job Object.

        """
        job_url = f'{self.alation_host}/api/v1/bulk_metadata/job/?id={job.id}'

        api_response = requests.get(job_url, headers={'Token': api_token},
                                    verify=self.verify_ssl)
        response_data = api_response.json()

        if api_response.status_code != 200:
            error_code, title, detail = self._format_error(response_data)
            API_LOGGER.error(
                f"Error querying the Alation Background Job {job.id}",
                extra={'API Call': 'Query Job',
                       'Method': 'GET',
                       'Host': self.alation_host,
                       'Job': job.id,
                       'Response': api_response.status_code,
                       'Error Code': error_code,
                       'Error Title': title,
                       'Error Detail': detail})

        else:
            API_LOGGER.debug(
                f"Successfully queried the Alation Background Job {job.id}",
                extra={'API Call': 'Query Job',
                       'Method': 'GET',
                       'Host': self.alation_host,
                       'Job': job.id,
                       'Response': api_response.status_code})

            job.load_from_api_response(response_data)

    def api_query_bi_servers(self, api_token: str) -> list:
        """Query the BI Servers in an Alation Environment.

        Args:
            api_token (str): Alation REST API Authentication Token.

        Returns:
            list: List of BIServer object instances.

        """
        bi_url = f"{self.bi_api_url}/"

        api_response = requests.get(bi_url, headers={'Token': api_token},
                                    verify=self.verify_ssl)
        response_data = api_response.json()

        if api_response.status_code != 200:
            error_code, title, detail = self._format_error(response_data)
            API_LOGGER.error(
                "Error querying the BI Servers in the Alation Environment.",
                extra={'API Call': 'Get BI Servers',
                       'Method': 'GET',
                       'Host': self.alation_host,
                       'Response': api_response.status_code,
                       'Error Code': error_code,
                       'Error Title': title,
                       'Error Detail': detail})

        else:
            API_LOGGER.debug(
                "Successfully queried the BI Servers in the Alation Environment.",
                extra={'API Call': 'Get BI Servers',
                       'Method': 'GET',
                       'Host': self.alation_host,
                       'Response': api_response.status_code})

            return [BIServer(api_response=response) for response in response_data]

    def api_create_bi_server(self, api_token: str, bi_server: list) -> int:
        """Create the Virtual BI Server using Alation REST GBMv2 APIs.

        Args:
            api_token (str): Alation REST API Authentication Token.
            bi_server (list): List of Virtual BI Servers to be Created/Updated.

        Returns:
            int: Newly created Virtual BI Server ID.

        """
        bi_url = f"{self.bi_api_url}/"
        payload = json.dumps(bi_server, default=str)
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Token": api_token
        }

        api_response = requests.post(bi_url, data=payload, headers=headers,
                                     verify=self.verify_ssl)
        response_data = api_response.json()

        if api_response.status_code != 200:
            error_code, title, detail = self._format_error(response_data)
            API_LOGGER.error(
                f"Error creating the Virtual BI Server: {bi_server}",
                extra={'API Call': 'Create BI Servers',
                       'Method': 'POST',
                       'Host': self.alation_host,
                       'Payload': payload,
                       'Response': api_response.status_code,
                       'Error Code': error_code,
                       'Error Title': title,
                       'Error Detail': detail})

        else:
            API_LOGGER.debug(
                f"Successfully created the Virtual BI Server: {bi_server}",
                extra={'API Call': 'Create BI Servers',
                       'Method': 'POST',
                       'Host': self.alation_host,
                       'Payload': payload,
                       'Response': api_response.status_code})

            return response_data.get('Server IDs')[0]

    def api_create_bi_objects(self, api_token: str, object_type: str, bi_objects: list) -> Job:
        """Create the Virtual BI Server Objects using Alation REST GBMv2 APIs.

        Args:
            api_token (str): Alation REST API Authentication Token.
            object_type (str): Type of Virtual BI Server Object to be Created.
            bi_objects (list): List of Virtual BI Server JSON objects to be Created.

        Returns:
            Job: Alation Background Job Object.

        """
        if object_type.upper() not in self.acceptable_objects:
            raise ValueError(f'GBMv2 does not accept the object type: {object_type.title()}')

        if not self.bi_server_id:
            raise ValueError(f'The Virtual BI Server ID is not set. Exiting Request.')

        api_urls = {
            'FOLDER': f'{self.bi_api_url}/{self._bi_server_id}/folder/',
            'CONNECTION': f'{self.bi_api_url}/{self.bi_server_id}/connection/',
            'DATASOURCE': f'{self.bi_api_url}/{self.bi_server_id}/datasource/',
            'DATASOURCE FIELD': f'{self.bi_api_url}/{self.bi_server_id}/datasource/column/',
            'REPORT': f'{self.bi_api_url}/{self.bi_server_id}/report/',
            'REPORT FIELD': f'{self.bi_api_url}/{self.bi_server_id}/report/column/'
        }
        payload = json.dumps(bi_objects, default=str)
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Token": api_token
        }

        api_response = requests.post(api_urls[object_type.upper()], data=payload,
                                     headers=headers, verify=self.verify_ssl)
        response_data = api_response.json()

        if api_response.status_code != 202:
            error_code, title, detail = self._format_error(response_data)
            API_LOGGER.error(
                f"Error submitting the request to create {len(bi_objects)} BI {object_type.title()}s",
                extra={'API Call': f'Create BI {object_type.title()}s',
                       'Method': 'POST',
                       'Host': self.alation_host,
                       'Payload': payload,
                       'Response': api_response.status_code,
                       'Error Code': error_code,
                       'Error Title': title,
                       'Error Detail': detail})

        else:
            API_LOGGER.info(
                f"Successfully submitted the request to create {len(bi_objects)} BI {object_type.title()}s"
                f" - Job ID: {response_data.get('job_id')}",
                extra={'API Call': f'Create BI {object_type.title()}s',
                       'Method': 'POST',
                       'Host': self.alation_host,
                       'Payload': payload,
                       'Response': api_response.status_code})

            return Job(job_id=response_data.get('job_id'))

    def api_query_object_ids(self, api_token: str, object_type: str, bi_objects: list):
        """Retrieve the Report IDs from Alation and update the batch list.

          Args:
              api_token (str): Alation REST API Authentication Token.
              object_type (str): Type of Virtual BI Server Object to be Created.
              bi_objects (list): List of Virtual BI Server JSON objects to be Created.

          Returns:
              bi_objects_out: List of objects with Alation IDS.

          """
        if object_type.upper() not in self.acceptable_objects:
            raise ValueError(f'GBMv2 does not accept the object type: {object_type.title()}')

        if not self.bi_server_id:
            raise ValueError(f'The Virtual BI Server ID is not set. Exiting Request.')

        api_urls = {
            'FOLDER': f'{self.bi_api_url}/{self._bi_server_id}/folder/',
            'CONNECTION': f'{self.bi_api_url}/{self.bi_server_id}/connection/',
            'DATASOURCE': f'{self.bi_api_url}/{self.bi_server_id}/datasource/',
            'DATASOURCE FIELD': f'{self.bi_api_url}/{self.bi_server_id}/datasource/column/',
            'REPORT': f'{self.bi_api_url}/{self.bi_server_id}/report/',
            'REPORT FIELD': f'{self.bi_api_url}/{self.bi_server_id}/report/column/'
        }
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Token": api_token
        }
        query_params = "?keyField=external_id&oids="
        # GUID size 32
        #prop_size = len(bi_objects[0].luid) + len(bi_objects[0].parent_luid) + 1
        prop_size = len(bi_objects[0].external_id()) + 1
        max_size = 8192 - len(query_params) - len(api_urls[object_type.upper()])
        batch_size = int(abs(max_size/prop_size))

        # get URL maximum size for nginx  8192, need to split this into smaller chunks
        request_batches = [bi_objects[x:x + batch_size] for x in range(0, len(bi_objects), batch_size)]
        for req_batch in request_batches:
            query_params = "?keyField=external_id&oids="
            for bi_object in req_batch:
                query_params += bi_object.external_id() + ','

            api_response = requests.get(api_urls[object_type.upper()]+query_params,
                                        headers=headers, verify=self.verify_ssl)
            response_data = api_response.json()

            for bi_object in req_batch:
                oid = next((obj for obj in response_data if
                            bi_object.external_id() == obj.get('external_id')), None)
                if oid is not None:
                    bi_object.oid = oid.get('id')
                else:
                    API_LOGGER.warning(
                        f"Warning: The object to be updated was not found in the catalog: "
                        f"{bi_object.external_id()}: {bi_object.name}: {bi_object}"
                    )

            if api_response.status_code != 200:
                error_code, title, detail = self._format_error(response_data)
                API_LOGGER.error(
                    f"Error submitting the request to retrieve IDS for {len(req_batch)} BI {object_type.title()}s",
                    extra={'API Call': f'Create BI {object_type.title()}s',
                           'Method': 'POST',
                           'Host': self.alation_host,
                           'Payload': '',
                           'Response': api_response.status_code,
                           'Error Code': error_code,
                           'Error Title': title,
                           'Error Detail': detail})

            else:
                API_LOGGER.info(
                    f"Successfully submitted the request to retrieve OIDs for {len(req_batch)} BI {object_type.title()}s",
                    extra={'API Call': f'Fill Object IDa {object_type.title()}s',
                           'Method': 'GET',
                           'Host': self.alation_host,
                           'Payload': '',
                           'Response': api_response.status_code})

    def api_generate_refresh_token(self) -> AlationAuth:
        """Generate the Alation API Refresh Token and initial AlationAuth Object.

        Returns:
            AlationAuth: Alation REST API Authentication Object.

        """
        token_url = f'{self.alation_host}/integration/v1/createRefreshToken/'
        request_data = {'username': self.username, 'password': self.password,
                        'name': self.refresh_token}

        api_response = requests.post(token_url, data=request_data,
                                     verify=self.verify_ssl)
        response_data = api_response.json()

        if api_response.status_code != 201:
            error_code, title, detail = self._format_error(response_data)
            API_LOGGER.error(
                "Error generating the Alation API Refresh Token",
                extra={'API Call': 'Generate Refresh Token',
                       'Method': 'POST',
                       'Host': self.alation_host,
                       'Response': api_response.status_code,
                       'Error Code': error_code,
                       'Error Title': title,
                       'Error Detail': detail})

        else:
            API_LOGGER.debug(
                "Successfully generated the Alation API Refresh Token",
                extra={'API Call': 'Generate Refresh Token',
                       'Method': 'POST',
                       'Host': self.alation_host,
                       'Response': api_response.status_code})

            return AlationAuth(refresh_response=response_data)

    def api_generate_access_token(self, alation_auth: AlationAuth):
        """Generate the Alation API Access Token and add the value to the
        AlationAuth Object.

        Args:
            alation_auth (AlationAuth): Alation REST API Authentication Object.

        """
        access_url = f'{self.alation_host}/integration/v1/createAPIAccessToken/'
        request_data = {'refresh_token': alation_auth.refresh_token,
                        'user_id': alation_auth.user_id}

        api_response = requests.post(access_url, data=request_data,
                                     verify=self.verify_ssl)
        response_data = api_response.json()

        if api_response.status_code != 201:
            error_code, title, detail = self._format_error(response_data)
            API_LOGGER.error(
                "Error generating the Alation API Access Token",
                extra={'API Call': 'Generate Access Token',
                       'Method': 'POST',
                       'Host': self.alation_host,
                       'Response': api_response.status_code,
                       'Error Code': error_code,
                       'Error Title': title,
                       'Error Detail': detail})

        else:
            API_LOGGER.debug(
                "Successfully generated the Alation API Access Token",
                extra={'API Call': 'Generate Access Token',
                       'Method': 'POST',
                       'Host': self.alation_host,
                       'Response': api_response.status_code})

            alation_auth.load_from_access_response(response_data)

    def api_validate_refresh_token(self, alation_auth: AlationAuth):
        """Check and Update the Status of the Alation API Refresh Token.

        Args:
            alation_auth (AlationAuth): Alation REST API Authentication Object.

        """
        validate_url = f'{self.alation_host}/integration/v1/validateRefreshToken/'
        request_data = {'refresh_token': alation_auth.refresh_token,
                        'user_id': alation_auth.user_id}

        api_response = requests.post(validate_url, data=request_data,
                                     verify=self.verify_ssl)
        response_data = api_response.json()

        if api_response.status_code != 200:
            error_code, title, detail = self._format_error(response_data)
            API_LOGGER.error(
                f"Error validating the Alation API Refresh Token '{alation_auth.refresh_token}'",
                extra={'API Call': 'Validate Refresh Token',
                       'Method': 'POST',
                       'Host': self.alation_host,
                       'Response': api_response.status_code,
                       'Error Code': error_code,
                       'Error Title': title,
                       'Error Detail': detail})

        else:
            API_LOGGER.debug(
                'Successfully validated the Alation API Access Token',
                extra={'API Call': 'Validate Refresh Token',
                       'Method': 'POST',
                       'Host': self.alation_host,
                       'Response': api_response.status_code})

            alation_auth.load_from_refresh_response(response_data)

    def api_validate_access_token(self, alation_auth: AlationAuth):
        """Check and Update the Status of the Alation API Access Token.

        Args:
            alation_auth (AlationAuth): Alation REST API Authentication Object.

        """
        validate_url = f'{self.alation_host}/integration/v1/validateAPIAccessToken/'
        request_data = {'api_access_token': alation_auth.access_token,
                        'user_id': alation_auth.user_id}

        api_response = requests.post(validate_url, data=request_data,
                                     verify=self.verify_ssl)
        response_data = api_response.json()

        if api_response.status_code != 200:
            error_code, title, detail = self._format_error(response_data)
            API_LOGGER.error(
                f"Error validating the Alation API Access Token '{alation_auth.access_token}'",
                extra={'API Call': 'Validate Access Token',
                       'Method': 'POST',
                       'Host': self.alation_host,
                       'Response': api_response.status_code,
                       'Error Code': error_code,
                       'Error Title': title,
                       'Error Detail': detail})

        else:
            API_LOGGER.debug(
                'Successfully validated the Alation API Access Token',
                extra={'API Call': 'Validate Access Token',
                       'Method': 'POST',
                       'Host': self.alation_host,
                       'Response': api_response.status_code})

            alation_auth.load_from_access_response(response_data)

    def api_create_lineage(self, api_token: str, payload: str) -> Job:
        """Create the Virtual BI Server using Alation REST GBMv2 APIs.

        Args:
            api_token (str): Alation REST API Authentication Token.
            payload (str):

        Returns:
            int: Newly created Virtual BI Server ID.

        """
        lineage_url = f"{self.alation_host}/integration/v2/dataflow/"
        #payload = json.dumps(bi_server, default=str)
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Token": api_token
        }

        api_response = requests.post(lineage_url, json=payload, headers=headers,
                                     verify=self.verify_ssl)
        response_data = api_response.json()

        if api_response.status_code != 202:
            error_code, title, detail, error = self._format_error(response_data)
            API_LOGGER.error(
                f"Error creating the lineage:",
                extra={'API Call': 'Create Lineage',
                       'Method': 'POST',
                       'Host': self.alation_host,
                       'Error Code': error_code,
                       'Error Title': title,
                       'Error Detail': detail,
                       'Error Detail1': error,
                       'Payload': payload,
                       'Response': api_response.status_code})

        else:
            API_LOGGER.debug(
                f"Successfully created lineage",
                extra={'API Call': 'Create Lineage',
                       'Method': 'POST',
                       'Host': self.alation_host,
                       'Payload': payload,
                       'Response': api_response.status_code})

            #self.check_jobs_status(self.alation_auth, job)
            return Job(job_id=response_data.get('job_id'))

    @property
    def bi_server_id(self) -> int:
        """Return the ID of the Virtual Alation BI Server.

        Returns:
            int: ID of the Virtual Alation BI Server.

        """
        return self._bi_server_id

    @bi_server_id.setter
    def bi_server_id(self, server_id: int):
        """Set the ID of the Virtual Alation BI Server.

        Args:
            server_id (int): Virtual Alation BI Server ID.

        """
        self._bi_server_id = server_id

    @staticmethod
    def _format_error(response: dict) -> tuple:
        """Format the error body a failed Rest API call.

        Args:
            response (dict): Response body of failed Rest API call

        Returns:
            tuple: Error Code, Error Title, Error Detail

        """
        error_code = response.get('code', None)
        error_title = response.get('title', None)
        error_detail = response.get('detail', None)
        error_errors = response.get('errors', None)


        return error_code, error_title, error_detail, error_errors
