"""Python Class and Functions for working with Alation Data Objects."""

import logging
from time import sleep

from .alation_rest import AlationRestAPI
from .models.alation.auth import AlationAuth
from .models.alation.bi_server import BIServer
from .models.alation.job import Job

LOGGER = logging.getLogger()


class AlationHelpers(AlationRestAPI):
    """Python Class for working with Alation Data Objects."""

    def __init__(self, configs: dict):
        """Create an instance of AlationHelper.

        Args:
            configs (dict): Script Environment Configurations.

        """
        super().__init__(configs=configs)

        self.configs = configs

    def check_jobs_status(self, alation_auth: AlationAuth, job: Job):
        """Query the Alation Background Job and Log Status until Job has completed.

        Args:
            alation_auth (AlationAuth): Alation REST API Authentication Object.
            job (Job): Alation Background Job.

        """
        while True:

            self.api_query_job(alation_auth.access_token, job)
            job.log_job()

            if job.completed:
                break
            else:
                sleep(self.configs['features_job_status_sleep'])

        sleep(1)

    def alation_authentication(self) -> AlationAuth:
        """Authenticate with the Alation REST API.

        Returns:
            AlationAuth: Alation REST API Authentication Object.

        """
        LOGGER.info("Authenticating with the Alation Catalog")
        base_auth = AlationAuth()
        base_auth.load_refresh_token(self.configs['alation_refresh_token_location'])
        base_auth.user_id = self.configs['alation_user_id']

        if base_auth.refresh_token:
            LOGGER.debug("Reusing Refresh Token")
            self.api_validate_refresh_token(base_auth)

            if base_auth.refresh_token_valid:
                LOGGER.debug("Refresh Token is Valid")
                api_auth = base_auth
            else:
                LOGGER.debug("Refresh Token is expired. Generating new Refresh token.")
                api_auth = self.api_generate_refresh_token()

        else:
            LOGGER.debug("No Refresh Token Found. Generating new Refresh Token.")
            api_auth = self.api_generate_refresh_token()

        LOGGER.debug("Storing Refresh Token for future use.")
        api_auth.store_refresh_token(self.configs['alation_refresh_token_location'])

        LOGGER.debug("Generating API Access Token")
        self.api_generate_access_token(api_auth)

        if api_auth.access_token:
            LOGGER.info("Successfully authenticated with the Alation Catalog")
        else:
            raise Exception("Could not generate the Alation API access token. Exiting script.")

        return api_auth

    def return_virtual_bi_server(self, api_token: str) -> BIServer:
        """Return the Alation Virtual BI Server that will catalog the Tableau Online Site.

        Args:
            api_token (str): Alation REST API Authentication Token.

        Returns:
            BIServer: Alation BIServer Object.

        """
        # Type Hinting
        bi_server: BIServer
        server: BIServer

        config_bi_server = self._create_bi_server_from_configs()

        # Check if the Virtual BI Server Already exists in Alation
        LOGGER.info('Checking if the Virtual BI Server already exists')
        bi_servers = self.api_query_bi_servers(api_token=api_token)
        bi_server = next((server for server in bi_servers if
                          server.title == config_bi_server.title), None)

        if bi_server:
            LOGGER.info(
                f"The Virtual BI Server '{bi_server.title}' already exists. (ID: {bi_server.id})")

        else:
            LOGGER.info(
                f"The Virtual BI Server '{config_bi_server.title} does not exists. Creating a Virtual BI"
                f"Server Now.")
            bi_server = config_bi_server
            bi_server.id = self.api_create_bi_server(api_token=api_token,
                                                     bi_server=[bi_server.api_json_payload])

            if bi_server.id:
                LOGGER.info(
                    f"Successfully created the BI Server '{bi_server.title}' (ID: {bi_server.id})")
            else:
                raise Exception("The Virtual BI Server ID could not be generated. Exiting Script.")

        return bi_server

    def _create_bi_server_from_configs(self) -> BIServer:
        """Create a Virtual BI Server from Configuration File.

        Returns:
            BIServer: Alation BIServer Object.

        """
        LOGGER.info('Building the Virtual BI Server based on Config File Properties')
        config_bi_server = BIServer()
        config_bi_server.title = self.configs['alation_bi_server_name']
        config_bi_server.connection_key = self.configs['alation_bi_connection_key']
        config_bi_server.datasource_key = self.configs['alation_bi_datasource_key']
        config_bi_server.folder_key = self.configs['alation_bi_folder_key']
        config_bi_server.report_key = self.configs['alation_bi_report_key']
        config_bi_server.uri = self.configs['tableau_host']

        return config_bi_server
