"""Python Class and Functions for working with Alation Data Objects."""

import logging
from time import sleep

from src.alation_rest import AlationRestAPI
from src.models.alation.auth import AlationAuth
from src.models.alation.job import Job

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
