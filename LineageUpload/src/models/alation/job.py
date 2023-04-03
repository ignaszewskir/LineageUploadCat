"""Alation Background Job."""

import logging

LOGGER = logging.getLogger()


class Job(object):
    """Alation Background Job."""

    def __init__(self, job_id: int, api_response: dict = None):
        """Create an instance of the Alation Background Job.

        Args:
            job_id (int): ID of the Alation Background Job.
            api_response (dict): Alation REST API Get Job Status Response Data.

        """
        self._id = job_id
        self._status = None
        self._message = None
        self._result = None

        if api_response:
            self.load_from_api_response(api_response)

    @property
    def id(self) -> int:
        """Return the Alation Job's ID.

        Returns:
            int: Alation Job's ID.

        """
        return self._id

    @id.setter
    def id(self, job_id: int):
        """Set the Alation Job's ID.

        Args:
            job_id (int): Alation Job's ID.

        """
        self._id = job_id

    @property
    def status(self) -> str:
        """Return the Alation Job's Status.

        Returns:
            str: Alation Job's Status.

        """
        return self._status

    @status.setter
    def status(self, status: str):
        """Set the Alation Job's Status.

        Args:
            status (str): Alation Job's Status,

        """
        self._status = status

    @property
    def completed(self) -> bool:
        """Return the Bool Value if Alation Job is Complete.

        Returns:
            bool: True or False if Alation Job is Complete.

        """
        return True if self.status.upper() in ['FAILED', 'SUCCESSFUL', 'SKIPPED'] else False

    @property
    def success(self) -> bool:
        """Return the Bool Value if Alation Job is Successful.

        Returns:
            bool: True or False if Alation Job is Successful.

        """
        return True if self.status.upper() == 'SUCCESSFUL' else False

    @property
    def message(self) -> str:
        """Return the Alation Job's Message describing the time taken to complete the job
        and the timestamp of job completion.

        Returns:
            str: Alation Job's Message.

        """
        return self._message

    @message.setter
    def message(self, message: str):
        """Set the Alation Job's Message describing the time taken to complete the job
        and the timestamp of job completion.

        Args:
            message (str): Alation Job's Message.

        """
        self._message = message

    @property
    def result(self) -> list:
        """Return the Alation Job Result describing the number of objects created and updated
        during the job.

        Returns:
            list: Alation Job's Result.

        """
        return self._result

    @result.setter
    def result(self, result: list):
        """Set the Alation Job Result describing the number of objects created and updated
        during the job.

        Args:
            result (list): Alation Job's Result.

        """
        self._result = result

    def load_from_api_response(self, api_response: dict):
        """Load the Object properties form Alation REST API Get Job Response.

        Args:
            api_response (dict): Alation REST API Get Job Response.

        """
        self.status = api_response.get('status')
        self.message = api_response.get('msg')
        self.result = api_response.get('result')

    def log_job(self):
        """Format the Log Messages of the Alation Job."""

        message = (
            f"Job: {self.id}, Status: {self.status}\n    "
            f"- Result: {self.result}")

        if not self.completed:
            LOGGER.debug(message)
            LOGGER.info(f"Job: {self.id}.... {self.status}")
        else:
            if self.success:
                LOGGER.info(message)
            else:
                LOGGER.error(message)
