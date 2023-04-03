"""Alation API Authentication Model."""

import os.path

from ...configs import ConfigEncryption


class AlationAuth(object):
    """Alation REST API Authentication."""

    def __init__(self, refresh_response: dict = None, access_response: dict = None,
                 key_location: str = None):
        """Create an instance of the AlationAuth Object.

        Args:
            refresh_response (dict): Alation API Refresh Token Response Data.
            access_response (dict): Alation API Access Token Response Data.
            key_location (str): Path to the Fernet Encryption key.

        """
        self._refresh_token = None
        self._user_id = None
        self._access_token = None
        self._refresh_token_status = None
        self._access_token_status = None
        self.key_location = key_location

        self.status_values = ['ACTIVE', 'EXPIRED', 'REVOKED']

        if refresh_response:
            self.load_from_refresh_response(refresh_response)

        if access_response:
            self.load_from_access_response(access_response)

    @property
    def refresh_token(self) -> str:
        """Return the Alation API Refresh Token.

        Returns:
            str: Alation API Refresh Token.

        """
        return self._refresh_token

    @refresh_token.setter
    def refresh_token(self, token: str):
        """Set the Alation API Refresh Token.

        Args:
            token (str): Alation API Refresh Token.

        """
        self._refresh_token = token

    @property
    def user_id(self) -> int:
        """Return the User ID assigned to the Alation API Refresh Token.

        Returns:
            int: User ID assigned to the Alation API Refresh Token.

        """
        return self._user_id

    @user_id.setter
    def user_id(self, user_id: int):
        """Set the User ID assigned to the Alation API Refresh Token.

        Args:
            user_id (int): User ID assigned to the Alation API Refresh Token.

        """
        self._user_id = user_id

    @property
    def access_token(self) -> str:
        """Return the Alation API Access Token.

        Returns:
            str: Alation API Access Token.

        """
        return self._access_token

    @access_token.setter
    def access_token(self, token: str):
        """Set the Alation API Access Token.

        Args:
            token (str): Alation API Access Token.

        """
        self._access_token = token

    @property
    def refresh_token_valid(self) -> bool:
        """Return the Status of the Alation API Refresh Token.

        Returns:
            bool: Alation API Refresh Token Status.

        """
        return True if self._refresh_token_status == 'ACTIVE' else False

    @refresh_token_valid.setter
    def refresh_token_valid(self, status: str):
        """Set the Status of the Alation API Refresh Token.

        Args:
            status (str): Alation API Refresh Token Status.

        """
        if status.upper() not in self.status_values:
            raise ValueError(
                f"'{status}' is not a supported API Refresh Token Status Value.")

        self._refresh_token_status = status.upper()

    @property
    def access_token_valid(self) -> bool:
        """Return the Status of the Alation API Access Token.

        Returns:
            bool: Alation API Access Token Status.

        """
        return True if self._access_token_status == 'ACTIVE' else False

    @access_token_valid.setter
    def access_token_valid(self, status: str):
        """Set the Status of the Alation API Access Token.

        Args:
            status (str): Alation API Access Token Status.

        """
        if status.upper() not in self.status_values:
            raise ValueError(
                f"'{status}' is not a supported API Access Token Status Value.")

        self._access_token_status = status.upper()

    def load_from_refresh_response(self, api_response: dict):
        """Load the Object Properties from Alation API Refresh Token Response Data.

        Args:
            api_response (dict): Alation API Refresh Token Response Data

        """
        self.refresh_token = api_response.get('refresh_token')
        self.user_id = api_response.get('user_id')
        self.refresh_token_valid = api_response.get('token_status')

    def load_from_access_response(self, api_response: dict):
        """Load the Object Properties from Alation API Access Token Response Data.

        Args:
            api_response (dict): Alation API Access Token Response Data

        """
        self.access_token = api_response.get('api_access_token')
        self.access_token_valid = api_response.get('token_status')

    def store_refresh_token(self, file_location: str):
        """Store an encrypted Alation API Refresh token value in a txt file.

        Args:
            file_location (str): Path to the file storing the encrypted Refresh token.

        """
        with open(file_location, 'w') as file:
            file.write(ConfigEncryption(key_location=self.key_location).encrypt_string(
                self.refresh_token))

    def load_refresh_token(self, file_location: str):
        """Load the encrypted Alation API Refresh token value in a txt file.

        Args:
            file_location (str): Path to the file storing the encrypted Refresh token.

        """
        try:
            file_size = os.path.getsize(file_location)
        except OSError:
            file_size = 0

        if file_size != 0:

            with open(file_location, 'r') as file:
                self.refresh_token = ConfigEncryption(self.key_location).decrypt_string(
                    file.read())
