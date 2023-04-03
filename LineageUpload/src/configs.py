"""Custom Class to work with Script Configurations."""

import configparser
import os
from cryptography.fernet import Fernet, InvalidToken


class ConfigEncryption(object):
    """Custom class to Encrypt and Decrypt Script Configurations."""

    def __init__(self, key_location: str = None):
        """Create an instance of ConfigEncryption.

        Args:
            key_location (str): Path to the Fernet Encryption key.

        """
        if key_location:
            self.key_location = key_location
        else:
            self.key_location = 'configs/.secrets/.configs.key'

        try:
            file_size = os.path.getsize(self.key_location)
        except OSError:
            file_size = 0

        if file_size == 0:
            self._generate_key()

        self.key = self._load_key()

    def encrypt_string(self, plaintext_string: str) -> str:
        """Encrypt a plaintext string and return encoded encrypted bytes value.

        Args:
            plaintext_string (str): String to be encrypted.

        Returns:
            str: Encrypted string value.

        """
        f = Fernet(self.key)
        encoded_string = plaintext_string.encode()

        return f.encrypt(encoded_string).decode()

    def decrypt_string(self, encrypted_value: str) -> str:
        """Decrypt an encoded bytes value and return plaintext string.

        Args:
            encrypted_value (str): Encoded encrypted bytes value.

        Returns:
            str: Decoded Plaintext String.

        """
        f = Fernet(self.key)

        try:
            decrypted_string = f.decrypt(encrypted_value.encode())
            return decrypted_string.decode()
        except InvalidToken:
            return encrypted_value

    def _generate_key(self):
        """Generate an Encryption Key and save it into a file."""

        key = Fernet.generate_key()

        with open(self.key_location, "wb") as key_file:
            key_file.write(key)

    # noinspection SpellCheckingInspection
    def _load_key(self) -> str:
        """Load the Encryption Key from the saved file.

        Returns:
            str: Fernet encryption key.

        """
        with open(self.key_location, 'r') as key_file:
            return key_file.read()


class ParseConfigs(ConfigEncryption):
    """Custom to class to create Script Configurations dictionary."""

    def __init__(self, key_location: str = None):
        """Create an instance of ParseConfigs.

        Args:
            key_location (str): Path to the Fernet Encryption key.
        """
        super().__init__(key_location=key_location)

    def generate_configs(self, file_location: str) -> dict:
        """Generate the Script Configuration Dictionary.

        Args:
            file_location (str): Path to the Configuration ini file.

        Returns:
            dict: Script Configurations.

        """
        configs = configparser.ConfigParser()
        configs.read(file_location)

        return {'alation_username': self.decrypt_string(configs['Alation']['username']),
                'alation_user_id': configs['Alation']['user_id'],
                'alation_password': self.decrypt_string(configs['Alation']['password']),
                'alation_refresh_token_name': self.decrypt_string(configs['Alation']['refresh_token_name']),
                'alation_refresh_token_location': configs['Alation']['refresh_token_location'],
                'alation_enable_ssl': configs['Alation'].getboolean('https'),
                'alation_ssl_cert': self._return_none_if_blank(configs['Alation']['ssl_cert']),
                'features_download_xml_request_size': int(configs['Features']['download_xml_request_size']),
                'features_download_json_request_size': int(configs['Features']['download_json_request_size']),
                'features_upload_request_size': int(configs['Features']['upload_request_size']),
                'features_job_status_sleep': int(configs['Features']['job_status_sleep']),
                'features_log_retention_period': int(configs['Features']['log_retention_period'])}

    def _return_none_if_blank(self, config_value: str) -> str:
        """Helper function to format the Configuration Dictionary. If string value is empty
        the function will return None.

        Args:
            config_value (str): Configuration Value to be formatted.

        Returns:
            str: Formatted configuration value.

        """
        return self.decrypt_string(config_value) if config_value != '' else None
