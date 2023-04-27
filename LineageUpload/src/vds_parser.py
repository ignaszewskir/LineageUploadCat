# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import argparse

import pandas

from src.configs import ParseConfigs
import pandas as pd
import logging.config
import requests

from src.alation_helpers import AlationHelpers
from src.models.alation.auth import AlationAuth

from src.logs import LoggingConfigs, LogHelper, LogRotater

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

logging.config.dictConfig(LoggingConfigs.logging_configs())

LOGGER = logging.getLogger()

alation_helper = None

class VDSParser(object):

    def __init__(self, configs: dict):
        """Create an instance of VDSParse

        Args:
            configs (dict): Script Environment Configurations.

        """
        self._output_filename = "logs/output.csv"

    def parse_and_create_target(self, pd_df_mapfile_in: pandas.DataFrame):

        pd_df_target_fd = pd_df_mapfile_in.drop_duplicates(subset="Target Field Name").fillna('')

        rows = []
        for index, row in pd_df_target_fd.iterrows():
            rows.append([f'schema_name.{row.get("Target Database / Table Name")}.{row.get("Target Field Name")}',
                         '',
                         f'{row.get("Data Type Conformity")}({row.get("Data Length")})',
                         '',
                         '',
                         str(row.get("Nullable")).lower(),
                         ])

        pd_out_csv = pd.DataFrame(rows, columns=['key', 'table_type', 'column_type', 'index_type', 'columns_name',
                                                 'nullable'])
        pd_out_csv.to_csv(self.output_filename, sep=",", index=False)

    def parse_and_create_source(self, pd_df_mapfile_in: pandas.DataFrame):

        pd_df_target_fd = pd_df_mapfile_in.drop_duplicates(subset="Source Field Name").fillna('')

        rows = []
        for index, row in pd_df_target_fd.iterrows():
            rows.append([f'schema_name.{row.get("Object/Source Table Name")}.{row.get("Source Field Name")}',
                         '',
                         f'{row.get("Source Data Type")}({row.get("Source Field Length")})',
                         '',
                         '',
                         "",
                         ])


        pd_out_csv = pd.DataFrame(rows, columns=['key', 'table_type', 'column_type', 'index_type', 'columns_name', 'nullable'])
        pd_out_csv.to_csv(self.output_filename, sep=",", index=False)

    @property
    def output_filename(self) -> str:
        """Return the Report's Alation Unique ID.

        Returns:
            str: Report's Unique ID.

        """
        return self._output_filename

    @output_filename.setter
    def output_filename(self, output_filename: str):
        """Set the Report's Alation Unique ID .

        Args:
            id (str): Report's Alation Unique ID.

        """
        self._output_filename = output_filename

