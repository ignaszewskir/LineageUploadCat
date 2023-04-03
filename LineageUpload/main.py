# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import argparse
from src.configs import ParseConfigs
import pandas as pd
import logging

from src.alation_helpers import AlationHelpers
from src.models.alation.auth import AlationAuth

LOGGER = logging.getLogger()

alation_helper = None

def main():

    """Main function."""

    # Capture command line arguments
    parser = argparse.ArgumentParser(description='Alation Custom Lineage Updater.')
    parser.add_argument('--configs', '-c', required=False,
                        help='Path to the Environment Config File')
#    parser.add_argument('--fields', '-f', required=False, default=True,
#                        type=strtobool, help='Catalog Mstr Fields in Alation')
    parser.add_argument('--input_source', '-i', required=True, default=True,
                        help='Caterpillar mapping document file location.')

    args = parser.parse_args()
    config_helper = ParseConfigs()
    #Add logging
    #log_helper = LogHelper()
    #log_helper.log_script_start()

    if args.configs:
        configs = config_helper.generate_configs(args.configs)
    else:
        configs = config_helper.generate_configs('configs/configs.ini')

    #if args.input_source:
    try:
        pd_df_mapfile_in = pd.read_excel(args.input_source, engine="openpyxl")
        print(pd_df_mapfile_in)
        print ('Yes')
        #pd_df_mapfile_in.

        LOGGER.log_header('REST API Authentication')
        #connector.mstr_df_pd = pd_df_mstr_in
        alation_helper = AlationHelpers(configs)
        alation_auth = alation_helper.alation_authentication()

        process_input_file(pd_df_mapfile_in, alation_auth)


    except Exception as main_error:
        LOGGER.error(main_error, exc_info=True)

    finally:
        LOGGER.log_header('Script Cleanup')
        # connector.tableau_sign_out(connector.ts_auth)
        #LOGGER.info('Rotating old Log Files')
        #LogRotater.rotate_logs(configs['features_log_retention_period'])
        LOGGER.info('Done!')

def process_input_file(pd_df_in: pd.DataFrame, alation_auth: AlationAuth):



    #filter the input file?


    alation_auth

    pass

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
