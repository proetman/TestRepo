"""
Created on Thu Apr  6 16:53:06 2017

@author: PaulRoetman
"""

from __future__ import division
from __future__ import print_function


# Import OS Functions
import argparse
import os
# import decimal
# import re
# import textwrap
# import time
# import math
# import numpy as np
import pandas as pd

# Import racq library for RedShift

import acc_lib as alib
# import racq_sendmail as rqmail

# import racq_syst_conn_lib as rqsconnlib

# --------------------------------------------------------------------
#
#                          constants
#
# --------------------------------------------------------------------

# --------------------------------------------------------------------
#
#                          fetch columns for table
#
# --------------------------------------------------------------------


def fetch_columns(p_df, p_tab):
    """ fetch columns for this table """

    alib.log_debug('start fetch columns for table {}'.format(p_tab))

    tab_ind = p_df['TABLE'] == p_tab

    col_df = p_df[tab_ind]
    new_df = col_df.sort_values(['COL_ID'])
    new_df.reset_index(drop=True, inplace=True)
    return new_df

# --------------------------------------------------------------------
#
#                          create unique tab list
#
# --------------------------------------------------------------------


def create_unique_tab_list(p_df):
    """ extract list of tables, make unique """

    alib.log_debug('start create unique tab list')

    tab_s = p_df['TABLE']
    tab_a = tab_s.unique()

    return tab_a

# --------------------------------------------------------------------
#
#                          open csv
#
# --------------------------------------------------------------------


def open_csv(p_csv):
    """ open csv, save as df """
    alib.log_debug('start open csv')

    if p_csv is None:
        return None

    l_csv = p_csv.replace('\\', '/')

    try:
        csv_df = pd.read_csv(l_csv,
                             names=['schema', 'table', 'column', 'data_type', 'col_id', 'PK', 'MANDATORY'],
                             header=None,
                             index_col=False)

    except FileNotFoundError as err:
        alib.p_e('Function open_csv, spreadsheet not found.')
        alib.p_e('       CSV File: [{}]'.format(p_csv))
        alib.p_e('\n       error text [{}]'.format(err))
        csv_df = None

    except Exception as err:
        alib.p_e('\nGeneric exception in function open_csv.')
        alib.p_e('       CSV File: [{}]'.format(p_csv))
        alib.p_e('\n       error text [{}]'.format(err))
        csv_df = None

    if csv_df is not None:
        csv_df.columns = csv_df.columns.str.upper()

    return csv_df

# --------------------------------------------------------------------
#
#                          template load extract
#
# --------------------------------------------------------------------


def template_load_extract(p_dir):
    """ open template for load """
    alib.log_debug('start template load extract')

    extract_template_filename = 'bcp_extract_cmd.template'

    l_file = '{}/{}'.format(p_dir, extract_template_filename)

    alib.log_debug('    extract template file: [{}]'.format(l_file))

    data = None
    with open(l_file, 'r') as myfile:
        data = myfile.read()

    return data


# --- Program Init
# --------------------------------------------------------------------
#
#                          initialise
#
# --------------------------------------------------------------------
#


def initialise(p_filename):
    """
    Necessary initialisations for command line arguments
    """
    # Logfile for logging
    log_filename = alib.log_filename_init(p_filename)
    if log_filename is None:
        print("\nError: Failed to initialise Log File Name. aborting\n")
        return alib.FAIL_GENERIC

    parser = argparse.ArgumentParser(description="""
     Example command lines:

    -d DEBUG --ss c:/tmp/x.xlsx
    --ss "C:/work/sample doc/Out of Service Codes.xlsx"
          """, formatter_class=argparse.RawTextHelpFormatter)

    # --- DB parameters ---

    def_txt = 'C:/work/SqlServer/tab_col.csv'
    parser.add_argument('--csv',
                        help='Enter the CSV filename with all tab columns',
                        default=def_txt,
                        required=False)

    def_txt = 'C:/Users/PaulRoetman/OneDrive - AUSTRALIAN CLUB CONSORTIUM PTY LTD/work/git/TestRepo/templates'
    parser.add_argument('--templates',
                        help='Enter the directory for the BCP templates',
                        default=def_txt,
                        required=False)

    def_txt = 'c:/work/bcp_commands'
    parser.add_argument('--bcp_target_dir',
                        help='Enter the directory where the BCP files will be created',
                        default=def_txt,
                        required=False)

    # Add debug arguments
    parser.add_argument('-d', '--debug',
                        help='Log messages verbosity: NONE (least), DEBUG (most)',
                        choices=('NONE', 'CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'),
                        default="INFO",
                        required=False)

    # Sort though the arguments, ensure mandatory are populated
    args = alib.args_validate(parser, log_filename)

    return (args, log_filename)

# --------------------------------------------------------------------
#
#                          main
#
# --------------------------------------------------------------------


def main():
    """
    This program tests that the classification document, the Sql Server DB and Redshift DB
    all agree with each other
    """

    args, dummy_l_log_filename_s = initialise('gen_bcp')

    # -- Initialise
    if not alib.init_app(args):
        return alib.FAIL_GENERIC

    extract_template_s = template_load_extract(args['templates'])


    csv_df = open_csv(args['csv'])
    if csv_df is None:
        alib.p_e('No CSV data found, aborting')
        return alib.FAIL_GENERIC

    tab_a = create_unique_tab_list(csv_df)

    for tab in tab_a:
        cols_df = fetch_columns(csv_df, tab)
        bcp_extract = bcp_create_extract(extract_template_s, tab, cols_df)

    alib.p_i('Done...')

# --------------------------------------------------------------------
#
#                          the end
#
# --------------------------------------------------------------------

if __name__ == "__main__":
    if main() == alib.SUCCESS:
        exit
    else:
        exit(-1)

# --- eof ---
