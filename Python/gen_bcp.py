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
#                          initialise
#
# --------------------------------------------------------------------


def open_csv(p_csv):
    """ open spreadhseet, save as df """

    if p_csv is None:
        return None

    l_csv = p_csv.replace('\\', '/')

    try:
        csv_df_dict = pd.read_excel(l_csv,
                                   sheetname=None,
                                   index_col=None)

    except FileNotFoundError as err:
        alib.p_e('Function open_ss, spreadsheet not found.')
        alib.p_e('       speadsheet: [{}]'.format(p_ss))
        alib.p_e('\n       error text [{}]'.format(err))
        ss_df_dict = None

    except Exception as err:
        alib.p_e('\nGeneric exception in function open_ss.')
        alib.p_e('       spreadsheet: [{}]'.format(p_ss))
        alib.p_e('\n       error text [{}]'.format(err))
        ss_df_dict = None

    if ss_df_dict is not None:
        for tab in ss_df_dict:
            l_df = ss_df_dict[tab]
            if len(l_df.columns) > 0:
                l_df.columns = l_df.columns.str.upper()

    return ss_df_dict


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

    parser.add_argument('--csv',
                        help='Enter the CSV filename with all tab columns',
                        required=True)

    def_txt = 'C:/Users/PaulRoetman/OneDrive - AUSTRALIAN CLUB CONSORTIUM PTY LTD/work/git/TestRepo/templates'
    parser.add_argument('--templates',
                        help='Enter the directory for the BCP templates',
                        default=def_txt,
                        required=True)

    def_txt = 'c:/work/bcp_commands'
    parser.add_argument('--templates',
                        help='Enter the directory for the BCP templates',
                        default=def_txt,
                        required=True)

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

    args, dummy_l_log_filename_s = initialise('validate_ss')

    # -- Initialise
    if not alib.init_app(args):
        return alib.FAIL_GENERIC

    csv_df = open_csv(args['csv'])

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
