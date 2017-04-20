"""
Generate TSV Fiels

"""

from __future__ import division
from __future__ import print_function


# Import OS Functions
import argparse
import datetime
import math
from openpyxl import Workbook
from openpyxl import load_workbook
import os
# import decimal
import re
# import textwrap
# import time
# import math

import numpy as np
import pandas as pd

import acc_lib as alib


# --------------------------------------------------------------------
#
#                          constants
#
# --------------------------------------------------------------------

CLUB_LIST = alib.CLUB_LIST
CLUB_TAGS = alib.CLUB_TAGS
OTHER_TAGS = alib.OTHER_TAGS

# Data
#         file_dict['club_file_short'] = file
#        file_dict['club_file_full'] = p_work_files['c'][counter]
#        file_dict['type'] = 'club'
#        file_dict['tag'] = 'cti numbers & scripts'
#        file_dict['club'] = 'ract'
#        file_dict['club_ss'] = open_ss(file_dict['club_file_full'])
#
#        file_dict['master_file_short'] = l_short_m_name
#        file_dict['master_file_full'] = l_m_file
#        file_dict['master_ss'] = open_ss(l_m_file)
# --------------------------------------------------------------------
#
#                          create dir
#
# --------------------------------------------------------------------

def vh_files(p_list):
    """ find hidden sheets """

    for f in p_list:
        print_filename = True

        short_name = '/'.join(f.split('/')[-5:])
        wb = load_workbook(filename = f.replace('/', '\\'))
        sheet_names = wb.get_sheet_names()

        for sheet in sheet_names:
            ws = wb[sheet]

            if ws.sheet_state != 'visible':

                if print_filename:
                    print_filename = False
                    alib.p_i('')
                    alib.p_i('Process file : {}'.format(short_name))

                alib.p_e('    sheet: {}, state: {}'.format(sheet, ws.sheet_state))

#        for s in sheet_ranges:
#            print('    sheet: {}'.format(s))

# --------------------------------------------------------------------
#
#                          create dir
#
# --------------------------------------------------------------------

def validate_hidden(p_files):
    """ find hidden sheets """

    vh_files(p_files['c'])
    vh_files(p_files['cc'])
    vh_files(p_files['m'])
    vh_files(p_files['mc'])

# --- Program Init
# --------------------------------------------------------------------
#
#                          initialise
#
# --------------------------------------------------------------------
#


def initialise():
    """
    Necessary initialisations for command line arguments
    """
    # Logfile for logging
    log_filename = alib.log_filename_init()
    if log_filename is None:
        print("\nError: Failed to initialise Log File Name. aborting\n")
        return alib.FAIL_GENERIC

    parser = argparse.ArgumentParser(description="""
     Example command lines:

    -d DEBUG --ss c:/tmp/x.xlsx
    --ss "C:/work/sample doc/Out of Service Codes.xlsx"
          """, formatter_class=argparse.RawTextHelpFormatter)

    # --- DB parameters ---
#    def_dir = 'C:/work/stuff/'
#    m_def_dir = 'master_OneDrive_2017-04-07/Master Validated Templates by Club (Controlled)'
#    mc_def_dir = 'master_common_OneDrive_2017-04-07/Master Validated Common Data Templates (Controlled)'

    parser.add_argument('--club_dir',
                        help='club files directory',
                        default=None,
                        required=False)

    parser.add_argument('--club_common_dir',
                        help='club common files directory',
                        default=None,
                        required=False)

    parser.add_argument('--m_dir',
                        help='master directory',
                        default=None,
                        required=False)

    parser.add_argument('--mc_dir',
                        help='master common directory',
                        default=None,
                        required=False)

    parser.add_argument('--all_dir',
                        help='directory containing all of the above',
                        default=None,
                        required=False)

    parser.add_argument('--quick_debug',
                        help='only load cc and mc files, used for debugging only',
                        default=None,
                        action='store_true',
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

    args, dummy_l_log_filename_s = initialise()

    # -- Initialise
    if not alib.init_app(args):
        return alib.FAIL_GENERIC

    work_dir = alib.load_dir(args)
    print('Loading files from {}'.format(work_dir['l_c_dir']))
    work_files = alib.load_files(work_dir, args['quick_debug'])

    #    work_dict = alib.load_matching_masterfile(work_files)
    #    alib.load_tags(work_dict)
    #    alib.print_filenames(work_dict)

    validate_hidden(work_files)

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
