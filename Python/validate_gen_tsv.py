"""
Generate TSV Fiels

"""

from __future__ import division
from __future__ import print_function


# Import OS Functions
import argparse
import os
# import decimal
import re
# import textwrap
# import time
# import math
import numpy as np
import pandas as pd

# Import racq library for RedShift

# import acc_lib as alib
# import racq_sendmail as rqmail

# import racq_syst_conn_lib as rqsconnlib

# --------------------------------------------------------------------
#
#                          constants
#
# --------------------------------------------------------------------

CLUB_LIST = alib.CLUB_LIST
CLUB_TAGS = alib.CLUB_TAGS
OTHER_TAGS = alib.OTHER_TAGS

# --------------------------------------------------------------------
#
#                          analyse shallow
#
# --------------------------------------------------------------------


def generate_tsv(p_work_dict, p_priority_df):
    """
    Perform some quick and dirty analsys on the files
    """
    # Loop through the tags, for each tag
    #     loop through all files. If the tag matches
    #         add result to new list (rep_list)
    l_filename = "c:/test_results/data/report_analyse_shallow.txt"
    with open(l_filename, "w+") as l_file_ptr:
        pass


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

    args, dummy_l_log_filename_s = initialise('validate_stage_3')

    # -- Initialise
    if not alib.init_app(args):
        return alib.FAIL_GENERIC

    home_dir = os.environ['USERPROFILE'].replace('\\', '/')
    priority_file = home_dir + '/OneDrive - AUSTRALIAN CLUB CONSORTIUM PTY LTD/work/git/'
    priority_file += 'TestRepo/Doc/priority_files.xlsx'
    priority_list_dict = alib.open_ss(priority_file)

    # pr iority_list_dict = alib.open_ss('C:/work/racq/ACCNational/TestRepo/Doc/priority_files.xlsx')
    priority_list_df = priority_list_dict['Sheet1']
    priority_list_df.columns = ['FUNCTION', 'NAME', 'EXEC_ORDER', 'LOAD_METHOD', 'HEX ETL']
    priority_list_df['NAME'] = priority_list_df['NAME'].str.lower()

    work_dir = alib.load_dir(args)
    work_files = alib.load_files(work_dir)

    work_dict = alib.load_matching_masterfile(work_files)

    alib.load_tags(work_dict)

    alib.print_filenames(work_dict)

    generate_tsv(work_dict, priority_list_df)

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
