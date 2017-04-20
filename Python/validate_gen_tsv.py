"""
Generate TSV Fiels

"""

from __future__ import division
from __future__ import print_function


# Import OS Functions
import argparse
import datetime
import math
import os
# import decimal
import re
# import textwrap
# import time
# import math

# import numpy as np
# import pandas as pd

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
#                          tsv - validate
#
# --------------------------------------------------------------------

def tsvg_validate(p_file):
    """ Verify count of tabs on every line in the file is the same """
    with open(p_file, "r") as l_file_ptr:
        content = l_file_ptr.readlines()

    tab_count = None
    line_count = 0
    for line in content:
        line_count += 1
        new_tab_count = line.count('\t')
        if tab_count is None:
            tab_count = new_tab_count
        else:
            if tab_count != new_tab_count:
                alib.p_e('file: {} incorrect tab count lineno {}; prev {}, curr {}'.
                         format(p_file, line_count, tab_count, new_tab_count))
                tab_count = new_tab_count


# --------------------------------------------------------------------
#
#                          tsv - fetch target filename
#
# --------------------------------------------------------------------


def tsvg_fetch_target_name(p_tsv_dir, p_curr_file, p_tab):
    """
    calculate the target filename
    """
    alib.log_debug('Start tsvg fetch target name')
    l_filename = p_tsv_dir
    l_filename += '/'

    # remove .xlsx
    l_filename += ''.join(p_curr_file.split('.')[:-1])

    full_path = os.path.dirname(l_filename)
    alib.dir_create(full_path)

    l_tab = p_tab.replace('.', '__').upper()
    l_tab = l_tab.replace(' ', '_').upper()
    l_tab = l_tab.replace('"', '_').upper()
    l_tab = l_tab.replace("'", '_').upper()

    l_filename += '__' + l_tab + '.tsv'

    alib.log_debug('End tsvg fetch target name, filename = {}'.format(l_filename))

    return l_filename

# ------------------------------------------------------------------------------------------
#
#                                       PRE PROCESS source
#
# ------------------------------------------------------------------------------------------


def tgsv_pre_process(p_df):
    """
    Remove non-ascii characters from any column that is of type str

    Parameters
    ----------
    p_df: panda dataframe
         data frame

    Returns
    -------
    res: boolean
      success or fail

    Example
    -------
    >>> pre_process(my_src_df)
    True

    """
    def clean_text(p_field):
        """
        Lambda function to replace characters that bryte cannot tolerate
        """
        dodgy_char = '’'
        dodgy_char2 = '‘'
        dodgy_char3 = '–'
        dodgy_char4 = '”'
        dodgy_char5 = '“'
        dodgy_char6 = '…'
        dodgy_char7 = '\r'
        dodgy_char8 = '\n'
        dodgy_char10 = '\t'
        dodgy_char12 = '–'
        dodgy_char15 = '‐'      # This is a different hyphon

        dodgy_char20 = '\xa0'   # this is some kind of weird space, but is non ascii. Every field is terminated with it.
        dodgy_char21 = '·'      # another bizarre hyphon
        dodgy_char22 = '│'
        dodgy_char23 = '√'

        if(p_field is None or
           isinstance(p_field, datetime.date) or
           not isinstance(p_field, str)):
            new_field = p_field
        else:

            new_field = re.sub(dodgy_char, "'", p_field)
            new_field = re.sub(dodgy_char2, "'", new_field)
            new_field = re.sub(dodgy_char3, "-", new_field)
            new_field = re.sub(dodgy_char4, '"', new_field)
            new_field = re.sub(dodgy_char5, '"', new_field)
            new_field = re.sub(dodgy_char6, '.', new_field)
            new_field = re.sub(dodgy_char7, ' ', new_field)
            new_field = re.sub(dodgy_char8, ' ', new_field)
            new_field = re.sub(dodgy_char10, ' ', new_field)
            new_field = re.sub(dodgy_char12, '-', new_field)
            new_field = re.sub(dodgy_char15, '-', new_field)

            new_field = re.sub(dodgy_char20, ' ', new_field)
            new_field = re.sub(dodgy_char21, '-', new_field)
            new_field = re.sub(dodgy_char22, '|', new_field)
            new_field = re.sub(dodgy_char23, ' ', new_field)

            tmp_field = re.sub('[^ -~]', 'X', new_field)

            if tmp_field != new_field:
                alib.p_e('ERROR:  FOUND NON ASCII CHAR - PANIC')
                alib.p_e('        orig data = [{}]'.format(new_field))
                alib.p_e('        mod  data = [{}]'.format(tmp_field))

            # remove trailing spaces
            new_field = re.sub(' *$', '', new_field)

        return new_field

    # start_str = 'Start pre_process size of p_df = [{}]'
    # rqlib.log_debug(start_str.format(len(p_df.index)))
    str_col = p_df.select_dtypes(include=['object'])
    for col in str_col.columns:
        p_df[col] = p_df[col].apply(lambda x: clean_text(x))

    return


# --------------------------------------------------------------------
#
#                          analyse shallow
#
# --------------------------------------------------------------------


def tsvg_validate_col_headings(p_df):
    """
    validate col heading is not Nan
    """
    new_col = 'column_heading_'
    new_col_ctr = 1
    col_headings = p_df.columns
    new_col_headings = []

    for col in col_headings:
        if isinstance(col, str):
            col = col.replace('\r', ' ')
            col = col.replace('\n', ' ')
            new_col_headings.append(col)
        else:
            if math.isnan(col):
                new_col_headings.append('{}{}'.format(new_col, new_col_ctr))
                new_col_ctr += 1

    alib.log_debug('Col Headings : {}'.format(col_headings))
    alib.log_debug('Mod Headings : {}'.format(new_col_headings))
    p_df.columns = new_col_headings

# --------------------------------------------------------------------
#
#                          analyse shallow
#
# --------------------------------------------------------------------


def tsv_generate(p_list, p_tsv_dir):
    """
    Perform some quick and dirty analsys on the files
    """
    alib.log_debug('start tsv generate')

    for f in p_list:

        if '/common data templates/' in f:
            short_name = '/'.join(f.split('/')[-3:])
        else:
            short_name = '/'.join(f.split('/')[-4:])
        print(short_name)

        l_ss = alib.open_ss(f)

        if l_ss is None:
            alib.p_e('Failed to open file {}'.format(f))
            continue

        alib.cleanup_ss(l_ss)

        for key, value in l_ss.items():
            # Now have a single tab from the spreadsheet as a dataframe
            l_tab_df = value
            l_tab_key = key

            # if this TAB has any data on it
            if len(l_tab_df.index) > 1:

                l_target_filename = tsvg_fetch_target_name(p_tsv_dir, short_name, l_tab_key)
                tsvg_validate_col_headings(l_tab_df)

                alib.log_debug('    Write to file {}'.format(l_target_filename))
                alib.log_debug('    number of lines {}'.format(len(l_tab_df.index)))

                # remove dodgy char
                tgsv_pre_process(l_tab_df)

                # write to disk
                l_tab_df.to_csv(l_target_filename,
                                sep='\t',
                                index=False,
                                date_format='%d-%m-%Y  %H:%M:%S')

                tsvg_validate(l_target_filename)

    alib.log_debug('end tsv generate')

# --------------------------------------------------------------------
#
#                          loop through files
#
# --------------------------------------------------------------------


def process_files(p_files, p_tsv_dir):
    """
    Perform some quick and dirty analsys on the files
    """

    tsv_generate(p_files['c'], p_tsv_dir)
    tsv_generate(p_files['cc'], p_tsv_dir)

    return

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

    args, dummy_l_log_filename_s = initialise('validate_gen_tsv')

    # -- Initialise
    if not alib.init_app(args):
        return alib.FAIL_GENERIC

    tsv_dir = alib.tsv_create_dir()
    if tsv_dir is None:
        alib.p_e('no tsv dir, aborting')
        return alib.FAIL_GENERIC

    work_dir = alib.load_dir(args)
    print('Loading files from {}'.format(work_dir['l_c_dir']))
    work_files = alib.load_files(work_dir, args['quick_debug'])

    #    work_dict = alib.load_matching_masterfile(work_files)
    #    alib.load_tags(work_dict)
    #    alib.print_filenames(work_dict)

    process_files(work_files, tsv_dir)

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
