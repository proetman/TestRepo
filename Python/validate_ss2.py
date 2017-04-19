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
# import pandas as pd

# Import racq library for RedShift

import acc_lib as alib
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

# --- compare dict
# --------------------------------------------------------------------
#
#                          compare columns
#
# --------------------------------------------------------------------


def cd_cols_validate(p_tab, p_ss_tab_cols, p_m_tab_cols):
    """ validate that the columns for each common tab are the same. """
    len_s = len(p_ss_tab_cols)
    len_m = len(p_m_tab_cols)

    if len_s == 0 and len_m == 0:
        alib.log_debug('both m and s have 0 columns')
        return

    if len_s < 1:
        alib.p_e('    on tab "{}", Club has no columns, but Master does'.format(p_tab))
        return

    if len_m < 1:
        alib.p_e('    on tab "{}", Master has no columns, but Club does'.format(p_tab))
        return

    l_ss_tab_cols = p_ss_tab_cols.str.upper()
    l_m_tab_cols = p_m_tab_cols.str.upper()

    for col in l_ss_tab_cols:
        if col not in l_m_tab_cols:
            alib.p_e('    on tab "{}", column [{}] in club, not in master'.format(p_tab, col))

    for col in l_m_tab_cols:
        if col not in l_ss_tab_cols:
            alib.p_e('    on tab "{}", column [{}] in master, not in club'.format(p_tab, col))

# --------------------------------------------------------------------
#
#                          compare columns
#
# --------------------------------------------------------------------


def cd_cols(p_ss_dict, p_m_dict):
    """ validate that the columns for each common tab are the same. """

    alib.log_debug('cd cols')
    l_ss_keys = p_ss_dict.keys()
    l_m_keys = p_m_dict.keys()

    for tab in l_m_keys:
        if tab in l_ss_keys:
            ss_df = p_ss_dict[tab]
            m_df = p_m_dict[tab]

            ss_tab_cols = ss_df.columns
            m_tab_cols = m_df.columns
            cd_cols_validate(tab, ss_tab_cols, m_tab_cols)

# --------------------------------------------------------------------
#
#                          compare columns
#
# --------------------------------------------------------------------


def cd_rowcount_compare(p_tab, p_ss_df, p_m_df):
    """ compare the rowcount for each common tab. """
    more_txt = '        ROWCOUNT: tab "{:30s}" has MORE data in club ({}) than master ({}), diff [{}]'
    less_txt = '        ROWCOUNT: tab "{:30s}" has LESS data in club ({}) than master ({}) diff [{}]'
    eq_txt = '        ROWCOUNT: tab "{:30s}" has same data in club ({}) than master ({})'

    len_ss = len(p_ss_df.index)
    len_m = len(p_m_df.index)

    retval = False

    if len_ss < len_m:
        alib.p_i(less_txt.format(p_tab, len_ss, len_m, len_ss-len_m))
        retval = True
    elif len_ss == len_m:
        alib.p_i(eq_txt.format(p_tab, len_ss, len_m))
        retval = False
    else:
        alib.p_i(more_txt.format(p_tab, len_ss, len_m, len_ss-len_m))
        retval = True

    return retval

# --------------------------------------------------------------------
#
#                          compare columns
#
# --------------------------------------------------------------------


def cd_rowcount(p_file, p_ss_dict, p_m_dict, p_totres):
    """ compare the rowcount for each common tab. """

    alib.log_debug('cd rowcount')
    perc_txt = '            Percentages: file: "{vF:60s}", tabs modified {vM:3.2f}%,'
    perc_txt += ' unchanged {vU:3.2f}%, count [{vC}], '
    l_ss_keys = p_ss_dict.keys()
    l_m_keys = sorted(p_m_dict.keys())

    modified_tab = 0
    unmod_tab = 0
    count = 0

    for tab in l_m_keys:
        if tab in l_ss_keys:
            ss_df = p_ss_dict[tab]
            m_df = p_m_dict[tab]

            count += 1
            diff_flag = cd_rowcount_compare(tab, ss_df, m_df)
            if diff_flag:
                modified_tab += 1
            else:
                unmod_tab += 1

    p_totres['unchanged'] += unmod_tab
    p_totres['modified'] += modified_tab
    p_totres['filecount'] += 1

    tot = modified_tab + unmod_tab
    mod = 0
    unmod = 0
    if tot > 0:
        mod = 100 * (modified_tab/tot)
        unmod = 100 * (unmod_tab/tot)

    alib.p_i(perc_txt.format(vM=mod, vU=unmod, vC=count, vF=p_file))

    return

# --------------------------------------------------------------------
#
#                          compare tabs
#
# --------------------------------------------------------------------


def cd_tabs(p_ss_dict, p_m_dict):
    """ make sure that the tabs for each spreadsheet are the same """

    alib.log_debug('cd tab')
    l_ss_keys = p_ss_dict.keys()
    l_m_keys = p_m_dict.keys()

    for tab in l_ss_keys:
        if tab not in l_m_keys:
            alib.p_e('    tab in club, not in master: "{}"'.format(tab))

    for tab in l_m_keys:
        if tab not in l_ss_keys:
            alib.p_e('    tab in master, not in club: "{}"'.format(tab))

# --------------------------------------------------------------------
#
#                          compare club t master
#
# --------------------------------------------------------------------


def compare_dict(p_file, p_ss_dict, p_m_dict, p_totres):
    """ compare master spreadsheet to club """

    alib.log_debug('compare dict')
    l_ss_keys = p_ss_dict.keys()
    l_m_keys = p_m_dict.keys()

    if l_ss_keys != l_m_keys:
        cd_tabs(p_ss_dict, p_m_dict)

    cd_cols(p_ss_dict, p_m_dict)
    cd_rowcount(p_file, p_ss_dict, p_m_dict, p_totres)
# --- Val Special Char
# --------------------------------------------------------------------
#
#                          validate special char
#
# --------------------------------------------------------------------


def validate_special_char(p_ss_dict):
    """ Check there are no "tab" characters in data."""

    # Have a spreadsheet
    for key, value in p_ss_dict.items():
        # Now have a single tab from the spreadsheet as a dataframe
        l_tab_df = value
        if len(l_tab_df.index) > 1:
            row_cnt = 0
            for dummy_j, l_row in l_tab_df.iterrows():
                row_cnt += 1
                # Now have a single row
                col_cnt = 0
                for col in l_row:
                    col_cnt += 1
                    if col is not None and isinstance(col, str):
                        # print('row {}, column {}'.format(row_cnt, col_cnt))
                        if '\t' in col:
                            alib.p_e('TAB Character found row {}, column {}'.format(row_cnt, col_cnt))
        elif len(l_tab_df.index) == 1:
            col_cnt = 0
            for col in l_tab_df:
                col_cnt += 1
                if col is not None and isinstance(col, str):
                    if '\t' in col:
                        alib.p_e('TAB Character found column {}'.format(col_cnt))
            # process a series
        else:
            continue

# --- Val Multi File

# --------------------------------------------------------------------
#
#                          replace club name with "CLUBNAME"
#
# --------------------------------------------------------------------


def vmfg_replace_club(p_str):
    l_str = p_str
    for c in CLUB_LIST:
        l_str = [f.replace(c.upper(), 'CLUBNAME') for f in l_str]
    return l_str

# --------------------------------------------------------------------
#
#                          validate mfg
#
# --------------------------------------------------------------------


def vmfg_report(p_tag, p_rep_list):
    """ Validate the groups of files """

    alib.log_debug('Start val multi file group {}'.format(p_tag))

    err_txt = '    FILE 1 "{vM}" is different to FILE 2 "{vC}"'
    err_txt2 = '        file1 tabs: [{vT}]'
    err_txt3 = '        file2 tabs: [{vT}]'

    for club in CLUB_LIST:
        if club not in p_rep_list:
            alib.p_e('Club file not found for file group "{}"'.format(p_tag))

    prev_keys = None

    for key, value in p_rep_list.items():
        if 'master_ss' in value:
            l_m_ss = value['master_ss']
            l_m_keys = l_m_ss.keys()
            l_new_keys = vmfg_replace_club(l_m_keys)
            l_new_keys.sort()

        l_name = value['club_file_short']
        l_c_ss = value['club_ss']
        l_ss_keys = l_c_ss.keys()
        l_new_keys = vmfg_replace_club(l_ss_keys)
        l_new_keys.sort()

        if prev_keys is None:
            prev_keys = l_new_keys
            prev_name = l_name
        else:
            if l_new_keys != prev_keys:
                alib.p_e('')
                alib.p_e(err_txt.format(vM=prev_name, vC=l_name))
                alib.p_e(err_txt2.format(vT=list(prev_keys)))
                alib.p_e(err_txt3.format(vT=list(l_new_keys)))

                for tab in l_new_keys:
                    if tab not in prev_keys:
                        alib.p_e('    tab in file2, not in file1: "{}"'.format(tab))

                for tab in prev_keys:
                    if tab not in l_new_keys:
                        alib.p_e('    tab in file1, not in file2: "{}"'.format(tab))
                alib.p_i('')

    return
# --------------------------------------------------------------------
#
#                          validate multi file groups
#
# --------------------------------------------------------------------


def validate_multi_file_groups(p_work_dict):
    """ Validate the groups of files """

    for l_tag in CLUB_TAGS:
        rep_list = {}
        for key, value in p_work_dict.items():
            if value['tag'] == l_tag:
                club = value['club']
                rep_list[club] = value

        vmfg_report(l_tag, rep_list)

    for l_tag in OTHER_TAGS:
        rep_list = {}
        for key, value in p_work_dict.items():
            if value['tag'] == l_tag:
                club = value['club']
                rep_list[club] = value

        vmfg_report(l_tag, rep_list)

    return

# --- val club file
# --------------------------------------------------------------------
#
#                          cleanup
#
# --------------------------------------------------------------------


def vcf_cleanup_ss(p_dict):
    """ cleanup tabs not comparing """
    alib.log_debug('start cleanup')

    if p_dict is None:
        alib.log_debug('    empty dict, returning none')
        return

    for tab in ('Version History', 'Configuration', 'Configuration Screens'):
        if tab in p_dict:
            del p_dict[tab]

    return

# --------------------------------------------------------------------
#
#                          validate club
#
# --------------------------------------------------------------------


def validate_club_file(p_work_dict):
    """ Validate each of the club files """

    alib.p_i('-------------------------', p_before=1)
    alib.p_i('Validate CLUB files')
    alib.p_i('-------------------------')

    totres = {}
    totres['unchanged'] = 0
    totres['modified'] = 0
    totres['filecount'] = 0
    totres['filemiss'] = 0

    for key, value in p_work_dict.items():

        l_club_filename = value['club_file_full']
        l_mast_filename = value['master_file_full']
        l_name = value['club_file_short']

        ss_df = alib.open_ss(l_club_filename)
        if ss_df is None:
            alib.p_e('    Unable to open club spreadsheet, please review previous errors raised')
            totres['filemiss'] += 1
            continue

        m_df = alib.open_ss(l_mast_filename)
        if m_df is None:
            alib.p_e('    Unable to open master spreadsheet, please review previous errors raised')
            totres['filemiss'] += 1
            continue

        # Tidy up dict.
        vcf_cleanup_ss(ss_df)
        vcf_cleanup_ss(m_df)

        if ss_df is not None:
            validate_special_char(ss_df)

        if ss_df is not None and m_df is not None:
            compare_dict(l_name, ss_df, m_df, totres)
        else:
            totres['filemiss'] += 1


    perc_txt = '            Overall Percentages: tabs modified {vM:3.2f}% ({vMC}), unchanged {vU:3.2f}%,'
    perc_txt += ' total tab count [{vC}] '

    totunmod = totres['unchanged']
    totmod = totres['modified']

    tot = totmod + totunmod
    mod = 0
    unmod = 0
    if tot > 0:
        mod = 100 * (totmod/tot)
        unmod = 100 * (totunmod/tot)

    alib.p_i(perc_txt.format(vM=mod, vU=unmod, vC=tot, vMC=totmod))
    alib.p_i('            Overall Percentages: Files processed: {}, files skipped {}'.format(totres['filecount'],
                                                                                             totres['filemiss']))
    return

# --- val master file
# --------------------------------------------------------------------
#
#                          validate master files
#
# --------------------------------------------------------------------


def vmf_file_in_dict(p_work_dict, p_m_file):
    """ verify master file is in work dict """

    alib.log_debug('validate master file in dict : {}'.format(p_m_file))

    retval = False
    for key, value in p_work_dict.items():
        l_m_file = value['master_file_short']
        if p_m_file == l_m_file:
            retval = True
            break

    alib.log_debug('return retval {}'.format(retval))
    return retval

# --------------------------------------------------------------------
#
#                          validate master files
#
# --------------------------------------------------------------------


def validate_master_files(p_work_files, p_work_dict):
    """ verify every master file is in work dict """

    l_m_files = p_work_files['m']
    l_mc_files = p_work_files['mc']

    for l_file in l_m_files:
        l_short_m_name = l_file.split('cd4/')[1]
        if not vmf_file_in_dict(p_work_dict, l_short_m_name):
            alib.p_e('Master file not in final file list: {}'.format(l_short_m_name))

    for l_file in l_mc_files:
        l_short_mc_name = l_file.split('/master validated common data templates (controlled)/')[1]
        if not vmf_file_in_dict(p_work_dict, l_short_mc_name):
            alib.p_e('Master file not in final file list: {}'.format(l_short_mc_name))

# --- Program Init
# --------------------------------------------------------------------
#
#                          initialise
#
# --------------------------------------------------------------------


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

    args, dummy_l_log_filename_s = initialise('validate_ss')

    # -- Initialise
    if not alib.init_app(args):
        return alib.FAIL_GENERIC

    home_dir = os.environ['USERPROFILE'].replace('\\', '/')
    default_sync_dir = home_dir + '/AUSTRALIAN CLUB CONSORTIUM PTY LTD/'
    default_sync_dir += 'Phase 3 - Deploy Phase - Phase 3/CARS Data and Data Management'

    #    default_sync_dir = 'C:/work/stuff/all_2017_apr_18'
    #    default_sync_dir = 'C:/work/stuff/all_2017_apr_19__1540'

    work_dir = alib.load_dir(args)

    work_files = alib.load_files(work_dir, args['quick_debug'])

    work_dict = alib.load_matching_masterfile(work_files)

    alib.load_tags(work_dict)

    alib.print_filenames(work_dict)

    # check every master file is in work dict
    validate_master_files(work_files, work_dict)

    validate_club_file(work_dict)
    validate_multi_file_groups(work_dict)

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
