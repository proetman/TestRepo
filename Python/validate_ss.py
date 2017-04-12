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

CLUB_LIST = ['aant', 'raa',  'racq', 'ract', 'rac']

SYNC_DIR = 'C:/Users/PaulRoetman/AUSTRALIAN CLUB CONSORTIUM PTY LTD/'
SYNC_DIR += 'Phase 3 - Deploy Phase - CARS Data and Data Management'

# -- club files
CLUB_DIR = SYNC_DIR + '/Data Templates by Club'
COMMON_DIR = SYNC_DIR + '/Common Data Templates'

# -- Master files
MASTER_DIR = SYNC_DIR + '/Master Validated Templates by Club (Controlled)/CD4'
MASTER_COMMON_DIR = SYNC_DIR + '/Master Validated Common Data Templates (Controlled)'

# --------------------------------------------------------------------
#
#                          get filenames
#
# --------------------------------------------------------------------


def get_filenames(p_dir):
    """ get all files """

#    list_of_files = {}
    dict_of_files = []
    for (dirpath, dummy_dirnames, filenames) in os.walk(p_dir):
        for filename in filenames:
            dict_of_files.append(os.sep.join([dirpath, filename]).replace('\\', '/'))
#            if 'Crib' not in filename:
#                continue
#            print(filename)
#            list_of_files[filename] = os.sep.join([dirpath, filename]).replace('\\', '/')

    return dict_of_files

# --------------------------------------------------------------------
#
#                          cleanup
#
# --------------------------------------------------------------------


def cleanup_filenames(p_list, p_type):
    """ get all files """

    if p_type == 'club':
        remove_str = 'Data Templates by Club/'
    elif p_type == 'club_common':
        remove_str = 'Common Data Templates/'
    elif p_type == 'master':
        remove_str = 'CD4/'
    else:
        remove_str = 'Master Validated Common Data Templates (Controlled)/'

    l_list = p_list
    p_list[:] = [x.split(remove_str)[1].lower() for x in l_list]
    p_list[:] = [x.replace('incident management (inc nap)', 'incident management') for x in l_list]

    if p_type == 'club_common':
        p_list[:] = ['common/' + x for x in l_list]

    return
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

#    for tab in p_ss_dict:
#        if tab in p_m_dict:
#            print('tab {} matches'.format(tab))

# --------------------------------------------------------------------
#
#                          cleanup
#
# --------------------------------------------------------------------


def cleanup_ss(p_dict):
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
#                          initialise
#
# --------------------------------------------------------------------


def open_ss(p_ss):
    """ open spreadhseet, save as df """

    if p_ss is None:
        return None

    l_ss = p_ss.replace('\\', '/')

    try:
        ss_df_dict = pd.read_excel(l_ss,
                                   sheetname=None,
                                   index_col=None)

    except FileNotFoundError as err:
        alib.p_e('Function open_ss, spreadsheet not found.')
        alib.p_e('       speadsheet: [{}]'.format(alib.format_filename(p_ss)))
        alib.p_e('\n       error text [{}]'.format(err))
        ss_df_dict = None

    except Exception as err:
        alib.p_e('\nGeneric exception in function open_ss.')
        alib.p_e('       spreadsheet: [{}]'.format(alib.format_filename(p_ss)))
        alib.p_e('\n       error text [{}]'.format(err))
        ss_df_dict = None

    if ss_df_dict is not None:
        for tab in ss_df_dict:
            l_df = ss_df_dict[tab]
            if len(l_df.columns) > 0:
                l_df.columns = l_df.columns.str.upper()

    return ss_df_dict


# --------------------------------------------------------------------
#
#                          validate tabs
#
# --------------------------------------------------------------------


def mcf_validate_tabs(p_list, p_clubs):
    """ validate tabs are same between all files. """
    def replace_club(p_str, p_list):
        l_str = p_str
        for c in p_list:
            l_str = [f.replace(c.upper(), 'CLUBNAME') for f in l_str]
        return l_str

    err_txt = '    FILE 1 "{vM}" is different to FILE 2 "{vC}"'
    err_txt2 = '        file1 tabs: [{vT}]'
    err_txt3 = '        file2 tabs: [{vT}]'

    master_keys = None
    master_name = None

    for file_name in p_list:
        l_file_name = CLUB_DIR + '/' + file_name
        ss_dict = open_ss(l_file_name)
        l_ss_keys = ss_dict.keys()
        l_new_keys = replace_club(l_ss_keys, p_clubs)
        l_new_keys.sort()

        if master_keys is None:
            master_keys = l_new_keys
            master_name = file_name
        else:
            if l_new_keys != master_keys:
                alib.p_e('')
                alib.p_e(err_txt.format(vM=master_name, vC=file_name))
                alib.p_e(err_txt2.format(vT=list(master_keys)))
                alib.p_e(err_txt3.format(vT=list(l_new_keys)))

                for tab in l_new_keys:
                    if tab not in master_keys:
                        alib.p_e('    tab in file2, not in file1: "{}"'.format(tab))

                for tab in master_keys:
                    if tab not in l_new_keys:
                        alib.p_e('    tab in file1, not in file2: "{}"'.format(tab))
                alib.p_i('')

# --------------------------------------------------------------------
#
#                          merge club
#
# --------------------------------------------------------------------


def merge_club_files(p_club_files):

    proc_files = []
    full_files = []

    for row in p_club_files:
        if club_specific_file(row):
            l_file = row.split('/')[-1]
            proc_files.append(l_file)
            full_files.append(row)

    # sort out a unique list of file types
    file_list = [i.split(' - ')[0] for i in proc_files]
    file_list = [i.split(' -')[0] for i in file_list]
    file_list = [i.split('_')[0] for i in file_list]

    uniq_file_list = list(set(file_list))

    alib.p_i('----------------------------------', p_before=1)
    alib.p_i('Validate multi club files, stage 1')
    alib.p_i('----------------------------------', p_after=1)

    for curr_file in uniq_file_list:
        alib.p_i('    process multi file type: "{}", validate file names'.format(curr_file))
        for l_club in CLUB_LIST:

            curr_list = [k for k in proc_files if curr_file in k and l_club in k]

            correct_name = '{} - {}.xlsx'.format(curr_file, l_club)
            correct_name2 = '{}_{}.xlsx'.format(curr_file, l_club)
            if correct_name in curr_list or correct_name2 in curr_list:
                pass
            else:
                alib.p_e('        correct name not found in set - "{}"'.format(correct_name))
                for f in curr_list:
                    alib.p_i('            incorrect filename: {}'.format(f))
                alib.p_i('')

    alib.p_i('----------------------------------', p_before=1)
    alib.p_i('Validate multi club files, stage 2')
    alib.p_i('----------------------------------', p_after=1)

    for curr_file in uniq_file_list:
        alib.p_i('    process multi file type : "{}", validate tabs between clubs'.format(curr_file))
        curr_list = [k for k in full_files if curr_file in k]
        mcf_validate_tabs(curr_list, CLUB_LIST)

    return
# --------------------------------------------------------------------
#
#                          validate club
#
# --------------------------------------------------------------------


def validate_club_files(p_club_files, p_m_files, p_mc_files):
    """ Validate each of the club files """

    alib.p_i('-------------------------', p_before=1)
    alib.p_i('Validate CLUB files')
    alib.p_i('-------------------------')

    totres = {}
    totres['unchanged'] = 0
    totres['modified'] = 0
    totres['filecount'] = 0
    totres['filemiss'] = 0

    # -- Loop through the club files
    for row in p_club_files:
        # print(row)
        l_target_file = '/'.join(row.split('/')[1:])
        l_club = row.split('/')[0]

        if l_club == 'common':
            l_file_name = COMMON_DIR + '/' + l_target_file
        else:
            l_file_name = CLUB_DIR + '/' + row

        # curr_file = row.split('/')[-1]
        alib.p_i('Validate club file: {}'.format(row), p_before=1)

        if l_target_file in p_m_files:
            m_file = l_target_file
            l_m_file_name = MASTER_DIR + '/' + m_file
        elif l_target_file in p_mc_files:
            m_file = l_target_file
            l_m_file_name = MASTER_COMMON_DIR + '/' + m_file
        else:
            alib.p_e('    No master file found for {}'.format(row))
            m_file = None
            totres['filemiss'] += 1
            continue

        if l_m_file_name is not None:
            l_m_file_name = l_m_file_name.replace('/incident management/', '/incident management (inc nap)/')

        ss_df = open_ss(l_file_name)
        if ss_df is None:
            alib.p_e('    Unable to open club spreadsheet, please review previous errors raised')
            totres['filemiss'] += 1
            continue

        m_df = open_ss(l_m_file_name)
        if m_df is None:
            alib.p_e('    Unable to open master spreadsheet, please review previous errors raised')
            totres['filemiss'] += 1
            continue

        # Tidy up dict.
        cleanup_ss(ss_df)
        cleanup_ss(m_df)

        if ss_df is not None and m_df is not None:
            compare_dict(row, ss_df, m_df, totres)
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

# --------------------------------------------------------------------
#
#                          club specific filename
#
# --------------------------------------------------------------------


def club_specific_file(p_filename):
    """ look for [club].xlsx on end of filename """
    retval = None
    for l_club in CLUB_LIST:
        l_club_test = '{}.xlsx'.format(l_club)
        l_len = len(l_club_test)
        l_file_test = p_filename[-l_len:]
        if l_file_test == l_club_test:
            retval = l_club
            break

    return retval


# --------------------------------------------------------------------
#
#                          validate master
#
# --------------------------------------------------------------------


def validate_master_files(p_files, p_club_files):
    """ validate club file exists for this master """

    alib.p_i('---------------------', p_before=1)
    alib.p_i('Validate MASTER files')
    alib.p_i('---------------------', p_after=1)

    l_success_message = "Validate Master file: {vF:70} OK"
    l_fail_message = "Validate Master file: {vF:70} FAIL"

    for row in p_files:
        l_club = club_specific_file(row)

        # loop through all clubs, validate exists.
        if l_club is None:
            for club in CLUB_LIST:
                target_row = '{}/{}'.format(club, row)
                alib.log_debug('    validate target row: [{}]'.format(target_row))
                if target_row not in p_club_files:
                    alib.p_e(l_fail_message.format(vF=target_row))
                    alib.p_e('    No CLUB file found for this file')
                else:
                    alib.p_i(l_success_message.format(vF=target_row))

        # just process this single club
        else:
            target_row = '{}/{}'.format(l_club, row)
            alib.log_debug('    validate target row: [{}]'.format(target_row))
            if target_row not in p_club_files:
                alib.p_e(l_fail_message.format(vF=target_row))
                alib.p_e('    No CLUB file found for this file')
            else:
                alib.p_i(l_success_message.format(vF=target_row))


# --------------------------------------------------------------------
#
#                          validate master common
#
# --------------------------------------------------------------------


def validate_master_common_files(p_files, p_club_files):
    """ validate club file exists for this master """

    alib.p_i('----------------------------', p_before=1)
    alib.p_i('Validate MASTER COMMON files')
    alib.p_i('----------------------------', p_after=1)

    l_success_message = "Validate Master Common file: {vF:80} OK"
    l_fail_message = "Validate Master Common file: {vF:80} FAIL"

    for row in p_files:
        # print(row)
        target_row = '{}/{}'.format('common', row)
        alib.log_debug('    validate target row: [{}]'.format(target_row))
        if target_row not in p_club_files:
            alib.p_e(l_fail_message.format(vF=target_row))
            alib.p_e('    No CLUB file found for this file')
        else:
            alib.p_i(l_success_message.format(vF=target_row))

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
                        default=CLUB_DIR,
                        required=False)

    parser.add_argument('--club_common_dir',
                        help='club common files directory',
                        default=COMMON_DIR,
                        required=False)

    parser.add_argument('--m_dir',
                        help='master directory',
                        default=MASTER_DIR,
                        required=False)

    parser.add_argument('--mc_dir',
                        help='master common directory',
                        default=MASTER_COMMON_DIR,
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

    club_files = get_filenames(args['club_dir'])
    club_common_files = get_filenames(args['club_common_dir'])
    m_files = get_filenames(args['m_dir'])
    mc_files = get_filenames(args['mc_dir'])

    cleanup_filenames(club_files, 'club')
    cleanup_filenames(club_common_files, 'club_common')
    cleanup_filenames(m_files, 'master')
    cleanup_filenames(mc_files, 'master club')

    club_files += club_common_files

#    validate_master_files(m_files, club_files)
#    validate_master_common_files(mc_files, club_files)
#
    validate_club_files(club_files, m_files, mc_files)
    merge_club_files(club_files)

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
