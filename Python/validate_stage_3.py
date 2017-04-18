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
import re
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
# -- these files will end in ' - club.xlsx' or '_club.xlsx'
CLUB_TAGS = ['cti numbers & scripts',
             'esp alerts',
             'esp response',
             'external service supplier',
             'incident management',
             'special situation',
             'stock movement management (surefire)',
             'mr risk assessment',
             'mr risk mitigation']

# -- these files will end in 'TAG.xlsx'
OTHER_TAGS = ['crib locations',
              'eta table',
              'message groups',
              'personnel node access',
              'personnel',
              'skills',
              'term app access - call, disp, caddbm',
              'term app access - inetveiwer',
              'term',
              'unit agency restriction',
              'units',
              'vehicle equipment',
              'vehicles']

# --- analyse shallow (old)
# --------------------------------------------------------------------
#
#                          fetch all row counts
#
# --------------------------------------------------------------------


def a_display_all_rowcounts(p_all_ss, p_all_tabs, p_row):
    """
    loop through all spreadsheets, get count of rows per tab.

    This code is a bit hairy....
    start with a dict of spreadsheets, 1 per club.
       within each of them is a dict of TABS
           within each TAB is the rows/columns of the actual spreadsheets

    So, loop through each club (l_club)
        loop through each tab
            count rows, add result to print dict for this club

    print headings
    loop through print dict (one per club)
        print result.

    """
    alib.p_i('{}Matrix report for "{}"'.format(10*' ', p_row), p_before=1, p_after=1)
    PRINT_CLUB_LIST = ['raa', 'ract', 'aant', 'rac', 'racq', 'master']

    exclude_tabs = ['Database Schema', 'Version History', 'Version Control']

    result = {}
    p_all_tabs.sort()

    for tab in p_all_tabs:
        pr_result = ''
        if tab in exclude_tabs:
            continue

        for l_club in PRINT_CLUB_LIST:

            if l_club in p_all_ss:
                l_ss = p_all_ss[l_club]
            else:
                l_ss = []

            # see if this tab is in this spreadsheet
            if tab in l_ss:
                tab_data = l_ss[tab]
                rowcount = len(tab_data.index)
            else:
                rowcount = ''                     # tab does not exists, set to -1
            pr_result += '{vR:15} '.format(vR=rowcount)

        # end for
        result[tab] = pr_result

    l_heading = '{:31}'.format('Tab')
    l_heading2 = '{} '.format(30 * '-')

    for l_club in PRINT_CLUB_LIST:
        l_heading += '{vR:>15} '.format(vR=l_club)
        l_heading2 += '{vR:>15} '.format(vR=15 * '-')

    alib.p_i(l_heading)
    alib.p_i(l_heading2)

    for key, value in result.items():
        alib.p_i('{vK:30} {vR}'.format(vK=key[:30], vR=value))

    return True


def a_display_all_rowcounts_across(p_all_ss, p_all_tabs, p_row):
    """
    loop through all spreadsheets, get count of rows per tab.

    This code is a bit hairy....
    start with a dict of spreadsheets, 1 per club.
       within each of them is a dict of TABS
           within each TAB is the rows/columns of the actual spreadsheets

    So, loop through each club (l_club)
        loop through each tab
            count rows, add result to print dict for this club

    print headings
    loop through print dict (one per club)
        print result.

    """
    alib.p_i('{}Matrix report for "{}"'.format(10*' ', p_row), p_before=1, p_after=1)

    result = {}
    for l_club, l_ss in p_all_ss.items():

        pr_result = ''
        # loop through all tabs
        for tab in p_all_tabs:
            if tab == 'Database Schema' or tab == 'Version History':
                continue

            # see if this tab is in this spreadsheet
            if tab in l_ss:
                tab_data = l_ss[tab]
                rowcount = len(tab_data.index)
            else:
                rowcount = -1                   # tab does not exists, set to -1
            pr_result += '{vR:15} '.format(vR=rowcount)

        # end for
        result[l_club] = pr_result

    l_heading = '{:31}'.format('Club')
    l_heading2 = '{} '.format(30 * '-')

    for tab in p_all_tabs:
        if tab == 'Database Schema' or tab == 'Version History':
            continue
        l_heading += '{vR:>15} '.format(vR=tab[:15])
        l_heading2 += '{vR:>15} '.format(vR=15 * '-')

    alib.p_i(l_heading)
    alib.p_i(l_heading2)

    for key, value in result.items():
        alib.p_i('{vK:30} {vR}'.format(vK=key, vR=value))

    return True


# --------------------------------------------------------------------
#
#                          fetch all tabs
#
# --------------------------------------------------------------------


def a_fetch_all_tabs(p_all_ss):
    """
    loop through all spreadsheets, get all tab names.
    """
    tab_list = []

    for key, value in p_all_ss.items():
        for tab in value.keys():
            tab_list.append(tab)

    tab_list = list(set(tab_list))

    return tab_list

# --------------------------------------------------------------------
#
#                          Load ss into dict
#
# --------------------------------------------------------------------


def a_load_ss(p_full_file_list, p_work_dir):
    """
    create a new dict, with each spreadsheet in it
    """

    l_ss_dict = {}

    for key, value in p_full_file_list.items():
        if key == 'master':
            value = value.replace('incident management/', 'incident management (inc nap)/')
            l_dir = p_work_dir['l_m_dir']
        else:
            l_dir = p_work_dir['l_c_dir']

        l_file_name = l_dir + '/' + value
        ss_dict = open_ss(l_file_name)
        if ss_dict is not None:
            l_ss_dict[key] = ss_dict
        else:
            alib.p_e('Unable to load spreadsheet {}, aborting'.format(l_file_name))
            l_ss_dict = None
            break

    return l_ss_dict

# --------------------------------------------------------------------
#
#                          analyse shallow
#
# --------------------------------------------------------------------


def a_all_clubs_exist(p_full_file_list, p_row):
    """
    validate this list has all clubs and master in it
    """

    all_files_found = True
    # file_name = None

    err_txt = 'Processing "{}", Not all Clubs/Master found for this file'.format(p_row)

    for l_club in CLUB_LIST:
        if l_club not in p_full_file_list:
            alib.p_e(err_txt, p_before=1)
            alib.p_e('    Missing club [{}] from file list'.format(l_club))

    if 'master' not in p_full_file_list:
        alib.p_e(err_txt, p_before=1)
        alib.p_e('    Missing club [{}] from file list'.format('master'))

    return all_files_found

# --------------------------------------------------------------------
#
#                          analyse shallow
#
# --------------------------------------------------------------------


def analyse_shallow_old(p_full_file_list, p_work_dir, p_row):
    """
    Perform some quick and dirty analsys on the files
    """

    a_all_clubs_exist(p_full_file_list, p_row)

    all_ss = a_load_ss(p_full_file_list, p_work_dir)
    if all_ss is None:
        alib.p_e('Issues loading spreadsheets, not performing any more analysis')
        return False

    all_tabs = a_fetch_all_tabs(all_ss)
    a_display_all_rowcounts(all_ss, all_tabs, p_row)

# --- analyse shallow (new)


def as_find_priority(p_priority_df, p_str):
    """ determine the priority based on file name or tag """

    row_ind = p_priority_df['NAME'] == p_str
    row = p_priority_df[row_ind]

    if len(row.index) == 1:

        exec_order = row['EXEC_ORDER'].values[0]
        hex_etl = row['HEX ETL'].values[0]
        ret_title = ' execution order {}, verification {}'.format(exec_order, hex_etl)

    elif len(row.index) > 1:
        ret_title = ' MULTIPLE MATCHING DATA for name {}'.format(p_str)

    elif p_str == 'external service supplier':
        exec_order = '28-43'
        hex_etl = 'HEX'
        ret_title = ' execution order {}, verification {}'.format(exec_order, hex_etl)

    elif p_str == 'incident management':
        exec_order = '16-27'
        hex_etl = 'HEX'
        ret_title = ' execution order {}, verification {}'.format(exec_order, hex_etl)

    elif p_str == 'stock movement management (surefire)':
        exec_order = '2-68'
        hex_etl = 'HEX'
        ret_title = ' execution order {}, verification {}'.format(exec_order, hex_etl)

    elif p_str == 'incident management - nap':
        exec_order = '16-27'
        hex_etl = 'HEX'
        ret_title = ' execution order {}, verification {}'.format(exec_order, hex_etl)

    elif p_str == 'out of service type agency':
        exec_order = '9'
        hex_etl = 'ETL'
        ret_title = ' execution order {}, verification {}'.format(exec_order, hex_etl)

    elif p_str == 'special situation type - field config':
        exec_order = '44'
        hex_etl = 'HEX'
        ret_title = ' execution order {}, verification {}'.format(exec_order, hex_etl)

    else:
        ret_title = ' NO MATCHING DATA for name {}'.format(p_str)

    return ret_title

# --------------------------------------------------------------------
#
#                          fetch all row counts
#
# --------------------------------------------------------------------


def as_pr(fptr, str):
    """ print to log file and to report file """

    alib.p_i(str)
    fptr.write(str + '\n')
    return
# --------------------------------------------------------------------
#
#                          fetch all row counts
#
# --------------------------------------------------------------------


def as_generate_report(p_tag, fptr, p_rep_list, p_priority_df):
    """
    loop through all spreadsheets, get count of rows per tab.

    This code is a bit hairy....
    start with a dict of spreadsheets, 1 per club.
       within each of them is a dict of TABS
           within each TAB is the rows/columns of the actual spreadsheets

    So, loop through each club (l_club)
        loop through each tab
            count rows, add result to print dict for this club

    print headings
    loop through print dict (one per club)
        print result.

    """
    all_tabs = as_get_all_tabs(p_rep_list)

    if p_tag == 'common':

        PRINT_CLUB_LIST = ['common', 'master']
        value = p_rep_list['common']
        ss_name = value['club_file_short'].split('/')[-1]
        pr_title = '{}Matrix report for "{}"'.format(10*' ', ss_name)

        short_name = ss_name.split('.')[0]
        priority_title = as_find_priority(p_priority_df, short_name)
#        row_ind = p_priority_df['NAME'] == short_name
#        row = p_priority_df[row_ind]
#        if len(row.index) == 1:
#            exec_order = row['EXEC_ORDER'].values[0]
#            hex_etl = row['HEX ETL'].values[0]
#            pr_title += ' execution order {}, verification {}'.format(exec_order, hex_etl)
#        elif len(row.index) > 1:
#            pr_title += ' MULTIPLE MATCHING DATA for name {}'.format(short_name)
#        else:
#            pr_title += ' NO MATCHING DATA for name {}'.format(short_name)
        pr_title += priority_title
        as_pr(fptr, '')
        as_pr(fptr, pr_title)

    else:

        PRINT_CLUB_LIST = ['raa', 'ract', 'aant', 'rac', 'racq', 'master']
        pr_title = '{}Matrix report for "{}"'.format(10*' ', p_tag)
        priority_title = as_find_priority(p_priority_df, p_tag)
#        row_ind = p_priority_df['NAME'] == p_tag
#        row = p_priority_df[row_ind]
#        if len(row.index) == 1:
#            exec_order = row['EXEC_ORDER'].values[0]
#            hex_etl = row['HEX ETL'].values[0]
#            pr_title += ' execution order {}, verification {}'.format(exec_order, hex_etl)
#        elif len(row.index) > 1:
#            pr_title += ' MULTIPLE MATCHING DATA for tag {}'.format(p_tag)
#        else:
#            pr_title += ' NO MATCHING DATA for tag {}'.format(p_tag)
        pr_title += priority_title
        as_pr(fptr, '')
        as_pr(fptr, pr_title)

    exclude_tabs = ['Database Schema', 'Version History', 'Version Control']

    result = {}
    all_tabs.sort()

    for tab in all_tabs:
        pr_result = ''
        if tab in exclude_tabs:
            continue

        for l_club in PRINT_CLUB_LIST:

            if l_club in p_rep_list:
                l_ss = p_rep_list[l_club]['club_ss']
#                if 'master' not in p_rep_list:
#                    l_ss = p_rep_list[l_club]['master_ss']
            else:
                l_ss = []

            if l_club == 'master':
                l_ss = []
                for key, value in p_rep_list.items():
                    if 'master_ss' in value:
                        l_ss = value['master_ss']
                        if l_ss is None:
                            l_ss = []
                            continue
                        else:
                            break

            # see if this tab is in this spreadsheet
            if tab in l_ss:
                tab_data = l_ss[tab]
                rowcount = len(tab_data.index)
            else:
                rowcount = ''                     # tab does not exists, set to -1
            pr_result += '{vR:15} '.format(vR=rowcount)

        # end for
        result[tab] = pr_result

    l_heading = '{:31}'.format('Tab')
    l_heading2 = '{} '.format(30 * '-')

    for l_club in PRINT_CLUB_LIST:
        l_heading += '{vR:>15} '.format(vR=l_club)
        l_heading2 += '{vR:>15} '.format(vR=15 * '-')

    as_pr(fptr, l_heading)
    as_pr(fptr, l_heading2)
#    alib.p_i(l_heading)
#    fptr.write(l_heading + '\n')
#    alib.p_i(l_heading2)
#    fptr.write(l_heading2 + '\n')

    for key, value in result.items():
        as_pr(fptr, '{vK:30} {vR}'.format(vK=key[:30], vR=value))
#        alib.p_i('{vK:30} {vR}'.format(vK=key[:30], vR=value))
#        fptr.write('{vK:30} {vR}\n'.format(vK=key[:30], vR=value))

    as_display_files(p_rep_list, fptr)

    return True


# --------------------------------------------------------------------
#
#                          as get all tabs
#
# --------------------------------------------------------------------

def as_get_all_tabs(p_rep_list):
    """ fetch a list of unique tabs for all spreadsheets """
    tab_list = []
    for key, value in p_rep_list.items():
        l_ss = value['club_ss']
        for tab in l_ss.keys():
            tab_list.append(tab)

    tab_list = list(set(tab_list))

    return tab_list

# --------------------------------------------------------------------
#
#                          as display files
#
# --------------------------------------------------------------------


def as_display_files(p_rep_list, fptr):
    """ display the files for this report """

    print_blank = True
    for key, value in p_rep_list.items():
        # club_file_short
        if print_blank:
            as_pr(fptr, '')
            print_blank = False

        l_filename = value['club_file_full']
        as_pr(fptr, '   FILE: "{}"'.format(l_filename))

# --------------------------------------------------------------------
#
#                          analyse shallow
#
# --------------------------------------------------------------------


def analyse_shallow(p_work_dict, p_priority_df):
    """
    Perform some quick and dirty analsys on the files
    """
    # Loop through the tags, for each tag
    #     loop through all files. If the tag matches
    #         add result to new list (rep_list)
    l_filename = "c:/test_results/data/report_analyse_shallow.txt"
    with open(l_filename, "w+") as l_file_ptr:

        for l_tag in CLUB_TAGS:
            rep_list = {}
            for key, value in p_work_dict.items():
                if value['tag'] == l_tag:
                    club = value['club']
                    rep_list[club] = value

            as_generate_report(l_tag, l_file_ptr, rep_list, p_priority_df)

        for l_tag in OTHER_TAGS:
            rep_list = {}
            for key, value in p_work_dict.items():
                if value['tag'] == l_tag:
                    club = value['club']
                    rep_list[club] = value

            as_generate_report(l_tag, l_file_ptr, rep_list, p_priority_df)

        for key, value in p_work_dict.items():
            rep_list = {}
            if value['tag'] is None:
                rep_list['common'] = value
                as_generate_report('common', l_file_ptr, rep_list, p_priority_df)


# --- generic stuff


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
    alib.log_debug('Open spreadsheet {}'.format(p_ss))

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


def mcf_validate_tabs(p_list, p_clubs, p_work_dir):
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
        l_file_name = p_work_dir['l_c_dir'] + '/' + file_name
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
#                          create full file list
#
# --------------------------------------------------------------------


def mcf_create_full_file_list(p_club_files, p_m_files, p_row):
    """
    Create an dict of files for this file type, one per club
        f['racq'] = 'full path to file'
        f['raa'] = 'full path to file'
        f['master'] =  'full path to file'
    """
    full_file_list = {}

    for file in p_club_files:
        l_filename = file.split('/')[-1]
        l_club = file.split('/')[0]
        if p_row in l_filename:
            full_file_list[l_club] = file

    for file in p_m_files:
        l_filename = file.split('/')[-1]
        if p_row in l_filename:
            full_file_list['master'] = file

    return full_file_list

# --------------------------------------------------------------------
#
#                          create full file list
#
# --------------------------------------------------------------------


def mcf_create_full_file_list_type2(p_club_files, p_m_files, p_mc_files, p_row):
    """
    Create an dict of files for this file type, one per club
        f['racq'] = 'full path to file'
        f['raa'] = 'full path to file'
        f['master'] =  'full path to file'
    """
    full_file_list = {}

    l_test_name = p_row + '.xlsx'

    #    for p in p_club_files:
    #        if p_row in p:
    #            alib.p_i('    verify - {}'.format(p))

    for file in p_club_files:
        l_filename = file.split('/')[-1]
        l_club = file.split('/')[0]
        if p_row in l_filename:

            # special case for 'incident management' and 'special situation', name includes to many files
            if p_row == 'incident management':
                if('comment category' in l_filename or
                   'supplier types' in l_filename or
                   'vehicle colour' in l_filename):
                    continue

            if p_row == 'special situation':
                if 'special situation type' in l_filename:
                    continue

            full_file_list[l_club] = file

    found_master = False

    for file in p_m_files:
        l_filename = file.split('/')[-1]
        if l_test_name == l_filename:
            full_file_list['master'] = file
            found_master = True
            break

    if not found_master:
        l_club_test_name = p_row + ' - racq.xlsx'
        l_club_test_name2 = p_row + '_racq.xlsx'
        for file in p_m_files:
            l_filename = file.split('/')[-1]
            if l_club_test_name == l_filename or l_club_test_name2 == l_filename:
                full_file_list['master'] = file
                found_master = True
                break

    return full_file_list


# --------------------------------------------------------------------
#
#                          merge club
#
# --------------------------------------------------------------------


def merge_club_files(p_all_files,  p_work_dir):

    l_club_files = p_all_files['c'] + p_all_files['cc']
    l_m_files = p_all_files['m']
    l_mc_files = p_all_files['mc']

    # Create lists of files with club name in it, eg 'esp alerts - aant.xlsx'
    # list of files with club name removed
    proc_files = []
    # list of files with club name included
    full_files = []

    # create a list of files with duplicates, but no club name, eg 'message groups.xlsx'
    # there is one message groups.xlsx file per club.
    dup_file = []

    for row in l_club_files:
        if club_specific_file(row):
            l_file = row.split('/')[-1]
            proc_files.append(l_file)
            full_files.append(row)
        else:
            l_file = row.split('/')[-1]
            dup_file.append(l_file)

    # sort out a unique list of file types
    file_list = [i.split(' - ')[0] for i in proc_files]
    file_list = [i.split(' -')[0] for i in file_list]
    file_list = [i.split('_')[0] for i in file_list]

    dup_file = [i.split('.')[0] for i in dup_file]

    uniq_file_list = list(set(file_list))
    uniq_file_list.sort()

    # only look at the file if there is more than the master and the club file.
    dup_files_list = [x for x in dup_file if dup_file.count(x) > 3]
    dup_files_list = list(set(dup_files_list))
    dup_files_list.sort()


#    uniq_file_list = ['special situation']
#    dup_files_list = []
    for row in uniq_file_list:
        full_file_list = mcf_create_full_file_list_type2(l_club_files, l_m_files, l_mc_files, row)
        analyse_shallow(full_file_list, p_work_dir, row)

    for row in dup_files_list:
        full_file_list = mcf_create_full_file_list(l_club_files, l_m_files, row)
        analyse_shallow(full_file_list, p_work_dir, row)

    return
    # at this point, have a list of all multi file groups  that have club name embeded (unique_file_list)
    # and another list that have no club name embeded (dup_files_list)

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
        mcf_validate_tabs(curr_list, CLUB_LIST, p_work_dir)

    return
# --------------------------------------------------------------------
#
#                          validate club
#
# --------------------------------------------------------------------


def validate_club_files(p_club_files, p_m_files, p_mc_files, p_work_dir):
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
            l_file_name = p_work_dir['l_cc_dir'] + '/' + l_target_file
        else:
            l_file_name = p_work_dir['l_c_dir'] + '/' + row

        # curr_file = row.split('/')[-1]
        alib.p_i('Validate club file: {}'.format(row), p_before=1)

        if l_target_file in p_m_files:
            m_file = l_target_file
            l_m_file_name = p_work_dir['l_m_dir'] + '/' + m_file
        elif l_target_file in p_mc_files:
            m_file = l_target_file
            l_m_file_name = p_work_dir['l_mc_dir'] + '/' + m_file
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

# --- Reports
# --------------------------------------------------------------------
#
#                          dump short names to file
#
# --------------------------------------------------------------------


def print_filenames(p_work_files):
    """ Print all the short names of the club files """

    alib.p_i('')
    alib.p_i('{:30} {:40} {:10}'.format('Tag', 'File', 'Club'))
    alib.p_i('{:30} {:40} {:10}'.format(30 * '-', 40 * '-', 10 * '-'))
    for key, value in p_work_files.items():
        curr_file = value['club_file_short'].split('/')[-1]
        curr_club = value['club']

        if curr_club is None:
            l_club = ''
        else:
            l_club = curr_club

        if value['tag'] is None:
            l_tag = ''
        else:
            l_tag = value['tag']
        alib.log_info('{:30} {:40} {:10}'.format(l_tag, curr_file, l_club))
    alib.p_i('')

# --- file operations

# --------------------------------------------------------------------
#
#                          load tags
#
# --------------------------------------------------------------------


def load_tags(p_work_dict):
    """  Tag files that are 1 per club """

    for key, value in p_work_dict.items():

        l_dir = value['club_file_short'].split('/')[0]
        if l_dir in CLUB_LIST:
            value['club'] = l_dir

        l_c_short_name = value['club_file_short']
        for l_tag in OTHER_TAGS:
            l_tag_str = '/' + l_tag + '.xlsx'
            len_tag = len(l_tag_str)
            test_str = l_c_short_name[-len_tag:]

            if l_tag_str == test_str:
                value['tag'] = l_tag
                continue

    print('NOW PROCESS CLUB TAGS')
    for key, value in p_work_dict.items():
        #        curr_file = value['club_file_short'].split('/')[-1]
        #        if curr_file == 'external service supplier_aant.xlsx':
        #            x = 1
        if value['tag'] is not None:
            continue

        l_c_short_name = value['club_file_short']
        if not club_specific_file(l_c_short_name):
            continue

        for l_tag in CLUB_TAGS:
            l_tag_str = '/' + l_tag + ' - '
            if l_tag_str in l_c_short_name:
                value['tag'] = l_tag
                continue

            l_tag_str = '/' + l_tag + '_'
            if l_tag_str in l_c_short_name:
                value['tag'] = l_tag
                continue


# --------------------------------------------------------------------
#
#                          lower case list
#
# --------------------------------------------------------------------

def load_lower_case(p_list):
    """convert list to all lower case """
    l_list = p_list
    p_list[:] = [x.lower() for x in l_list]

    return

# --------------------------------------------------------------------
#
#                          cleanup
#
# --------------------------------------------------------------------


def load_cleanup_filename(p_list, p_type):
    """ get all files """

    if p_type == 'club':
        remove_str = 'data templates by club/'
    elif p_type == 'club_common':
        remove_str = 'common data templates/'
    elif p_type == 'master':
        remove_str = 'cd4/'
    else:
        remove_str = 'master validated common data templates (controlled)/'

    l_list = p_list
    p_list[:] = [x.split(remove_str)[1] for x in l_list]

    return

# --------------------------------------------------------------------
#
#                          load master file
#
# --------------------------------------------------------------------


def load_master_file(p_value, p_work_files):
    """ find the correct master file for this club file """

    l_m_files = p_work_files['m']
    l_mc_files = p_work_files['mc']

    l_c_short_name = p_value['club_file_short']
    l_c_type = p_value['type']

    if l_c_type == 'club':
        l_file_list = l_m_files
    else:
        l_file_list = l_mc_files

    for file in l_file_list:
        # only do this for club files, not common.
        l_file = file
        if l_c_type == 'club':
            l_file = l_file.replace('/incident management (inc nap)/', '/incident management/')

        len_c = len(l_c_short_name)
        len_c -= 1
        part_master_file = l_file[-len_c:]

        # remove leading CD4,
        part_master_file = '/'.join(part_master_file.split('/')[1:])

        if len(part_master_file) == 0:
            continue

        # remove leading club name from filename (the first aant) aant/dispatch/esp alerts - aant.xlsx
        part_club_file = '/'.join(l_c_short_name.split('/')[1:])

        no_club_name = ''
        if club_specific_file(part_club_file):
            for c in CLUB_LIST:
                curr_c = ' - {}.xlsx'.format(c.lower())
                no_club_name = part_club_file.replace(curr_c, '.xlsx')

        if part_club_file == part_master_file or no_club_name == part_master_file:
            return(file)

    return None


# --------------------------------------------------------------------
#
#                          match c files to m files
#
# --------------------------------------------------------------------


def load_matching_masterfile(p_work_files):
    """
    Initialise the directories where the files live
    file structure
        club file short name (dict index)
        club file name (actual name, including full path????)
        master file short name
        master file actual name (including full path)
    """

    l_c_files = p_work_files['c'].copy()
    l_cc_files = p_work_files['cc'].copy()

    load_cleanup_filename(l_c_files, 'club')
    load_cleanup_filename(l_cc_files, 'club_common')

    work_dict = {}

    counter = 0
    for file in l_c_files:

        file_dict = {}
        file_dict['club_file_short'] = file
        file_dict['club_file_full'] = p_work_files['c'][counter]
        file_dict['type'] = 'club'
        file_dict['tag'] = None
        file_dict['club'] = None
        file_dict['club_ss'] = open_ss(file_dict['club_file_full'])

        work_dict[file] = (file_dict)
        counter += 1

    counter = 0
    for file in l_cc_files:

        file_dict = {}
        file_dict['club_file_short'] = file
        file_dict['club_file_full'] = p_work_files['cc'][counter]
        file_dict['type'] = 'club common'
        file_dict['tag'] = None
        file_dict['club'] = None
        file_dict['club_ss'] = open_ss(file_dict['club_file_full'])

        work_dict[file] = (file_dict)
        counter += 1

    for key, value in work_dict.items():

        l_m_file = load_master_file(value, p_work_files)
        if l_m_file is not None:
            if value['type'] == 'club':
                l_short_m_name = l_m_file.split('cd4/')[1]
            else:
                l_short_m_name = l_m_file.split('/master validated common data templates (controlled)/')[1]
        else:
            l_short_m_name = None

        value['master_file_short'] = l_short_m_name
        value['master_file_full'] = l_m_file
        value['master_ss'] = open_ss(l_m_file)

    alib.p_i('')
    for key, value in work_dict.items():
        if value['master_file_short'] is None:
            alib.p_e('club file with no master  [{}]'.format(value['club_file_short']))
    alib.p_i('')

    return work_dict


# --------------------------------------------------------------------
#
#                          get filenames
#
# --------------------------------------------------------------------


def load_os_filenames(p_dir):
    """ get all files """

    dict_of_files = []
    for (dirpath, dummy_dirnames, filenames) in os.walk(p_dir):
        for filename in filenames:

            result = re.match('.*130417.xlsx$', filename, flags=re.IGNORECASE)
            if result:
                continue

            result = re.match('.* v1.xlsx$', filename, flags=re.IGNORECASE)
            if result:
                continue

            result = re.match('.* 13apr.xlsx$', filename, flags=re.IGNORECASE)
            if result:
                continue

            result = re.match('.* 13april2017.xlsx$', filename, flags=re.IGNORECASE)
            if result:
                continue

            result = re.match('.* original_issue.xlsx$', filename, flags=re.IGNORECASE)
            if result:
                continue

            result = re.match('.* 13apr.xlsx$', filename, flags=re.IGNORECASE)
            if result:
                continue

            dict_of_files.append(os.sep.join([dirpath, filename]).replace('\\', '/'))

#            if 'Crib' not in filename:
#                continue
#            print(filename)
#            list_of_files[filename] = os.sep.join([dirpath, filename]).replace('\\', '/')

    return dict_of_files


# --------------------------------------------------------------------
#
#                          setup files
#
# --------------------------------------------------------------------


def load_files(p_work_dir):
    """ load all files into a dict. """

    all_files = {}
    all_files['c'] = load_os_filenames(p_work_dir['l_c_dir'])
    all_files['cc'] = load_os_filenames(p_work_dir['l_cc_dir'])
    all_files['m'] = load_os_filenames(p_work_dir['l_m_dir'])
    all_files['mc'] = load_os_filenames(p_work_dir['l_mc_dir'])

    load_lower_case(all_files['c'])
    load_lower_case(all_files['cc'])
    load_lower_case(all_files['m'])
    load_lower_case(all_files['mc'])

    return all_files

# --------------------------------------------------------------------
#
#                          setup dir
#
# --------------------------------------------------------------------


def load_dir(p_args):
    """ Initialise the directories where the files live """

    #    home_dir = os.environ['USERPROFILE'].replace('\\', '/')
    #    default_sync_dir = home_dir + '/AUSTRALIAN CLUB CONSORTIUM PTY LTD/'
    #    default_sync_dir += 'Phase 3 - Deploy Phase - Phase 3/CARS Data and Data Management'

    default_sync_dir = 'C:/work/stuff/all_2017_apr_18'
    # -- club files
    default_club_dir = default_sync_dir + '/Data Templates by Club'
    default_club_common_dir = default_sync_dir + '/Common Data Templates'

    # -- Master files
    default_master_dir = default_sync_dir + '/Master Validated Templates by Club (Controlled)/CD4'
    default_master_common_dir = default_sync_dir + '/Master Validated Common Data Templates (Controlled)'

    work_dir = {}

    if p_args['club_dir'] is None:
        work_dir['l_c_dir'] = default_club_dir
    else:
        work_dir['l_c_dir'] = p_args['club_dir']

    if p_args['club_common_dir'] is None:
        work_dir['l_cc_dir'] = default_club_common_dir
    else:
        work_dir['l_cc_dir'] = p_args['club_common_dir']

    if p_args['m_dir'] is None:
        work_dir['l_m_dir'] = default_master_dir
    else:
        work_dir['l_m_dir'] = p_args['m_dir']

    if p_args['mc_dir'] is None:
        work_dir['l_mc_dir'] = default_master_common_dir
    else:
        work_dir['l_mc_dir'] = p_args['mc_dir']

    return work_dir

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

    priority_list_dict = open_ss('C:/Users/PaulRoetman/OneDrive - AUSTRALIAN CLUB CONSORTIUM PTY LTD/work/git/TestRepo/Doc/priority_files.xlsx')
    priority_list_df = priority_list_dict['Sheet1']
    priority_list_df.columns = ['FUNCTION', 'NAME', 'EXEC_ORDER', 'LOAD_METHOD', 'HEX ETL']
    priority_list_df['NAME'] = priority_list_df['NAME'].str.lower()

    work_dir = load_dir(args)
    work_files = load_files(work_dir)

    work_dict = load_matching_masterfile(work_files)

    load_tags(work_dict)

    print_filenames(work_dict)

    analyse_shallow(work_dict, priority_list_df)

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
