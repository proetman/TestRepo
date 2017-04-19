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

CLUB_LIST = alib.CLUB_LIST
CLUB_TAGS = alib.CLUB_TAGS
OTHER_TAGS = alib.OTHER_TAGS


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
        ss_dict = alib.open_ss(l_file_name)
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
        ss_dict = alib.open_ss(l_file_name)
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
        if alib.club_specific_file(row):
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

        ss_df = alib.open_ss(l_file_name)
        if ss_df is None:
            alib.p_e('    Unable to open club spreadsheet, please review previous errors raised')
            totres['filemiss'] += 1
            continue

        m_df = alib.open_ss(l_m_file_name)
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
        l_club = alib.club_specific_file(row)

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