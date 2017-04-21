"""
Created on Thu Apr  6 16:53:06 2017

@author: PaulRoetman
"""

from __future__ import division
from __future__ import print_function


# Import OS Functions
import argparse
import os
import shutil
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

# --------------------------------------------------------------------
#
#                          print and write to file
#
# --------------------------------------------------------------------


def as_pr(fptr, str):
    """ print to log file and to report file """

    alib.p_i(str)
    fptr.write(str + '\n')
    return


# --------------------------------------------------------------------
#
#                          find priority
#
# --------------------------------------------------------------------


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


def as_generate_report(p_tag, fptr, p_rep_list, p_priority_df, p_hex_dir):
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
    p_copy_to_hex = None

    if p_tag == 'common':

        PRINT_CLUB_LIST = ['common', 'master']
        value = p_rep_list['common']
        ss_name = value['club_file_short'].split('/')[-1]
        pr_title = '{}Matrix report for "{}"'.format(10*' ', ss_name)

        short_name = ss_name.split('.')[0]
        priority_title = as_find_priority(p_priority_df, short_name)

        if 'verification Hex' in priority_title:
            if p_hex_dir is not None:
                p_copy_to_hex = p_hex_dir + '/common'

        pr_title += priority_title
        as_pr(fptr, '')
        as_pr(fptr, pr_title)

    else:

        PRINT_CLUB_LIST = ['raa', 'ract', 'aant', 'rac', 'racq', 'master']
        pr_title = '{}Matrix report for "{}"'.format(10*' ', p_tag)
        priority_title = as_find_priority(p_priority_df, p_tag)

        if 'verification Hex' in priority_title:
            if p_hex_dir is not None:
                p_copy_to_hex = p_hex_dir

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

    as_display_files(p_rep_list, fptr, p_copy_to_hex)

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


def as_display_files(p_rep_list, fptr, p_copy_to_hex):
    """ display the files for this report """

    print_blank = True
    for key, value in p_rep_list.items():
        # club_file_short
        if print_blank:
            as_pr(fptr, '')
            print_blank = False

        l_filename = value['club_file_full']
        as_pr(fptr, '   FILE: "{}"'.format(l_filename))
        if p_copy_to_hex is not None:
            target_file = p_copy_to_hex + '/' + value['club_file_short']
            alib.p_i('Hex file: {}'.format(target_file))
            dir_loc = os.path.dirname(target_file)
            alib.dir_create(dir_loc)
            shutil.copy2(l_filename, dir_loc)


# --------------------------------------------------------------------
#
#                          analyse shallow
#
# --------------------------------------------------------------------


def analyse_shallow(p_work_dict, p_priority_df, p_hex_dir):
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

            as_generate_report(l_tag, l_file_ptr, rep_list, p_priority_df, p_hex_dir)

        for l_tag in OTHER_TAGS:
            rep_list = {}
            for key, value in p_work_dict.items():
                if value['tag'] == l_tag:
                    club = value['club']
                    rep_list[club] = value

            as_generate_report(l_tag, l_file_ptr, rep_list, p_priority_df, p_hex_dir)

        for key, value in p_work_dict.items():
            rep_list = {}
            if value['tag'] is None:
                rep_list['common'] = value
                as_generate_report('common', l_file_ptr, rep_list, p_priority_df, p_hex_dir)

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

    parser.add_argument('--hex_dir',
                        help='directory put the hex files',
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

    if args['all_dir'] is not None:
        args['all_dir'] = alib.validate_dir_param(args['all_dir'])

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

    if args['hex_dir'] is None:
        l_hex = None
    else:
        l_hex = args['hex_dir']

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

    analyse_shallow(work_dict, priority_list_df, l_hex)

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
