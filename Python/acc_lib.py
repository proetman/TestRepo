"""
RACQ Lib.py
Generic RACQ Library
"""
import logging

import collections

import mmap
import os
import socket
import platform
import tabulate
import difflib

import re
import sys
import time
from datetime import datetime
import pyodbc
import pandas as pd
import numpy as np
# import racq_conn_lib as rqconnlib

# --------------------------------------------------------------------
#
#                          Constants
#
# --------------------------------------------------------------------

# global dict TEST_RESULT_DIRS is created by fetch_log_dirs and contains the following entried:
# TEST_RESULT_DIRS['log']
# TEST_RESULT_DIRS['data']
# TEST_RESULT_DIRS['error']
# TEST_RESULT_DIRS['commands']
# TEST_RESULT_DIRS['reports']

SUCCESS = 0
FAIL_GENERIC = -1
FAIL_NO_DB_CONNECT = -2
DAYS_FOR_DEEP_ANALYSE = 5

EMAIL_DEFAULT = 'paul.roetman@accnational.com.au,marinos.stylianou@racq.com.au'

# RSReservedWords =  [ 'slartibartfast' ]
# below is the "official" list of Redshift reserved words from
#  http://docs.aws.amazon.com/redshift/latest/dg/r_pg_keywords.html
# The following words are not were not in the list but Bryte thinks
# they are so they have been added:
#
# INTERVAL

DB_TYPE_REDSHIFT = 'RedShift'
DB_TYPE_ORACLE = 'Oracle'
DB_TYPE_MSSQL = 'MSSQL'

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


MAIL_HEADER = """\
<html>
<body>
<pre style="font: monospace">
"""

MAIL_HEADER_WITH_BORDER = """\
<html>
<body>
<style>
table {
    border-collapse: collapse;
}
table, th, td {
    border: 1px solid black;
}
</style>
<pre style="font: monospace"
>
"""

MAIL_FOOTER = """\
</pre>
</body>
</html>
"""

MAIL_FROM_USER = 'noreply@racq.com.au'

# --- Compare
# --------------------------------------------------------------------
#
#        Tab Compare no chunks
#
# --------------------------------------------------------------------


def tab_compare_df(ps_df, pt_df, p_tab):
    """
    Compare the results from two SQL statements.

    assumptions:
        * Column names HAVE to be IDENTITICAL between the two queries
        * the sql you pass in, is appropriate for the database connecting to.

    args:
        * ps_df   -- Source data (from file)
        * pt_df   -- Target data (from db)
        * p_tab   -- table name for display purposes only.

    returns:
        * True - the tables are identical
        * False - the tables are not identical
        Any differences are reported to log_error.
    """
    log_info('')
    log_info('Comparing table {}'.format(p_tab))

    source_df = ps_df
    target_df = pt_df

    len_s = len(source_df.index)
    len_t = len(target_df.index)

    p_i('Count of source data records = {}'.format(len_s))
    p_i('Count of target data records = {}'.format(len_t))

    if len_s == 0 and len_t == 0:
        log_info('Source and target table {} are empty'.format(p_tab))
        return True, None, None

    s_rem_df = pd.DataFrame(data=None, index=None, columns=source_df.columns)
    t_rem_df = pd.DataFrame(data=None, index=None, columns=source_df.columns)

    s_rem_df.columns = s_rem_df.columns.str.upper()
    t_rem_df.columns = t_rem_df.columns.str.upper()

    # --- Setup timing

    record_counter = len(s_rem_df.index)
    p_d('Remove Matched rows')

    s_rem_df, t_rem_df = tab_compare_del_common(source_df, target_df, p_run_preprocessor=False)

    # --- Finished comparison, start testing the remainder.

    res = False

    if (len(s_rem_df.index) == 0) and (len(t_rem_df.index) == 0):
        log_debug('s_rem_df.len = 0 and t_rem_df.len = 0')
        res = True

    s_rem_df.reset_index(inplace=True, drop=True)
    t_rem_df.reset_index(inplace=True, drop=True)

    if res:
        log_info('Count of rows compared after removing dupplicates = {}'.
                 format(record_counter))
    else:
        log_info('INFO - the result from the two sql statements is different')
        tab_compare_log_diff(s_rem_df, t_rem_df, p_tab)

    return res, s_rem_df, t_rem_df

# --------------------------------------------------------------------
#
#        push different rows to remainder
#
# --------------------------------------------------------------------


def tab_compare_del_common(p_s_df, p_t_df, p_run_preprocessor=False):
    """
    Removes common rows from 2 data frames with the option to preprocess the frames
    """

    if isinstance(p_s_df, pd.DataFrame) and isinstance(p_t_df, pd.DataFrame):
        pass
    else:
        log_error('ERROR: Unable to del common rows between dataframes, as ')
        log_error(' one of the data frames passed in is probably a series.')
        log_error('size of p_s_df = {}'.format(p_s_df.shape))
        log_error('size of p_t_df = {}'.format(p_t_df.shape))
        return(p_s_df, p_t_df)

    #    if p_run_preprocessor:
    #        pre_process_source(p_s_df)
    #        pre_process_target(p_t_df)

    if len(p_s_df.index) == 0 and len(p_t_df.index) == 0:
        return p_s_df, p_t_df

    try:
        # First get a list of match exact
        cols = list(p_t_df.columns)
        common_rows = pd.merge(p_s_df, p_t_df, on=cols, how='inner').drop_duplicates()

        # Add a flag to highlight which rows are do be removed
        common_rows['key'] = 'common row to be removed'

        # Run a left join on the two df, rows that are common will get the
        # extra column (key = common row to be remove)
        # Delete any row where the key is NOT NULL, then drop the key column
        p_s_df_new = pd.merge(p_s_df, common_rows, on=cols, how='left')
        p_s_df_new = p_s_df_new[p_s_df_new['key'].isnull()]
        p_s_df_new.drop('key', axis=1, inplace=True)

        # Repeat on target df
        p_t_df_new = pd.merge(p_t_df, common_rows, on=cols, how='left')
        p_t_df_new = p_t_df_new[p_t_df_new['key'].isnull()]
        p_t_df_new.drop('key', axis=1, inplace=True)

        # return the new dataframes, without the common rows between them.
        return p_s_df_new, p_t_df_new

    except MemoryError as err:
        log_error('ERROR - Unable to compare and delete source and target.')
        log_error('        memory failed when removing common records between 2 data frames')
        log_error('        aborting table comparison')
        log_error(err)
        return p_s_df, p_t_df

    except TypeError as err:
        log_error('ERROR - Unable to compare and delete source and target.')
        log_error('        Normally due to unhashable type (bytearray)')
        log_error('        Please compare manually')
        log_error('        aborting table comparison')
        log_error(err)
        return p_s_df, p_t_df


# ---------------------------------------------------------------------
#
#                 pandas log difference
#
# ---------------------------------------------------------------------


def tab_compare_log_diff(p_pd1, p_pd2, p_tab):
    """
    Log the differences between two pandas to log_error

    Parameters

       * pd1   : source panda
       * pd2   : target panda
       * p_pk  : list of primary key columns, eg ['col1','col2']
       * p_tab : one of the table names (for reporting only)
    """
    log_debug('Start tab_compare_log_diff')

    e_template = 'rownum [{vR:5}] column [{vF:20}] '
    e_template += 'source: [{v_source}] target: [{v_target}]'

    smaller_size = min(len(p_pd1.index), len(p_pd2.index))

    pd1 = p_pd1[:smaller_size]
    pd2 = p_pd2[:smaller_size]

    pd1.columns = pd1.columns.str.upper()
    pd2.columns = pd2.columns.str.upper()

    pd1_not_printed = p_pd1[smaller_size:]
    pd2_not_printed = p_pd2[smaller_size:]

    pd1.reset_index(inplace=True)
    pd2.reset_index(inplace=True)

    log_debug('count source = ({}), count target = ({})'.format(len(p_pd1.index),
                                                                len(p_pd2.index)))

    if smaller_size == 0:
        p_e('Either source or target count is 0 after matched rows are removed for {}'
            .format(p_tab))
    else:
        log_debug('Try a compare')

        try:
            # First, create a matrix of rows that are different (true = same, false = diff)
            eql = ((pd1 == pd2) | ((pd1 != pd1) & (pd2 != pd2)))
            # pivot the column headings to the row index (from 1 row per diff,
            #                                                to 1 row per diff per col)
            eq_stacked = eql.stack()
            # extract only the rows where there is a difference
            eq_changed = eq_stacked[eq_stacked == False]

            # difference_locations = np.where(pd1 != pd2)
            difference = np.where(eql == False)
            changed_from = pd1.values[difference]
            changed_to = pd2.values[difference]

        except TypeError as error:
            # this error occures when pd1 and pd2 are very very different in nature.
            # as such, one part of the data may be all numeric, and another all string
            # the difference program freaks out and crashes when comparing.
            log_error('ERROR - Differences found in result sets, but they are too great')
            log_error('        for the logging program to print. Please review manually.')
            log_error('        This difference occurs when the two data sets have different')
            log_error('        types, and cannot be compared programatically.')

            return

        if (changed_from.size == 0 and
                changed_to.size == 0 and
                len(pd1_not_printed.index) == 0 and
                len(pd2_not_printed) == 0):

            log_error('WARNING: Hmmm difference reported, but no difference found')
            log_error('         time to panic. Table {}'.format(p_tab))
            return
        else:

            log_error('')
            log_error('{} Difference found'.format(p_tab))
            log_error('/--------------------------------------------------------------\\')

            diff = pd.DataFrame({'from': changed_from, 'to': changed_to}, index=eq_changed.index)
            # print(diff)
            print_counter = 0
            count_errors = len(diff.index)

            for index, row in diff.iterrows():
                # Only print the first 100 errors per table.
                if print_counter > 100:
                    log_error('ERROR - Too many errors, only displaying first 100 out of {}'
                              .format(count_errors))
                    break

                row_number = index[0]
                row_column = index[1]
                row_error_from = row['from']
                row_error_to = row['to']

                if(isinstance(row_error_from, (bytearray, bytes)) or
                   isinstance(row_error_to, (bytearray, bytes))):
                    log_error('Cannot display error, non-printable characters')
                else:
                    log_error(e_template.format(vR=row_number,
                                                vF=row_column,
                                                v_source=row_error_from,
                                                v_target=row_error_to))
                log_error('')
                print_counter += 1

            log_error('Total error count for {} - {}'
                      .format(p_tab, count_errors))

            log_error('\\--------------------------------------------------------------/')

    # endif smallersize != 0

    if len(pd1_not_printed.index) > 0:
        if len(pd1_not_printed.index) > 100:
            p_top_100 = 100
        else:
            p_top_100 = len(pd1_not_printed.index)
        print_df = pd1_not_printed.select_dtypes(exclude=['object'])
        printstr = tabulate.tabulate(print_df.head(n=p_top_100), headers='keys',
                                     tablefmt='psql',
                                     floatfmt='0.2f')
        log_error('       records compared after matched rows removed = [{}]'
                  .format(smaller_size))
        log_error('SOURCE records not included in DIFF due to size mismatch. Showing ' +
                  'max 100 out of [{}]\n{}'
                  .format(len(pd1_not_printed.index), printstr))

    if len(pd2_not_printed.index) > 0:
        if len(pd2_not_printed.index) > 100:
            p_top_100 = 100
        else:
            p_top_100 = len(pd2_not_printed.index)
        print_df = pd2_not_printed.select_dtypes(exclude=['object'])
        printstr = tabulate.tabulate(print_df.head(n=p_top_100), headers='keys',
                                     tablefmt='psql',
                                     floatfmt='0.2f')
        log_error('       records compared after matched rows removed = [{}]'
                  .format(smaller_size))
        log_error('TARGET records not included in DIFF due to size mismatch. Showing ' +
                  'max 100 out of [{}]\n{}'
                  .format(len(pd2_not_printed.index), printstr))

    return

# --- Reports
# --------------------------------------------------------------------
#
#                          dump short names to file
#
# --------------------------------------------------------------------


def print_filenames(p_work_files):
    """ Print all the short names of the club files """

    p_i('')
    p_i('{:30} {:40} {:10}'.format('Tag', 'File', 'Club'))
    p_i('{:30} {:40} {:10}'.format(30 * '-', 40 * '-', 10 * '-'))
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
        log_info('{:30} {:40} {:10}'.format(l_tag, curr_file, l_club))
    p_i('')

# --- file operations
# --------------------------------------------------------------------
#
#                          initialise
#
# --------------------------------------------------------------------


def open_ss(p_ss):
    """ open spreadhseet, save as df """
    log_debug('Open spreadsheet {}'.format(p_ss))

    if p_ss is None:
        return None

    l_ss = p_ss.replace('\\', '/')

    try:
        ss_df_dict = pd.read_excel(l_ss,
                                   sheetname=None,
                                   index_col=None)

    except FileNotFoundError as err:
        p_e('Function open_ss, spreadsheet not found.')
        p_e('       speadsheet: [{}]'.format(format_filename(p_ss)))
        p_e('\n       error text [{}]'.format(err))
        ss_df_dict = None

    except Exception as err:
        p_e('\nGeneric exception in function open_ss.')
        p_e('       spreadsheet: [{}]'.format(format_filename(p_ss)))
        p_e('\n       error text [{}]'.format(err))
        ss_df_dict = None

    if ss_df_dict is not None:
        for tab in ss_df_dict:
            l_df = ss_df_dict[tab]
            if len(l_df.columns) > 0:
                l_df.columns = l_df.columns.str.upper()

    return ss_df_dict

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

    p_i('')
    for key, value in work_dict.items():
        if value['master_file_short'] is None:
            p_e('club file with no master  [{}]'.format(value['club_file_short']))
    p_i('')

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

    home_dir = os.environ['USERPROFILE'].replace('\\', '/')
    default_sync_dir = home_dir + '/AUSTRALIAN CLUB CONSORTIUM PTY LTD/'
    default_sync_dir += 'Phase 3 - Deploy Phase - Phase 3/CARS Data and Data Management'

    default_sync_dir = 'C:/work/stuff/all_2017_apr_18'
    default_sync_dir = 'C:/work/stuff/all_2017_apr_19__1540'

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

# --- Sql Server
# --------------------------------------------------------------------
#
#                          fetch tab col data
#
# --------------------------------------------------------------------


def fetch_tab_col(p_db_conn):
    """ fetch all tables and columns """

    sql = """
        SELECT  UPPER(c.table_schema)          AS "schema",
                UPPER(c.table_name)            AS "table",
                UPPER(c.column_name)           AS "column",
                c.data_type                    as "data_type",
                c.ORDINAL_POSITION             as "col_id",
                case
                    when pk.COLUMN_NAME = c.column_name THEN 'Y'
                    ELSE 'N'
                END                            as "pk",
                case is_nullable
                    when 'NO'     then 'Y'
                    else               'N'
                end                            as "mandatory"
         FROM information_schema.columns c
             LEFT OUTER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE pk
                 ON  OBJECTPROPERTY (OBJECT_ID (pk.CONSTRAINT_SCHEMA + '.' +
                                     pk.CONSTRAINT_NAME),'IsPrimaryKey') = 1
                 AND pk.TABLE_NAME = c.table_name
                 AND pk.COLUMN_NAME =  c.column_name
                 AND pk.TABLE_SCHEMA = c.table_schema
                        """

    dd_df = read_table_data(p_db_conn, sql, p_description='Schema')

    return dd_df

# --------------------------------------------------------------------
#
#                          fetch columns for table
#
# --------------------------------------------------------------------


def fetch_columns(p_df, p_tab):
    """ fetch columns for this table """

    log_debug('start fetch columns for table {}'.format(p_tab))

    tab_ind = p_df['TABLE'] == p_tab.upper()

    col_df = p_df[tab_ind]
    new_df = col_df.sort_values(['COL_ID'])
    new_df.reset_index(drop=True, inplace=True)
    return new_df

# --- Read Data from database
# --------------------------------------------------------------------
#
#        Read Data into Dataframe
#
# --------------------------------------------------------------------


def read_table_data(p_conn, p_sql,
                    p_description=None,
                    p_display_info=True,
                    p_force_uppercase_headings=True):
    """
    xx Read table data and load into dataframe
    Any issues, and it returns None

    """
    log_debug('start read table data, sql = [{}]'.format(p_sql))

    if p_sql is None:
        print('No SQL passed into function, returning None')
        return None

    if p_description is None:
        l_desc = '        Read data'
    else:
        l_desc = '        Read {} data'.format(p_description)

    if p_display_info:
        p_i(l_desc, p_end=" ")
        sys.stdout.flush()

    try:
        l_df = pd.read_sql(p_sql, p_conn)

    except pd.io.sql.DatabaseError as err:
        p_e('ERROR raised running SQL, please manually review')
        p_e('      error text: {}'.format(err))
        odbc_sql_exec(p_conn['conn'], 'rollback')
        return None

    if p_force_uppercase_headings:
        l_df.columns = l_df.columns.str.upper()

    if p_display_info:
        count_rows = len(l_df.index)
        p_i('{} rows'.format(count_rows))

    log_debug('    read table data, row count fetch = [{}]'.format(len(l_df.index)))

    return l_df

# --- DB Connect
# --------------------------------------------------------------------
#
#                          connect to sql server
#
# --------------------------------------------------------------------


def db_connect_mssql(p_con):
    """
    Create the connect string for python to connect to sql server db.
    Examples on how to create odbc connects can be found here
        http://www.visokio.com/kb/db/dsn-less-odbc
    Data is pulled from Alias and User file to create this connection.
    Returns None on fail.
    """

    l_host = p_con['host']
    l_instance = p_con['instance']
    l_schema = p_con['schema']
    l_db = p_con['db']

    e_template = 'ERROR: Failed to connect to MSSQL Database. '
    e_template += ' Server={vHost}\{vI}; Database={vDB}'
    e_template += ' connect string: {vStr}'

    connect_template = r'Driver={{SQL Server Native Client 11.0}};Server={vHost}\{vI};'
    connect_template += 'Database={vDB};Trusted_Connection=yes;'

    connect_template = r'Driver={{SQL Server Native Client 11.0}};Server={vHost}\{vI};'
    connect_template += 'Database={vDB};Trusted_Connection=yes;'

    connect_template = r'Driver={{SQL Server}};Server={vHost}\{vI};'
    connect_template += 'Database={vDB};Trusted_Connection=yes;'

    connect_str = connect_template.format(vHost=l_host,
                                          vI=l_instance,
                                          vDB=l_schema)

    try:

        db_conn = pyodbc.connect(connect_str)

    except pyodbc.ProgrammingError as err:
        print(err)
        error_msg = e_template.format(vHost=l_host,
                                      vStr=connect_str,
                                      vDB=l_db)
        p_e(error_msg)
        p_e(err)
        return None

    except pyodbc.DatabaseError as err:
        print(err)
        error_msg = e_template.format(vHost=l_host,
                                      vStr=connect_str,
                                      vDB=l_db)
        p_e(error_msg)
        p_e(err)
        return None

    except pyodbc.Error as err:
        print(err)
        error_msg = e_template.format(vHost=l_host,
                                      vStr=connect_str,
                                      vDB=l_db)
        p_e(error_msg)
        p_e(err)
        return None

    return db_conn

# --------------------------------------------------------------------
#
#              run ODBC (Redshift/MS SQL) SQL  fetch data and count
#
# --------------------------------------------------------------------


def odbc_sql_fetch(p_conn, p_sql, p_arraysize=10000):
    """
    Execute Redshift or MSSQL SQL statement, return the rows as a list.
    returns None if there is no data found.
    """

    log_debug('Start odbc sql fetch, sql = [{}]'.format(p_sql))

    e_template = '\nracq_conn_lib.odbc_sql_fetch failed when executing sql : {vSQL}'
    e_template += '\nERROR: during data fetch: err = {vErr}'

    try:
        cur = p_conn.cursor()
        cur.arraysize = p_arraysize
        cur.execute(p_sql)
        rows = cur.fetchall()
        len_rows = len(rows)

    except pyodbc.Error as err:
        log_critical(e_template.format(vSQL=p_sql, vErr=err))
        odbc_sql_exec(p_conn, 'rollback')
        rows = None
        len_rows = 'None'

    log_debug('finish odbc sql fetch, row count = {}'.format(len_rows))

    cur.close()
    return rows

# --------------------------------------------------------------------
#
#                          odbc sql exec
#
# --------------------------------------------------------------------


def odbc_sql_exec(p_conn, p_sql):
    """
    Execute SQL Statement
    return rowcount
    """

    log_debug('    odbc execute SQL: [' + p_sql + '].')

    try:
        cur = p_conn.cursor()

        cur.execute(p_sql)
        rowcount = cur.rowcount
        cur.close()

    except pyodbc.ProgrammingError as err:
        cur.close()
        odbc_sql_exec(p_conn, 'rollback')
        log_debug('    result FAIL')
        log_critical("\nexec_sqlddl  Error({0})".format(err))
        rowcount = None

    except pyodbc.Error as err:
        cur.close()
        odbc_sql_exec(p_conn, 'rollback')
        log_critical("\nexec_sqlddl  ProgrammingError({0})".format(err))
        log_critical("\nexec_sqlddl  Failed executing sql : " + p_sql)
        rowcount = None

    if rowcount is None:
        log_debug('    execute ok, return rowcount = None')
    else:
        log_debug('    execute ok, return rowcount = {}'.format(rowcount))

    return rowcount

# --- Init
# --------------------------------------------------------------------
#
#                          init (every program should run this)
#
# --------------------------------------------------------------------


def init_app(p_args, p_print_date=True):

    global g_params
    # g_params = {}

    g_params = p_args

#    systest_conn_d = rqconnlib.db_conn_syst()
#    if systest_conn_d is None:
#        return False

    g_params['run_date_str'] = time.strftime("%d/%m/%Y")
    g_params['run_time_str'] = time.strftime("%H:%M:%S")
#    g_params['syst_conn_d'] = systest_conn_d
    g_params['osuser'] = os.getlogin()
    g_params['local_host'] = socket.gethostname()

    if p_print_date:
        # Display the run date, this will end up in the .out files to delimit each days run.
        run_datetime = time.strftime("%d-%M-%Y %H:%M:%S")
        p_i('Run Date: {}'.format(run_datetime))

    return True

# --------------------------------------------------------------------
#
#                          init (every program should run this)
#
# --------------------------------------------------------------------


def init():
    global g_params
    g_params = {}

    # Display the run date, this will end up in the .out files to delimit each days run.
    run_datetime = time.strftime("%d-%M-%Y %H:%M:%S")
    p_i('Run Date: {}'.format(run_datetime))

    return

# --------------------------------------------------------------------
#
#                          safe upper
#
# --------------------------------------------------------------------


def safe_upper(p_str):
    """ Convert str to upper case"""
    new_str = None
    if p_str is not None:
        if isinstance(p_str, str):
            new_str = p_str.upper()

    return new_str


def safe_lower(p_str):
    """ Convert str to lower case"""
    new_str = None

    if p_str is not None:
        if isinstance(p_str, str):
            new_str = p_str.lower()

    return new_str


# --- OS paths etc
# --------------------------------------------------------------------
#
#                          fetch arg numb
#
# --------------------------------------------------------------------


def fetch_arg_numb(p_name):
    """
    Find the number for this argument.
    return the number, and the value (if it exists)
    """
    argind = 0

    try:

        while sys.argv[argind] != p_name:
            argind += 1

    except IndexError:
        argind = 0

    if argind > 0 and argind < len(sys.argv) - 1:
        retval = sys.argv[argind + 1]
    else:
        retval = None

    return argind, retval

# --------------------------------------------------------------------
#
#                          Fetch arg Name
#
# --------------------------------------------------------------------


def fetch_arg_name():

    # -- Load TAB

    argind, retval = fetch_arg_numb('--tab')

    # -- if that fails, try loading account name
    if argind == 0:
        argind, retval = fetch_arg_numb('--account_name')

    # -- if that fails, try loading account name
    if argind == 0:
        argind, retval = fetch_arg_numb('--account')

    # -- if that fails, try loading run_all_accounts
    #    if argind == 0:
    #        argind, retval = fetch_arg_numb('--run_all_accounts')

    if argind == 0:
        print('ERROR: must select at least one of these parameters:')
        print('          tab, account_name or account')
        print('      aborting.')
        retval = None
    else:
        print('')
        print('    Process: {}'.format(retval))
        print('')

    return retval

# --------------------------------------------------------------------
#
#                          Fetch arg Name
#
# --------------------------------------------------------------------


def fetch_arg_name2():

    argind, retval = fetch_arg_numb('--help')
    if argind != 0:
        return 'help'

    argind, retval = fetch_arg_numb('--account')

    if argind == 0:
        print('ERROR: must select an ACCOUNT to run:')
        print('      aborting.')
        retval = None
    else:
        print('')
        print('    Process: {}'.format(retval))
        print('')

    return retval


# --------------------------------------------------------------------
#
#                          Setup RESULT dir
#
# --------------------------------------------------------------------


def setup_result_dir(p_root, p_dir):
    """
    Create dir if required.
    Add dir to global arrary.
    """

    new_path = p_root + p_dir
    if dir_create(new_path):
        TEST_RESULT_DIRS[p_dir] = new_path

    return

# --------------------------------------------------------------------
#
#                          log dir fetch
#
# --------------------------------------------------------------------


def fetch_log_dirs():
    """ Fetch a list of subfolder names to be used for logging
    If needed, create the subfolders"""

    # this is generally called during initialise, so debugging has not
    # yet been enabled.

    # log_debug('Start log fetch_dirs')

    l_root = os.environ['SystemDrive'] + '/test_results/'

    global TEST_RESULT_DIRS
    TEST_RESULT_DIRS = {}

    setup_result_dir(l_root, 'log')
    setup_result_dir(l_root, 'data')
    setup_result_dir(l_root, 'commands')
    setup_result_dir(l_root, 'error')
    setup_result_dir(l_root, 'reports')
    setup_result_dir(l_root, 'tmp')

    #    new_path = l_root + 'log'
    #    if dir_create(new_path):
    #        TEST_RESULT_DIRS['log'] = new_path

    # this is generally called during initialise, so debugging has not
    # yet been enabled.

    # log_debug('End log dir fetch, returning {}'.format(TEST_RESULT_DIRS))

    return TEST_RESULT_DIRS


# --- OS db Commands
# --------------------------------------------------------------------
#
#                          Search File for String (RE)
#
# --------------------------------------------------------------------


def file_search(curr_file_name, search_string):
    """
    Search for a string in a file with regular expression search
    Return Codes
        True - String Found
        False - String Not Found

        If the file does not exist, or there is any other fail, return False

    Note: cannot do a case insensitive search on an entire file.
    """
    # Test if the file exists
    # Test if the file has size > 0 (mmap failes on filesize == 0)

    if os.path.isfile(curr_file_name) and os.stat(curr_file_name).st_size > 0:

        try:
            my_file = open(curr_file_name, 'r+')
        except FileNotFoundError as err:
            print('ERROR: File not found in fileSearch')
            print('       filename : {}'.format(curr_file_name))
            print('       error details : {}'.format(err))
            return False

        search_string_bytes = bytes(search_string, 'utf-8')
        data = mmap.mmap(my_file.fileno(), 0)
        my_search_result = re.search(search_string_bytes, data)

        my_file.close()

        if my_search_result:
            return True

    return False

# --------------------------------------------------------------------
#
#                          Rename File
#
# --------------------------------------------------------------------


def file_rename(p_old_file, p_new_file):
    """
    Rename file OldFileName -> NewFileName
         file_rename(Old,New)
    If the new file already exists, return False.
    If the old file does not exist, return False.
    If there is a catastrophic fail, exit.
    Otherwise - return True!
    """

    log_debug('start file_rename')

    if os.path.isfile(p_new_file):
        return False

    if os.path.isfile(p_old_file):

        try:
            os.rename(p_old_file, p_new_file)

        except OSError as err:
            print("\nFailed to rename file {0} to {1}, aborting\n".
                  format(p_old_file, p_new_file))
            print("OS Error({0}): {1}".format(err.errno, err.strerror))
            print("\n")
            exit(2)

        return True

    return False

# --------------------------------------------------------------------
#
#                          Remove File
#
# --------------------------------------------------------------------


def file_remove(p_file):
    """
    Delete file
    """
    log_debug('start file_remove')

    if os.path.isfile(p_file):

        try:
            os.remove(p_file)

        except OSError as err:
            print("\nFailed to remove file :{0}, aborting\n")
            print("OS Error({0}): {1}".format(err.errno, err.strerror))
            print("\n")
            exit(2)


# --------------------------------------------------------------------
#
#                          create empty file
#
# --------------------------------------------------------------------


def file_touch(path):
    """
    update timestamp of a file
    or create empty file.
    """
    with open(path, 'a'):
        os.utime(path, None)

# --------------------------------------------------------------------
#
#                          tail
#
# --------------------------------------------------------------------


def file_tail(file_name):
    """
       Read the last line of a file
    """
    try:
        my_file = open(file_name, 'r')
        try:
            result = collections.deque(my_file, 1).pop()
        except IndexError as coll_err:
            print('ERROR Reading last line of file')
            print('       error details : {}'.format(coll_err))
            result = ''
        my_file.close()

    # File does not exist (yet), must be first run!
    except FileNotFoundError:
        result = ''

    return result
# --------------------------------------------------------------------
#
#                          fetch python root dir
#
# --------------------------------------------------------------------


def fetch_root_dir():
    """ Fetch the location of the directory "Final"   """

    python_root_dir = os.environ['PYTHONPATH'][:-9]
    python_root_dir = python_root_dir.replace('\\', '/')

    return python_root_dir

# --------------------------------------------------------------------
#
#                          fetch final dir
#
# --------------------------------------------------------------------


def fetch_final_dir():
    """ Fetch the location of the directory "Final"   """

    log_debug('Fetch final dir')
    result = '{}/{}'.format(fetch_root_dir(), 'Final')
    log_debug('  final dir [{}]'.format(result))

    return result

# --------------------------------------------------------------------
#
#                          fetch final dir
#
# --------------------------------------------------------------------


def fetch_info_class_dir():
    """ Fetch the location of the directory "Final"   """

    log_debug('Fetch info class dir')
    result = '{}/{}'.format(fetch_root_dir(), 'Final/classification_validation')
    log_debug('  info class dir [{}]'.format(result))

    return result


# --------------------------------------------------------------------
#
#                          safe open
#
# --------------------------------------------------------------------


def safe_open(p_file_name, p_mode):
    """
    Open the file.
    print error message to log file if fail.
    """
    try:
        result = open(p_file_name, p_mode)

    # File does not exist (yet), must be first run!
    except IOError as e:
        result = None
        p_e('Failed to open file [{}]'.foramt(p_file_name))
        p_e('    error raised: {}'.format(e))

    return result


# --------------------------------------------------------------------
#
#                          generate file name
#
# --------------------------------------------------------------------
# Create a full path file name from the directory and the file name
#   only here as we have multiple OS to work with.


def file_generate_name(p_dir, p_file):
    """
    create filename with respect to OS considerations.
    """

    log_debug('start gen_filename')
    return p_dir + '/' + p_file

# --------------------------------------------------------------------
#
#                          Return Date Time string for filename
#
# --------------------------------------------------------------------


def now():
    """
    Return Date_Time as a string %Y%m%d_%H%M%S
    """
    return time.strftime("%Y%m%d_%H%M%S")

    # --------------------------------------------------------------------
#
#                          Return Date Time with MS string for filename
#
# --------------------------------------------------------------------


def now_with_ms():
    """
    Return Date_Time as a string %Y%m%d_%H%M%S_ms
    """

    curr = datetime.now()
    microsec = curr.microsecond
    return '{}_{}'.format(time.strftime("%Y%m%d_%H%M%S"), microsec)

# --------------------------------------------------------------------
#
#                          is Windoze
#
# --------------------------------------------------------------------


def is_windows():
    """
    Determine if running on windows box
    """
    log_debug('start isWindows')

    return platform.system() == 'Windows'

# --------------------------------------------------------------------
#
#                          Remove Dir
#
# --------------------------------------------------------------------


def dir_remove(p_dir):
    """
    remove directory
    Assumes directory is empty
    Full path name required.
    """
    log_debug('start remove_dir')

    if os.path.isdir(p_dir):

        try:
            os.rmdir(p_dir)

        except OSError as err:
            print("\nFailed to remove directory :{0}, aborting\n")
            print("OS Error({0}): {1}".format(err.errno, err.strerror))
            print("\n")
            exit(2)

# --------------------------------------------------------------------
#
#                          fetch Work Dir
#
# --------------------------------------------------------------------


def dir_fetch_workdir():
    """
    Fetch the current working directory
    """
    log_debug('start fetchwork_dir')
    work_dir = TEST_RESULT_DIRS['log']
    if is_windows():
        work_dir = work_dir.replace('\\', '/')

    return work_dir

# --------------------------------------------------------------------
#
#                          Create Dir
#
# --------------------------------------------------------------------


def dir_create(p_dir):
    """
    Create Directory
    """
    log_debug('start create_dir')

    if os.path.isdir(p_dir):
        return True
    else:
        try:
            os.makedirs(p_dir)

        except OSError as err:
            log_warning("\nFailed to create directory :{0}, aborting\n")
            log_warning("OS Error({0}): {1}".format(err.errno, err.strerror))
            log_warning("\n")
            return False

        except PermissionError as err:
            p_e('Failed to create LOG directory: {}'.format(p_dir))
            p_e('OS error: {}'.format(err))
            return False

    return True

# --------------------------------------------------------------------
#
#                          validate args
#
# --------------------------------------------------------------------


def args_validate(p_parser, p_log_file):
    """
    Validate program arguments
    """
    # Setup Arguments and parse them

    args = vars(p_parser.parse_args())

    # Setup Logging

    logging_level = log_level(args['debug'])

    if logging_level is not None:
        # Bug in spyder v2 and v3.
        # http://stackoverflow.com/questions/24259952/logging-module-does-not-print-in-ipython
        root = logging.getLogger()
        for handler in root.handlers[:]:
            root.removeHandler(handler)

        logging.basicConfig(level=logging_level,
                            format='%(asctime)s %(levelname)-8s %(message)s',
                            datefmt='%a, %d %b %Y %H:%M:%S',
                            filename=p_log_file,
                            filemode='w')

        log_info('\n')
        log_info('rs_lib.validate_args: Logging level: [{}]'.format(logging_level))

        print("\nprogram log_file: " + p_log_file.replace('/', '\\') + "\n")

    # print the arguments to log file at level INFO
    log_args(args)

    return args

# --- Create Mail

# --------------------------------------------------------------------
#
#                         write line to file
#
# --------------------------------------------------------------------


def write_line_to_file(p_file, p_line, p_include_timestamp):
    """
    Write the line to the file, including timestamp if requested.
    """
    if p_include_timestamp:
        p_file.write(p_line)
    else:
        p_file.write(p_line[35:])

    return


# --------------------------------------------------------------------
#
#                         write_line_containing
#
# --------------------------------------------------------------------


def write_line_containing(p_source_txt, p_target_txt, p_text, p_include_timestamp=True):
    """
   Open the source txt file with read only permit  and Read each line.
   if the line contains the supplied text then write that line to the target txt
   """
    l_log = open(p_source_txt)
    line = l_log.readline()
    while line:
        if p_text in line:
            write_line_to_file(p_target_txt, line, p_include_timestamp)
        line = l_log.readline()
    l_log.close()
    return

# --------------------------------------------------------------------
#
#                         write_line_containing
#
# --------------------------------------------------------------------


def write_line_not_containing(p_source_txt, p_target_txt, p_text_dict, p_include_timestamp=True):
    """
   Open the source txt file with read only permit  and Read each line.
   if the line contains the supplied text then write that line to the target txt
   """
    l_log = open(p_source_txt)
    line = l_log.readline()
    while line:

        include_line = True

        for txt in p_text_dict:
            if txt in line:
                include_line = False
                break

        if include_line:
            write_line_to_file(p_target_txt, line, p_include_timestamp)

        line = l_log.readline()

    l_log.close()

    return

# --------------------------------------------------------------------
#
#                         write_chunk_from_to
#
# --------------------------------------------------------------------


def write_chunk_from_to(p_source_txt, p_target_txt, p_text_from, p_text_to,
                        p_include_timestamp=True):
    """
   Open the source txt file with read only permit  and Read each line.
   if the line contains the supplied text then write that line to the target txt
   """
    l_log = open(p_source_txt)
    line = l_log.readline()
    while line:
        if p_text_from in line:
            while p_text_to not in line:

                write_line_to_file(p_target_txt, line, p_include_timestamp)

                line = l_log.readline()
        else:
            line = l_log.readline()

    l_log.close()
    return

# --- Logging
# --------------------------------------------------------------------
#
#                          display filename
#
# --------------------------------------------------------------------


def format_filename(p_file):
    """ format filename with backslash, not forward slash """
    return p_file.replace('/', '\\')

# --------------------------------------------------------------------
#
#                          Print and DEbug
#
# --------------------------------------------------------------------


def p_d(p_str, p_end=None):
    """
    print str to screen
    and print to debug
    """
    if p_end is None:
        print(p_str)
    else:
        print(p_str, end=p_end)
    log_debug(p_str)

# --------------------------------------------------------------------
#
#                          Print and Error
#
# --------------------------------------------------------------------


def p_e(p_str, p_end=None, p_before=None, p_after=None):
    """
    print str to screen
    and print to debug
    """
    if p_before is not None:
        for i in range(0, p_before):
            log_error('')

    if p_end is None:
        print(p_str)
    else:
        print(p_str, end=p_end)
    log_error(p_str)

    if p_after is not None:
        for i in range(0, p_after):
            log_error('')

# --------------------------------------------------------------------
#
#                          Print and Error
#
# --------------------------------------------------------------------


def p_c(p_str, p_end=None):
    """
    print str to screen
    and print to debug
    """
    if p_end is None:
        print(p_str)
    else:
        print(p_str, end=p_end)
    log_error(p_str)

# --------------------------------------------------------------------
#
#                          Print and Info
#
# --------------------------------------------------------------------


def p_i(p_str, p_end=None, p_before=None, p_after=None):
    """
    print str to screen
    and print to debug
    """
    if p_before is not None:
        for i in range(0, p_before):
            log_info('')

    if p_end is None:
        print(p_str)
    else:
        print(p_str, end=p_end)
    log_info(p_str)

    if p_after is not None:
        for i in range(0, p_after):
            log_info('')

# --------------------------------------------------------------------
#
#                          Shutdown logging
#
# --------------------------------------------------------------------


def logging_close():
    """
    Close the logging file name
    Mainly used for unit testing, when a log file is opened for each test
    """
    logging.shutdown()
# --------------------------------------------------------------------
#
#                          fetch Work Dir
#
# --------------------------------------------------------------------


def log_filename_determine(p_work_dir, p_program):
    """
    Fetch log file name
    """
    log_debug('start log_filename_determine')

    # Get the time in milliseconds
    curr = datetime.now()
    debug_microsec = curr.microsecond

    debug_datetime = time.strftime("%Y%m%d_%H%M%S")

    log_file_template = '{vwDir}/{vProg}_{vDT}_{vMS}.log'

    log_file = log_file_template.format(vwDir=p_work_dir,
                                        vProg=p_program,
                                        vDT=debug_datetime,
                                        vMS=debug_microsec)
    return log_file

    # This is no longer required, switched over to including microseconds
    # in the file name. That will always be unique.
    #
    # Have done this so that the "sleep 1" commands can be removed from unit testing.
    #
    #    if os.path.isfile(log_file):
    #
    #        # ascii 97,123 is 'a' to 'z'
    #        for character in range(97, 123):
    #            log_file = log_file_template2.format(vwDir=p_work_dir,
    #                                                 vProg=p_program,
    #                                                 vDT=debug_datetime,
    #                                                 vChar=chr(character))
    #
    #            # if the file does not exist, then break out of loop.
    #            # have a filename that can be used for logging.
    #            if not os.path.isfile(log_file):
    #                break


# --------------------------------------------------------------------
#
#                          init log file
#
# --------------------------------------------------------------------


def log_filename_init(over_ride_name=None):
    """
    Determine log file name
       default parameter is none, will then create a file with
       the program name as the basis for the file.
    """
    #
    # Strip off any leading path to the program executable
    #          remove  "c:/tmp" from "c:/tmp/prog.py".
    #          Leaves prog.py
    #
    # Then split of the first part of the program name
    #          prog.py  --> prog
    #
    fetch_log_dirs()
    if over_ride_name is None:
        program_name = sys.argv[0].split("/")[-1]
    else:
        program_name = over_ride_name

    program_base_name = os.path.basename(program_name).split('.')[0]

    # --------------------------------------------
    # Setup Debug Info

    work_dir = dir_fetch_workdir()

    if not dir_create(work_dir):
        return None
    log_filename = log_filename_determine(work_dir, program_base_name)

    return log_filename

# --------------------------------------------------------------------
#
#                          log Level
#
# --------------------------------------------------------------------


def log_level(p_level):
    """
    Determine the Log Level for debug
    """

    # Determine the logging level

    if p_level == 'DEBUG':
        return_code = logging.DEBUG

    elif p_level == 'INFO':
        return_code = logging.INFO

    elif p_level == 'WARNING':
        return_code = logging.WARNING

    elif p_level == 'ERROR':
        return_code = logging.ERROR

    elif p_level == 'CRITICAL':
        return_code = logging.CRITICAL

    elif p_level == 'NONE':
        return_code = None

    else:
        print('ERROR: Failed to set correct debug level - aborting')
        exit(1)

    return return_code

# --------------------------------------------------------------------
#
#                          log Args
#
# --------------------------------------------------------------------


def log_args(p_args):
    """
    Print all arguments
    """
    # If there are no argments, do nothing.
    if len(p_args) > 0:

        # Display heading
        log_info("\n")
        log_info("Display program arguments")
        log_info("-------------------------")
        log_info("{0:<30s} {1:<50s}".format('Argument', 'Value'))
        log_info("{0:<30s} {1:<50s}".format('------------------------------',
                 '--------------------------------------------------'))
        ord_args = collections.OrderedDict(sorted(p_args.items()))
        # Display Arguments
        for arg in ord_args:
            if isinstance(ord_args[arg], bool):
                if ord_args[arg]:
                    log_info("{0:<30s} {1:<50s}".format(arg, 'True'))
                else:
                    log_info("{0:<30s} {1:<50s}".format(arg, 'False'))
            else:
                log_info("{0:<30s} {1!s:<50s}".format(arg, ord_args[arg]))

        log_info("\n")

# ----------------------------------------------------------------
#
#                 log {Level}
#
# ----------------------------------------------------------------


def log_critical(print_string):
    """
    Log Critical Error message to file and screen
    """
    logging.critical(print_string)
    print("\n{}\n".format(print_string))


def log_debug(print_string):
    """ Debug to log file """
    # print('debuging this str {}'.format(str))
    logging.debug(print_string)


def log_info(print_string):
    """ Info to log file """
    logging.info(print_string)


def log_warning(print_string):
    """ Warning to log file """
    logging.warning(print_string)


def log_error(print_string):
    """ Error  to log file """
    logging.error(print_string)

# --- Debug Stuff
# --------------------------------------------------------------------
#
#                          debug results
#
# --------------------------------------------------------------------


def debug_results(p_type, p_s_df, p_t_df, p_tab):
    """
    This routine is for debugging data only, it should not be used for displaying
    any results.
    The routines are to find differences in data, to assist with coding issues
    (eg comparing NaN to None, looking for unprintable char, etc)
    """

    log_debug('Start debug results2')

    if p_type is None:
        log_debug('Debug Type is None, doing nothing')

    elif p_type == 'a':
        debug_test_a(p_s_df, p_t_df)

    elif p_type == 'b':
        debug_test_b(p_s_df, p_t_df)

    elif p_type == 'c':
        debug_test_c(p_s_df, p_t_df, p_tab)

    elif p_type == 'c1':
        debug_test_c_deep1(p_s_df, p_t_df, p_tab)

    elif p_type == 'c2':
        debug_test_c_deep2(p_s_df, p_t_df, p_tab)

    elif p_type == 'd':
        debug_test_d(p_s_df, p_t_df, p_tab)

    return

# --------------------------------------------------------------------
#
#                          debug test
#
# --------------------------------------------------------------------


def debug_test_a(p_csv_df, p_tab_df):
    """
    Remove one column at a time, and run the comparison.
    When the error count drops considerably, you have found the problem child column.
    """
    new_cols = list(p_csv_df.columns)

    # p_tab_df.fillna(value=np.nan, inplace=True)

    csv_rem_df, tab_rem_df = tab_compare_del_common(p_csv_df, p_tab_df)
    error_count = len(csv_rem_df.index)
    p_d('test cols = {} (none removed)'.format(new_cols))
    p_d('    error count = {}'.format(error_count))

    while len(new_cols) > 1:
        new_cols = new_cols[:-1]
        p_d('test cols = {}'.format(new_cols))
        new_csv_df = p_csv_df[new_cols]
        new_tab_df = p_tab_df[new_cols]

        csv_rem_df, tab_rem_df = tab_compare_del_common(new_csv_df, new_tab_df)
        error_count = len(csv_rem_df.index)
        p_d('    error count = {}'.format(error_count))

    return
# --------------------------------------------------------------------
#
#                          debug test 3
#
# --------------------------------------------------------------------


def debug_test_b(pd1, pd2):
    """
    Remove one column at a time, and run the comparison.
    When the error stops getting raised, you have found the problem child column.
    (i.e. the column that raised a value error when running a comparison of DF's)
    """

    new_cols = list(pd1.columns)

    while len(new_cols) > 1:
        new_cols = new_cols[:-1]
        p_d('test cols = {}'.format(new_cols))

        new_csv_df = pd1[new_cols]
        new_tab_df = pd2[new_cols]

        try:
            eql = ((new_csv_df == new_tab_df) | ((new_csv_df != new_csv_df) &
                                                 (new_tab_df != new_tab_df)))

            p_d('len of eq1 is {}'.format(len(eql.index)))

        except ValueError:
            p_d('Failed on value error')

    return


# --------------------------------------------------------------------
#
#                          debug test
#
# --------------------------------------------------------------------


def debug_test_c(p_csv_df, p_tab_df, p_tab):
    """
    Loop through each column and run the comparison
    """
    new_cols = list(p_csv_df.columns)
    p_d('len of new list is {}'.format(len(new_cols)))

    for col in new_cols:
        p_d('Processing column {}'.format(col))

        new_csv_df = p_csv_df[col].to_frame()
        new_tab_df = p_tab_df[col].to_frame()
        csv_rem_df, tab_rem_df = tab_compare_del_common(new_csv_df, new_tab_df)
        error_count = len(csv_rem_df.index) + len(tab_rem_df.index)
        p_d('    Debug_test2 column {}, error_count = {}'.format(col, error_count))
        if error_count > 0:
            log_error('        Errors found on tab.col {}.{} = {}'
                      .format(p_tab, col, error_count))

    return

# --------------------------------------------------------------------
#
#                          debug test
#
# --------------------------------------------------------------------


def debug_test_c_deep1(p_csv_df, p_tab_df, p_tab):
    """
    This code is derived from debug_test2,
    It can be used where the two rows that are being compared are from the same Primary key.
    It does a very thorough comparison of the fields.
    """

    new_cols = list(p_csv_df.columns)
    p_d('len of new list is {}'.format(len(new_cols)))

    for col in new_cols:
        p_d('Processing column {}'.format(col))

        new_csv_df = p_csv_df[col].to_frame()
        new_tab_df = p_tab_df[col].to_frame()
        csv_rem_df, tab_rem_df = tab_compare_del_common(new_csv_df, new_tab_df)
        error_count = len(csv_rem_df.index) + len(tab_rem_df.index)
        p_d('    Debug_test4 column {}, error_count = {}'.format(col, error_count))
        if error_count > 0:
            log_error('        Errors found on tab.col {}.{} = {}'
                      .format(p_tab, col, error_count))

            # NOTE: this only works when the different rows match on both sides
            # otherwise use the next for loop.
            for i, row in csv_rem_df.iterrows():
                p_d('            processing row {}'.format(i))
                src = csv_rem_df[col][i]
                trg = tab_rem_df[col][i]

                if isinstance(src, int) or isinstance(trg, int):
                    pass
                else:
                    for pos, rrow in enumerate(difflib.ndiff(src, trg)):
                        if rrow[0] == ' ':
                            continue
                        elif rrow[0] == '-':
                            p_d(u'                Delete "{}" from position {} ({})'.
                                format(rrow[-1], pos, ord(rrow[-1])))
                        elif rrow[0] == '+':
                            p_d(u'                Add "{}" to position {} ({})'.
                                format(rrow[-1], pos, ord(rrow[-1])))

    return

# --------------------------------------------------------------------
#
#                          debug test
#
# --------------------------------------------------------------------


def debug_test_c_deep2(p_csv_df, p_tab_df, p_tab):
    """
    This code is derived from debug_test2 and debug_test4,
    It can be used where the two rows that are being compared are from the same Primary key.
    It does a very thorough comparison of the fields.
    """

    new_cols = list(p_csv_df.columns)
    p_d('len of new list is {}'.format(len(new_cols)))

    for col in new_cols:
        p_d('Processing column {}'.format(col))

        new_csv_df = p_csv_df[col].to_frame()
        new_tab_df = p_tab_df[col].to_frame()
        csv_rem_df, tab_rem_df = tab_compare_del_common(new_csv_df, new_tab_df)
        error_count = len(csv_rem_df.index) + len(tab_rem_df.index)
        p_d('    Debug_test4 column {}, error_count = {}'.format(col, error_count))
        if error_count > 0:
            log_error('        Errors found on tab.col {}.{} = {}'
                      .format(p_tab, col, error_count))

            # this loop is much slower, as it compares EVERY row in the original dataframe
            # use the previous loop to just compare differences.

            for row, idx in new_csv_df.iterrows():
                p_d('            processing row {}'.format(row))
                src = new_csv_df[col][idx]
                trg = new_tab_df[col][idx]

                if isinstance(src, int) or isinstance(trg, int):
                    pass
                else:
                    if src is not None and trg is not None:
                        for pos, rrow in enumerate(difflib.ndiff(src, trg)):
                            if rrow[0] == ' ':
                                continue
                            elif rrow[0] == '-':
                                p_d(u'                Delete "{}" from position {} ({})'.
                                    format(rrow[-1], pos, ord(rrow[-1])))
                            elif rrow[0] == '+':
                                p_d(u'                Add "{}" to position {} ({})'.
                                    format(rrow[-1], pos, ord(rrow[-1])))

    return
# --------------------------------------------------------------------
#
#                          debug test d
#
# --------------------------------------------------------------------


def debug_test_d(p_csv_df, p_tab_df, p_tab):
    """
    similar to test_c, but first remove the rows that do not have a common PK

    """
    new_cols = list(p_csv_df.columns)
    p_d('len of new list is {}'.format(len(new_cols)))

    for col in new_cols:
        p_d('Processing column {}'.format(col))

        new_csv_df = p_csv_df[col].to_frame()
        new_tab_df = p_tab_df[col].to_frame()
        csv_rem_df, tab_rem_df = tab_compare_del_common(new_csv_df, new_tab_df)
        error_count = len(csv_rem_df.index) + len(tab_rem_df.index)
        p_d('    Debug_test4 column {}, error_count = {}'.format(col, error_count))
        if error_count > 0:
            log_error('        Errors found on tab.col {}.{} = {}'
                      .format(p_tab, col, error_count))

            # NOTE: this only works when the different rows match on both sides
            # otherwise use the next for loop.
            for i, row in csv_rem_df.iterrows():
                p_d('            processing row {}'.format(i))
                src = csv_rem_df[col][i]
                trg = tab_rem_df[col][i]

                if isinstance(src, int) or isinstance(trg, int):
                    pass
                else:
                    for pos, rrow in enumerate(difflib.ndiff(src, trg)):
                        if rrow[0] == ' ':
                            continue
                        elif rrow[0] == '-':
                            p_d(u'                Delete "{}" from position {} ({})'.
                                format(rrow[-1], pos, ord(rrow[-1])))
                        elif rrow[0] == '+':
                            p_d(u'                Add "{}" to position {} ({})'.
                                format(rrow[-1], pos, ord(rrow[-1])))

    return

# --- eof ---
