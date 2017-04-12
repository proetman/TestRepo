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

import re
import sys
import time
from datetime import datetime
import pandas as pd

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

ROOT_DIR_BI = '//intranet/teams/Technology/Technology Systems/BI/BI Team Documents'

INFO_CLASS_SPREADSHEET = ROOT_DIR_BI + \
    '/10.Group Data Insights/5.Source Data Replication/Information Classification.xlsx'

EMAIL_DEFAULT = 'paul.roetman@racq.com.au,marinos.stylianou@racq.com.au'

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


RS_BRYTE_RESRVD_WORDS = \
    ['AES128', 'AES256', 'ACTION', 'ALL', 'ALLOWOVERWRITE', 'ANALYSE', 'ANALYZE', 'AND', 'ANY',
     'ARRAY',
     'AS', 'ASC', 'AUTHORIZATION', 'BACKUP', 'BETWEEN', 'BINARY', 'BLANKSASNULL', 'BOTH',
     'BYTEDICT', 'BZIP2', 'CASE', 'CAST', 'CHECK', 'COLLATE', 'COLUMN', 'COMMENT', 'CONSTRAINT',
     'CREATE', 'CREDENTIALS', 'CROSS', 'CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP',
     'CURRENT_USER', 'CURRENT_USER_ID', 'DATE', 'DATETIME', 'DEFAULT', 'DEFERRABLE', 'DEFLATE',
     'DEFRAG', 'DELTA',
     'DELTA32K', 'DESC', 'DISABLE', 'DISTINCT', 'DO', 'ELSE', 'EMPTYASNULL', 'ENABLE', 'ENCODE',
     'ENCRYPT', 'ENCRYPTION', 'END', 'EXCEPT', 'EXPLICIT', 'FALSE', 'FOR', 'FOREIGN', 'FREEZE',
     'FROM', 'FULL', 'GLOBALDICT256', 'GLOBALDICT64K', 'GRANT', 'GROUP', 'GZIP', 'HAVING',
     'IDENTITY', 'IGNORE', 'ILIKE', 'IN', 'INITIALLY', 'INNER', 'INTERSECT', 'INTERVAL', 'INTO',
     'IS', 'ISNULL',
     'JOIN', 'KEY', 'LANGUAGE', 'LEADING', 'LEFT', 'LIKE', 'LIMIT', 'LOCALTIME', 'LOCALTIMESTAMP',
     'LUN',
     'LUNS',
     'LZO', 'LZOP', 'MINUS', 'MOSTLY13', 'MOSTLY32', 'MOSTLY8', 'NATURAL', 'NEW', 'NOT', 'NOTNULL',
     'NULL', 'NULLS', 'OFF', 'OFFLINE', 'OFFSET', 'OID', 'OLD', 'ON', 'ONLY', 'OPEN', 'OR', 'ORDER',
     'OUTER', 'OVERLAPS', 'PARALLEL', 'PARTITION', 'PASSWORD', 'PERCENT', 'PERMISSIONS', 'PLACING',
     'PRIMARY',
     'RAW', 'READRATIO', 'RECOVER', 'REFERENCES', 'RESPECT', 'REJECTLOG', 'RESORT', 'RESTORE',
     'RIGHT', 'SELECT', 'SESSION_USER', 'SIMILAR', 'SOME', 'SYSDATE', 'SYSTEM', 'TABLE', 'TAG',
     'TDES', 'TEXT255', 'TEXT32K', 'THEN', 'TIME', 'TIMESTAMP', 'TO', 'TOP', 'TRAILING', 'TRUE',
     'TYPE',
     'TRUNCATECOLUMNS', 'UNION', 'UNIQUE', 'USER', 'USING', 'VALID', 'VERBOSE', 'WALLET', 'WHEN',
     'WHERE', 'WITH', 'WITHOUT', 'YEAR']

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

# --- Info Classification Functions

# --------------------------------------------------------------------
#
#                          Load Info Class
#
# --------------------------------------------------------------------


def load_info_classification(p_conn, p_class_tab, p_multiple_schema):
    """
    Load the spreadsheet info classification into a dataframe.
    return the dataframe.

    Special case for Cisco where all table names and column names are case sensitive.

    Note: only the table and column columns are NOT converted to uppercase, the rest are.

    """

    if p_multiple_schema:

        p_i('    Fetch info classification from spreadsheet (multiple schemas)')
        ss_info_class_df = ss_info_classification_fetch_mod(p_class_tab, p_uppercase=True)
        if ss_info_class_df is None:
            return None

        ss_info_class_df['OWNER'] = ss_info_class_df['OWNER'].str.upper()

        if '.' in p_conn['schema']:
            l_schema = p_conn['schema'].split('.')[1]
        else:
            l_schema = p_conn['schema']

        this_owner = ss_info_class_df['OWNER'][0].split(':')[0] + ':' + l_schema.upper()
        schema_ind = ss_info_class_df['OWNER'] == this_owner

        new_info_class_df = ss_info_class_df[schema_ind]

    else:

        case_sensitive = False
        if p_class_tab == 'Cisco':
            case_sensitive = True

        ss_info_class_df = ss_info_classification_fetch(p_class_tab, p_uppercase=True,
                                                        p_case_sensitive=case_sensitive)
        if ss_info_class_df is None:
            return None

        new_info_class_df = ss_info_class_df

    # -- Test loaded data
    if ss_info_classification_cols_contains_nulls(new_info_class_df):
        p_e('')
        p_e('Warning - Data not fully populated in Info Classification Spreadsheet.')
        p_e('          Continue processing anyway....')
        p_e('')

    return new_info_class_df

# --------------------------------------------------------------------
#
#                          fetch mapping rules
#
# --------------------------------------------------------------------


def ss_info_classification_fetch(p_tab_name, p_uppercase=False, p_case_sensitive=False):
    """
    Load the Info classification spreadsheet into a dataframe.

    Columns to use: 'Table Name',  'Column', 'Primary Key', 'Treatment', 'Data Type'

    Current available sheets within the spreadsheet are (23-Sep-2016):
        Arnie
        BillingCenter
        ClaimsCenter
        CAD
        CARS
        Cisco
        CoreMetrics
        CTP
        DAX
        EWFM
        ETLSpike
        Focus
        Genesys
        Genesys_dnu
        GlassesGuide
        HAIncident
        IQ
        LifeIns
        LifetimeGuarantee
        LimeSurvey
        MARS
        MRMTables
        MRMViews
        PolicyCenter
        Rewards
        Subscriptions
    """
    data_dir = TEST_RESULT_DIRS['data']
    ic_file = 'Information Classification'
    h5file_template = '{wd}/{file}_{sheet}.h5'
    h5file = h5file_template.format(wd=data_dir,
                                    file=ic_file,
                                    sheet=p_tab_name)

    if os.path.isfile(h5file):
        h5file_disp = h5file.replace('/', '\\')
        print('\nReading hd5 copy of Info Classification spreadsheet from {}'.format(h5file_disp))
        print('if out of date, just delete the h5 file, it will be regenerated automagically\n')
        ss_df = pd.read_hdf(h5file, 'ic')
    else:

        try:
            print('... ... Opening spreadsheet, this takes a minute or so')
            ss_df = pd.read_excel(INFO_CLASS_SPREADSHEET,
                                  p_tab_name,
                                  skiprows=0,
                                  parse_cols=[0, 1, 2, 5, 8],
                                  index_col=None)
            ss_df.columns = ['Table_Name', 'Column', 'Primary_Key', 'Treatment', 'Data_Type']
            ss_df.reset_index(drop=True, inplace=True)

            # -- Remove blanks, this will stop the pickle error on save to h5
            ss_df['Primary_Key'].where(pd.notnull(ss_df['Primary_Key']), 'N', inplace=True)

            # -- convert to upper case.
            if p_case_sensitive is False:
                ss_df.loc[:, 'Column'] = ss_df.loc[:, 'Column'].str.upper()

            ss_df_allc_ind = ss_df['Column'].str.upper() == 'ALL COLUMNS'
            ss_df.loc[ss_df_allc_ind, 'Data_Type'] = 'ALL COLUMNS'

            # Convert some columns to uppper case (including column headings)
            if p_uppercase:
                for col in ss_df.columns:
                    if p_case_sensitive and col in ('Table_Name', 'Column'):
                        pass
                    else:
                        ss_df.loc[:, col] = ss_df.loc[:, col].str.upper()

                ss_df.columns = ss_df.columns.str.upper()

            hdf = pd.HDFStore(h5file)
            hdf.put('ic', ss_df)
            hdf.close()

        except FileNotFoundError as err:
            p_e('Function ss_info_classification_fetch, spreadsheet not found. Aborting.')
            p_e('       speadsheet: [{}]'.format(INFO_CLASS_SPREADSHEET))
            p_e('\n       error text [{}]'.format(err))
            return None

        except Exception as err:
            p_e('\nGeneric exception in function ss_info_classification_fetch.')
            p_e('       probably incorrect sheet name. Aborting.')
            p_e('       spreadsheet: [{}]'.format(INFO_CLASS_SPREADSHEET))
            p_e('       sheet name: [{}]'.format(p_tab_name))
            p_e('\n       error text [{}]'.format(err))
            return None

    # -- if there are nulls, work out which columns have the null values.
    #
    # Note: You may receive the error:
    #   PerformanceWarning:  your performance may suffer as PyTables will pickle object types ....
    # This only occurs if there are NULLS.
    # and there may always be nulls in the primary key column.
    #

    return ss_df

# --------------------------------------------------------------------
#
#                         dump cleanup ss
#
# --------------------------------------------------------------------


def dump_cleanup_ss(p_ss_dict):
    """
    Cleanup the dictionary of all tabs from the Info Class spreadsheet
    """
    # {k:v for k,v in p_ss_dict.items() if k[0:2] == 'i_'}

    new_dict = {}

    for key, value in p_ss_dict.items():
        if key[0:2] == 'i_':
            pass
        else:
            new_dict[key] = p_ss_dict[key]

    return new_dict


# --------------------------------------------------------------------
#
#                         dump cleanup ss
#
# --------------------------------------------------------------------

def dump_save(p_tab, p_ss_df, p_standard):
    """
    Cleanup the dictionary of all tabs from the Info Class spreadsheet
    """

    data_dir = TEST_RESULT_DIRS['data']
    ic_file = 'Information Classification'

    if p_standard:
        cols = [0, 1, 2, 5, 8]
        col_names = ['Table_Name', 'Column', 'Primary_Key', 'Treatment', 'Data_Type']
        h5file_template = '{wd}/{file}_{sheet}.h5'
    else:
        cols = [0, 1, 2, 5, 7, 8]
        col_names = ['Table_Name', 'Column', 'Primary_Key', 'Treatment', 'Owner', 'Data_Type']
        h5file_template = '{wd}/{file}_{sheet}_mod.h5'

    h5file = h5file_template.format(wd=data_dir,
                                    file=ic_file,
                                    sheet=p_tab)

    # -- Remove columns not required.
    counter = 0
    for col in p_ss_df.columns:
        if counter in cols:
            pass
        else:
            p_ss_df.drop(col, axis=1, inplace=True)

        counter += 1

    # -- Update column headings
    p_ss_df.reset_index(drop=True, inplace=True)
    p_ss_df.columns = col_names

    # -- Remove blanks, this will stop the pickle error on save to h5
    p_ss_df['Primary_Key'].where(pd.notnull(p_ss_df['Primary_Key']), 'N', inplace=True)

    # -- convert to upper case.
    p_ss_df.loc[:, 'Column'] = p_ss_df.loc[:, 'Column'].str.upper()

    p_ss_df_allc_ind = p_ss_df['Column'].str.upper() == 'ALL COLUMNS'
    p_ss_df.loc[p_ss_df_allc_ind, 'Data_Type'] = 'ALL COLUMNS'

    # Convert some columns to uppper case (including column headings)
    for col in p_ss_df.columns:
        if col in ('Table_Name', 'Column'):
            pass
        else:
            p_ss_df.loc[:, col] = p_ss_df.loc[:, col].str.upper()

    p_ss_df.columns = p_ss_df.columns.str.upper()

    # Test for null values
    ss_info_classification_cols_contains_nulls(p_ss_df)

    hdf = pd.HDFStore(h5file)
    hdf.put('ic', p_ss_df)
    hdf.close()

# --------------------------------------------------------------------
#
#                          fetch mapping rules
#
# --------------------------------------------------------------------


def dump_all_h5_info_classification():
    """
    open info class spreadsheet
    for each tab (with a few exceptions)
        save to h5 file as standard
        save to h5 file as multi schema
    """

    # load all tabs in standard format
    try:
        ss_standard_df = pd.read_excel(INFO_CLASS_SPREADSHEET, None)
        ss_multi_df = pd.read_excel(INFO_CLASS_SPREADSHEET, None)

    except FileNotFoundError as err:
        p_e('Function dump_all_h5_info_classification, spreadsheet not found. Aborting.')
        p_e('       speadsheet: [{}]'.format(INFO_CLASS_SPREADSHEET))
        p_e('\n       error text [{}]'.format(err))
        return None

    except Exception as err:
        p_e('\nGeneric exception in function dump_all_h5_info_classification.')
        p_e('       spreadsheet: [{}]'.format(INFO_CLASS_SPREADSHEET))
        p_e('\n       error text [{}]'.format(err))
        return None

    # remove tabs that start with "i_"
    ss_standard_df = dump_cleanup_ss(ss_standard_df)
    ss_multi_df = dump_cleanup_ss(ss_multi_df)

    for tab, ss_df in ss_standard_df.items():
        p_i('    Save standard tab {}'.format(tab))
        dump_save(tab, ss_df, p_standard = True)

    for tab, mm_df in ss_multi_df.items():
        p_i('    Save multi tab {}'.format(tab))
        dump_save(tab, mm_df, p_standard = False)

    return

# --------------------------------------------------------------------
#
#                          fetch mapping rules
#
# --------------------------------------------------------------------


def ss_info_classification_cols_contains_nulls(p_df):
    """
    Test to see if any of the classifcation columns contain nulls.
    """
    # -- if there are nulls, work out which columns have the null values.
    #
    # Note: You may receive the error:
    #   PerformanceWarning:  your performance may suffer as PyTables will pickle object types ....
    # This only occurs if there are NULLS.
    # and there may always be nulls in the primary key column.
    #
    overall_test_result = False

    test_for_nulls = p_df.isnull()
    test_for_nulls_result = test_for_nulls.any().any()

    if test_for_nulls_result:
        for col in p_df.columns:
            col_test_for_nulls = p_df[col].isnull()
            col_test_for_nulls_result = col_test_for_nulls.any().any()
            if col_test_for_nulls_result:

                overall_test_result = True

                p_e('* * * WARNING * * * WARNING * * * WARNING * * *')
                p_e('* * * WARNING * * * WARNING * * * WARNING * * *')
                p_e('    WARNING - NULL data found in spreadsheet')
                p_e('    Column {} should be fully populated'.format(col))
                p_e('    -- some values are blank')
                p_e('* * * WARNING * * * WARNING * * * WARNING * * *')
                p_e('* * * WARNING * * * WARNING * * * WARNING * * *')
                p_e('')

    return overall_test_result

# --------------------------------------------------------------------
#
#                          fetch mapping rules
#
# --------------------------------------------------------------------


def ss_info_classification_fetch_mod(p_tab_name, p_uppercase=False):
    """
    This is a modified version of the function rqlib.ss_info_classification_fetch.
    It fetches the additional column of "owner", and uses this to determine which
    schema the data is stored in.

    Load the Info classification spreadsheet into a dataframe.

    Columns to use: 'Table Name',  'Column', 'Primary Key', 'Treatment', 'Data_Type, Owner'

    """
    data_dir = TEST_RESULT_DIRS['data']
    ic_file = 'Information Classification'
    h5file_template = '{wd}/{file}_{sheet}_mod.h5'
    h5file = h5file_template.format(wd=data_dir,
                                    file=ic_file,
                                    sheet=p_tab_name)

    if os.path.isfile(h5file):
        h5file_disp = h5file.replace('/', '\\')
        print('\nReading hd5 copy of Info Classification spreadsheet from {}'.format(h5file_disp))
        print('if out of date, just delete the h5 file, it will be regenerated automagically\n')
        ss_df = pd.read_hdf(h5file, 'ic')
    else:

        try:
            print('... ... Opening spreadsheet, this takes a minute or so')
            ss_df = pd.read_excel(INFO_CLASS_SPREADSHEET,
                                  p_tab_name,
                                  skiprows=0,
                                  parse_cols=[0, 1, 2, 5, 7, 8],
                                  index_col=None)
            ss_df.columns = ['Table_Name',
                             'Column',
                             'Primary_Key',
                             'Treatment',
                             'Owner',
                             'Data_Type']

            ss_df.reset_index(drop=True, inplace=True)

            # -- Remove blanks, this will stop the pickle error on save to h5
            ss_df['Primary_Key'].where(pd.notnull(ss_df['Primary_Key']), 'N', inplace=True)

            # -- Remove blanks from data type for all columns type records.
            ss_df.loc[:, 'Column'] = ss_df.loc[:, 'Column'].str.upper()
            ss_df_allc_ind = ss_df['Column'] == 'ALL COLUMNS'
            ss_df.loc[ss_df_allc_ind, 'Data_Type'] = 'ALL COLUMNS'

            # Convert all spreadsheet data to uppper case (including column headings)
            if p_uppercase:
                for col in ss_df.columns:
                    ss_df[col] = ss_df.loc[:, col].str.upper()

                ss_df.columns = ss_df.columns.str.upper()

            # -- Replace any NaN with None in Object columns.
            string_df = ss_df.select_dtypes(include=['object'])
            for col in string_df.columns:
                ss_df[col].where(pd.notnull(ss_df[col]), None, inplace=True)

            p_i('Save Info Class doc to h5 file {}'.format(h5file.replace('/', '\\')))
            hdf = pd.HDFStore(h5file)
            hdf.put('ic', ss_df)
            hdf.close()

        except FileNotFoundError as err:
            print('\nERROR: ss_info_classification_fetch, spreadsheet not found. Aborting.')
            print('       speadsheet: [{}]'.format(INFO_CLASS_SPREADSHEET))
            print('\n       error text [{}]'.format(err))
            return None

        except Exception as err:
            print('\nERROR: generic exception in function ss_info_classification_fetch.')
            print('       probably incorrect sheet name. Aborting.')
            print('       spreadsheet: [{}]'.format(INFO_CLASS_SPREADSHEET))
            print('       sheet name: [{}]'.format(p_tab_name))
            print('\n       error text [{}]'.format(err))
            return None

    return ss_df

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
#                          Test Oracle Version
#
# --------------------------------------------------------------------


def test_oracle_version_ms_proc(p_conn):
    """
    Test to see if the oracle version is 10.2.0.4
    If it is, then millisecond processing will be disabled due to oracle bug.
    """

    ora_ver = fetch_oracle_version(p_conn)

    if ora_ver is None:
        p_e('Unable to determine oracle version, please review coding, aborting')
        exit(-1)

    elif '10.2.0.4' in ora_ver:
        p_i('Oracle version 10.2.0.4 detected, millisecond processing will be disabled.')
        return True

    return False

# --------------------------------------------------------------------
#
#                          Fetch Oracle Version
#
# --------------------------------------------------------------------


def fetch_oracle_version(p_conn):
    """ Fetch the version of oracle """
    log_debug('fetch oracle version')

    sql = "select banner from v$version where upper(banner) like 'ORACLE DATABASE%'"

    fetched_rows = rqconnlib.ora_sql_fetch(p_conn, sql)
    if len(fetched_rows) == 1:
        fetched_ora_ver = fetched_rows[0][0]
    else:
        fetched_ora_ver = None

    return fetched_ora_ver

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


def p_e(p_str, p_end=None):
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

# --- eof ---
