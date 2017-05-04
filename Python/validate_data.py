"""
Validate spreadsheet data is same as db data

"""
from __future__ import division
from __future__ import print_function

import argparse
# import datetime
import re
import textwrap

# import numpy as np
import pandas as pd

import acc_lib as alib


# --------------------------------------------------------------------
#
#             Global /  Constants
#
# --------------------------------------------------------------------

# --- conv utils
# ------------------------------------------------

def is_number(l_str):
    """ test to see if a string contains a numeric value only """
    try:
        float(l_str)
        return True
    except ValueError:
        return False

# ------------------------------------------------

def is_float(l_str):
    """ test to see if a string contains a numeric value only """
    try:
        int(l_str)
        return True
    except ValueError:
        return False

# ------------------------------------------------


def clean_nan(p_field):
    """
    Lambda function to replace nan with None
    """
    if p_field == 'nan':
        new_field = None
    else:
        new_field = p_field

    return new_field

# ------------------------------------------------


def clean_None(p_field):
    """
    Lambda function to replace nan with None
    """
    if p_field is None:
        new_field = None
    else:
        new_field = p_field

    return new_field

# ------------------------------------------------


def add_time(p_val):
    retval = p_val + ' 00:00:00'
    return retval

# ------------------------------------------------


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

    dodgy_char20 = '\xa0'   # this is some kind of weird space, but is non ascii.
    #                       # Every field is terminated with it.
    dodgy_char21 = '·'      # another bizarre hyphon
    dodgy_char22 = '│'
    dodgy_char23 = '√'

#    if(p_field is None or
#       isinstance(p_field, datetime.date) or
#       not isinstance(p_field, str)):
    if p_field is None:
        new_field = p_field
    else:
        #        if p_field[0:4].upper() == 'Alla'.upper():
        #            print('    {}, len = {}'.format(p_field, len(p_field)))
        #            x  = 1

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

        if "example data" in tmp_field.lower():
            alib.p_e('ERROR: EXAMPLE DATA string found in data')
            alib.p_e('        orig data = [{}]'.format(p_field))

        # remove trailing spaces
        new_field = re.sub(' *$', '', new_field)

        if is_number(new_field):
            if new_field[-2:] == ".0":
                new_field = new_field[:-2]

        if new_field[:2] == '0x':
            new_field = new_field[2:]

        if is_number(new_field):
            if is_float(new_field):
                if new_field[-5:] == '00001':
                    new_field = new_field[:-1]

    return new_field

# --- process
# --------------------------------------------------------------------
#
#                          setup load details
#
# --------------------------------------------------------------------


def process(p_ld, p_conn_details, p_work_dict, p_debug_type):
    """
    process a load dict
    """
    retval = alib.SUCCESS
    l_short_name = p_ld[0]
    l_details = p_ld[1]

    print('processing LD[0] = {}'.format(l_short_name))

    l_sheet_name = l_details[0]

    # --------------------------------------------------------------------
    # Fetch the table data, all in one dataframe

    l_create_table = l_details[2]
    l_compare_table = l_create_table.split('.')[1]

    tab_col_df = alib.fetch_tab_col(p_conn_details['conn'])             # Fetch ALL table column info
    cols_df = alib.fetch_columns(tab_col_df, l_compare_table)    # Narrow down to a single table
    l_target_df = fetch_table(p_conn_details, l_compare_table, cols_df)

    l_target_df2 = l_target_df.where((pd.notnull(l_target_df)), None)
    for col in l_target_df2.columns:
        l_target_df2[col] = l_target_df2[col].astype(str)
        l_target_df2[col] = l_target_df2[col].apply(lambda x: clean_text(x))
        l_target_df2[col] = l_target_df2[col].apply(lambda x: clean_nan(x))

    mask = l_target_df2.applymap(lambda x: x is None)
    cols = l_target_df2.columns[(mask).any()]
    for col in l_target_df2[cols]:
        l_target_df2.loc[mask[col], col] = 'None'
        # l_target_df2[col] = l_target_df2[col].apply(lambda x: clean_None(x))

    l_target_df = l_target_df2
    # --------------------------------------------------------------------
    # Fetch the spreadsheet data, merge all clubs into one dataframe.

    first_df = True
    count_loaded_df = 0

    for dummy_key, value in p_work_dict.items():
        # -- handle the club files
        if value['tag'] == l_short_name:
            # short_name = value['club_file_short']
            count_loaded_df += 1
            # club_type = value['type']
            l_sheet = l_sheet_name.format(vClub=value['club'])

            # find the name of the spreadsheet TAB to compare, start with "personnel - {vClub}__PERSL.tsv"
            l_tmp = l_sheet.split('__')[1]
            l_ss_tab = l_tmp.split('.')[0]

            # l_dir = '/'.join(short_name.split('/')[:-1])
            l_ss = alib.open_ss2(value['club_file_full'])
            l_ss = capitalize_keys(l_ss)

            if first_df:
                if l_ss_tab not in l_ss:
                    tmp_tab = l_ss_tab.replace('_',' ')
                    if tmp_tab in l_ss:
                        l_ss_tab = tmp_tab

                l_df = l_ss[l_ss_tab]
                l_df2 = l_df.where((pd.notnull(l_df)), None)
                for col in l_df.columns:
                    l_df2[col] = l_df2[col].astype(str)
                    l_df2[col] = l_df2[col].apply(lambda x: clean_text(x))

                l_source_df = l_df2
                first_df = False
            else:
                if l_ss_tab not in l_ss:
                    tmp_tab = l_ss_tab.replace('_',' ')
                    if tmp_tab in l_ss:
                        l_ss_tab = tmp_tab

                l_df = l_ss[l_ss_tab]
                l_df2 = l_df.where((pd.notnull(l_df)), None)
                for col in l_df.columns:
                    l_df2[col] = l_df2[col].astype(str)
                    l_df2[col] = l_df2[col].apply(lambda x: clean_text(x))

                l_source_df = l_source_df.append(l_df2)

    # special case
    if l_short_name == 'disposition codes':
        l_source_df['TYCOD'] = l_source_df['TYCOD'].apply(lambda x: x.zfill(3))

    # another special case
    if l_short_name == 'esp alerts':
        # match column headins (from spreadsheet) to that of the database
        l_source_df.columns = ['UNID', 'SUBJECT', 'EXPIRY', 'MESSAGE']
        l_source_df['EXPIRY'] = l_source_df['EXPIRY'].apply(lambda x: add_time(x))

    # --------------------------------------------------------------------
    # Now start the process of comparing table to spreadsheet

    #    x = l_source_df['USR_ID'] == 'allan0l'
    #    l_source_df = l_source_df[x]

    if count_loaded_df == 0:
        alib.p_e('ERROR: No DF loaded, not comparing')
        alib.p_e('... Compare failed for {} ....'.format(l_compare_table))
        return alib.FAIL_GENERIC

    my_result, s_df, t_df = alib.tab_compare_df(l_source_df, l_target_df, l_compare_table)

    if my_result:
        alib.p_i('... Compare OK for {} ....'.format(l_compare_table))
        retval = alib.SUCCESS
    else:
        if p_debug_type is not None:
            alib.debug_results(p_debug_type, s_df, t_df, l_compare_table)
        alib.p_e('... Compare failed for {} ....'.format(l_compare_table))
        retval = alib.FAIL_GENERIC

    return retval
# --------------------------------------------------------------------
#
#                          capitalize keys
#
# --------------------------------------------------------------------


def capitalize_keys(l_dict):
    """
    create a new empty dict
    assign the new key to be the uppercase version of the old key
    create new key with the value from the old dict
    return the new dict.
    """

    result = {}

    for key, value in l_dict.items():
        upper_key = key.upper()
        result[upper_key] = value

    return result

# --- Fetch Info
# --------------------------------------------------------------------
#
#                          read xlsx
#
# --------------------------------------------------------------------


def read_xlsx(p_xlsx):
    """ open xlsx, save as df """
    alib.log_debug('read xlsx')

    if p_xlsx is None:
        return None

    l_xlsx = p_xlsx.replace('\\', '/')

    try:
        l_df = pd.read_excel(l_xlsx, index_col=None)

    except FileNotFoundError as err:
        alib.p_e('Function open_csv, spreadsheet not found.')
        alib.p_e('       CSV File: [{}]'.format(p_xlsx))
        alib.p_e('\n       error text [{}]'.format(err))
        l_df = None

    except Exception as err:
        alib.p_e('\nGeneric exception in function open_csv.')
        alib.p_e('       CSV File: [{}]'.format(p_xlsx))
        alib.p_e('\n       error text [{}]'.format(err))
        l_df = None

    if l_df is not None:
        l_df.columns = l_df.columns.str.upper()

        for col in l_df.columns:
            l_df[col] = l_df[col].astype(str)

    return l_df


# --------------------------------------------------------------------
#
#                          read csv
#
# --------------------------------------------------------------------


def read_csv(p_csv):
    """ open csv, save as df """
    alib.log_debug('read csv')

    if p_csv is None:
        return None

    l_csv = p_csv.replace('\\', '/')

    try:
        l_df = pd.read_csv(l_csv, index_col=False)

    except FileNotFoundError as err:
        alib.p_e('Function open_csv, spreadsheet not found.')
        alib.p_e('       CSV File: [{}]'.format(p_csv))
        alib.p_e('\n       error text [{}]'.format(err))
        l_df = None

    except Exception as err:
        alib.p_e('\nGeneric exception in function open_csv.')
        alib.p_e('       CSV File: [{}]'.format(p_csv))
        alib.p_e('\n       error text [{}]'.format(err))
        l_df = None

    if l_df is not None:
        l_df.columns = l_df.columns.str.upper()
        for col in l_df.columns:
            l_df[col] = l_df[col].astype(str)

    return l_df


# --------------------------------------------------------------------
#
#                          fetch tables
#
# --------------------------------------------------------------------


#    def fetch_file(p_file, p_cols_df):
#        """
#        Fetch the data from file
#        """
#        alib.log_debug('Start fetch file')
#
#        l_file_extension = p_file.split('.')[-1]
#
#        l_is_xlsx = l_file_extension.lower() == 'xlsx'
#        l_is_csv = l_file_extension.lower() == 'csv'
#
#        if not l_is_xlsx and not l_is_csv:
#            alib.p_e('File must be an xlsx or csv for validation')
#            return None
#
#        if l_is_xlsx:
#            l_df = read_xlsx(p_file)
#        else:
#            l_df = read_csv(p_file)
#
#        return l_df


# --------------------------------------------------------------------
#
#                          fetch tables
#
# --------------------------------------------------------------------


def fetch_table(p_condet, p_table, p_cols_df):
    """
    Fetch the table data from database
    """
    alib.log_debug('Start fetch table')

    #    l_cols_l = p_cols_df['COLUMN'].tolist()
    #    _sql_cols = ','.join(l_cols_l).lower()
    _sql_cols = generate_sql_columns(p_table, p_cols_df)

    _sql_template = '''
               select {vCols}
               from   {vSchema}.{vDB}.{vTab}
               '''

    # where USR_ID = 'allan0l'

    _sql = _sql_template.format(vCols=_sql_cols,
                                vSchema=p_condet['schema'],
                                vDB=p_condet['db'],
                                vTab=p_table)

    alib.log_debug('    fetch table sql = [{}]'.format(_sql))

    tab_df = alib.read_table_data(p_condet['conn'], _sql)

    if tab_df is None:
        alib.log_debug('    row count fetched = None')
    else:
        alib.log_debug('    row count fetched = {}'.format(len(tab_df.index)))
        for col in tab_df.columns:
            tab_df[col] = tab_df[col].astype(str)

    return tab_df

# --- Generate SQL
# --------------------------------------------------------------------
#
#                          generate sql template
#
# --------------------------------------------------------------------


def generate_sql_templates(p_col_type):
    """
    determine which conversion to string is correct
    """


    # double_text = "round({vT}.[{vF}],6) as [{vF}]" forces panda to convert to sci notation.
    double_text = "round({vT}.[{vF}],3) as [{vF}]"

    varbinary_text = "CONVERT(varchar(max),{vT}.[{vF}],2) as [{vF}]"
    datems_text = "convert(varchar(30), {vT}.[{vF}], 121) as [{vF}]"
    date_text = "convert(varchar(30), {vT}.[{vF}], 121) as [{vF}]"
    time_text = "convert(varchar(30), {vT}.[{vF}], 108) as [{vF}]"
    str_text = "rtrim({vT}.[{vF}]) as [{vF}]"
    default_text = "{vT}.{vF} as [{vF}]"
    clob_text = "' ' as {vF}"

    # -- Test for data types.
    l_col_type = p_col_type.upper()

    l_numb_float = re.match('NUMERIC.*,', l_col_type, re.I)

    if 'VARBINARY' in l_col_type:
        l_type = 'binary'

    elif 'DBFLT' in l_col_type or l_numb_float or 'FLOAT' in l_col_type:
        l_type = 'float'

    elif 'DATETIME' in l_col_type or 'SMALLDATE' in l_col_type or 'TIMESTAMP' in l_col_type:
        l_type = 'datetime'

    elif 'DATETIME2' in l_col_type.split(' ') and '(6)' in l_col_type.split(' '):
        l_type = 'date'

    elif 'DATE' in l_col_type:
        l_type = 'date'

    elif 'TIME' in l_col_type:
        l_type = 'time'

    elif 'CHAR' in l_col_type:
        l_type = 'string'

    elif 'LONG' in l_col_type:
        l_type = 'long'

    elif 'INT' in l_col_type:
        l_type = 'int'

    else:
        l_type = 'string'

    # -- Now convert data type into a format string

    if l_type == 'string':
        curr_str = str_text

    elif l_type == 'binary':
        curr_str = varbinary_text

    elif l_type == 'clob':
        curr_str = clob_text

    elif l_type == 'datetime':
        curr_str = datems_text

    elif l_type == 'date':
        curr_str = date_text

    elif l_type == 'float':
        curr_str = double_text

    elif l_type == 'int':
        curr_str = default_text

    elif l_type == 'time':
        curr_str = time_text

    else:
        curr_str = default_text

    return curr_str
# --------------------------------------------------------------------
#
#                          sql columns
#
# --------------------------------------------------------------------


def generate_sql_columns(p_table, p_col_df):
    """
    Generate the SQL for all the the select column name list
    """

    # -- Setup local variables
    sql_columns = None

    # -- Loop through columns, generating sql as you go.
    for dummy_i, row in p_col_df.iterrows():

        l_col_name = row['COLUMN']
        l_data_type = row['DATA_TYPE']

        # Special Case for the event type table, these columns are NOT in the spreadsheet.
        # so do not fetch the columns when generating the sql.
        if p_table.upper() == 'EVENT_TYPE':
            if l_col_name.upper() in ('ASSIGN_CASE_STATE', 'CASE_NUM_ID', 'MAJEVT_ID', 'PENDING_TIMER_DEFAULT'):
                continue

        curr_str_template = generate_sql_templates(l_data_type)

        curr_str = curr_str_template.format(vF=l_col_name, vT=p_table)

        # Special case - these fields are hard coded, no need to compare
        if p_table.upper() == 'RAS_EXT_SP_ALERT':
            if l_col_name.upper() in ('ALERT_ID',
                                      'CDTS',
                                      'CPERS',
                                      'CTERM',
                                      'PRIORITY',
                                      'UDTS',
                                      'UPERS',
                                      'UTERM'):
                continue

        # special case
        #        if p_table.upper() == 'EVENT_TYPE' and l_col_name.upper() == 'ADVISED_EVENT':
        #            curr_str = """
        #                       case
        #                           when advised_event = 'F' then 'N'
        #                           when advised_event = 'T' then 'Y'
        #                           else                     null
        #                       end   [ADVISED_EVENT] """

        # special case
        #        if p_table.upper() == 'EVENT_TYPE' and l_col_name.upper() == 'AUTO_RESPONSE':
        #            curr_str = """
        #                       case
        #                           when auto_response = 'F' then 'N'
        #                           when auto_response = 'T' then 'Y'
        #                           else                     null
        #                       end   [AUTO_RESPONSE] """

        # -- Add to cumulative string.
        if sql_columns is None:
            sql_columns = curr_str
        else:
            sql_columns += ', {}'.format(curr_str)

    return sql_columns

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

    Run on local machine:
    -d DEBUG  -t table -f file.xlsx --target_conn localhost

    Run on Terminal Server:
    -d DEBUG -t table -f file.xlsx  --target_db instance.user@host:db (this may change)
    -d DEBUG -t table -f file.csv   --target_db instance.user@host:db (this may change)

    --target_db localhost
    --short_code "unit agency restriction"

          """, formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('--target_db',
                        help='DB Connection: "localhost" or instance.user@host:db',
                        required=True)

    parser.add_argument('--short_code',
                        help='only run "short_name" load',
                        required=False)

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

    # -- Special Debug
    help_txt = \
        """
        A  : progressively remove one column at a time, and re-run the compare. When
             count of errors reduces, found issue.
        B  : same as TEST_A, but looking for a VALUE_ERROR. When no longer raised,
             found problem column.
        C  : Compare 1 col at a time, good for finding non-printable char.
        C1 : same as TEST_C, but prints a "diff" of result.
        C2 : same as TEST_C, but prints a different "diff"
        D  : same as test_c_deep_1, can probably be removed\n
               """

    parser.add_argument('--debug_type',
                        help=textwrap.dedent(help_txt),
                        choices=['a', 'b', 'c', 'c1', 'c2', 'd'],
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
    This program tests that data in CSV file matches data in a table.
    """

    args, dummy_l_log_filename_s = initialise('validate_data')

    # -- Initialise
    if not alib.init_app(args):
        return alib.FAIL_GENERIC

    # -- Connect Target DB
    connect_details = {}
    #   from   {vSchema}.{vDB}.{vTab}
    #   from    'AdventureWorks2012'.'HumanResources'.'Shift'

    if args['target_db'] == 'localhost':
        connect_details['host'] = 'localhost'
        connect_details['instance'] = 'SQLEXPRESS'
        connect_details['schema'] = 'AdventureWorks2012'
        connect_details['db'] = 'dbo'

    else:
        # running on terminal server
        target_conn = args['target_db']
        connect_details['host'] = args['target_host']
        connect_details['instance'] = args['targetinstance']
        connect_details['schema'] = args['target_schema']
        connect_details['db'] = args['target_db']

    db_conn = alib.db_connect_mssql(connect_details)
    connect_details['conn'] = db_conn
    # -------------------------------------
    # Fetch program arguments

    p_debug_type = args['debug_type']

    # -------------------------------------
    # -- Fetch setup data

    work_dir = alib.load_dir(args)
    work_files = alib.load_files(work_dir)

    work_dict = alib.load_matching_masterfile(work_files, p_load_excel=False)

    alib.load_tags(work_dict)

    # -- end standard setup

    load_details = alib.setup_load_details()

    # -------------------------------------
    # -- loop through tables to process

    #    Valid short codes
    #    -----------------
    #    term

    # p1   personnel                               -- compare ok
    # p2   personnel node access                   -- compare ok
    # p3   event types and sub-types               -- compare ok
    # p4   disposition codes                       -- compare ok (auto remove of duplicates)
    # p7   eta table                               -- compare ok
    # p9   out of service codes                    -- compare ok
    # p11  out of service type agency              -- compare ok
    # p11  vehicles                                -- vehicles - rac.xlsx, page unit person = 0
    # p12  units                                   -- problem with floating point numbers
    # p15  membership pricing level (surefire)     -- data fix - change from id to product code
    # p46  esp alerts                              -- waiting on data fixes
    # p47  skills                                  -- 8 extra rows in source ticket 9966
    # p48  vehicle equipment                       -- waiting on RACT data
    # p65  term app access - inetveiwer            -- compare ok
    # p99  unit agency restriction                 -- compare ok

    retval = alib.SUCCESS

    for ldet in load_details.items():
        if args['short_code'] is not None:
            if args['short_code'] != ldet[0]:
                continue
        process(ldet, connect_details, work_dict, p_debug_type)

    db_conn.close()
    return retval

# ------------------------------------------------------------------------------

if __name__ == "__main__":
    l_retval = main()
    if l_retval == alib.SUCCESS:
        exit
    else:
        exit(l_retval)

# --- eof ---
