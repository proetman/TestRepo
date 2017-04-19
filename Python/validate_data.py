"""
Validate spreadsheet data is same as db data

"""
from __future__ import division
from __future__ import print_function

import argparse
import re
import numpy as np
import pandas as pd
import textwrap

# Import racq library for RedShift

import acc_lib as alib


# --------------------------------------------------------------------
#
#             Global /  Constants
#
# ------------------------------------------------------------------

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


def fetch_file(p_file, p_cols_df):
    """
    Fetch the data from file
    """
    alib.log_debug('Start fetch file')

    l_file_extension = p_file.split('.')[-1]

    l_is_xlsx = l_file_extension.lower() == 'xlsx'
    l_is_csv = l_file_extension.lower() == 'csv'

    if not l_is_xlsx and not l_is_csv:
        alib.p_e('File must be an xlsx or csv for validation')
        return None

    if l_is_xlsx:
        l_df = read_xlsx(p_file)
    else:
        l_df = read_csv(p_file)

    return l_df


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

    double_text = "round({vT}.[{vF}],6) as [{vF}]"
    varbinary_text = "CONVERT(varchar(max),{vT}.[{vF}],2)"
    datems_text = "convert(varchar(30), {vT}.[{vF}], 121) as [{vF}]"
    date_text = "convert(varchar(30), {vT}.[{vF}], 121) as [{vF}]"
    time_text = "convert(varchar(30), {vT}.[{vF}], 108) as [{vF}]"
    str_text = "rtrim({vT}.[{vF}]) as [{vF}]"
    default_text = "{vT}.{vF}"
    clob_text = "' ' as {vF}"

    # -- Test for data types.
    l_col_type = p_col_type.upper()

    l_numb_float = re.match('NUMERIC.*,', l_col_type, re.I)

    if 'VARBINARY' in l_col_type:
        l_type = 'binary'

    elif 'DBFLT' in l_col_type or l_numb_float:
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

        curr_str_template = generate_sql_templates(l_data_type)

        curr_str = curr_str_template.format(vF=l_col_name, vT=p_table)

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
          """, formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('--target_db',
                        help='DB Connection: "localhost" or instance.user@host:db',
                        required=True)

    parser.add_argument('-t', '--table',
                        help='Table name for compare',
                        required=True)

    parser.add_argument('-f', '--filename',
                        help='File name for compare (csv or xlsx)',
                        required=True)

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

    args, l_log_filename_s = initialise('validate_data')

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
        connect_details['db'] = 'HUMANRESOURCES'

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

    p_table = args['table']
    p_file = args['filename']
    p_debug_type = args['debug_type']

    # -------------------------------------
    # -- Fetch the tables to process
    tab_col_df = alib.fetch_tab_col(db_conn)             # Fetch ALL table column info
    cols_df = alib.fetch_columns(tab_col_df, p_table)    # Narrow down to a single table
    target_df = fetch_table(connect_details, p_table, cols_df)
    source_df = fetch_file(p_file, cols_df)

    my_result, s_df, t_df = alib.tab_compare_df(source_df, target_df, p_table)

    if my_result:
        alib.p_i('... Compare OK for {} ....'.format(p_table))
        retval = alib.SUCCESS
    else:
        if p_debug_type is not None:
            alib.debug_results(p_debug_type, source_df, target_df, p_table)
        alib.p_e('... Compare failed for {} ....'.format(p_table))
        retval = alib.FAIL_GENERIC

    db_conn.close()
    return retval

# ------------------------------------------------------------------------------

if __name__ == "__main__":
    retval = main()
    if retval == alib.SUCCESS:
        exit
    else:
        exit(retval)

# --- eof ---
