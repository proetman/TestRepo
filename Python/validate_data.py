"""
Validate spreadsheet data is same as db data


Data Structures:

Global Information
------------------
    g_params['account']              : Which application (MRM, Mars, etc)
    g_params['env']                  : SDLC (Prod, UAT, Dev, etc)
    g_params['syst_conn_d']          : Connection to System Test database
    g_params['source_conn']          : Connection to Source
    g_params['target_conn']          : Connection to Target
    g_params['ss_info_class_df']     : entire info class for this spreadsheet tab
    g_params['info_class_tab']       : Info Class spreadsheet for this application
    g_params['multiple_schemas']     : Flag to denote multiple schema
    g_params['run_date_str']         : Date of run as string
    g_params['run_time_str']         : Time of run as string
    g_params['run_type']             : Big, Transact, Daily, QuickVal, etc
    g_params['run_mode']             : init, restart, close
    g_params['table']                : Run for a single table only
    g_params['mail']                 : Who to mail
    g_params['debug_type']           : a,b,c,c1,c2,d  -- Run special debug

Batch Processing
----------------
    l_batch_info['acronym']          : derived from rqlib.g_params['account']
    l_batch_info['env']              : derived from rqlib.g_params['type']
    l_batch_info['batch_mode']       : p_batch_mode                             # init, restart, close, reopen
    l_batch_info['run_type']         : derived from rqlib.g_params['run_type']  # daily, big, transact, etc
    l_batch_info['batch_id']         : current batch id, derived from syst_batch_seq


Table Specific Information
-------------------------
    table_info['tab']                : Current table processing
    table_info['ss_info_tab_df']     : Portion of Info Class ss for this table
    table_info['replicate_cols_df']  : Only which columns are flagged for replication
    table_info['ic_pkcols']          : Info class ss primary key columns
    table_info['source_pkcols']      : Source table primary key columns
    table_info['target_pkcols']      : Target table primary key columns
    table_info['all_columns_s']      : Series of all columns to be replicated
    table_info['pk_columns_s']       : Series of all pk columns from ic ss
    table_info['create_date_col']    : Column containing creation date
    table_info['update_date_col']    : Column containing update date
    table_info['exclude_cols_s']     : Series of columns that do not get validated
    table_info['pk_lag_date']        : Time difference between source and target for this table
    table_info['all_columns_no_excludes_str'] : All columns as a csv string, with excludes removed

    tabinfo_d['source_sql_byrow']    : SQL generated to validate a single primary key
    tabinfo_d['target_sql_byrow']    : SQL generated to validate a single primary key

Deep Analyse
------------
    analyse_errors['UPDATE_NOT_IN_TARGET'] = 0
    analyse_errors['ID_NOT_IN_TARGET'] = 0
    analyse_errors['DELETED_RECORD'] = 0
    analyse_errors['PK_REUSED'] = 0
    analyse_errors['PK_REUSED_NOT_IN_TARGET'] = 0

    analyse_errors['FALSE_POSITIVE'] = 0
    analyse_errors['ERROR_FAIL'] = 0
    analyse_errors['UNKOWN'] = 0

DB Connection Information
-------------------------
    l_conn['conn']                   : Connection to database, used to execute sql
    l_conn['user']                   : Username within database
    l_conn['schema']                 : Schema that owns the tables
    l_conn['db_name']                : Database Name
    l_conn['dbtype']                 : Database Type (Redshift, Oracle, MSSQL)



"""
from __future__ import division
from __future__ import print_function

# Import OS Functions
import argparse
import cx_Oracle
# import decimal
import re
import textwrap
import time
# import math
import numpy as np
import pandas as pd

# Import racq library for RedShift

import racq_lib as rqlib
import racq_conn_lib as rqconnlib
import racq_db_compare_lib as rqdbclib
import racq_db_comp_analyse_lib as rqdbcalib
import racq_db_comp_reports_lib as rqdbcrlib
import racq_batch_lib as rqblib
# import racq_sendmail as rqmail

import racq_syst_conn_lib as rqsconnlib

# --------------------------------------------------------------------
#
#             Global /  Constants
#
# ------------------------------------------------------------------
# rqlib.g_params is a global dictionary created in main and containing all command
# line parameters

# Minimum cells to report
MAX_ROW_COL_COUNT = 10000000

DB_TYPE_REDSHIFT = rqlib.DB_TYPE_REDSHIFT
DB_TYPE_ORACLE = rqlib.DB_TYPE_ORACLE
DB_TYPE_MSSQL = rqlib.DB_TYPE_MSSQL

DAYS_FOR_DEEP_ANALYSE = rqlib.DAYS_FOR_DEEP_ANALYSE

BT_INIT = rqblib.BT_INIT
BT_RUN = rqblib.BT_RUN
BT_FINISH_OK = rqblib.BT_FINISH_OK
BT_FINISH_ERR = rqblib.BT_FINISH_ERR
BT_FINISH_TOO_MANY_ERRORS = rqblib.BT_FINISH_TOO_MANY_ERRORS

# Number of errors before not saving any issues.
BT_MAX_ERROR_COUNT = rqblib.BT_MAX_ERROR_COUNT

BTE_OK = rqblib.BTE_OK
BTE_FAILED = rqblib.BTE_FAILED

# --- Generic Stuff

# --------------------------------------------------------------------
#
#                          determine preprocess
#
# --------------------------------------------------------------------


def tab_preproc(p_tabinfo_d, p_mode):
    """
    Determine if this tab, ??? or this column ???
    requires preprocessing
    """
    rqlib.log_debug('Start tab preproc')

    l_preproc_cols_s = p_tabinfo_d['preproc_cols_s']
    retval = False

    # -- handle small tables
    if p_mode == 'small':
        rqlib.log_debug('    processing small')
        if 'all columns' in l_preproc_cols_s.values:
            retval = True
        else:
            retval = False
    else:
        rqlib.p_e('DETERMINE IF TABLE REQUIRES PRE-PROCESSING NOT YET WRITTEN FOR THIS MODE')
        rqlib.p_e('    MODE = '.format(p_mode))
        retval = None

    rqlib.log_debug('end tab preproc, returning {}'.format(retval))
    return retval

# --------------------------------------------------------------------
#
#                          remove single value from string
#
# --------------------------------------------------------------------


def remove_non_unique_str(p_str, p_val):
    """
    remove value from string
       start "a,b,c,d", remove d
       end   "a,b,c"
    """
    str_list = list(p_str.split(','))
    if p_val not in str_list:
        return p_str

    processed_list = list(p_str.split(','))
    final_str = None
    for p_val in str_list:
        processed_list.remove(p_val)
        # See if this field is still in the list.
        if p_val in processed_list:
            continue

        if final_str is None:
            final_str = p_val
        else:
            final_str += ',{}'.format(p_val)

    return final_str

# --------------------------------------------------------------------
#
#                          process ok list add
#
# --------------------------------------------------------------------


def process_ok_list_add(p_tab, p_completed_ok_list, p_ok_file):
    """
    Add a value to the list of tables that has processed ok, then save the file
    This works for both table and tab_col values.
    """
    rqdbclib.append_obj(p_ok_file, p_tab)
    # p_completed_ok_list.append(p_tab)
    # rqdbclib.save_obj(p_ok_file, p_completed_ok_list)

    return

# --- progress Counter

# --------------------------------------------------------------------
#
#                          progress counter init
#
# --------------------------------------------------------------------


def progress_counter_init(p_table_list):
    """
    Initialise the progress counter
    """
    rqlib.log_debug('progress counter init')

    if p_table_list is None:
        l_length_table_list = 0
        rqlib.p_i('WARNING: table count for progress counter is zero')
    else:
        l_length_table_list = len(p_table_list.index)

    progress = {}
    progress['counter'] = 0
    progress['total'] = l_length_table_list

    rqlib.log_debug('    length table list : {}'.format(l_length_table_list))

    return progress


# --------------------------------------------------------------------
#
#                          progress counter display
#
# --------------------------------------------------------------------


def progress_counter_display(p_progress, p_increment=False):
    """
    Initialise the progress counter
    """
    rqlib.log_debug('progress counter display')

    if p_increment:
        p_progress['counter'] += 1

    if p_progress['total'] == 0:
        rqlib.p_i('WARNING: progress counter total is 0, this should be > 0')
        curr_progress = 0
    else:
        curr_progress = (p_progress['counter'] / p_progress['total']) * 100

    print('           progress = {vP:3.2f}%, {vC} out of {vT:10}'.format(vP=curr_progress,
                                                                         vC=p_progress['counter'],
                                                                         vT=p_progress['total']))

    rqlib.log_debug('    curr_progress : {}'.format(curr_progress))

    return

# --------------------------------------------------------------------
#
#                          progress counter init
#
# --------------------------------------------------------------------


def error_rows_counter_init(p_list):
    """
    Initialise the error_row counter
    """
    rqlib.log_debug('progress counter init')

    if p_list is None:
        l_length_table_list = 0
        rqlib.p_i('WARNING: table count for progress counter is zero')
    else:
        l_length_table_list = len(p_list)

    er = {}
    er['counter'] = 0
    er['total'] = l_length_table_list

    rqlib.log_debug('    er list : {}'.format(l_length_table_list))

    return er


# --------------------------------------------------------------------
#
#                          progress counter display
#
# --------------------------------------------------------------------


def error_rows_counter_display(p_er, p_increment=False):
    """
    display the error_row counter
    """
    rqlib.log_debug('progress counter display')

    if p_increment:
        p_er['counter'] += 1

    if p_er['total'] == 0:
        rqlib.p_i('WARNING: progress counter total is 0, this should be > 0')
        curr_progress = 0
    else:
        curr_progress = (p_er['counter'] / p_er['total']) * 100

    print('           error rows = {vP:3.2f}%, {vC} out of {vT:10}'.format(vP=curr_progress,
                                                                           vC=p_er['counter'],
                                                                           vT=p_er['total']))

    rqlib.log_debug('    error rows progress : {}'.format(curr_progress))

    return


# --- Fetch Info
# --------------------------------------------------------------------
#
#                          fetch tables
#
# --------------------------------------------------------------------


def fetch_tables(p_classification, p_batch_id, p_single_table=None):
    """
    Fetch the tables from
    """
    rqlib.log_debug('Start fetch tables for batch ID: {}'.format(p_batch_id))

    _sql_txt = '''
               select case
                         when bt_status = '{vStatus_init}'         then 1
                         when bt_status = '{vStatus_err}'          then 2
                         when bt_status = '{vStatus_to_many_err}'  then 9
                         when bt_status = '{vStatus_ok}'           then 10
                         else                                           20
                      end                                           as orderby_col,
                      bt_table_name      as table_name,
                      tc_column_name     as column_name,
                      tc_create_column   as create_date_col,
                      tc_update_column   as update_date_col,
                      bt_status
               from   syst_batch_table bt
                   left join syst_table_classification  tc on  tc_acronym     = bt_acronym
                                                           and tc_env         = bt_env
                                                           and tc_table_name  = bt_table_name
                                                           and tc_classification =
                                                                      bt_table_classification
               where  bt_id = {vBatchID}
               and    bt_table_classification = '{vC}'
               and    bt_column_name is null   -- this is table level info, so exclude columns
               '''

    _sql = _sql_txt.format(vBatchID=p_batch_id,
                           vStatus_init=BT_INIT,
                           vStatus_err=BT_FINISH_ERR,
                           vStatus_to_many_err=BT_FINISH_TOO_MANY_ERRORS,
                           vStatus_ok=BT_FINISH_OK,
                           vC=p_classification)

    if p_single_table is not None:
        l_single_table = p_single_table.lower()
        rqlib.log_debug('    Single table selected [{}]'.format(l_single_table))
        _sql += " and bt_table_name = lower('{}')".format(l_single_table)

    _sql += ' order by 1,2'
    rqlib.log_debug('    fetch tables sql = [{}]'.format(_sql))

    rqlib.p_i('    Fetch tables to process for {}'.format(p_classification))
    tab_df = rqdbclib.read_table_data(rqlib.g_params['syst_conn_d'], _sql, p_display_info=False)

    tab_df = tab_df.drop('ORDERBY_COL', axis=1)

    # remove unused columns.
    if p_classification == 'exclude_cols':
        tab_df = tab_df.drop('CREATE_DATE_COL', axis=1)
        tab_df = tab_df.drop('UPDATE_DATE_COL', axis=1)
    elif p_classification in ('daily', 'small'):
        tab_df = tab_df.drop('COLUMN_NAME', axis=1)

    rqlib.log_debug('    row count fetched = {}'.format(len(tab_df.index)))

    return tab_df

# --------------------------------------------------------------------
#
#                          fetch tables
#
# --------------------------------------------------------------------


def fetch_exceptions(p_classification, p_single_table=None):
    """
    Fetch the excludes
    """
    rqlib.log_debug('Start fetch exceptions for {}'.format(p_classification))

    _sql_txt = '''
               select tc_table_name       as "table_name",
                      tc_create_column    as "create_date_col",
                      tc_update_column    as "update_date_col",
                      tc_column_name      as "column_name"
               from   syst_table_classification
               where  tc_acronym = '{vA}'
               and    tc_env    = '{vT}'
               and    tc_classification = '{vP}'
               '''
    _sql = _sql_txt.format(vA=rqlib.g_params['account'],
                           vT=rqlib.g_params['env'],
                           vP=p_classification)

    if p_single_table is not None:
        l_single_table = p_single_table.upper()
        rqlib.log_debug('    Single table selected [{}]'.format(l_single_table))
        _sql += " and tc_table_name = lower('{}')".format(l_single_table)

    rqlib.log_debug('    fetch exceptions sql = [{}]'.format(_sql))

    rqlib.p_i('    Fetch exceptions to process for {}'.format(p_classification))
    tab_df = rqdbclib.read_table_data(rqlib.g_params['syst_conn_d'], _sql, p_display_info=False)

    rqlib.log_debug('    row count fetched = {}'.format(len(tab_df.index)))

    return tab_df

# --------------------------------------------------------------------
#
#                          Fetch Oracle Version
#
# --------------------------------------------------------------------


def fetch_oracle_version(p_conn):
    """ Fetch the version of oracle """
    rqlib.log_debug('fetch oracle version')

    sql = "select banner from v$version where upper(banner) like 'ORACLE DATABASE%'"

    fetched_rows = rqconnlib.ora_sql_fetch(p_conn, sql)
    if len(fetched_rows) == 1:
        fetched_ora_ver = fetched_rows[0][0]
    else:
        fetched_ora_ver = None

    return fetched_ora_ver

# --------------------------------------------------------------------
#
#                          Fetch Column Data type
#
# --------------------------------------------------------------------


def fetch_col_datatype(p_ic_tab_df, p_col):
    """ Fetch the data type from the info class spreadsheet """

    col_data_type = list(p_ic_tab_df.loc[p_ic_tab_df['COLUMN'] == p_col]['DATA_TYPE'].values)
    if col_data_type == []:
        rqlib.p_e('Error looking for column name in p_ic_tab_df. failed p_col = {}'.format(p_col))
        return None
    l_datatype = col_data_type[0].upper()

    return l_datatype

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
        rqlib.p_e('Unable to determine oracle version, please review coding, aborting')
        exit(-1)

    elif '10.2.0.4' in ora_ver:
        rqlib.p_i('Oracle version 10.2.0.4 detected, millisecond processing will be disabled.')
        return True

    return False

# --------------------------------------------------------------------
#
#                          Test for Large Datasets
#
# --------------------------------------------------------------------


def test_for_large_datasets(p_conn, p_schema=None):
    """ Look in sql server for tables with large row * col count."""

    rqlib.log_debug('Run test_for_large_datasets')

    # Note: need to add schema in here somewhere, not sure where!

    head_template = '{vCellCount:>20} {vTab:>40} {vRow:>20} {vCol:>15}'
    det_template = '{vCellCount:>20,} {vTab:>40} {vRow:>20,} {vCol:>15.0f}'
    date_cols_heading = 'Date Columns for million row tables'
    date_cols_template = '{vTab:>30} {vRow:>20} {vCol:>30} {vEnv:>4} {vMand:>4}'
    sql = """
          select t.table_name as TabName,
                 t.num_rows   as rowcnt,
                 count(*)     as column_count,
                 num_rows * count(*) as total_cell_count
          from all_tables t
              left join all_tab_columns c on  c.owner = t.owner
                                          and c.table_name = t.table_name
          where t.owner = upper('{vSchema}')
          group by t.owner, t.table_name, t.num_rows
          having num_rows * count(*) > {vMaxRowCol}
          order by 4 desc nulls last
           """.format(vMaxRowCol=MAX_ROW_COL_COUNT,
                      vSchema=p_schema)

    rqlib.log_debug('sql = {}'.format(sql))

    large_dataset_df = pd.read_sql(sql, p_conn)

    if len(large_dataset_df.index) < 1:
        rqlib.p_i('There are no row/column counts greater than {}'.format(MAX_ROW_COL_COUNT))
    else:
        # --- Print Heading
        rqlib.p_i('')
        rqlib.p_i(head_template.format(vCellCount='Cell count',
                                       vTab='Table',
                                       vRow='Rows',
                                       vCol='Columns'))

        rqlib.p_i(head_template.format(vCellCount='-' * 20,
                                       vTab='-' * 40,
                                       vRow='-' * 20,
                                       vCol='-' * 15))
        # --- Print Data
        for i, row in large_dataset_df.iterrows():
            rqlib.p_i(det_template.format(vCellCount=row.TOTAL_CELL_COUNT,
                                          vTab=row.TABNAME,
                                          vRow=row.ROWCNT,
                                          vCol=row.COLUMN_COUNT))
        rqlib.p_i('')
        rqlib.p_i('')

        for i, row in large_dataset_df.iterrows():
            rqlib.p_i("'{}','x','x'".format(row.TABNAME))

        rqlib.p_i('')

    sql = """
          select t.table_name as TabName,
                 t.num_rows   as rowcnt,
                 count(*)     as column_count,
                 num_rows * count(*) as total_cell_count
          from all_tables t
              left join all_tab_columns c on  c.owner = t.owner
                                          and c.table_name = t.table_name
          where t.owner = upper('{vSchema}')
          group by t.owner, t.table_name, t.num_rows
          having num_rows * count(*) > {vMaxRowCol}
          order by 2 desc nulls last
           """.format(vMaxRowCol=MAX_ROW_COL_COUNT,
                      vSchema=p_schema)

    rqlib.log_debug('sql = {}'.format(sql))

    large_dataset_df = pd.read_sql(sql, p_conn)

    if len(large_dataset_df.index) < 1:
        pass   # This has already been reported.
    else:
        # --- Print Heading
        rqlib.p_i('')
        rqlib.p_i(head_template.format(vCellCount='Cell count',
                                       vTab='Table',
                                       vRow='Rows',
                                       vCol='Columns'))

        rqlib.p_i(head_template.format(vCellCount='-' * 20,
                                       vTab='-' * 40,
                                       vRow='-' * 20,
                                       vCol='-' * 15))
        # --- Print Data
        for i, row in large_dataset_df.iterrows():
            rqlib.p_i(det_template.format(vCellCount=row.TOTAL_CELL_COUNT,
                                          vTab=row.TABNAME,
                                          vRow=row.ROWCNT,
                                          vCol=row.COLUMN_COUNT))
        rqlib.p_i('')
        rqlib.p_i('')

    # --- List of tables with date columns.

    sql = """
          select t.table_name   as TABNAME,
                 t.num_rows     as ROWCNT,
                 nvl(c.column_name,'no date cols')  as COL,
                 nvl(c.data_type,'.')               as DATA_TYPE,
                 nvl(c.nullable,'.')               as NULLABLE
          from all_tables t
              left join all_tab_columns c on  c.owner = t.owner
                                          and c.table_name = t.table_name
                                          and ( c.data_type like 'TIMESTAMP%'
                                               or c.data_type like '%DATE%')
          where t.owner = upper('{vSchema}')
          and   t.num_rows > 1000000
          order by 2 desc nulls last
           """.format(vMaxRowCol=MAX_ROW_COL_COUNT,
                      vSchema=p_schema)

    rqlib.log_debug('sql = {}'.format(sql))

    report_df = pd.read_sql(sql, p_conn)

    if len(report_df.index) < 1:
        rqlib.p_i('There are no tables with 1,000,000 rows, cannot report Columns')
    else:
        # --- Print Heading
        rqlib.p_i('')
        rqlib.p_i(date_cols_heading)
        rqlib.p_i(date_cols_template.format(vTab='Table',
                                            vRow='Rows',
                                            vCol='Columns',
                                            vEnv='Type',
                                            vMand='Mand'))

        rqlib.p_i(date_cols_template.format(vTab='-' * 30,
                                            vRow='-' * 20,
                                            vCol='-' * 30,
                                            vEnv='-' * 4,
                                            vMand='-' * 4))
        # --- Print Data

        for i, row in report_df.iterrows():
            rqlib.p_i(date_cols_template.format(vTab=row.TABNAME,
                                                vRow=row.ROWCNT,
                                                vCol=row.COL,
                                                vEnv=row.DATA_TYPE,
                                                vMand=row.NULLABLE))
        rqlib.p_i('')
        rqlib.p_i('')

        rqlib.p_i('')

    return


# ------------------------------------------------------------------------------------------
#
#                                       GENERATE SQL FETCH PK
#
# ------------------------------------------------------------------------------------------


def generate_sql_fetch_pk(p_ic_tab_df, p_rs, p_schema):
    """
    create a string of columns that consist of the primary key only

    Parameters
    ----------
    p_ic_tab_df: Dataframe consisting of the Info Class data for this table only
         A Data Frame

    p_rs: Is this a redshift database column?
        A Boolean

    Returns
    -------
    res: string
      the columns contained in the primary key

    returns None if there is not primary key

    Example
    -------
    >>> generate_sql_fetch_pk(p_ic_tab_df)
    pkcol1,pkcol2,pkcol3

    """
    start_str = 'Start generate_sql_fetch_pk p_ic_tab_df.len [{}]'
    rqlib.log_debug(start_str.format(len(p_ic_tab_df.index)))

    pkcols_df = p_ic_tab_df.loc[p_ic_tab_df['PRIMARY_KEY'] == 'Y']
    pkcols = pkcols_df['COLUMN']

    first_column = True
    result = None

    for col in pkcols:
        if p_rs:
            actual_col = rqdbclib.convert_rs_reserved_word(col, p_schema)
        else:
            actual_col = col

        if first_column:
            result = actual_col
            first_column = False
        else:
            result += ',{}'.format(actual_col)

    end_str = 'End generate_sql_fetch_pk result [{}]'
    rqlib.log_debug(end_str.format(result))

    return result

# --------------------------------------------------------------------
#
#                          tab list filter
#
# --------------------------------------------------------------------


def validate_inf_tab_cols(p_ic_tab_df):
    """ Test to see if all columns are set to DO NOT REPLICATE"""

    result = p_ic_tab_df.TREATMENT.unique()

    if len(result) == 1:
        newresult = str(result).lower()
        if 'do not replicate' in newresult:
            return False

    return True

# --------------------------------------------------------------------
#
#                          tab list filter
#
# --------------------------------------------------------------------


def tab_list_filter(p_full_list, p_big_df, p_big, p_small):
    """
    Go through the full list of tables and filter out what is not required
    depending on the parameters selected (big, small or all)
    """
    rqlib.log_debug('start tab list filter')

    if p_big:
        big_list = p_big_df.TABLENAME.tolist()
        for tab in big_list:
            if tab not in p_full_list:
                rqlib.p_e('Table [{}] is in BIG list, but not in FULL list of tables to process'
                          .format(tab))
        return_list = big_list

    elif p_small:
        big_list = p_big_df['TABLENAME'].tolist()
        new_list = []
        for tab in p_full_list:
            if tab not in big_list:
                new_list.append(tab)
        return_list = new_list

    else:
        return_list = p_full_list

    return return_list

# --------------------------------------------------------------------
#
#                          tab list filter
#
# --------------------------------------------------------------------


def big_tab_list_create(p_full_list, p_big_df):
    """
    Create a list of all big tables.
    Verify that all big tables are on the full table list.
    """
    err_template = 'Table [{}] is in BIG list, but not in FULL list of tables to process'

    rqlib.log_debug('start bit tab list')

    big_list = p_big_df.TABLENAME.tolist()
    for tab in big_list:
        if tab not in p_full_list:
            rqlib.p_e(err_template.format(tab))

    return big_list

# --- Generate SQL

# --------------------------------------------------------------------
#
#                          generate where template
#
# --------------------------------------------------------------------


def generate_where_templates(p_db_type, p_col_type, p_millisec_processing):
    """
    determine which conversion to string is correct
    """

    if p_db_type == DB_TYPE_REDSHIFT:

        double_text = "round({vF},6)"
        varbinary_text = "'{vF}'"
        datems_text = "to_char('{vF}','yyyy-mm-dd hh24:mi:ss.ms')"
        date_text = "to_char('{vF}','yyyy-mm-dd hh24:mi:ss')"
        int_text = "{vF}"
        str_text = "'{vF}'"
        default_text = "'{vF}'"

        if not rqlib.g_params['millisec_proc']:
            datems_text = date_text

    elif p_db_type == DB_TYPE_ORACLE:

        double_text = "round({vF},6)"
        varbinary_text = "unknown_function('{vF}')"
        datems_text = "to_char('{vF}','yyyy-mm-dd hh24:mi:ss.ff3')"
        date_text = "to_char('{vF}','yyyy-mm-dd hh24:mi:ss')"
        int_text = "{vF}"
        str_text = "'{vF}'"
        default_text = "'{vF}'"

        if not rqlib.g_params['millisec_proc']:
            datems_text = date_text

    elif p_db_type == DB_TYPE_MSSQL:

        double_text = "round({vF},6)"
        varbinary_text = "CONVERT(varchar(max),'{vF}',2)"
        datems_text = "convert(varchar(30), '{vF}', 121)"
        date_text = "convert(varchar(30), '{vF}', 121)"
        int_text = "{vF}"
        str_text = "'{vF}'"
        default_text = "'{vF}'"

    else:

        rqlib.p_e('ERROR - connection dbtype is unknown for generate where columns')
        rqlib.p_e('       must be Redshift, Oracle or MSSQL')
        rqlib.p_e('       got [{}]'.format(p_db_type))
        return None

    # -- Test for data types.

    l_numb_float = re.match('NUMERIC.*,', p_col_type, re.I)

    if 'VARBINARY' in p_col_type:
        l_type = 'binary'

    elif 'DBFLT' in p_col_type or l_numb_float:
        l_type = 'float'

    elif 'DATETIME' in p_col_type or 'SMALLDATE' in p_col_type or 'TIMESTAMP' in p_col_type:
        l_type = 'datetime'

    elif 'DATETIME2' in p_col_type.split(' ') and '(6)' in p_col_type.split(' '):
        l_type = 'date'

    elif 'DATE' in p_col_type:
        l_type = 'date'

    elif 'CHAR' in p_col_type:
        l_type = 'string'

    elif 'INT' in p_col_type:
        l_type = 'int'

    else:
        l_type = 'string'

    # -- Now convert data type into a format string

    if l_type == 'string':
        curr_str = str_text

    elif l_type == 'binary':
        curr_str = varbinary_text

    elif l_type == 'int':
        curr_str = int_text

    elif l_type == 'datetime':
        curr_str = datems_text

    elif l_type == 'date':
        curr_str = date_text

    elif l_type == 'float':
        curr_str = double_text

    else:
        curr_str = default_text

    return curr_str


# --------------------------------------------------------------------
#
#                          generate sql template
#
# --------------------------------------------------------------------


def generate_sql_templates(p_db_type, p_col_type, p_millisec_processing):
    """
    determine which conversion to string is correct
    """

    if p_db_type == DB_TYPE_REDSHIFT:

        double_text = "round({vF},6) as {vF}"
        varbinary_text = "rtrim({vF}) as {vF}"
        datems_text = "to_char({vF},'yyyy-mm-dd hh24:mi:ss.ms') as {vF}"
        date_text = "to_char({vF},'yyyy-mm-dd hh24:mi:ss') as {vF}"
        str_text = "rtrim({vF}) as {vF}"
        default_text = "{vT}.{vF}"
        clob_text = "{vF} as {vF}"
        long_text = "{vF} as {vF}"

        if not rqlib.g_params['millisec_proc']:
            datems_text = date_text

    elif p_db_type == DB_TYPE_ORACLE:

        double_text = "round({vT}.[{vF}],6) as [{vF}]"
        varbinary_text = "unknown_function({vF}) as {vF}"
        datems_text = "to_char({vF},'yyyy-mm-dd hh24:mi:ss.ff3') as {vF}"
        date_text = "to_char({vF},'yyyy-mm-dd hh24:mi:ss') as {vF}"
        str_text = "rtrim({vF}) as {vF}"
        default_text = "{vT}.{vF}"
        clob_text = "' ' as {vF}"
        long_text = "' ' as {vF}"

        if not rqlib.g_params['millisec_proc']:
            datems_text = date_text

    elif p_db_type == DB_TYPE_MSSQL:

        double_text = "round({vT}.[{vF}],6) as [{vF}]"
        varbinary_text = "CONVERT(varchar(max),{vT}.[{vF}],2)"
        datems_text = "convert(varchar(30), {vT}.[{vF}], 121) as [{vF}]"
        date_text = "convert(varchar(30), {vT}.[{vF}], 121) as [{vF}]"
        str_text = "rtrim({vT}.[{vF}]) as [{vF}]"
        default_text = "{vT}.{vF}"
        clob_text = "' ' as {vF}"

    else:

        rqlib.p_e('ERROR - connection dbtype is unknown for generate sql columns')
        rqlib.p_e('       must be Redshift, Oracle or MSSQL')
        rqlib.p_e('       got [{}]'.format(p_db_type))
        return None

    # -- Test for data types.

    l_numb_float = re.match('NUMERIC.*,', p_col_type, re.I)

    if 'VARBINARY' in p_col_type:
        l_type = 'binary'

    elif 'DBFLT' in p_col_type or l_numb_float:
        l_type = 'float'

    elif 'DATETIME' in p_col_type or 'SMALLDATE' in p_col_type or 'TIMESTAMP' in p_col_type:
        l_type = 'datetime'

    elif 'DATETIME2' in p_col_type.split(' ') and '(6)' in p_col_type.split(' '):
        l_type = 'date'

    elif 'DATE' in p_col_type:
        l_type = 'date'

    elif 'CHAR' in p_col_type:
        l_type = 'string'

    elif 'CLOB' in p_col_type or 'BLOB' in p_col_type:
        l_type = 'clob'

    elif 'LONG' in p_col_type:
        l_type = 'long'

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

    elif l_type == 'long':
        curr_str = long_text

    else:
        curr_str = default_text

    return curr_str

# --------------------------------------------------------------------
#
#                          sql columns
#
# --------------------------------------------------------------------


def generate_sql_columns(p_conn, p_table_info, p_columns):
    """
    Generate the SQL for all the the select column name list
    """

    # -- Setup local variables
    sql_columns = None

    l_tab = p_table_info['tab']
    l_ic_tab_df = p_table_info['ss_info_tab_df']
    l_dbtype = p_conn['dbtype']
    l_is_redshft = l_dbtype == DB_TYPE_REDSHIFT
    l_exclude_cols_s = p_table_info['exclude_cols_s']

    l_columns = p_columns.upper()

    # -- Loop through columns, generating sql as you go.

    for col in l_columns.split(','):

        # -- exclude any columns as listed.
        if l_exclude_cols_s is not None:
            if col in l_exclude_cols_s.values:
                continue

        curr_str = ''
        l_datatype = fetch_col_datatype(l_ic_tab_df, col)
        if l_datatype is None:
            rqlib.p_e('Error determining datatype for column {}'.format(col))
            return None

        if l_is_redshft:
            actual_col = rqdbclib.convert_rs_reserved_word(col, p_conn['schema'])
            actual_col = actual_col.lower()
        else:
            actual_col = col.lower()

        curr_str_template = generate_sql_templates(l_dbtype, l_datatype,
                                                   rqlib.g_params['millisec_proc'])
        curr_str = curr_str_template.format(vF=actual_col, vT=l_tab)

        # -- Add to cumulative string.
        if sql_columns is None:
            sql_columns = curr_str
        else:
            sql_columns += ', {}'.format(curr_str)

    return sql_columns
# --------------------------------------------------------------------
#
#                          sql columns
#
# --------------------------------------------------------------------


def generate_where_columns(p_conn, p_columns, p_ic_tab_df):
    """
    Generate the SQL for all the the select column name list
    """
    l_dbtype = p_conn['dbtype']

    # -- Sort out templates

    if l_dbtype == DB_TYPE_REDSHIFT:

        date_text = "and {v} = to_date({v2},'yyyy-mm-dd hh24:mi:ss')"
        str_text = "and {v} = rtrim({v2})"

        if rqlib.g_params['millisec_proc']:
            ts_text = "and {v} = to_timestamp({v2},'yyyy-mm-dd hh24:mi:ss.ms')"
        else:
            ts_text = "and {v} = to_timestamp({v2},'yyyy-mm-dd hh24:mi:ss')"

    elif l_dbtype == DB_TYPE_ORACLE:

        date_text = "and {v} = to_date({v2},'yyyy-mm-dd hh24:mi:ss')"
        str_text = "and {v} = rtrim({v2})"

        if rqlib.g_params['millisec_proc']:
            ts_text = "and {v} = to_timestamp({v2},'yyyy-mm-dd hh24:mi:ss.ff3')"
        else:
            ts_text = "and {v} = to_timestamp({v2},'yyyy-mm-dd hh24:mi:ss')"

    elif l_dbtype == DB_TYPE_MSSQL:

        date_text = "and {v} = {v2}"
        str_text = "and {v} = rtrim({v2})"

        ts_text = "and {v} = {v2}"

    sql_columns = ''
    for col in p_columns.split(','):

        curr_str = ''
        col_data_type = fetch_col_datatype(p_ic_tab_df, col)

        sub_col = col.lower()

        if l_dbtype == DB_TYPE_REDSHIFT:
            actual_col = rqdbclib.convert_rs_reserved_word(col, p_conn['schema'])
            actual_col = actual_col.lower()
        elif l_dbtype == DB_TYPE_ORACLE:
            actual_col = col.lower()
        elif l_dbtype == DB_TYPE_MSSQL:
            actual_col = col

        if 'CHAR' in col_data_type[0].upper():
            curr_str = str_text.format(v=actual_col, v2="'{" + '{}_val'.format(sub_col) + "}'")

        elif 'TIMESTAMP' in col_data_type[0].upper():
            curr_str = ts_text.format(v=actual_col, v2="'{" + '{}_val'.format(sub_col) + "}'")

        elif 'DATE' in col_data_type[0].upper():
            curr_str = date_text.format(v=actual_col, v2="'{" + '{}_val'.format(sub_col) + "}'")
        else:
            curr_str = ' and {v} = {v2}'.format(v=actual_col,
                                                v2='{' + '{}_val'.format(sub_col) + '}')

        sql_columns += ' {} '.format(curr_str)

    return sql_columns

# --------------------------------------------------------------------
#
#                          generate sql
#
# --------------------------------------------------------------------


def generate_big_where_clause(p_db_type, p_table_info):
    """
    Create a where clause for DAILY processing

    -- only fetch records created 40 minutes hour after the lag date
       There is 30 minute window taken off lag date for commit times
       We need to move 10 minutes forward of the actual lag time
       This should reduce the number of false positives (records created in source, not
       yet shipped to target).

    """
    rqlib.log_debug('Start generate big where clause')

    l_create_col = p_table_info['create_date_col']
    l_update_col = p_table_info['update_date_col']
    l_lag_date = p_table_info['pk_lag_date']

    l_where = ''

    if p_db_type == DB_TYPE_ORACLE:

        date_str = "trunc(sysdate - {vDays})".format(vDays=DAYS_FOR_DEEP_ANALYSE)
        lag_str = "(to_date('{}', 'dd-mon-yyyy hh24:mi:ss') + (40/(24*60*60)))".format(l_lag_date)

        l_where += " and (    {} >= {} ".format(l_create_col, date_str)
        l_where += "       or {} >= {})".format(l_update_col, date_str)

        sql_txt = " and ( {} <= {} )"
        l_where += sql_txt.format(l_create_col, lag_str)

    elif p_db_type == DB_TYPE_MSSQL:

        date_str = "convert(date, (DATEADD(day,-{vDays},getdate())))".format(
            vDays=DAYS_FOR_DEEP_ANALYSE)
        lag_str = "convert(date, (DATEADD(minute,40,'{}')))".format(l_lag_date)

        l_where += " and (    {} >= {} ".format(l_create_col, date_str)
        l_where += "       or {} >= {}) ".format(l_update_col, date_str)

        sql_txt = " and ( {} <= '{}' )"
        l_where += sql_txt.format(l_create_col, l_lag_date)

    elif p_db_type == DB_TYPE_REDSHIFT:

        date_str = "trunc(dateadd(day, -{vDays}, convert_timezone('Australia/Brisbane',sysdate)))"
        date_txt = date_str.format(vDays=DAYS_FOR_DEEP_ANALYSE)
        l_where += " and (    {} >= {} ".format(l_create_col, date_txt)
        l_where += "       or {} >= {})".format(l_update_col, date_txt)

    rqlib.log_debug('    daily where clause [{}]'.format(l_where))
    return l_where

# --------------------------------------------------------------------
#
#                          generate sql
#
# --------------------------------------------------------------------


def generate_big_sql(p_conn, p_table_info, p_columns):
    """
    Create SQL from Info Classification data
    """

    rqlib.log_debug('Start generate big sql for table: {}'.format(p_table_info['tab']))

    # -- Setup variables
    l_tab = p_table_info['tab']
    l_dbtype = p_conn['dbtype']
    l_count_limit = rqlib.g_params['count_limit']
    l_is_redshft = l_dbtype == DB_TYPE_REDSHIFT
    l_is_mssql = l_dbtype == DB_TYPE_MSSQL
    l_is_daily = rqlib.g_params['run_type'] == 'daily'

    if rqlib.g_params['run_type'] == 'daily':
        daily_where_clause = generate_big_where_clause(l_dbtype, p_table_info)

    sql_columns = generate_sql_columns(p_conn, p_table_info, p_columns)

    if sql_columns is None:
        return None

    rqlib.log_debug('    sql columns: [{}]'.format(sql_columns))

    # -- Create SQL

    sql = 'select '

    if l_count_limit is not None and l_dbtype in (DB_TYPE_MSSQL, DB_TYPE_REDSHIFT):
        sql += ' top {} '.format(l_count_limit)

    if l_is_mssql:
        sql += '{} from {}.{}.{} '.format(sql_columns,
                                          p_conn['schema'],
                                          p_conn['db_name'],
                                          l_tab)
    else:
        sql += '{} from {}.{} '.format(sql_columns, p_conn['schema'], l_tab)




    if l_is_redshft:
        sql += " where end_dt = to_date('31/12/9999','dd/mm/yyyy') "
    else:
        sql += " where 1=1 "

    if l_is_daily and daily_where_clause is not None:
        sql += daily_where_clause

    if l_count_limit is not None and l_dbtype == DB_TYPE_ORACLE:
        sql += " and rownum <= {}".format(l_count_limit)

    sql += ' order by {}'.format(p_table_info['source_pkcols'])

    rqlib.log_debug('final sql = {}'.format(sql))

    return sql

# --------------------------------------------------------------------
#
#                          generate sql BY ROW
#
# --------------------------------------------------------------------


def generate_big_sql_byrow(p_conn, p_table_info, p_columns):
    """
    Create SQL from Info Classification data
    This is for QUICK VAL only, for validating a single row
    for a single row (PK)
    """

    rqlib.log_debug('create sql by row for table {}'.format(p_table_info['tab']))

    # -- Setup variables
    l_ic_tab_df = p_table_info['ss_info_tab_df']
    l_tab = p_table_info['tab']
    l_dbtype = p_conn['dbtype']
    l_is_redshft = l_dbtype == DB_TYPE_REDSHIFT
    l_ic_tab_df = p_table_info['ss_info_tab_df']

    sql_columns = generate_sql_columns(p_conn, p_table_info, p_columns)

    if sql_columns is None:
        return None

    sql = 'select '

    sql += '{} from {}.{} '.format(sql_columns, p_conn['schema'], l_tab)

    if l_is_redshft:
        sql += " where end_dt = to_date('31/12/9999','dd/mm/yyyy') "
    else:
        sql += " where 1=1 "

    where_clause = generate_where_columns(p_conn, p_table_info['source_pkcols'], l_ic_tab_df)

    sql += where_clause

    rqlib.log_debug('final sql = {}'.format(sql))

    return sql

# --------------------------------------------------------------------
#
#                          generate sql BY ROW
#
# --------------------------------------------------------------------


def generate_single_row_sql(p_conn, p_table_info, p_columns):
    """
    Create SQL from Info Classification data
    This is for QUICK VAL only, for validating a single row
    for a single row (PK)
    """

    rqlib.log_debug('create single row sql for table {}'.format(p_table_info['tab']))

    # -- Setup variables
    # l_ic_tab_df = p_table_info['ss_info_tab_df']
    l_tab = p_table_info['tab']
    l_dbtype = p_conn['dbtype']
    l_is_redshft = l_dbtype == DB_TYPE_REDSHIFT
    # l_ic_tab_df = p_table_info['ss_info_tab_df']
    l_is_mssql = l_dbtype == DB_TYPE_MSSQL
    l_is_genesys = rqlib.g_params['account'] == 'genesys'

    sql_columns = generate_sql_columns(p_conn, p_table_info, p_columns)

    if sql_columns is None:
        return None

    sql = 'select '

    if l_is_mssql:
        sql += '{} from {}.{}.{} '.format(sql_columns,
                                          p_conn['schema'],
                                          p_conn['db_name'],
                                          l_tab)
    else:
        sql += '{} from {}.{} '.format(sql_columns, p_conn['schema'], l_tab)

    if l_is_redshft and not l_is_genesys:
        sql += " where end_dt = to_date('31/12/9999','dd/mm/yyyy') "
    else:
        sql += " where 1=1 "

    rqlib.log_debug('final sql = {}'.format(sql))

    return sql

# --------------------------------------------------------------------
#
#                          generate sql
#
# --------------------------------------------------------------------


def generate_sql_small(p_conn, p_table_info, p_columns):
    """
    Create SQL from Info Classification data
    """

    rqlib.log_debug('create sql for table {}'.format(p_table_info['tab']))

    # -- Setup variables
    l_tab = p_table_info['tab']
    l_dbtype = p_conn['dbtype']
    l_count_limit = rqlib.g_params['count_limit']
    l_is_redshft = l_dbtype == DB_TYPE_REDSHIFT
    l_is_mssql = l_dbtype == DB_TYPE_MSSQL
    l_is_genesys = rqlib.g_params['account'] == 'genesys'
    l_target_to_midnight = rqlib.g_params['target_to_midnight']
    l_condition = rqlib.g_params['add_condition']

    sql_columns = generate_sql_columns(p_conn, p_table_info, p_columns)
    if sql_columns is None:
        return None

    # -- Create SQL

    sql = 'select '

    if l_count_limit is not None and l_dbtype in (DB_TYPE_MSSQL, DB_TYPE_REDSHIFT):
        sql += ' top {} '.format(l_count_limit)

    if l_is_mssql:
        sql += '{} from {}.{}.{} '.format(sql_columns,
                                          p_conn['schema'],
                                          p_conn['db_name'],
                                          l_tab)
    else:
        sql += '{} from {}.{} '.format(sql_columns, p_conn['schema'], l_tab)

    # -- Process SQL Server Condition

    if l_is_mssql and l_condition is not None:
        l_inner_join = None

        for col in p_table_info['source_pkcols'].split(','):
            actual_col = '[' + col + ']'
            if l_inner_join is None:
                l_inner_join = ' on KeyTab.{}={}.{}'.format(actual_col, l_tab, actual_col)
            else:
                l_inner_join += ' and KeyTab.{}={}.{}'.format(actual_col, l_tab, actual_col)

        sql += ' inner join (values '

        l_val = l_condition[l_condition.find('(', l_condition.find('(') + 1) + 1:]
        sql += l_val
        sql += '  as KeyTab({}) {}'.format(p_table_info['source_pkcols'], l_inner_join)

    if l_is_redshft and not l_is_genesys:
        sql += " where end_dt = to_date('31/12/9999','dd/mm/yyyy') "
    else:
        sql += " where 1=1 "

    # Target to midnight should be moved
    if l_is_redshft and l_target_to_midnight:
        sql += """
               and eff_dt < trunc(convert_timezone('Australia/Brisbane',sysdate))
               and end_dt > trunc(convert_timezone('Australia/Brisbane',sysdate))
               """
# -- this will be modified to fetch data from the table syst_batch_table_errors
#    if rqlib.g_params['add_condition'] is not None:
#        sql += rqlib.g_params['add_condition']

    if l_count_limit is not None and l_dbtype == DB_TYPE_ORACLE:
        sql += " and rownum <= {}".format(l_count_limit)

    if not l_is_mssql and l_condition is not None:
        if rqlib.g_params['add_condition'] is not None:
            sql += rqlib.g_params['add_condition']

    sql += ' order by {}'.format(p_table_info['source_pkcols'])

    rqlib.log_debug('final sql = {}'.format(sql))

    return sql

# --- Process
# --------------------------------------------------------------------
#
#                          process big
#
# --------------------------------------------------------------------


def process_big(p_batch_info, p_tabinfo_d):
    """
    Compare the data for this table, one column at a time
       batch id -- Current batch
       tabinfo  -- everything need to know about this table
    """

    rqlib.log_debug('Start process big')

    l_new_preproc_l = p_tabinfo_d['new_preproc_candidates_l']
    l_tab = p_tabinfo_d['tab']
    l_batch_id = p_batch_info['batch_id']

    l_tab_col_df = rqblib.fetch_table_columns(p_batch_info, p_tabinfo_d, 'big')

    l_source_conn = rqlib.g_params['source_conn']
    l_target_conn = rqlib.g_params['target_conn']

    l_pkcols = p_tabinfo_d['source_pkcols']

    # if this is a huge table, then only process 1000 rows at a time.
    if rqlib.g_params['run_type'] == 'huge':
        big_and_chunky = True
    else:
        big_and_chunky = False

    overall_success = True

    for dummy_i, row in l_tab_col_df.iterrows():

        l_col = row['BT_COLUMN_NAME']

        l_status = row['BT_STATUS']
        l_tab_col = '{}.{}'.format(l_tab, l_col)

        if l_status == BT_FINISH_OK:
            rqlib.log_debug('    column already finished ok, skipping {}'.format(l_tab_col))
            continue
        elif l_status == BT_FINISH_ERR:
            rqlib.log_debug('    column already finished with error, skipping {}'.format(l_tab_col))
            overall_success = False
            continue

        l_process_cols = '{},{}'.format(l_pkcols, l_col)

        source_sql = generate_big_sql(l_source_conn, p_tabinfo_d, l_process_cols)
        target_sql = generate_big_sql(l_target_conn, p_tabinfo_d, l_process_cols)

        rqlib.log_debug('SOURCE SQL: {}'.format(source_sql))
        rqlib.log_debug('TARGET SQL: {}'.format(target_sql))

        if source_sql is None or target_sql is None:
            rqlib.p_e('Failed to generate SQL')
            rqlib.p_e('SOURCE SQL: {}'.format(source_sql))
            rqlib.p_e('TARGET SQL: {}'.format(target_sql))
            return False

        print('... Compare {}....'.format(l_tab_col))

        # --------------------------------------------
        # Set the status of this tab.col to RUNNING

        set_status = rqblib.batch_process_table(l_batch_id, BT_INIT, l_tab, l_col)
        if not set_status:
            rqlib.p_e('Unable to set RUNNING status for this tab.col {}'.format(l_tab_col))
            rqlib.p_e('SKIPPING COLUMN')
            overall_success = False
            continue

        # this may need to be changed to handle columns as well as tables
        preprocess_flag = False
        if l_tab_col in l_new_preproc_l:
            preprocess_flag = True

        if big_and_chunky:
            my_result, s_df, t_df = rqdbclib.tab_compare(l_source_conn,
                                                         l_target_conn,
                                                         source_sql, target_sql,
                                                         l_pkcols,
                                                         p_chunk=rqdbclib.DEFAULT_SQL_CHUNK,
                                                         p_tab=l_tab,
                                                         p_run_preprocessor=preprocess_flag,
                                                         p_EDH_mode=True)
        else:
            source_df = rqdbclib.read_table_data(l_source_conn, source_sql, p_description='source')
            target_df = rqdbclib.read_table_data(l_target_conn, target_sql, p_description='target')

            # always use source columns, they do not end in underscore
            target_df.columns = source_df.columns
            # source_df.columns = target_df.columns

            if preprocess_flag:
                rqlib.p_d('pre process data source')
                rqdbclib.pre_process_source(source_df)
                rqlib.p_d('pre process data target')
                rqdbclib.pre_process_target(target_df)

            rqlib.p_d('Start compare')
            my_result, s_df, t_df = rqdbclib.tab_compare_df_no_chunks(source_df, target_df,
                                                                      l_pkcols,
                                                                      p_tab=l_tab,
                                                                      p_print_errors=False,
                                                                      p_EDH_mode=True)
        # Finished compares, now do something with the results.
        if my_result:
            rqlib.p_i('... Compare OK for {} ....'.format(l_tab_col))
            retval = BT_FINISH_OK
        else:
            rqlib.p_e('... Compare failed for {} ....'.format(l_tab_col))
            if len(s_df.index) + len(t_df.index) > BT_MAX_ERROR_COUNT:
                rqlib.p_e('... Compare failed for {} TO MANY ERRORS TO PROCESS....'
                          .format(l_tab_col))
                retval = BT_FINISH_TOO_MANY_ERRORS
                overall_success = False
            else:
                rqlib.p_e('... Compare failed for {}....'.format(l_tab_col))
                insert_batch_table_errors(s_df, t_df, p_batch_info, p_tabinfo_d, l_col)
                retval = BT_FINISH_ERR
                overall_success = False

        # -----------------------------------------
        # Update batch table for this TAB.COL

        save_tab_result(l_batch_id, retval, l_tab, l_col)

    # If this returns true, it will set the success at table level (i.e. at BIG, not BIG_BY_COL)
    if overall_success:
        return BT_FINISH_OK
    else:
        return BT_FINISH_ERR

# --------------------------------------------------------------------
#
#                          validate pk cols
#
# --------------------------------------------------------------------


# def validate_pkcols(p_sql_info):
#    """
#     if cr_col is in source_pkcols, remove it.
#     if upd_col is in source_pkcols, remove it.
#     same for target_pkcols.
#    """
#    cr = p_sql_info['cr_col']
#    upd = p_sql_info['upd_col']
#
#    p_sql_info['source_pkcols'] = remove_non_unique_str(p_sql_info['source_pkcols'], cr)
#    p_sql_info['source_pkcols'] = remove_non_unique_str(p_sql_info['source_pkcols'], upd)
#    p_sql_info['target_pkcols'] = remove_non_unique_str(p_sql_info['target_pkcols'], cr)
#    p_sql_info['target_pkcols'] = remove_non_unique_str(p_sql_info['target_pkcols'], upd)
#
#    return

# --------------------------------------------------------------------
#
#                          process daily (quick val)
#
# --------------------------------------------------------------------


def process_daily(p_batch_info, p_tabinfo_d):
    """
    Compare last rqlib.DAYS_FOR_DEEP_ANALYSE days of data for this table
    """
    rqlib.log_debug('Start process daily')

    l_source_conn = rqlib.g_params['source_conn']
    l_target_conn = rqlib.g_params['target_conn']

    # -- Create a list of columns
    #    remove any duplicate cols between PK and cr/up (must keep cr/up, so remove pk)
    # -- columns must be in source db format. The sql generator will convert to RS when required.
    columns = '{},{},{}'.format(p_tabinfo_d['source_pkcols'],
                                p_tabinfo_d['create_date_col'],
                                p_tabinfo_d['update_date_col'])
    columns = remove_non_unique_str(columns, p_tabinfo_d['create_date_col'])
    columns = remove_non_unique_str(columns, p_tabinfo_d['update_date_col'])

    source_sql = generate_big_sql(l_source_conn, p_tabinfo_d, columns)
    target_sql = generate_big_sql(l_target_conn, p_tabinfo_d, columns)

    p_tabinfo_d['source_sql_byrow'] = generate_big_sql_byrow(l_source_conn, p_tabinfo_d, columns)
    p_tabinfo_d['target_sql_byrow'] = generate_big_sql_byrow(l_target_conn, p_tabinfo_d, columns)

    csv_tab_result = {}
    #    csv_init(csv_tab_result, p_tab, p_tabinfo_d)

    rqlib.log_debug('SOURCE SQL: {}'.format(source_sql))
    rqlib.log_debug('TARGET SQL: {}'.format(target_sql))

    if source_sql is None or target_sql is None:
        rqlib.p_e('Failed to generate SQL')
        rqlib.p_e('SOURCE SQL: {}'.format(source_sql))
        rqlib.p_e('TARGET SQL: {}'.format(target_sql))
        return False

    source_df = rqdbclib.read_table_data(l_source_conn, source_sql, p_description='source')
    target_df = rqdbclib.read_table_data(l_target_conn, target_sql, p_description='target')
    # always use source columns, they do not end in underscore
    target_df.columns = source_df.columns

    if source_df is None or target_df is None:
        rqlib.p_e('FAILED to fetch data from either source or target')
        return False

    len_s = len(source_df.index)

    rqlib.p_d('Start compare')
    # rqdbcrlib.print_html_report('Compare table {}'.format(p_tab), p_skip_before=1)
    my_result, s_df, t_df = rqdbclib.tab_compare_df_no_chunks(source_df, target_df,
                                                              p_tabinfo_d['source_pkcols'],
                                                              p_tab=p_tabinfo_d['tab'],
                                                              p_print_errors=False)

    # If compare fails, try the failed rows from scratch
    if not my_result:
        # actual_lag_date = generate_sql_fetch_max_date(p_target_conn, p_tab, sql_info)
        data = {}
        data['source_full_df'] = source_df
        data['target_full_df'] = target_df
        data['source_rem_df'] = s_df
        data['target_rem_df'] = t_df

        real_error_count = rqdbcalib.analyse_results2(data, p_tabinfo_d, csv_tab_result)

        if real_error_count > 0:

            err_perc = (1 - ((len_s - real_error_count) / len_s)) * 100
            txt = '... Compare failed {}, error percentage = {}%, source rows {}, real errors {}'
            l_txt = txt.format(p_tabinfo_d['tab'], round(err_perc, 2), len_s, real_error_count)
            rqlib.p_e(l_txt)
            rqdbcrlib.print_summary_report(l_txt)
            csv_tab_result['ERROR PERCENTAGE'] = err_perc
            return_code = False

        else:
            rqlib.p_i('... Errors found, all false positives {} ....'.format(p_tabinfo_d['tab']))
            l_txt = '... Compare OK for {} ....'.format(p_tabinfo_d['tab'])

            rqlib.p_i(l_txt)
            rqdbcrlib.print_summary_report(l_txt)
            return_code = True

    else:
        p_txt = '... Compare OK for {} ....'.format(p_tabinfo_d['tab'])
        rqlib.p_i(p_txt)
        rqdbcrlib.print_summary_report(p_txt)
        return_code = True

    if 'csv_report' in rqlib.g_params:
        global_csv_report = rqlib.g_params['csv_report']
        global_csv_report[p_tabinfo_d['tab']] = csv_tab_result

    if return_code:
        return BT_FINISH_OK
    else:
        return BT_FINISH_ERR

# --------------------------------------------------------------------
#
#                          process small
#
# --------------------------------------------------------------------


def process_small(p_batch_info, p_tabinfo_d):
    """
    Process the small tables.
       batch id -- Current batch
       tabinfo  -- everything need to know about this table
    """
    rqlib.log_debug('Start process small')

    preprocess_flag = tab_preproc(p_tabinfo_d, 'small')
    l_tab = p_tabinfo_d['tab']

    # if this is a huge table, then only process 1000 rows at a time.
    if rqlib.g_params['run_type'] == 'transact':
        big_and_chunky = True
    else:
        big_and_chunky = False


    l_new_preproc_l = p_tabinfo_d['new_preproc_candidates_l']

    l_source_conn = rqlib.g_params['source_conn']
    l_target_conn = rqlib.g_params['target_conn']

    l_cols = p_tabinfo_d['all_columns_no_excludes_str']

    source_sql = generate_sql_small(l_source_conn, p_tabinfo_d, l_cols)
    target_sql = generate_sql_small(l_target_conn, p_tabinfo_d, l_cols)

    rqlib.log_debug('SOURCE SQL: {}'.format(source_sql))
    rqlib.log_debug('TARGET SQL: {}'.format(target_sql))

    if source_sql is None or target_sql is None:
        rqlib.p_e('Failed to generate SQL')
        rqlib.p_e('SOURCE SQL: {}'.format(source_sql))
        rqlib.p_e('TARGET SQL: {}'.format(target_sql))
        return False

    rqlib.p_i('')
    rqlib.p_i('Comparing table {}'.format(l_tab))
    overall_status = True

    # NOTE: p_EDH_mode will stop the creation of command line where clause, and compare diff
    if big_and_chunky:
        my_result, s_df, t_df = rqdbclib.tab_compare(l_source_conn,
                                                     l_target_conn,
                                                     source_sql,
                                                     target_sql,
                                                     p_tabinfo_d['source_pkcols'],
                                                     p_chunk=rqdbclib.DEFAULT_SQL_CHUNK,
                                                     p_tab=l_tab,
                                                     p_convert_emptystring_to_null=True,
                                                     p_run_preprocessor=preprocess_flag,
                                                     p_EDH_mode=True)


    else:
        my_result, s_df, t_df = rqdbclib.tab_compare_no_chunks(l_source_conn,
                                                               l_target_conn,
                                                               source_sql,
                                                               target_sql,
                                                               p_tabinfo_d['source_pkcols'],
                                                               l_tab,
                                                               p_convert_emptystring_to_null=True,
                                                               p_run_preprocessor=preprocess_flag,
                                                               p_EDH_mode=True)

    if my_result:
        rqlib.p_i('... Compare OK for {} ....'.format(l_tab))
        retval = BT_FINISH_OK
    else:
        rqlib.p_e('... Compare failed for {} ....'.format(l_tab))
        if len(s_df.index) + len(t_df.index) > BT_MAX_ERROR_COUNT:
            rqlib.p_e('... Compare failed for {} TO MANY ERRORS TO PROCESS....'.format(l_tab))
            retval = BT_FINISH_TOO_MANY_ERRORS
            overall_status = False
        else:
            rqlib.p_e('... Compare failed for {}....'.format(l_tab))
            insert_batch_table_errors(s_df, t_df, p_batch_info, p_tabinfo_d)
            retval = BT_FINISH_ERR
            overall_status = False

    rqlib.log_debug('end process small, retval = {}'.format(retval))
    return retval

# --------------------------------------------------------------------
#
#                          process small
#
# --------------------------------------------------------------------


def process_errors(p_batch_info, p_tabinfo_d):
    """
    Process the individual rows with errors for a single table.
       batch id -- Current batch
       tabinfo  -- everything need to know about this table
    """
    rqlib.log_debug('Start process errors')

    preprocess_flag = tab_preproc(p_tabinfo_d, 'small')
    l_new_preproc_l = p_tabinfo_d['new_preproc_candidates_l']

    l_source_conn = rqlib.g_params['source_conn']
    l_target_conn = rqlib.g_params['target_conn']

    l_cols = p_tabinfo_d['all_columns_no_excludes_str']

    orig_source_sql = generate_single_row_sql(l_source_conn, p_tabinfo_d, l_cols)
    orig_target_sql = generate_single_row_sql(l_target_conn, p_tabinfo_d, l_cols)

    error_rows = fetch_batch_table_errors(p_batch_info, p_tabinfo_d)

    error_rows_counter = error_rows_counter_init(error_rows)
    error_rows_counter_display(error_rows_counter)



    overall_result = True
    # Process each row in error table
    for row in error_rows:

        l_source_where = row[0]
        l_target_where = row[1]
        l_column_name = row[2]
        curr_rowid = row[3]

        if l_column_name is not None:
            # processing a single column of a table
            l_source_sql = generate_single_row_sql(l_source_conn, p_tabinfo_d, l_column_name)
            l_target_sql = generate_single_row_sql(l_target_conn, p_tabinfo_d, l_column_name)
        else:
            l_source_sql = orig_source_sql
            l_target_sql = orig_target_sql

        source_sql = l_source_sql + l_source_where
        target_sql = l_target_sql + l_target_where

        my_result, s_df, t_df = rqdbclib.tab_compare_no_chunks(l_source_conn,
                                                               l_target_conn,
                                                               source_sql,
                                                               target_sql,
                                                               p_tabinfo_d['source_pkcols'],
                                                               p_tabinfo_d['tab'],
                                                               p_convert_emptystring_to_null=True,
                                                               p_run_preprocessor=preprocess_flag,
                                                               p_EDH_mode=True)

        if my_result:
            if not rqblib.update_batch_table_errors_ok(curr_rowid):
                rqlib.log_error('Failed to update BTE table with success status')
                overall_result = False
        else:
            rqlib.log_error('Row failed compare - source pk = {}'.format(l_source_where))
            rqlib.log_error('                     target pk = {}'.format(l_target_where))
            overall_result = False
            display_diff(s_df, t_df, p_tabinfo_d['tab'])
            if not rqblib.update_batch_table_errors_failed(curr_rowid):
                rqlib.log_error('Failed to update BTE table with failed status')

        error_rows_counter_display(error_rows_counter, p_increment=True)

    if overall_result:
        retval =  BT_FINISH_OK
    else:
        retval =  BT_FINISH_ERR

    print('return from process_error: {}'.format(retval))
    return retval

# --- failed pk

# ---------------------------------------------------------------------
#
#                 remove non primary key columns from df
#
# ---------------------------------------------------------------------


def ibte_remove_non_pk_col(p_df, p_pk_cols):
    """
    Remove any column from DF that is not part of the primary key
    """
    # --
    keep_l = p_pk_cols.split(',')

    retval_df = p_df[keep_l]

    return retval_df

# ---------------------------------------------------------------------
#
#                 insert batch table errors
#
# ---------------------------------------------------------------------


def insert_batch_table_errors(s_rem_df, t_rem_df, p_batch_info, p_tabinfo_d, p_col=None):
    """
    1. remove all non primary key columns from source and target.
    2. create unique list of pk values from new df.
    3. create where clause for each unique pk.
    4. insert into syst_batch_table_errors.
    """

    rqlib.log_debug('Start insert batch table errors')
    # -- Setup local data

    l_source_conn = rqlib.g_params['source_conn']
    l_ic_tab_df = p_tabinfo_d['ss_info_tab_df']
    l_dbtype = l_source_conn['dbtype']
    l_ms_proc = rqlib.g_params['millisec_proc']

    # Step 1
    s_rem_pk_df = ibte_remove_non_pk_col(s_rem_df, p_tabinfo_d['source_pkcols'])
    t_rem_pk_df = ibte_remove_non_pk_col(t_rem_df, p_tabinfo_d['source_pkcols'])

    # Step 2
    all_pk_df = pd.concat([s_rem_pk_df, t_rem_pk_df], join='outer')
    all_pk_df.drop_duplicates(inplace=True)

    # Step 3
    # We now have a unique list of PK that are different between source and target
    for dummy_i, row in all_pk_df.iterrows():

        # Construct a where clause
        source_where_str = ''
        rs_where_str = ''

        for col in all_pk_df.columns:

            l_datatype = fetch_col_datatype(l_ic_tab_df, col)
            if l_datatype is None:
                rqlib.p_e('Error determining datatype for column {}'.format(col))
                return None

            source_col = col
            source_str_template = generate_where_templates(l_dbtype, l_datatype, l_ms_proc)
            source_str = source_str_template.format(vF=row[col])
            source_where_str += " and {} = {}".format(source_col, source_str)

            rs_col = rqdbclib.convert_rs_reserved_word(col, l_source_conn['schema'])
            rs_col = rs_col.lower()
            rs_str_template = generate_where_templates(DB_TYPE_REDSHIFT, l_datatype, l_ms_proc)
            rs_str = rs_str_template.format(vF=row[col])
            rs_where_str += " and {} = {}".format(rs_col, rs_str)

        error_data = {}
        error_data['source'] = source_where_str
        error_data['target'] = rs_where_str

        # print(error_data['source'])
        # print(error_data['target'])

        # -- step 4 - insert error.
        if not rqblib.batch_table_error(p_batch_info, p_tabinfo_d, error_data, p_col):
            return False

    return

# ---------------------------------------------------------------------
#
#                 insert batch table errors
#
# ---------------------------------------------------------------------


def fetch_batch_table_errors(p_batch_info, p_tabinfo_d):
    """
    Create a df for all the rows with errors
    """
    rqlib.log_debug('Start fetch batch table errors')

    e_template = '\nfetch batch table errors failed when executing sql : {vSQL}'
    e_template += '\nERROR: during data fetch: code = {vCode}'
    e_template += '\n       err.message: {vMessage}, context = {vContext}'

    _sql = """
           select bte_source_where_clause,
                  bte_target_where_clause,
                  bte_column_name,
                  rowid                     as bte_rowid
            from syst_batch_table_errors
            where bte_acronym     = :l_acronym
            and   bte_env         = :l_env
            and   bte_id          = :l_id
            and   bte_table_name  = :l_table_name
            and   bte_status      = :l_status
            """

    l_values = {"l_acronym": p_batch_info['acronym'],
                "l_env": p_batch_info['env'],
                "l_id": p_batch_info['batch_id'],
                "l_table_name": p_tabinfo_d['tab'].lower(),
                "l_status": BTE_FAILED}

    rqlib.log_debug('Execute insert sql {}'.format(_sql))

    l_syst_conn_d = rqlib.g_params['syst_conn_d']
    l_conn = l_syst_conn_d['conn']

    rowcount = None

    try:
        cur = l_conn.cursor()
        cur.execute(_sql, **l_values)
        rows = cur.fetchall()
        rowcount = cur.rowcount
        cur.close()

    except cx_Oracle.DatabaseError as err_result:
        err, = err_result.args
        rqconnlib.ora_sql_exec(l_conn, 'rollback')
        rqlib.p_e(e_template.format(vSQL=_sql,
                                    vCode=err.code,
                                    vMessage=err.message,
                                    vContext=err.context))
        rowcount = None
        rows = None

    rqlib.log_debug('end batch table error rowcount = {}'.format(rowcount))

    return rows

# ---------------------------------------------------------------------
#
#                 pandas display PK values
#
# ---------------------------------------------------------------------


def tab_compare_log_diff_create_sql(p_df, p_pk, p_row, p_tab, p_col):
    """
    Create a printable version of the primary key.

    p_df -- list of unique pk that failed.
    p_pk -- primary key
    p_row -- which row to in p_df to create where
    p_col -- error where issue was discovered

    """
    single_row = p_df.loc[p_row]
    # p_pk = p_pk.lower()
    # single_row_data = single_row[p_pk.upper()]

    return_str = 'select {} from {} where 1=1 '.format(p_col, p_tab)

    valid_cols = []
    current_headings = list(p_df.columns.values)
    for col in p_pk.split(','):
        col_new = col.upper()
        if col_new == 'index':
            continue
        elif col_new in current_headings:
            valid_cols.append(col_new)
        elif col_new + '_' in current_headings:
            valid_cols.append(col_new + '_')
        else:
            valid_cols.append('Unknown Column Name')

    for col in valid_cols:
        return_str += " and {} = '{}'".format(col, single_row[col.upper()])
    #    for col in p_pk.split(','):
    #        return_str += " and {} = '{}'".format(col, single_row[col.upper()])

    return return_str

# --------------------------------------------------------------------
#
#                          display diff
#
# --------------------------------------------------------------------


def display_diff(p_pd1, p_pd2, p_tab):
    """
    Log the differences between two pandas to log_error

    Parameters

       * pd1   : source panda
       * pd2   : target panda
       * p_tab : one of the table names (for reporting only)
    """
    rqlib.log_debug('Start display_diff')

    e_template = 'rownum [{vR:5}] column [{vF:20}] '
    e_template += 'source: [{v_source}] target: [{v_target}]'

    len_s = len(p_pd1.index)
    len_t = len(p_pd2.index)

    if len_s < 1 or len_s > 1:
        rqlib.log_info('Unable to display difference, len source != 1, it is {}'.format(len_s))
        return

    if len_t < 1 or len_t > 1:
        rqlib.log_info('Unable to display difference, len target != 1, it is {}'.format(len_s))
        return
    pd1 = p_pd1
    pd2 = p_pd2

    pd1.reset_index(inplace=True)
    pd2.reset_index(inplace=True)

    rqlib.log_debug('Try a compare')

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
        rqlib.log_error('ERROR - Differences found in result sets, but they are too great')
        rqlib.log_error('        for the logging program to print. Please review manually.')
        rqlib.log_error('        This difference occurs when the two data sets have different')
        rqlib.log_error('        types, and cannot be compared programatically.')

        return

    if changed_from.size == 0 and changed_to.size == 0:

        rqlib.log_error('WARNING: Hmmm difference reported, but no difference found')
        rqlib.log_error('         time to panic. Table {}'.format(p_tab))
        return
    else:

        rqlib.log_error('')
        rqlib.log_error('{} Difference found'.format(p_tab))
        rqlib.log_error('/--------------------------------------------------------------\\')

        diff = pd.DataFrame({'from': changed_from, 'to': changed_to}, index=eq_changed.index)
        # print(diff)
        print_counter = 0
        count_errors = len(diff.index)

        for index, row in diff.iterrows():

            row_number = index[0]
            row_column = index[1]
            row_error_from = row['from']
            row_error_to = row['to']

            if(isinstance(row_error_from, (bytearray, bytes)) or
               isinstance(row_error_to, (bytearray, bytes))):
                rqlib.log_error('Cannot display error, non-printable characters')
            else:
                rqlib.log_error(e_template.format(vR=row_number,
                                                  vF=row_column,
                                                  v_source=row_error_from,
                                                  v_target=row_error_to))
            rqlib.log_error('')
            print_counter += 1

        rqlib.log_error('Total error count for {} - {}'
                        .format(p_tab, count_errors))

        rqlib.log_error('\\--------------------------------------------------------------/')

# --- Post Process

# --------------------------------------------------------------------
#
#                          csv init
#
# --------------------------------------------------------------------


def csv_init(p_csv, p_tab, p_sql_info):
    """
    Initialise the data for the CSV record.
    """
    rqlib.log_debug('start csv init')

    p_csv['Table Name'] = p_tab

    p_source_conn = rqlib.g_params['source_conn']
    p_target_conn = rqlib.g_params['target_conn']

    s_sql = 'select count(*) from {}.{}'.format(p_source_conn['schema'].lower(),
                                                p_sql_info['tab'])

    t_sql = 'select count(*) from {}.{}'.format(p_target_conn['schema'],
                                                p_sql_info['tab'].lower())

    s_result = rqdbclib.read_table_data_array(p_source_conn['conn'], s_sql)
    t_result = rqdbclib.read_table_data_array(p_target_conn['conn'], t_sql)

    # Counts sometimes come back as a float, rather than an integer.
    s_retval = rqconnlib.check_sql_result_for_decimal(s_result[0][0])
    t_retval = rqconnlib.check_sql_result_for_decimal(t_result[0][0])

    p_csv['Count Source Records'] = s_retval
    p_csv['Count Target Records'] = t_retval

    rqlib.log_debug('    total source rec count {}, sql {}'.format(p_csv['Count Source Records'],
                                                                   s_sql))
    rqlib.log_debug('    total target rec count {}, sql {}'.format(p_csv['Count Target Records'],
                                                                   t_sql))

    # -- Initialise these to 0, they only get set if there are real errors found.
    p_csv['results'] = None
    p_csv['ERROR PERCENTAGE'] = 0.0

    return

# --------------------------------------------------------------------
#
#                         post process logfile success
#
# --------------------------------------------------------------------


def post_process_logfile_success(p_log_filename):
    """
    Go through the log file and extract 'Compare..' and 'no-real-error' lines to show
    that what was tested and passed
    """
    #  Open the file with read only permit
    l_log = open(p_log_filename)
    l_err_flnm = (rqlib.TEST_RESULT_DIRS['error'] +
                  '/' +
                  p_log_filename.split('__')[1].split('.')[0] +
                  '.html')

    l_error_lines = open(l_err_flnm, "w")

    # Read the first line
    line = l_log.readline()

    # If the file is not empty keep reading line one at a time
    # till the file is empty

    l_error_lines.write(rqlib.MAIL_HEADER)

    while line:
        if 'Compare' in line:
            l_error_lines.write(line)
        if 'no real errors' in line:
            l_error_lines.write(line)

        line = l_log.readline()

    l_error_lines.write(rqlib.MAIL_FOOTER)

    l_log.close()
    l_error_lines.close()

    return l_err_flnm, None


# --------------------------------------------------------------------
#
#                         post_prosess_logfile
#
# --------------------------------------------------------------------


def post_process_logfile_error(p_log_filename):
    """
    Go through the log file and extract ERROR lines as well as python command lines
    """
    # p_source, p_pk_only, p_quick_val):
    l_source = rqlib.g_params['p_info_class_tab']
    # l_pk_only = rqlib.g_params['pk_only']
    l_quick_val = rqlib.g_params['run_type'] == 'daily'
    l_table = rqlib.g_params['table']

    if l_quick_val:
        l_rprt_flnm = (rqlib.TEST_RESULT_DIRS['reports'] +
                       '/' +
                       l_source + '_pk_quick_val_20' + p_log_filename.split('20')[1] +
                       '.html')
        l_report_lines = open(l_rprt_flnm, "w")

        l_report_lines.write(rqlib.MAIL_HEADER)
        l_report_lines.write('Summary results from {}\n'.format(l_source) +
                             'Quick Validation report run on {}\n'
                             .format(time.strftime("%Y/%m/%d")))

        if l_table is not None:
            l_report_lines.write(' \n')
            l_report_lines.write('Run only for table: {}'.format(l_table))

        l_report_lines.write(' \n')

        rqlib.write_line_containing(p_log_filename, l_report_lines, 'ERROR    ... Compare failed')

        l_report_lines.write(' \n')
        l_report_lines.write('For more detailed info, see below.\n')
        l_report_lines.write('Note: False Positives, and deleted rows are not counted ' +
                             'in the "real errors".\n')
        l_report_lines.write(' \n')
        rqlib.write_chunk_from_to(p_log_filename, l_report_lines, 'Error Code',
                                  'Error codes definitions')
        l_report_lines.write(' \n')
        l_report_lines.write(' \n')
        l_report_lines.write(rqdbclib.PK_QUICK_VAL_DEFN)
        l_report_lines.write(' \n')
        l_report_lines.write(rqlib.MAIL_FOOTER)

        return l_rprt_flnm, None
    else:
        l_err_flnm = (rqlib.TEST_RESULT_DIRS['error'] +
                      '/' +
                      p_log_filename.split('__')[1].split('.')[0] +
                      '.txt')

        l_pthn_cmd = (rqlib.TEST_RESULT_DIRS['commands'] +
                      '/' +
                      p_log_filename.split('__')[1].split('.')[0] +
                      '.txt')
        l_error_lines = open(l_err_flnm, "w")
        l_pthn_lines = open(l_pthn_cmd, "w")
        rqlib.write_line_containing(p_log_filename, l_error_lines, 'ERROR')
        rqlib.write_line_containing(p_log_filename, l_pthn_lines, 'python', False)
        l_error_lines.close()
        l_pthn_lines.close()
        return l_err_flnm, l_pthn_cmd

# --------------------------------------------------------------------
#
#                         post_prosess_logfile
#
# --------------------------------------------------------------------


def post_process_logfile_quick_val_attach(p_log_filename):
    """
    Go through the log file and extract ERROR lines as well as python command lines
    """
    l_source = rqlib.g_params['p_info_class_tab']
    l_rprt_flnm = (rqlib.TEST_RESULT_DIRS['reports'] +
                   '/' +
                   l_source + '_detail_pk_quick_val_20' + p_log_filename.split('20')[1] +
                   '.html')
    l_report_lines = open(l_rprt_flnm, "w")
    lines_to_strip = ['INFO     FALSE_POSITIVE', 'Skip table ']
    rqlib.write_line_not_containing(p_log_filename, l_report_lines, lines_to_strip)

    return l_rprt_flnm

# --- Table Setup
# --------------------------------------------------------------------
#
#                          create dict with table info
#
# --------------------------------------------------------------------


def init_table_info(p_table_row, p_exclude_cols_df, p_preproc_cols_df):
    """
    create a dictionary with specific infomation for a single table
    """

    # -- Fetch Global info
    curr_table = p_table_row['TABLE_NAME'].upper()
    l_source_conn = rqlib.g_params['source_conn']
    l_target_conn = rqlib.g_params['target_conn']
    l_info_class_df = rqlib.g_params['ss_info_class_df']

    # -------------------------------------
    # -- Fetch local info
    # -------------------------------------

    l_ss_info_tab_df = rqdbclib.fetch_tab_df(curr_table, l_info_class_df)

    l_replicate_df = l_ss_info_tab_df.loc[l_ss_info_tab_df['TREATMENT'] == 'REPLICATE']

    # Primary Keys from IC, Source and Target
    pkcols_ic_df = l_ss_info_tab_df.loc[l_ss_info_tab_df['PRIMARY_KEY'] == 'Y']
    pkcols_source_str = generate_sql_fetch_pk(l_ss_info_tab_df, False, l_source_conn['schema'])
    pkcols_target_str = generate_sql_fetch_pk(l_ss_info_tab_df, True, l_target_conn['schema'])

    # Create a series with all columns that are replicated.
    all_columns_s = l_replicate_df['COLUMN'].reset_index(drop=True)
    pk_columns_s = pkcols_ic_df['COLUMN'].reset_index(drop=True)

    all_columns_str = ','.join(col for col in all_columns_s)

    # Create a series of exclude columns
    l_exclude_df = p_exclude_cols_df.loc[p_exclude_cols_df['TABLE_NAME'] == curr_table.lower()]
    l_exclude_s = l_exclude_df['COLUMN_NAME'].reset_index(drop=True)

    all_columns_no_excludes_str = ','.join(col for col in all_columns_s
                                           if col.lower() not in l_exclude_s.values)

    # -- create a series of columns to pre-proc
    l_preproc_df = p_preproc_cols_df.loc[p_preproc_cols_df['TABLE_NAME'] == curr_table.lower()]
    l_preproc_s = l_preproc_df['COLUMN_NAME'].reset_index(drop=True)
    if len(l_preproc_s) == 0:
        l_preproc_s = pd.Series(['ALL COLUMNS'])

    # -- Fetch lag date
    l_lag_date = rqdbclib.fetch_lag_date(l_target_conn,
                                         curr_table,
                                         p_table_row['CREATE_DATE_COL'],
                                         p_table_row['UPDATE_DATE_COL'])

    # -------------------------------------
    # Create a dictionary for the data
    # -------------------------------------

    table_info = {}

    table_info['tab'] = curr_table
    table_info['ss_info_tab_df'] = l_ss_info_tab_df
    table_info['replicate_cols_df'] = l_replicate_df

    table_info['ic_pkcols'] = pkcols_ic_df
    table_info['source_pkcols'] = pkcols_source_str
    table_info['target_pkcols'] = pkcols_target_str

    table_info['all_columns_s'] = all_columns_s
    table_info['pk_columns_s'] = pk_columns_s
    table_info['all_columns_str'] = all_columns_str
    table_info['all_columns_no_excludes_str'] = all_columns_no_excludes_str

    table_info['create_date_col'] = p_table_row['CREATE_DATE_COL']
    table_info['update_date_col'] = p_table_row['UPDATE_DATE_COL']

    table_info['exclude_cols_s'] = l_exclude_s
    table_info['preproc_cols_s'] = l_preproc_s

    table_info['pk_lag_date'] = l_lag_date

    table_info['new_preproc_candidates_l'] = []

    return table_info

# --------------------------------------------------------------------
#
#                          initialise
#
# --------------------------------------------------------------------


def save_tab_result(p_batch_id, p_tab_result, p_tab, p_col=None):
    """
    Save the results for this table comparison to the database.
    """

    if p_tab_result in (BT_FINISH_OK, BT_FINISH_ERR, BT_FINISH_TOO_MANY_ERRORS):
        set_status = rqblib.batch_process_table(p_batch_id, p_tab_result, p_tab, p_col)
        if not set_status:
            rqlib.p_e('Unable to set BT_STATUS for this table {}, result {}'.format(p_tab,
                                                                                    p_tab_result))
    else:
        rqlib.p_e('Unknown result for save_tab_result, status = {}'.format(p_tab_result))

    rqsconnlib.syst_commit()
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
    log_filename = rqlib.log_filename_init(p_filename)
    if log_filename is None:
        print("\nError: Failed to initialise Log File Name. aborting\n")
        return rqlib.FAIL_GENERIC

    parser = argparse.ArgumentParser(description="""
     Example command lines:

    -d DEBUG  --account mrm --type prod --run_mode init --run_type daily

    -d DEBUG --account mrm --type prod --run_mode restart --run_type daily


    --account genesys --env dev --run_type subhr --run_mode init
    --account genesys --env dev --run_type subhr --run_mode restart -t ag2_agent_campaign_subhr


    Run Types
    -  small: process entire table in single fetch
    -  transact: process all columns, but on fetch by chunk
    -  big: process column + pk at a time, but fetch all data
    -  huge: process column + pk, but fetch by chunk.
    -  other: either default to small, or are special

          """, formatter_class=argparse.RawTextHelpFormatter)

    # --- DB parameters ---

    parser.add_argument('--account',
                        help='Fetch source and target from st_accounts table',
                        required=True)

    parser.add_argument('--env',
                        help='What environment type? prod, uat, st (system test), dev, other?',
                        default='prod',
                        required=False)

    parser.add_argument('-t', '--table',
                        help='Table data comparison for this table only',
                        required=False)

    # --- Run Mode

    help_txt = '''
               INIT     : Create a new run ID, and force close all others for this acronym/type.
               RESTART  : Rerun all tables that have not yet completed, and then any PK's failed.
               CLOSE    : Mark this run as complete.
               REOPEN   : Reopen a closed run (not yet coded).\n
               '''

    parser.add_argument('--run_mode',
                        help=textwrap.dedent(help_txt),
                        choices=['init', 'restart', 'close', 'reopen'],
                        default='restart',
                        required=True)

    # --- Run Types (big, small, etc)

    help_txt = """
               DAILY    : Run the tables flagged as Quick Val Daily (only {vD} days data validated).
               QUICKVAL : Run the tables flagged as Quick Val
               ---- (When choosing mode of INIT, all huge, big, transact and small are the same)
               HUGE     : Validate the huge tables (col by col, chunks of 1000 rows)
               BIG      : Validate the big tables (chunks of 1000 rows)
               TRANSACT : Validate the transactions tables (entire table, exclude LAG rows????)
               SMALL    : Validate any table not in Huge, Big or Transact.\n
               """.format(vD=DAYS_FOR_DEEP_ANALYSE)

    parser.add_argument('--run_type',
                        help=textwrap.dedent(help_txt),
                        # choices=['daily', 'quickval', 'huge', 'big', 'transact', 'small'],
                        default='small',
                        required=True)

    parser.add_argument('-c', '--count_limit',
                        help='only select count rows from table (debugging only)',
                        required=False)

    help_txt = 'Add condition to where clause, eg "and id = 12345" (requires manual OK)'
    parser.add_argument('--add_condition',
                        help=help_txt,
                        required=False)

    parser.add_argument('--run_diff',
                        help='run DIFF on individual error rows (single row only)',
                        required=False)

    parser.add_argument('--target_to_midnight',
                        help='Only fetch data in target up to Midnight',
                        action='store_true',
                        required=False)

    # -- Mail parameters

    parser.add_argument('--mail_default',
                        help='Send email, either enter email address or leave blank for default',
                        required=False,
                        action='store_true')

    parser.add_argument('--mail',
                        help='Send log to this csv list of email addresses',
                        required=False)

    # Add debug arguments
    parser.add_argument('-d', '--debug',
                        help='Log messages verbosity: NONE (least), DEBUG (most)',
                        choices=('NONE', 'CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'),
                        default="INFO",
                        required=False)

    # --- Special Debug

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
    args = rqlib.args_validate(parser, log_filename)

    if 'help' not in args:

        if args['mail'] is not None and args['mail_default'] is True:
            print('ERROR, please set either --mail or --mail_default, not both!')
            return rqlib.FAIL_GENERIC

        if args['mail_default']:
            args['mail'] = rqlib.EMAIL_DEFAULT

        # -- Convert arguments to upper case where required.
        args['table'] = rqlib.safe_upper(args['table'])

        # -- Validate SAVE info

        if(args['count_limit'] is None and
           args['add_condition'] is None and
           args['debug_type'] is None):

            args['Save_OK_Data'] = True
        else:
            # only looking at a subset of data, so do not save "success" back to file.
            rqlib.p_i('WARNING: Not saving any results, restricted conditions apply')
            args['Save_OK_Data'] = False

        args['Save_OK_Data'] = True

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

    # --  command line arguments
    source_system_name = rqlib.fetch_arg_name2()
    if source_system_name is None:
        return -1

    args, l_log_filename_s = initialise('validate_edh_data__' + source_system_name)

    # -- Initialise
    if not rqlib.init_app(args):
        return rqlib.FAIL_GENERIC

    # -- Connect Source and Target DB
    if not rqsconnlib.syst_source_target_connect():
        return rqlib.FAIL_NO_DB_CONNECT

    # ------------- Fetch Mapping ----------------------

    ss_info_class_df = rqlib.load_info_classification(rqlib.g_params['source_conn'],
                                                      rqlib.g_params['info_class_tab'],
                                                      rqlib.g_params['multiple_schemas'])
    rqlib.g_params['ss_info_class_df'] = ss_info_class_df

    if ss_info_class_df is None:
        return rqlib.FAIL_GENERIC

    l_batch_info = rqblib.batch_process()
    if l_batch_info is None:
        return rqlib.FAIL_GENERIC

    # -- Finished processing the batch stuff, only restart continues on....
    if rqlib.g_params['run_mode'] in ('init', 'close', 'reopen'):
        return rqlib.SUCCESS

    l_batch_id = l_batch_info['batch_id']

    # -------------------------------------
    # -- Fetch the tables to process

    table_df = fetch_tables(rqlib.g_params['run_type'],
                            l_batch_id,
                            p_single_table=rqlib.g_params['table'])

    exclude_cols_df = fetch_exceptions('exclude_cols', p_single_table=rqlib.g_params['table'])
    preproc_cols_df = fetch_exceptions('preproc_cols', p_single_table=rqlib.g_params['table'])
    # -------------------------------------
    # -- Setup run type


    if rqlib.g_params['run_type'] == 'daily':
        tab_function = process_daily

    # -- Small and transact use the same function
    elif rqlib.g_params['run_type'] == 'small' or rqlib.g_params['run_type'] == 'transact':
        tab_function = process_small

    # -- big and huge use the same function
    elif rqlib.g_params['run_type'] == 'big' or rqlib.g_params['run_type'] == 'huge':
        tab_function = process_big

    elif rqlib.g_params['run_type'] == 'something_else':
        """ select your own process function! """
        pass
    else:
        tab_function = process_small

    # -------------------------------------
    # -- Setup progress counter

    progress_counter = progress_counter_init(table_df)
    progress_counter_display(progress_counter)
    overall_success = True

    # -------------------------------------
    # -- Loop through tables, and process!

    for dummy_i, table_row in table_df.iterrows():

        tabinfo_d = init_table_info(table_row, exclude_cols_df, preproc_cols_df)
        l_tab = tabinfo_d['tab']
        print('    Process table {}'.format(l_tab))

        if table_row['BT_STATUS'] == BT_FINISH_ERR:
            if process_errors(l_batch_info, tabinfo_d) == BT_FINISH_OK:
                # Row comparison succeeded
                set_status = rqblib.batch_process_table(l_batch_id, BT_FINISH_OK, l_tab)
                if not set_status:
                    # unable to update status in table for this row.
                    rqlib.p_e('Unable to set FINISH OK status for this table {}'.format(l_tab))
                    overall_success = False
            else:
                # Row comparison failed
                overall_success = False

        elif table_row['BT_STATUS'] in (BT_INIT, BT_RUN, BT_FINISH_TOO_MANY_ERRORS):

            # Note: for big, huge, etc, the TABLE level info is looked at here.
            #       if all columns are OK, then the TABLE is set to OK.
            set_status = rqblib.batch_process_table(l_batch_id, BT_INIT, l_tab)
            if not set_status:
                rqlib.p_e('Unable to set RUNNING status for this table {}'.format(l_tab))
                rqlib.p_e('SKIPPING TABLE')
                overall_success = False
                continue

            # ----------------------
            # --- Validate the table
            # ----------------------
            tab_result = tab_function(l_batch_info, tabinfo_d)
            # ----------------------
            # --- END Validate
            # ----------------------

            # Note: for big, huge, etc, the TABLE level info is looked at here.
            #       if all columns are OK, then the TABLE is set to OK.
            if tab_result != BT_FINISH_OK:
                overall_success = False

            save_tab_result(l_batch_id, tab_result, l_tab)

        elif table_row['BT_STATUS'] in (BT_FINISH_OK):
            rqlib.log_info('    table already completed OK, skipping: {}'.format(l_tab))

        else:
            rqlib.p_e('NOT YET CODED FOR TABLE STATUS {}'.format(table_row['BT_STATUS']))

        progress_counter_display(progress_counter, p_increment=True)

    # -- end of Batch
    # TODO:
    #   Add function to check status of all tables. If ok, then set batch OK.
    # rqblib.batch_close(l_batch_id, p_overall_success)
    # -------------------------------------
    # -- disconnect DB

    rqblib.update_batch_status(l_batch_id)

    rqsconnlib.syst_source_target_disconnect()

    # Note: calling program looks for these error messages, rather than error codes.
    if overall_success:
        rqlib.p_i('Finished with no Errors')
        retval = rqlib.SUCCESS
    else:
        rqlib.p_i('Finished with errors')
        retval = rqlib.FAIL_GENERIC

    return retval

#    # -- Is this Unicode?
#
#    if rqlib.g_params['unicode']:
#        os.environ["NLS_LANG"] = ".AL32UTF8"       # Enable unicode processing
#

#    # ---------- Test for large cell counts -------
#
#    if rqlib.g_params['report_cell_count']:
#        test_for_large_datasets(p_source_conn['conn'], p_source_conn['schema'])
#        return 0
#
#    # ------------- Oracle Version ---------------------
#    rqlib.g_params['millisec_proc'] = True
#
#    if test_oracle_version_ms_proc(p_source_conn['conn']):
#        rqlib.g_params['millisec_proc'] = False
#
#    if rqlib.g_params['disable_millisec_testing']:
#        rqlib.g_params['millisec_proc'] = False

    # ------------- Setup Report output ----------------------

    # create rqlib.g_params for html_report and summary_report. Open the files ready for output.
    # rqdbcrlib.create_report_files(l_log_filename_s)

    # ------------- Setup for CSV output ---------------------

    # rqlib.g_params['csv_report'] = {}

    # ------------- Fetch Prev Run info ----------------------

#    rqlib.p_i('... Fetch Prev Run info')

#    table_lists = rqdbclib.fetch_table_lists(ss_info_class_df, rqlib.g_params['p_info_class_tab'])
#    if table_lists is None:
#        rqlib.p_e('There was a problem loading previous file runs, please rectify')
#        return -1
#
#    transact_df = None
#    big_df = None


#
#    if rqlib.g_params['pk_quick_val_daily']:
#
#        tab_list = fetch_tables['big']
#        tab_function = process_big_quick_val
#        big_df = rqdbclib.df_load_from_csv(rqlib.g_params['p_info_class_tab'],
#                                           BIG_TABLE_DF_FILENAME,
#                                           BIG_TABLE_DF_COLUMNS)
#        # remove tables that do not have create and update columns
#        rqdbclib.remove_nodate_rows(big_df)
#        # -- now remove anything if pk_quick_val_run_default is set
#        rqdbclib.remove_nondefault_rows(big_df)
#
#        # Overwrite the list of tables with the new cleaned up list.
#        tab_list = rqdbclib.remove_nodate_vals(tab_list, big_df)
#
#    elif args['add_condition'] is not None:
#        tab_list = table_lists['full']
#        tab_function = process_small
#
#    elif args['big']:
#        tab_list = table_lists['big']
#        tab_function = process_big
#        big_df = rqdbclib.df_load_from_csv(rqlib.g_params['p_info_class_tab'],
#                                           BIG_TABLE_DF_FILENAME,
#                                           BIG_TABLE_DF_COLUMNS)
#        rqdbclib.validate_process_tab_in_list(tab_list, rqlib.g_params['table'])
#
#    elif args['transact']:
#        tab_list = table_lists['transact']
#        tab_function = process_transact
#        rqdbclib.validate_process_tab_in_list(tab_list, rqlib.g_params['table'])
#        transact_df = rqdbclib.df_load_from_csv(rqlib.g_params['p_info_class_tab'],
#                                                TRANSACT_TABLE_DF_FILENAME,
#                                                TRANSACT_TABLE_DF_COLUMNS)
#    else:
#        tab_list = table_lists['small']
#        tab_function = process_small
#        rqdbclib.validate_process_tab_in_list(tab_list, rqlib.g_params['table'])
#
#    tabcol_with_non_ascii_char = []
#
#    exclude_cols_df = rqdbclib.df_load_from_csv(rqlib.g_params['p_info_class_tab'],
#                                                EXCLUDE_COLS_DF_FILENAME,
#                                                EXCLUDE_COLS_DF_COLUMNS)

#
#
#
#    # --- display tables that are not replicated
#    rqdbclib.report_tables_not_processed(ss_info_class_df)
#
#
#    # def preprocess_table_list_save(p_tab, p_df):
#    rqlib.p_i('')
#    for tabcol in tabcol_with_non_ascii_char:
#        rqlib.p_i('Please add {} to PreProcess File, non ascii char found'
#                  .format(tabcol))
#
#    rqdbcrlib.close_report_files()
#
#    # -- only send mail if email parameter set.
#    if rqlib.g_params['mail'] is not None:
#        if p_overall_success:
#            p_errors_list, p_python_cmd = post_process_logfile_success(l_log_filename_s)
#        else:
#            p_errors_list, p_python_cmd = post_process_logfile_error(l_log_filename_s)
#
#        if rqlib.g_params['pk_quick_val']:
#            # p_attach_1 = post_process_logfile_quick_val_attach(p_log_filename)
#
#            rqmail.send_email(rqlib.MAIL_FROM_USER,
#                              rqlib.g_params['mail'],
#                              p_subject=rqlib.g_params['p_info_class_tab'] +
#                              " Quick Error Summary Report",
#                              p_inline=rqlib.g_params['summary_report_file'],
#                              p_attach=rqlib.g_params['html_report_file'],
#                              p_attach2=rqlib.g_params['sql_report_file'])
#
#        else:
#            rqmail.send_email(rqlib.MAIL_FROM_USER,
#                              rqlib.g_params['mail'],
#                              p_subject=rqlib.g_params['p_info_class_tab'] + " Error Report",
#                              p_attach=p_errors_list,
#                              p_attach2=l_log_filename_s,
#                              p_inline=p_python_cmd)
#
#    p_target_conn['conn'].close()
#    p_source_conn['conn'].close()
#
#    rqdbcrlib.process_csv_report()
#
#    # Note: calling program looks for these error messages, rather than error codes.
#    if p_overall_success:
#        rqlib.p_i('Finished with no Errors')
#        retval = 0
#    else:
#        rqlib.p_i('Finished with errors')
#        retval = -1
#
#    return retval

# ------------------------------------------------------------------------------

if __name__ == "__main__":
    retval = main()
    if retval == rqlib.SUCCESS:
        exit
    else:
        exit(retval)

# --- eof ---
