"""
Validate spreadsheet data is same as db data

"""
from __future__ import division
from __future__ import print_function

import argparse
# import re
# import numpy as np
# import pandas as pd
import textwrap

# Import racq library for RedShift

import acc_lib as alib


# --------------------------------------------------------------------
#
#             Global /  Constants
#
# --------------------------------------------------------------------


# --- load details
# --------------------------------------------------------------------
#
#                          setup load details
#
# --------------------------------------------------------------------


def setup_load_details():
    """
    create a dict of all load details
    """

    ld = {}
    ld['term'] = ['term', 'Term.template', 'dbo.term.Table.sql']
    ld['personnel'] = ['persl', 'Persl.template', 'dbo.persl.Table.sql']
    ld['disposition codes'] = ['disposition_type', 'Disposition_Type.template', 'dbo.disposition_type.Table.sql']
    ld['skills'] = ['persl_skill', 'persl_skill.template', 'dbo.persl_skill.Table.sql']
    ld['vehicles'] = ['def_vehic', 'Def_Vehic.template', 'dbo.def_vehic.Table.sql']
    ld['units'] = ['def_unit', 'Def_Unit.template', 'dbo.def_unit.Table.sql']
    ld['eta table'] = ['resp_tme', 'resp_tme.template', 'dbo.resp_tme.Table.sql']

    ld['out of service type agency'] = ['out_of_service_type_agency', 'Out_Of_Service_Type_Agency.template',
                                        'dbo.out_of_service_type_agency.Table.sql']

    ld['event types and sub-types'] = ['Event Types and Sub Types Data', 'event_type.template',
                                       'dbo.event_type.Table.sql']

    ld['out of service codes'] = ['out_of_service_type', 'Out_Of_Service_Type.template',
                                  'dbo.out_of_service_type.Table.sql']

    return ld

# --- process
# --------------------------------------------------------------------
#
#                          setup load details
#
# --------------------------------------------------------------------


def process(p_code_dir, p_ld):
    """
    process a load dict
    """
    alib.log_debug('')
    alib.log_debug('Start process for {}'.format(p_ld[0]))
    # -- setup local variables
    l_short_name = p_ld[0]
    l_details = p_ld[1]

    l_table_name = l_details[0]
    l_template = l_details[1]
    l_create_table = l_details[2]

    l_fmt_file = p_code_dir + '/templates/' + l_template
    l_sql_file = p_code_dir + '/sql/' + l_create_table

    alib.log_debug('    short name    : {}'.format(l_short_name))
    alib.log_debug('    table name    : {}'.format(l_table_name))
    alib.log_debug('    fmt file      : {}'.format(l_fmt_file.replace('/', '\\')))
    alib.log_debug('    sql file      : {}'.format(l_sql_file.replace('/', '\\')))

vDB
vFmtDir
vDataDir



    return

# --------------------------------------------------------------------
#
#                          setup load details
#
# --------------------------------------------------------------------


def get_fmt_file(p_ld):
    """
    process a load dict
    """

# --- Program Init
# --------------------------------------------------------------------
#
#                          initialise
#
# --------------------------------------------------------------------


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

    Run on local machine:
    -d DEBUG  --target_conn localhost

          """, formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('--target_db',
                        help='DB Connection: "localhost" or instance.user@host:db',
                        required=True)

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

    parser.add_argument('--code_dir',
                        help='directory containing sql and fmt files',
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

    if args['code_dir'] is not None:
        args['code_dir'] = alib.validate_dir_param(args['code_dir'])

    return (args, log_filename)

# --------------------------------------------------------------------
#
#                          main
#
# --------------------------------------------------------------------


def main():
    """
    this program will test the load data.
    """

    args, l_log_filename_s = initialise()

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

    l_code_dir = args['code_dir']
    # -------------------------------------
    # -- Fetch the tables to process

    load_details = setup_load_details()
    for ld in load_details.items():
        process(l_code_dir, ld)


    db_conn.close()

    alib.p_i('Done...')
    return alib.SUCCESS

# ------------------------------------------------------------------------------

if __name__ == "__main__":
    retval = main()
    if retval == alib.SUCCESS:
        exit
    else:
        exit(retval)

# --- eof ---
