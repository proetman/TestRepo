"""
Validate spreadsheet data is same as db data

"""
from __future__ import division
from __future__ import print_function

import argparse
# import re
# import numpy as np
# import pandas as pd

import subprocess
# import textwrap

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

    ld['term'] = ['term - {vClub}__TERM.tsv',
                  'Term.template',
                  'dbo.term.Table.sql',
                  'term.fmt']

    ld['personnel'] = ['personnel - {vClub}__PERSL.tsv',
                       'Persl.template',
                       'dbo.persl.Table.sql',
                       'Persl.fmt']

    ld['personnel node access'] = ['personnel node access - {vClub}__NODEN.tsv',
                                   'Noden.template',
                                   'dbo.noden.Table.sql',
                                   'noden.fmt']

    ld['disposition codes'] = ['disposition codes__DISPOSITION_TYPE.tsv',
                               'Disposition_Type.template',
                               'dbo.disposition_type.Table.sql',
                               'Disposition_Type.fmt']

    ld['skills'] = ['skills - {vClub}__PERSL_SKILL.tsv',
                    'persl_skill.template',
                    'dbo.persl_skill.Table.sql',
                    'persl_skill.fmt_NOT_YET_EXISTS']

    ld['vehicles'] = ['vehicles - {vClub}__DEF_VEHIC.tsv',
                      'Def_Vehic.template',
                      'dbo.def_vehic.Table.sql',
                      'def_vehic.fmt']

    ld['units'] = ['units - {vClub}__DEF_UNIT.tsv',
                   'Def_Unit.template',
                   'dbo.def_unit.Table.sql',
                   'def_unit.fmt']

    ld['eta table'] = ['eta table - {vClub}__ETA.tsv',
                       'resp_tme.template',
                       'dbo.resp_tme.Table.sql',
                       'resp_tme.fmt']

    ld['out of service type agency'] = ['out of service type agency__OUT_OF_SERVICE_TYPE_AGENCY.tsv',
                                        'Out_Of_Service_Type_Agency.template',
                                        'dbo.out_of_service_type_agency.Table.sql',
                                        'Out_Of_Service_Type_Agency.fmt']

    ld['event types and sub-types'] = ['event types and sub-types__EVENT_TYPES_AND_SUB_TYPES_DATA.tsv',
                                       'event_type.template',
                                       'dbo.event_type.Table.sql',
                                       'event_type.fmt']

    ld['out of service codes'] = ['out of service codes__OUT_OF_SERVICE_TYPE.tsv',
                                  'Out_Of_Service_Type.template',
                                  'dbo.out_of_service_type.Table.sql',
                                  'Out_Of_Service_Type.fmt']

    ld['term app access - inetveiwer'] = ['term app access - inetveiwer - {vClub}__APP_ACCESS.tsv',
                                          'app_access.template',
                                          'dbo.app_access.Table.sql',
                                          'app_access.fmt']

    ld['vehicle equipment'] = ['vehicle equipment - {vClub}__VEHICLE_EQUIPMENT.tsv',
                               'def_equip.template',
                               'dbo.def_equip.Table.sql',
                               'def_equip.fmt']

    ld['unit agency restriction'] = ['unit agency restriction - {vClub}__DEF_UNIT_RESTR_AG_AVAIL.tsv',
                                     'def_unit_restr_ag_avail.template',
                                     'dbo.def_unit_restr_ag_avail.Table.sql',
                                     'def_unit_restr_ag_avail.fmt']

    ld['esp alerts'] = ['esp alerts - {vClub}__ALERTS.tsv',
                        'ras_ext_sp_alert.template',
                        'dbo.ras_ext_sp_alert.Table.sql',
                        'ras_ext_sp_alert.fmt']

    ld['membership pricing level (surefire)'] = ['membership pricing level (surefire)__RAS_MEM_PRICING-LEV.tsv',
                                                 'ras_mem_pricing_lev.template',
                                                 'dbo.ras_mem_pricing_lev.Table.sql',
                                                 'ras_mem_pricing_lev.fmt']

    return ld

# --- process
# --------------------------------------------------------------------
#
#                          setup load details
#
# --------------------------------------------------------------------


def process(p_dir, p_ld, p_conn_details, p_work_dict):
    """
    process a load dict
    """
    alib.p_i('')
    alib.p_i('Start process for {}'.format(p_ld[0]))
    # -- setup local variables
    l_short_name = p_ld[0]
    l_details = p_ld[1]

    l_sheet_name = l_details[0]
    l_template = l_details[1]
    l_create_table = l_details[2]
    l_fmt_file = l_details[3]
    # l_create_table ='dbo.def_unit.Table.sql'

    l_data_dir = p_dir['data']
    l_code_dir = p_dir['code']
    l_temp_dir = p_dir['temp']

    tsv_results = fetch_tsv_file(p_work_dict, l_short_name, l_sheet_name, l_data_dir)

    # l_fmt_file = p_code_dir + '/templates/' + l_template

    alib.log_debug('    short name    : {}'.format(l_short_name))
    alib.log_debug('    table name    : {}'.format(l_sheet_name))

    l_sql_tmp_file = create_tmp_sql_file(l_code_dir, l_temp_dir, p_conn_details, l_create_table)

    run_sql_cmd = 'sqlcmd -S {vHost}\\{vInstance} -i {vSql} '.format(vHost=p_conn_details['host'],
                                                                     vInstance=p_conn_details['instance'],
                                                                     vSql=l_sql_tmp_file.replace('/', '\\'))

    run_job(run_sql_cmd)

    for tsv_file in tsv_results:
        alib.p_i('')
        alib.p_i('        Load file {}'.format(tsv_file.replace('/', '\\')))

        l_template_tmp_file = create_tmp_template_file(l_code_dir, l_temp_dir, p_conn_details,
                                                       l_template, tsv_file, l_fmt_file)

        run_sql_cmd = 'sqlcmd -S {vHost}\\{vInstance} -i {vSql} '.format(vHost=p_conn_details['host'],
                                                                         vInstance=p_conn_details['instance'],
                                                                         vSql=l_template_tmp_file.replace('/', '\\'))

        run_fmt_job(run_sql_cmd)

    return


# --------------------------------------------------------------------
#
#                          CREATE TMP SQL FILE
#
# --------------------------------------------------------------------


def fetch_tsv_file(p_work_dict, p_short_name, p_sheet_name, p_data_dir):
    """
    create a sql file that has all the variables replaced.
    """
    alib.log_debug('start fetch tsv file for [{}]'.format(p_short_name))
    print('short name {}'.format(p_short_name))
    print('data dir   {}'.format(p_data_dir))

    tsv_name = None

    tsv_results = []

    for key, value in p_work_dict.items():
        # -- handle the club files
        if value['tag'] == p_short_name:

            short_name = value['club_file_short']
            club_type = value['type']
            l_sheet = p_sheet_name.format(vClub=value['club'])
            l_dir = '/'.join(short_name.split('/')[:-1])

            if club_type == 'club':
                tsv_name = p_data_dir + '/data templates by club/' + l_dir + '/' + l_sheet
            else:
                tsv_name = p_data_dir + '/common data templates/' + l_dir + '/' + l_sheet

            tsv_results.append(tsv_name)

        # -- handle the "common" files
        if value['tag'] is None and value['type'] == 'club common':
            if p_short_name in value['club_file_short']:

                short_name = value['club_file_short']
                club_type = value['type']
                l_sheet = p_sheet_name.format(vClub=value['club'])
                l_dir = '/'.join(short_name.split('/')[:-1])

                if club_type == 'club':
                    tsv_name = p_data_dir + '/data templates by club/' + l_dir + '/' + l_sheet
                else:
                    tsv_name = p_data_dir + '/common data templates/' + l_dir + '/' + l_sheet

                tsv_results.append(tsv_name)

    return tsv_results

# --------------------------------------------------------------------
#
#                          CREATE TMP SQL FILE
#
# --------------------------------------------------------------------


def create_tmp_sql_file(p_code_dir, p_data_dir, p_conn_details, p_sql_file):
    """
    create a sql file that has all the variables replaced.
    """
    alib.log_debug('Start create tmp sql file')

    l_schema = p_conn_details['schema']

    l_sql_file = p_code_dir + '/sql/' + p_sql_file
    alib.log_debug('    sql file      : {}'.format(l_sql_file.replace('/', '\\')))

    l_sql_data = read_file(l_sql_file)
    l_data_dir = p_data_dir.replace('/', '\\')

    l_new_data = []
    for line in l_sql_data:
        l_new_data.append(line.format(vDB=l_schema,
                                      vFmtDir=l_data_dir,
                                      vDataDir=l_data_dir))

    l_sql_target_file = p_data_dir + '/' + p_sql_file

    write_file(l_sql_target_file, l_new_data)

    return l_sql_target_file


# --------------------------------------------------------------------
#
#                          CREATE TMP FMT FILE
#
# --------------------------------------------------------------------


def create_tmp_template_file(p_code_dir, p_tmp_dir, p_conn_details, p_template, p_tsv_file, p_fmt_file):
    """
    create a fmt file that has all the variables replaced.
    """
    alib.log_debug('Start create tmp fmt file')

    l_schema = p_conn_details['schema']

    l_template_file = p_code_dir + '/templates/' + p_template
    l_fmt_file = p_code_dir + '/fmt/' + p_fmt_file

    alib.log_debug('    fmt file      : {}'.format(l_template_file.replace('/', '\\')))

    l_template_data = read_file(l_template_file)
    l_data_dir = p_tmp_dir.replace('/', '\\')
    l_data_dir = l_data_dir + '\\'

    l_new_data = []
    for line in l_template_data:
        l_new_data.append(line.format(vDB=l_schema,
                                      vFmtFile=l_fmt_file.replace('/', '\\'),
                                      vTSVFile=p_tsv_file.replace('/', '\\')))

    l_template_target_file = p_tmp_dir + '/' + p_template + '.sql'

    write_file(l_template_target_file, l_new_data)

    return l_template_target_file

# --------------------------------------------------------------------
#
#                          READ FILE
#
# --------------------------------------------------------------------


def read_file(p_file):
    """
    read an entire file, return it as a string.
    p_file as a parameter should be the full path name to the file.
    """

    with open(p_file, "r") as myfile:
        data = myfile.readlines()

    return data

# --------------------------------------------------------------------
#
#                          WRITE FILE
#
# --------------------------------------------------------------------


def write_file(p_file, p_list):
    """
    read an entire file, return it as a string.
    p_file as a parameter should be the full path name to the file.
    """

    with open(p_file, "w+") as myfile:
        for line in p_list:
            myfile.write(line)

    return

# --- process

# --------------------------------------------------------------------
#
#                      run FMT job
#
# --------------------------------------------------------------------


def run_fmt_job(p_cmd):
    """
    run job
    if fail, send error to log file.
    """
    alib.p_i('        Run Command: [{}]'.format(''.join(p_cmd)))

    job = subprocess.Popen(p_cmd,
                           stdin=subprocess.PIPE,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)

    output, error = job.communicate()

    mod_output = output.decode('ascii')
    if('Error in insert' in mod_output or
       'Bulk load data conversion error' in mod_output or
       'Cannot insert the value' in mod_output or
       'Msg ' in mod_output):
        alib.p_e('        Errors found, please review log file')
        alib.log_error('{}'.format(mod_output))
    else:
        alib.p_i('        Success')
        alib.log_debug('        output = {}'.format(output))
        alib.p_i('')

    if len(error) > 0:
        alib.log_error('    error =  = {}'.format(error))

# --------------------------------------------------------------------
#
#                      run job
#
# --------------------------------------------------------------------


def run_job(p_cmd):
    """
    run job
    if fail, send error to log file.
    """
    alib.p_i('')
    alib.p_i('Run Command: [{}]'.format(''.join(p_cmd)))

    job = subprocess.Popen(p_cmd,
                           stdin=subprocess.PIPE,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)

    output, error = job.communicate()

    if b'There is already an object named' in output:
        alib.p_i('    Errors found, please review log file')
    else:
        alib.p_i('    Success')

    alib.log_info('    output = {}'.format(output))
    if len(error) > 0:
        alib.log_error('    error =  = {}'.format(error))

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
                        help='DB Connection: "localhost" or paul@win-khgvd5br678\Adventureworks2014:dbo',
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

    parser.add_argument('--data_dir',
                        help='directory for data files',
                        default=None,
                        required=False)

    parser.add_argument('--temp_dir',
                        help='directory for temp build files',
                        default='c:/temp',
                        required=False)

    parser.add_argument('--short_code',
                        help='only run "short_name" load',
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

    if args['data_dir'] is not None:
        args['data_dir'] = alib.validate_dir_param(args['data_dir'])

    if args['temp_dir'] is not None:
        args['temp_dir'] = alib.validate_dir_param(args['temp_dir'])

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

    my_work_dir = {}
    my_work_dir['data'] = args['data_dir']
    my_work_dir['code'] = args['code_dir']
    my_work_dir['temp'] = args['temp_dir']

    # -------------------------------------
    # -- Fetch the tables to process

    work_dir = alib.load_dir(args)
    work_files = alib.load_files(work_dir)

    work_dict = alib.load_matching_masterfile(work_files, p_load_excel=False)

    alib.load_tags(work_dict)

    alib.print_filenames(work_dict)

    # -------------------------------------
    # -- Fetch the tables to process
    #
    #    Valid short codes
    #    -----------------
    #    term

    # p1   personnel                               -- round 1 error check complete.
    # p2   personnel node access                   -- round 1 error check complete.
    # p3   event types and sub-types               -- round 0 error check complete - (shannon)
    # p4   disposition codes                       -- round 1 error check complete.
    # p7   eta table                               -- round 1 error check complete.
    # p9   out of service codes                    -- round 1 error check complete.
    # p11  out of service type agency              -- round 1 error check complete.
    # p11  vehicles                                -- round 1 error check complete.
    # p12  units                                   -- round 1 error check complete.
    # p15  membership pricing level (surefire)     -- round 1 error check complete.
    # p46  esp alerts                              -- coding
    # p47  skills
    # p48  vehicle equipment
    # p65  term app access - inetveiwer
    # p99  unit agency restriction                 -- round 1 in prog - duplicates to report.


    load_details = setup_load_details()
    for ld in load_details.items():
        if args['short_code'] is not None:
            if args['short_code'] != ld[0]:
                continue
        process(my_work_dir, ld, connect_details, work_dict)

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
