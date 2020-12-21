#!/usr/bin/env python3

import yaml
import argparse
import os
import copy
import astropy.table

from weaveworkflow.mos.workflow.mos_stage1 import create_mos_field_cat
from astropy.table import Table

params_file = "params.yaml"

with open(params_file, 'r') as fd:
    params = yaml.safe_load(fd)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Makes fits files of fields from footprints.')

    parser.add_argument('--output',
                        help="""Where to place the output field file.""")

    parser.add_argument('submission',
                        help="""Which top level object in params.yaml to 
                        process.""")

    args = parser.parse_args()

    assert args.submission in params['submission'], \
        f"Didnt find {args.submission} in {params_file}"


    mos_field_template = params['field_template']

    output_field_file = args.output

    output_directory = os.path.dirname(output_field_file)
    if not os.path.isdir(output_directory):
        os.makedirs(output_directory)

    task = params['submission'][args.submission]['footprint']

    # Reformat fits table into expected form
    field_table = Table.read(task['footprint_file'])
    field_table['PROGTEMP'] = task['progtemp']
    field_table['OBSTEMP'] = task['obstemp']
    field_table.rename_column('NAME', 'FIELD_NAME')
    field_table.rename_column('RA', 'FIELD_RA')
    field_table.rename_column('DEC', 'FIELD_DEC')

    # Duplicate table for each survey
    surveys = task['surveys']
    per_survey_tables = []
    for targsrvy, max_fibres in zip(surveys['targsrvys'], surveys[
        'max_fibres']):
        field_table['TARGSRVY'] = targsrvy
        field_table['MAX_FIBRES'] = max_fibres
        per_survey_tables += [copy.deepcopy(field_table)]
    field_table = astropy.table.vstack(per_survey_tables)

    trimester = task['keywords']['trimester']
    report_verbosity = task['keywords']['report_verbosity']
    author = task['keywords']['author']
    cc_report = task['keywords']['cc_report']


    create_mos_field_cat(mos_field_template, field_table, output_field_file,
                         trimester, author, report_verbosity=report_verbosity,
                         cc_report=cc_report)

