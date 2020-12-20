#!/usr/bin/env python3

import yaml
import argparse

from weaveworkflow.mos.workflow.mos_stage1 import create_mos_field_cat
from astropy.table import Table

params_file = "params.yaml"

with open(params_file, 'r') as fd:
    params = yaml.safe_load(fd)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Makes fits files of fields from footprints.')

    parser.add_argument('strand',
                        help="""Which top level object in params.yaml to 
                        process.""")

    args = parser.parse_args()

    assert args.strand in params['strands'], \
        f"Didnt find {args.strand} in {params_file}"


    mos_field_template = params['field_template']

    output_field_file = params['strands'][args.strand]['field_file']
    task = params['strands'][args.strand]['footprint']

    # Reformat fits table into expected form
    field_table = Table.read(task['footprint_file'])
    field_table['PROGTEMP'] = task['progtemp']
    field_table['OBSTEMP'] = task['obstemp']
    field_table.rename_column('NAME', 'FIELD_NAME')
    field_table.rename_column('RA', 'FIELD_RA')
    field_table.rename_column('DEC', 'FIELD_DEC')
    surveys = task['surveys']
    for targsrvy, max_fibres in zip(surveys['targsrvys'],surveys['max_fibres']):
        field_table['TARGSRVY'] = targsrvy
        field_table['MAX_FIBRES'] = max_fibres

    trimester = task['keywords']['trimester']
    report_verbosity = task['keywords']['report_verbosity']
    author = task['keywords']['author']
    cc_report = task['keywords']['cc_report']


    create_mos_field_cat(mos_field_template, field_table, output_field_file,
                         trimester, author, report_verbosity=report_verbosity,
                         cc_report=cc_report)

