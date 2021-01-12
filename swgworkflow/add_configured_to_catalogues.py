#!/usr/bin/env python3

import argparse
import logging
import os
import pandas as pd
import astropy.table
from astropy.table import Table

from swgworkflow.xmlanalysis import parse_configured_xmls


def add_configured_to_catalogues(xml_file_list, target_cat, output_dir,
                                 overwrite=False):
    input_basename_wo_ext = os.path.splitext(os.path.basename(target_cat))[0]
    output_basename_wo_ext = input_basename_wo_ext + '-configured'
    output_file = os.path.join(output_dir, output_basename_wo_ext + '.fits')

    # If the output file already exists, delete it or continue with the next
    # one

    if os.path.exists(output_file):
        if overwrite == True:
            logging.info('Removing previous file: {}'.format(output_file))
            os.remove(output_file)
        else:
            logging.info(
                'Skipping catalogue {} as its output already exists: {}'.format(
                    target_cat, output_file))
            return

    catalog_targets = Table.read(target_cat)

    # Parse all the XMLs into pandas dataframes describing the targets and
    # Summarising the fields
    summaries, xml_targets = parse_configured_xmls(xml_file_list)

    # ICD-30 says uniqueness within a targsrvy is enforced on (targid,obstemp,
    # progtemp) so match on these
    extra_columns = summaries[['field_name', 'progtemp', 'obstemp']]
    xml_targets = xml_targets.merge(extra_columns, on=['field_name'],
                                    how='left')

    # Construct a table with columns (targsrvy,targid,obstemp,progtemp) tto
    # cross match to catalogue, and how many times each target was sent
    # to configure, and how many times it was assigned a fibre
    df = xml_targets.value_counts(subset=['targsrvy', 'targid', 'progtemp',
                                          'obstemp', 'assigned']).to_frame()
    df = df.unstack(level=[4], fill_value=0)
    df.columns = df.columns.to_flat_index()
    df.reset_index(inplace=True)
    df['CONFIGURED'] = df[(0, True)] + df[(0, False)]
    df['ASSIGNED'] = df[(0, True)]
    df.drop(columns=[(0, False)], inplace=True)
    df.drop(columns=[(0, True)], inplace=True)
    df.rename(columns={"targsrvy":"TARGSRVY", "targid":"TARGID",
                       "progtemp":"PROGTEMP", "obstemp": "OBSTEMP"},
              inplace=True)
    assigned_table = Table.from_pandas(df)
    # Assigned table should now have the columns TARGSRVY, TARGSRVY,
    # PROGTEMP, OBSTEMP (to join to the catalogue uniquely on) together with
    # CONFIGURED (how many times target was sent to configure) and ASSIGNED (
    # how many times target was assigned a fibre)

    # TODO line added to fix a mismatch in GA-HighLat catalogues. Remove when
    #  Sergey fixes it
    catalog_targets['PROGTEMP'][
        catalog_targets['PROGTEMP'] == '11331+'] = '11331.1+'

    catalogue_appended = astropy.table.join(catalog_targets, assigned_table,
                                            join_type='left')
    assert len(catalogue_appended) == len(catalog_targets), \
        'Size mismatch when cross-matching between catalogues'

    # If there was no cross match to xmls then it was never configured
    catalogue_appended['ASSIGNED'].fill_value = 0
    catalogue_appended['ASSIGNED'] = catalogue_appended['ASSIGNED'].filled()
    catalogue_appended['CONFIGURED'].fill_value = 0
    catalogue_appended['CONFIGURED'] = catalogue_appended['CONFIGURED'].filled()

    name_df = \
    xml_targets.groupby(['targsrvy', 'targid', 'progtemp', 'obstemp'])[
        'field_name'].apply('|'.join).reset_index()
    name_df.rename(columns={"targsrvy":"TARGSRVY", "targid":"TARGID",
                       "progtemp":"PROGTEMP", "obstemp": "OBSTEMP",
                            "field_name": "FIELD_NAME"},
              inplace=True)
    name_table = Table.from_pandas(name_df)
    catalogue_appended = astropy.table.join(catalogue_appended, name_table,
                                            join_type='left')
    assert len(catalogue_appended) == len(catalog_targets), \
        'Size mismatch when cross-matching between catalogues'

    # Finally write to fits file
    catalogue_appended.write(output_file)
    return output_file




if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='After configuring xml files add this information back to '
                    'the catalogues')

    parser.add_argument('xml_file', nargs='+',
                        help="""One or more input OB XML files""")

    parser.add_argument('--catalogues', nargs='+',
                        help="""Catalogues containing targets""")

    parser.add_argument('--outdir', dest='output_dir', default='output',
                        help="""name of the directory which will contain the
                        output catalogues""")

    parser.add_argument('--overwrite', action='store_true',
                        help='overwrite the output files')

    parser.add_argument('--log_level', default='info',
                        choices=['debug', 'info', 'warning', 'error'],
                        help='the level for the logging messages')

    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper()))

    if not os.path.exists(args.output_dir):
        logging.info('Creating the output directory')
        os.mkdir(args.output_dir)

    for target_cat in args.catalogues:
        # Clean after adding the last catalogue if we were asked to
        add_configured_to_catalogues(xml_file_list=args.xml_file,
                                     target_cat=target_cat,
                                     output_dir=args.output_dir,
                                     overwrite=args.overwrite)
