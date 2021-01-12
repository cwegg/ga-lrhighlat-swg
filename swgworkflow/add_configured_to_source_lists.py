#!/usr/bin/env python3

import argparse
import logging
import os
import numpy as np
import pandas as pd
import astropy.table
from astropy.table import Table

from swgworkflow.xmlanalysis import parse_configured_xmls


def _match_masked_columns(col1, col2, mask1=None, mask2=None):
    if mask1 is None:
        mask1 = np.zeros(col1.shape, dtype=bool)
    if mask2 is None:
        mask2 = np.zeros(col2.shape, dtype=bool)

    good_col1_values = col1[~mask1]
    good_col1_idx = np.nonzero(~mask1)[0]
    good_col2_values = col2[~mask2]
    good_col2_idx = np.nonzero(~mask2)[0]
    _, col1_ind_unmasked, col2_ind_unmasked = np.intersect1d(
        good_col1_values, good_col2_values, return_indices=True)
    col1_idx = good_col1_idx[col1_ind_unmasked]
    col2_idx = good_col2_idx[col2_ind_unmasked]
    return col1_idx, col2_idx


def _unmask_column(table, column):
    if hasattr(table[column], 'mask'):
        table[column].fill_value = 0
        unmasked_column = table[column].filled()
        mask = table[column].mask
    else:
        unmasked_column = table[column]
        mask = None
    return unmasked_column, mask


def _match_source_to_target_lists(target_list, target_column_name, source_list,
                                  source_column_name, source_list_mask=None):

    target_column, target_mask = _unmask_column(target_list, target_column_name)
    source_column, source_mask = _unmask_column(source_list, source_column_name)

    target_ind, source_ind = _match_masked_columns(
        col1=target_column, col2=source_column,
        mask1=target_mask, mask2=source_mask)
    return target_ind, source_ind


def _get_output_filename(source_file, output_dir,
                         suffix='-configured',
                         extension='.fits'):
    input_basename_wo_ext = os.path.splitext(os.path.basename(source_file))[0]
    output_basename_wo_ext = input_basename_wo_ext + suffix
    output_file = os.path.join(output_dir, output_basename_wo_ext + extension)
    return output_file


def _check_output_file(output_file, overwrite=False):
    # If the output file already exists, delete it if overwrite, if not
    # return false
    if os.path.exists(output_file):
        if overwrite == True:
            logging.info('Removing previous file: {}'.format(output_file))
            os.remove(output_file)
            return True
        else:
            logging.info(
                'Skipping {} as it already exists.'.format(output_file))
            return False
    return True


def add_columns_to_source_list(source_file, target_cats, output_dir,
                               new_columns, default_values,
                               overwrite=False):
    output_file = _get_output_filename(source_file, output_dir,
                                       suffix='-configured',
                                       extension='.fits')

    # If the output file already exists, delete it or continue with the next
    # one
    if not _check_output_file(output_file, overwrite):
        return

    # Read the source list and add our new columns
    source_list = Table.read(source_file)
    for column, default in zip(new_columns, default_values):
        source_list[column] = default

    for target_cat in target_cats:
        target_list = Table.read(target_cat)

        # Check the requested columns actually exist in the target catalogue
        for column in new_columns:
            assert column in target_list.columns, \
                "Didn't find {} in {}. ".format(column, target_cat)

        # Get indexes of rows where GAIA_ID matches
        # We need to match only the good, non-masked entries in the target list
        target_column_name, source_column_name = 'GAIA_ID', 'SOURCE_ID'
        source_list_mask = (source_list['GAIA_REV_ID'] != 0)
        target_ind_gaia, source_ind_gaia = _match_source_to_target_lists(
            target_list, target_column_name, source_list, source_column_name,
            source_list_mask=source_list_mask)

        # Append indexes of rows where there is no GAIA_ID in source list,
        # but PS_ID matches a target in the target list
        target_column_name, source_column_name = 'PS_ID', 'PS1_ID'
        source_list_mask = (source_list['GAIA_REV_ID'] == 0)
        target_ind_ps, source_ind_ps = _match_source_to_target_lists(
            target_list, target_column_name, source_list, source_column_name,
            source_list_mask=source_list_mask)

        target_ind = np.append(target_ind_gaia, target_ind_ps)
        source_ind = np.append(source_ind_gaia, source_ind_ps)

        # Assert that the source list has the default values and copy across
        for column, default in zip(new_columns, default_values):
            if np.any(source_list[column][source_ind] != default):
                msg = "Found ambiguous matches from catalogue {} to source " \
                      "list {}. This may happen if two surveys target the " \
                      "same object.".format(target_cat,source_file)
                logging.warning(msg)
            source_list[column][source_ind] = target_list[column][
                target_ind]

    source_list.write(output_file)

    return output_file


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='After configuring xml files add this information back to '
                    'the catalogues')

    parser.add_argument('source_file', nargs='+',
                        help="""One or more source lists""")

    parser.add_argument('--catalogues', nargs='+',
                        help="""Catalogues containing targets""")

    parser.add_argument('--outdir', dest='output_dir', default='output',
                        help="""name of the directory which will contain the
                        output source lists""")

    parser.add_argument('--overwrite', action='store_true',
                        help='overwrite the output files')

    parser.add_argument('--log_level', default='info',
                        choices=['debug', 'info', 'warning', 'error'],
                        help='the level for the logging messages')

    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper()))

    if not os.path.exists(args.output_dir):
        logging.info('Creating the output directory')
        os.makedirs(args.output_dir, exist_ok=True)

    new_columns = ('GA_TARGBITS', 'TARGPROG', 'TARGPRIO', 'CONFIGURED',
                   'ASSIGNED')
    default_values = (0, 40*' ', 0.0, 0, 0)

    for source_file in args.source_file:
        # Clean after adding the last catalogue if we were asked to
        add_columns_to_source_list(source_file=source_file,
                                   target_cats=args.catalogues,
                                   new_columns=new_columns,
                                   default_values=default_values,
                                   output_dir=args.output_dir,
                                   overwrite=args.overwrite)
