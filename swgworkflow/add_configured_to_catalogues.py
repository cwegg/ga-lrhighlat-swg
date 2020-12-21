#!/usr/bin/env python3

import argparse
import logging
import os


def add_configured_to_catalogues(xml_file_list, target_cat, output_dir,
                                 overwrite=False):
    input_basename_wo_ext = os.path.splitext(os.path.basename(target_cat))[0]
    output_basename_wo_ext = input_basename_wo_ext + '-configured'
    output_file = os.path.join(output_dir, output_basename_wo_ext + '.xml')

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
