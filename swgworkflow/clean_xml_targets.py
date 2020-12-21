#!/usr/bin/env python3

import argparse
import logging
import os
import numpy as np
from astropy.table import Table
from astropy.coordinates import SkyCoord

from ifu.workflow.utils.classes import OBXML


def clean_xml_targets(ob_xml):
    # Remove any template targets
    for field in ob_xml.fields.getElementsByTagName('field'):
        for target in field.getElementsByTagName('target'):
            if target.getAttribute('targsrvy') == '%%%':
                field.removeChild(target)


def clean_targets(xml_file_list, output_dir, overwrite=False):
    output_file_list = []

    for xml_file in xml_file_list:

        for xml_file in xml_file_list:

            # Check that the input XML exists and is a file

            assert os.path.isfile(xml_file)

            # Choose the output filename depedending on the input filename

            input_basename_wo_ext = \
                os.path.splitext(os.path.basename(xml_file))[0]

            if (input_basename_wo_ext.endswith('-c') or
                    input_basename_wo_ext.endswith('-')):
                output_basename_wo_ext = input_basename_wo_ext + 'c'
            else:
                output_basename_wo_ext = input_basename_wo_ext + '-c'

            output_file = os.path.join(output_dir,
                                       output_basename_wo_ext + '.xml')

            # Save the output filename for the result

            output_file_list.append(output_file)

            # If the output file already exists, delete it or continue with the next
            # one

            if os.path.exists(output_file):
                if overwrite == True:
                    logging.info(
                        'Removing previous file: {}'.format(output_file))
                    os.remove(output_file)
                else:
                    logging.info(
                        'Skipping file {} as its output already exists: {}'.format(
                            xml_file, output_file))
                    continue

            ob_xml = OBXML(xml_file)

            clean_xml_targets(ob_xml)

            ob_xml.write_xml(output_file)

        return output_file_list


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Clean XML fields of template targets')

    parser.add_argument('xml_file', nargs='+',
                        help="""one or more input OB XML files""")

    parser.add_argument('--outdir', dest='output_dir', default='output',
                        help="""name of the directory which will contain the
                        output XML files""")

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

    clean_targets(xml_file_list=args.xml_file,
                  output_dir=args.output_dir,
                  overwrite=args.overwrite)
