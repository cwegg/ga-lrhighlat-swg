#!/usr/bin/env python3

import argparse
import copy
import logging

import astropy.table
import numpy as np
from astropy.table import Table


def alter_catalogue(input_file, output_file, downsample_low_prio=1.0,
                    sky_downsample=0.6,
                    overwrite=False, seed=None, targprio_map=None):
    if seed is not None:
        np.random.seed(seed)
    source_catalogue = Table.read(input_file)

    high_priority = source_catalogue[(source_catalogue['TARGPRIO'] > 4.0)]
    low_priority = source_catalogue[(source_catalogue['TARGPRIO'] == 2.0) |
                                    (source_catalogue['TARGPRIO'] == 4.0)]
    sky = source_catalogue[(source_catalogue['TARGPRIO'] == 1.0)]

    idx = np.random.choice(len(low_priority),
                           int(len(low_priority) * downsample_low_prio),
                           replace=False)

    idx_sky = np.random.choice(len(sky),
                               int(len(sky) * sky_downsample),
                               replace=False)

    downsampled_catalogue = astropy.table.vstack([high_priority, low_priority[
        idx], sky[idx_sky]])

    if targprio_map is not None:
        new_catalogue = copy.deepcopy(downsampled_catalogue)
        remove_row_idx = []
        for old_targprio, new_targprio in targprio_map:
            idx = (downsampled_catalogue['TARGPRIO'] == old_targprio)
            if new_targprio == 0:
                remove_row_idx += list(idx.nonzero()[0])
            else:
                new_catalogue['TARGPRIO'][idx] = new_targprio
    if len(remove_row_idx) > 0:
        new_catalogue.remove_rows(remove_row_idx)

    new_catalogue.write(output_file, overwrite=overwrite)
    logging.info('Downsample {}. Original catalogue has {} sources. Final has {'
                 '}'.format(downsample_low_prio, len(source_catalogue),
                            len(new_catalogue)))
    return new_catalogue


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Alter SV Exp2 Catalogue by hand to configure tests')

    parser.add_argument('--input_catalogue', help="""Input SV Exp 2 
    Catalogue""")

    parser.add_argument('--output_catalogue', help="""Output SV Exp 2 
    Catalogue""")

    parser.add_argument('--downsample_low_prio', help="""Downsample SV Exp2 
    Catalogue targets with prio 2 and 4 by this fraction""", default=1.0,
                        type=float)

    parser.add_argument('--sky_downsample', help="""Downsample SV Exp2 
    sky targets by this fraction""", default=1.0, type=float)

    parser.add_argument('--only_prio', help="""Only keep targets with targprio 
    greater or equal to this value""", default=0.0, type=float)

    parser.add_argument('--overwrite', action='store_true',
                        help='overwrite the output catalogue')

    parser.add_argument('--log_level', default='info',
                        choices=['debug', 'info', 'warning', 'error'],
                        help='the level for the logging messages')

    parser.add_argument('--seed', default=1, type=int)

    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper()))

    targprio_map = [(10.0, 10.0), (9.0, 8.0), (8.0, 2.0),
                    (6.0, 3.0), (4.0, 2.0), (2.0, 1.0)]

    # Make the targprio map to zero when we want to drop those targets
    for idx, (old_targprio, new_targprio) in enumerate(targprio_map):
        if old_targprio < args.only_prio:
            targprio_map[idx] = (old_targprio, 0.0)

    alter_catalogue(args.input_catalogue, args.output_catalogue,
                    downsample_low_prio=args.downsample_low_prio,
                    overwrite=args.overwrite, seed=args.seed,
                    targprio_map=targprio_map, sky_downsample=args.sky_downsample)
