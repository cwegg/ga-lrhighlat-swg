#!/usr/bin/env python3
import argparse
import glob
import logging
import os
import os.path

import dataframe_image as dfi
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from astropy.table import Table

from swgworkflow.xmlanalysis import parse_configured_xmls


def plot_assignment(configured_table, x='GAIA_RA', y='GAIA_DEC', figsize=(12, 7), flipy=False):
    fig, ax = plt.subplots(1, 2, figsize=figsize)
    idx = (configured_table['ASSIGNED'] == False)
    ax[0].plot(configured_table[x][idx],
               configured_table[y][idx],
               'r.', markersize=4, label='Not Assigned', alpha=0.5)
    idx = (configured_table['ASSIGNED'] == True)
    ax[0].plot(configured_table[x][idx],
               configured_table[y][idx],
               'g.', markersize=4, label='Assigned', alpha=0.5)

    ax[0].set_xlabel(x)
    ax[0].set_ylabel(y)
    ax[0].legend()
    if flipy:
        cur_ylim = ax[0].get_ylim()
        ax[0].set_ylim(cur_ylim[::-1])

    idx = (configured_table['ASSIGNED'] == False)
    scatter = ax[1].scatter(configured_table[x][idx],
                            configured_table[y][idx],
                            s=configured_table['TARGPRIO'][idx], c='r', alpha=0.5)
    idx = (configured_table['ASSIGNED'] == True)
    ax[1].scatter(configured_table[x][idx],
                  configured_table[y][idx],
                  s=configured_table['TARGPRIO'][idx],
                  c='g', label='Not Assigned', alpha=0.5)
    ax[1].set_xlabel(x)
    ax[1].set_ylabel(y)
    if flipy:
        cur_ylim = ax[1].get_ylim()
        ax[1].set_ylim(cur_ylim[::-1])

    # produce a legend with a cross section of sizes from the scatter
    handles, labels = scatter.legend_elements(prop="sizes", alpha=0.6)
    ax[1].legend(handles, labels, loc="upper right", title="TARGPRIO")
    return fig


def assignment_vs_targprio(targets, figsize=(5, 8)):
    fig, ax = plt.subplots(2, 1, sharex='all', figsize=figsize)
    sns.barplot(
        data=targets,
        x="TARGPRIO", y="ASSIGNED",
        palette="dark", alpha=.6, ax=ax[0]
    )
    ax[0].set_ylabel('Assignment\nProbability')
    ax[0].set_xlabel(None)

    sns.countplot(
        data=targets,
        x="TARGPRIO",
        palette="dark", alpha=.6, ax=ax[1]
    )
    ax[1].set_ylabel('Number of\nTargets')
    fig.tight_layout()
    return fig


def add_distance_to_field_center(df, summaries, radius_boundaries=(0.0, 0.1, 0.2, 0.4, 1.0)):
    from astropy.coordinates import SkyCoord
    df['distance_to_center'] = '>1deg'
    target_coord = SkyCoord(ra=df.GAIA_RA,
                            dec=df.GAIA_DEC,
                            unit='deg')

    for field_name in df.FIELD_NAME.unique():
        ra = summaries[summaries.field_name == field_name].ra
        dec = summaries[summaries.field_name == field_name].dec
        field_center = SkyCoord(ra=ra, dec=dec, unit='deg')
        offset = field_center.separation(target_coord).deg

        for min_dist, max_dist in zip(radius_boundaries[:-1], radius_boundaries[1:]):
            ind = (offset > min_dist) & (offset < max_dist) & (df.FIELD_NAME == field_name)
            df.loc[ind, 'distance_to_center'] = f'From {min_dist} to {max_dist}'


def plot_assigned_vs_distance_to_field_center(targets, summaries,
                                              radius_boundaries=(0.0, 0.1, 0.2, 0.4, 1.0)):
    targets = targets.copy()
    add_distance_to_field_center(targets, summaries, radius_boundaries)
    distances = targets['distance_to_center'].unique()

    fig, axs = plt.subplots(len(distances), 3, sharex='all', figsize=(10, 10))
    count_axs, assigned_axs, fraction_axs = axs.T

    for distance, count_ax, assigned_ax, fraction_ax in zip(distances, count_axs, assigned_axs, fraction_axs):
        df = targets[targets['distance_to_center'] == distance]
        sns.countplot(data=df, x='TARGPRIO', palette="dark", alpha=.6, ax=count_ax)
        count_ax.set_xlabel(None)
        count_ax.set_ylabel('# Targets')

        sns.countplot(data=df[df['ASSIGNED'] > 0], x='TARGPRIO', palette="dark", alpha=.6, ax=assigned_ax)
        assigned_ax.set_xlabel(None)
        assigned_ax.set_ylabel('Fibres Assigned')
        assigned_ax.set_title(f'Distance to field center [deg]: {distance}')

        sns.barplot(data=df, x='TARGPRIO', y='ASSIGNED', palette="dark", alpha=.6, ax=fraction_ax)
        fraction_ax.set_xlabel(None)
        fraction_ax.set_ylabel('Fraction of Targets\nAssigned Fibres')

    count_axs[-1].set_xlabel('TARGPRIO')
    fraction_axs[-1].set_xlabel('TARGPRIO')
    fig.tight_layout()
    return fig


def summary_by_source_list(files):
    data = []
    for file in files:
        basename = os.path.basename(file)
        basename = basename.split('-', 1)[0]
        source_list = Table.read(file)
        df = source_list.to_pandas()
        t = df[['CONFIGURED', 'ASSIGNED']].sum()
        t['source_list'] = basename
        t['FractionAssigned'] = t['ASSIGNED'] / t['CONFIGURED']
        data.append(t.to_frame().transpose().set_index('source_list'))
    source_lists = pd.concat(data)
    return source_lists

def extract_targprogs(targets):
    targprogs = set()
    ignore = ('POI', '')
    for targprog_string in targets.TARGPROG.unique():
        this_targprogs = targprog_string.strip().split('|')
        for this_targprog in this_targprogs:
            if this_targprog not in ignore:
                targprogs.add(this_targprog)
    return targprogs


def targprog_confusion(targets):
    targprogs = extract_targprogs(targets)
    confusion = []
    for class_one in targprogs:
        for class_two in targprogs:
            count = (targets.TARGPROG.str.contains('|' + class_one + '|', regex=False) &
                     targets.TARGPROG.str.contains('|' + class_two + '|', regex=False)).sum()
            confusion.append({'TARGPROG 1': class_one,
                              'TARGPROG 2': class_two,
                              'Count': count})
    confusion = pd.DataFrame(confusion)
    return confusion.pivot("TARGPROG 1", "TARGPROG 2", "Count")


def plot_confusion(df, title=None, **kwargs):
    from matplotlib.colors import LogNorm

    numbers = targprog_confusion(df)
    log_norm = LogNorm(vmin=0.5, vmax=numbers.max().max())
    ret = sns.heatmap(numbers, annot=True, fmt="d", norm=log_norm, **kwargs)
    ret.set_title(title)
    return ret


def summary_by_targprog(targets):
    targprogs = extract_targprogs(targets)
    table = []
    for this_class in targprogs:
            count = targets.TARGPROG.str.contains('|'+this_class+'|',regex=False).sum()
            assigned = (targets.TARGPROG.str.contains('|'+this_class+'|',regex=False) & \
                        targets.ASSIGNED>0).sum()
            table.append({'TARGPROG': this_class,
                             'CONFIGURED': count,
                          'ASSIGNED': assigned,
                        'FractionAssigned': assigned/count})
    summary_by_targprog = pd.DataFrame(table)
    return summary_by_targprog



if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Make plots of GA-LRHighLat configured fields')

    parser.add_argument('--submission', default='all',
                        help="""Submission from params.yaml to plot""")

    parser.add_argument('--plot_dir', default='plot/',
                        help="""name of the directory which will contain the 
                        plots""")

    parser.add_argument('--output_dir', default='output',
                        help="""name of the directory which contains the 
                        configured fields""")

    parser.add_argument('--file_format', default='png',
                        choices=['png', 'pdf'],
                        help="""Plot format""")

    parser.add_argument('--plot', default='all',
                        choices=['all', 'summary', 'fields', 'sky', 'targprio', 'distance',
                                 'colormag', 'source_list', 'targprog'],
                        help="""Plots to produce""")

    parser.add_argument('--by-field', dest='plot_by_field', action='store_true')
    parser.add_argument('--no-by-field', dest='plot_by_field', action='store_false')
    parser.set_defaults(plot_by_field=True)

    parser.add_argument('--log_level', default='info',
                        choices=['debug', 'info', 'warning', 'error'],
                        help='the level for the logging messages')

    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper()))

    if not os.path.exists(args.output_dir):
        logging.info('Creating the output directory')
        os.mkdir(args.output_dir)

    plot_by_field = args.plot_by_field

    # Very ugly parsing!!!!
    if args.plot == 'all':
        default = True
    else:
        default = False
    (plot_summary, plot_fields, plot_sky, plot_by_targprio, plot_by_distance, plot_color_mag,
     plot_summary_by_source_list, plot_targprog_confusion, plot_summary_by_targprog, plot_targprio_by_sourcelist) = \
        [default] * 10
    if args.plot == 'summary':
        plot_summary = True
    elif args.plot == 'fields':
        plot_fields = True
    elif args.plot == 'sky':
        plot_sky = True
    elif args.plot == 'targprio':
        plot_by_targprio = True
    elif args.plot == 'distance':
        plot_by_distance = True
    elif args.plot == 'colormag':
        plot_color_mag = True
    elif args.plot == 'source_list':
        plot_summary_by_source_list = True
    elif args.plot == 'targprog':
        plot_targprog_confusion = True
    elif args.plot == 'targprog':
        plot_summary_by_targprog = True
    elif args.plot == 'source_list':
        plot_targprio_by_sourcelist = True

    submission, plot_format = args.submission, args.file_format

    plot_prefix = args.plot_dir + submission
    os.makedirs(args.plot_dir, exist_ok=True)

    configured_catalogue_file = glob.glob(f'{args.output_dir}/{submission}/catalogs-configured/*.fits')[0]
    configured_xmls = f'{args.output_dir}/{submission}/05-configured/*.xml'
    source_lists = f'{args.output_dir}/{submission}/source-lists-configured/*.fits'
    logging.info(f'Using configured catalogue: {configured_catalogue_file}')
    logging.info(f'Parsing configured xmls from: {configured_xmls}')
    logging.info(f'Using internal source lists from: {source_lists}')
    logging.info(f'Creating plots in: {args.plot_dir}')

    sns.set()  # set plot style

    table = Table.read(configured_catalogue_file)
    table = table[table['CONFIGURED'] > 0]  # we only care about targets that got passed to configure
    configured_table = table.to_pandas()
    # Convert bytes to strings for pandas
    for column in configured_table.columns:
        if isinstance(configured_table[column][0], (bytes, bytearray)):
            configured_table[column] = configured_table[column].str.decode("utf-8")
    targets = configured_table[configured_table.TARGUSE == 'T']
    sky = configured_table[configured_table.TARGUSE == 'S']
    summaries, xml_targets = parse_configured_xmls(configured_xmls)

    if plot_summary:
        dfi.export(summaries, f'{plot_prefix}_summary.{plot_format}')

    if plot_fields and plot_by_field:
        for field_name in targets.FIELD_NAME.unique():
            field_configured_table = targets[targets['FIELD_NAME'] == field_name]
            fig = plot_assignment(field_configured_table, x='GAIA_RA', y='GAIA_DEC')
            fig.suptitle(f'{field_name} target distribution')
            fig.savefig(f'{plot_prefix}_{field_name}_TargetDistribution.{plot_format}')
            plt.close()

    if plot_sky and plot_by_field:
        for field_name in sky.FIELD_NAME.unique():
            field_configured_table = sky[sky['FIELD_NAME'] == field_name]
            fig = plot_assignment(field_configured_table, x='GAIA_RA', y='GAIA_DEC')
            fig.suptitle(f'{field_name} sky fibre distribution')
            fig.savefig(f'{plot_prefix}_{field_name}_SkyDistribution.{plot_format}')
            plt.close()

    if plot_by_targprio:
        fig = assignment_vs_targprio(targets)
        fig.savefig(f'{plot_prefix}_Overall_Assignment_Probablity.{plot_format}')
        if plot_by_field:
            g = sns.catplot(
                data=targets,
                x="TARGPRIO", y="ASSIGNED",
                palette="dark", alpha=.6, kind="bar", col='FIELD_NAME',
                col_wrap=3
            )
            g.savefig(f'{plot_prefix}_Assignment_Probablity_By_Field.{plot_format}')
            for field_name in sky.FIELD_NAME.unique():
                field_targets = targets[targets['FIELD_NAME'] == field_name]
                fig = assignment_vs_targprio(field_targets)
                fig.suptitle(f'{field_name}')
                fig.tight_layout()
                fig.savefig(f'{plot_prefix}_{field_name}_Assignment_Probablity.{plot_format}')
                plt.close()

    if plot_by_distance:
        for field_name in sky.FIELD_NAME.unique():
            field_targets = targets[targets['FIELD_NAME'] == field_name]
            fig = plot_assigned_vs_distance_to_field_center(targets, summaries)
            fig.suptitle(f'{field_name} assignment vs distance to cluster center')
            fig.tight_layout()
            fig.savefig(f'{plot_prefix}_{field_name}_assignment_vs_distance.{plot_format}')
            plt.close()

    if plot_color_mag:
        field_configured_table = targets.copy()
        field_configured_table['PS_MAG_G-PS_MAG_R'] = field_configured_table['PS_MAG_G'] - field_configured_table[
            'PS_MAG_R']
        fig = plot_assignment(field_configured_table, x='PS_MAG_G-PS_MAG_R', y='PS_MAG_G', flipy=True)
        fig.savefig(f'{plot_prefix}_Overall_ColorMag.{plot_format}')
        plt.close()
        if plot_by_field:
            for field_name in targets.FIELD_NAME.unique():
                field_configured_table = targets[targets['FIELD_NAME'] == field_name].copy()
                field_configured_table['PS_MAG_G-PS_MAG_R'] = field_configured_table['PS_MAG_G'] - field_configured_table[
                    'PS_MAG_R']
                fig = plot_assignment(field_configured_table, x='PS_MAG_G-PS_MAG_R', y='PS_MAG_G', flipy=True)
                fig.suptitle(f'{field_name} Color-Magnitude Diagram')
                fig.savefig(f'{plot_prefix}_{field_name}_ColorMag.{plot_format}')
                plt.close()

    if plot_summary_by_source_list:
        files = glob.glob(source_lists)
        df = summary_by_source_list(files)
        df['FractionAssigned'] = df['FractionAssigned'].map('{:,.2f}'.format)
        dfi.export(df, f'{plot_prefix}_source_list_summary.{plot_format}')

    if plot_targprog_confusion:
        fig, ax = plt.subplots(2, 1, figsize=(8, 8))
        _ = plot_confusion(targets[targets.CONFIGURED > 0], ax=ax[0], title='Configured')
        _ = plot_confusion(targets[targets.ASSIGNED > 0], ax=ax[1], title='Assigned')
        fig.tight_layout()
        fig.savefig(f'{plot_prefix}_Confusion.{plot_format}')

    if plot_summary_by_targprog:
        df = summary_by_targprog(targets)
        df['FractionAssigned'] = df['FractionAssigned'].map('{:,.2f}'.format)
        dfi.export(df, f'{plot_prefix}_targ_prog_summary.{plot_format}')

    if plot_targprio_by_sourcelist:
        data = []
        files = glob.glob(source_lists)
        print(f'Source list, configured, assigned')
        for file in files:
            basename = os.path.basename(file)
            basename = basename.split('-', 1)[0]
            source_list = Table.read(file)
            df = source_list.to_pandas()
            df['source_list'] = basename
            data.append(df[['source_list', 'TARGPRIO', 'CONFIGURED', 'ASSIGNED']])
            print(f"{basename}, {source_list['CONFIGURED'].sum()}, {source_list['ASSIGNED'].sum()}")
        source_lists = pd.concat(data)
        g = sns.catplot(
            data=source_lists[source_lists['CONFIGURED'] > 0],
            x="TARGPRIO", y="ASSIGNED",
            palette="dark", alpha=.6, kind="bar", col='source_list',
            col_wrap=3
        )
        g.savefig(f'{plot_prefix}_Assignment_Probablity_By_Source_List.{plot_format}')
        g = sns.catplot(
            data=source_lists[source_lists['ASSIGNED'] > 0],
            x="TARGPRIO",
            palette="dark", alpha=.6, kind="count", col='source_list',
            col_wrap=3
        )
        g.savefig(f'{plot_prefix}_Assignment_Number_By_Source_List.{plot_format}')
