#!/usr/bin/env python3
import argparse
import logging
import multiprocessing
import os.path
import subprocess
import time


def _is_tool(name):
    """Check whether `name` is on PATH and marked as executable."""
    from shutil import which
    return which(name) is not None


def _run_command(command):
    return subprocess.check_output(command, shell=True).decode("utf-8").strip()


def _copy_empty_xmls(xml_file_list, output_dir):
    """Helper function that removes the targets from the files in xml_file_list"""
    import xml.etree.ElementTree as ET
    output_file_list = []
    for xml_file in xml_file_list:
        output_file = os.path.join(output_dir, os.path.basename(xml_file))
        # read xml
        tree = ET.parse(xml_file)
        root = tree.getroot()
        field_list = root.findall('.//field')
        assert len(field_list) == 1, "Only a single field is allowed in MOS configurations"
        field = root.find('.//field')
        # delete targets
        for target in list(field)[::-1]:
            field.remove(target)
        # write to file
        tree.write(output_file)
        output_file_list.append(output_file)
    return output_file_list


def _filter_xmls_by_targprio(xml_file_list, intermediate_files, output_dir, targprio_boundary=-1):
    """We take a list of intermediate files (intermediate_files), and add the targets from the files in xml_file_list
    that have targprio > targprio_boundary.

    This multistage configuration, first configuring the highest targprio targets seperately, freezing the fibres and
    then adding more lower priority targets.
    """
    import xml.etree.ElementTree as ET

    output_file_list = []

    assert len(intermediate_files) == len(xml_file_list)

    for xml_file, intermediate_file in zip(xml_file_list, intermediate_files):
        # read intermediate xml
        intermediate_tree = ET.parse(intermediate_file)
        intermediate_root = intermediate_tree.getroot()
        intermediate_field = intermediate_root.find('.//field')
        num_initial_targets = len(intermediate_root.findall('.//target'))

        # uniqueness of target in xml is set by targsrvy and targid
        def targ_uid(target):
            targsrvy = target.get('targsrvy', None)
            targid = target.get('targid', None)
            return targsrvy, targid

        intermediate_targets = set()
        for target in intermediate_root.findall('.//target'):
            intermediate_targets.add(targ_uid(target))

        # read input xml
        input_root = ET.parse(xml_file).getroot()
        # find targets > targprio_boundary and add them
        for target in input_root.findall('.//target'):
            if float(target.get('targprio', default='-inf')) > targprio_boundary and \
                    targ_uid(target) not in intermediate_targets:
                intermediate_field.append(target)

        # write intermediate xml to output_dir
        output_file = os.path.join(output_dir, os.path.basename(xml_file))
        intermediate_tree.write(output_file)
        output_file_list.append(output_file)
        num_final_targets = len(intermediate_root.findall('.//target'))
        num_total_targets = len(input_root.findall('.//target'))

        logging.info(f'File {xml_file} with {num_total_targets} targets.  Initially {num_initial_targets} targets in'
                     f' {intermediate_file}. Afterwards {num_final_targets} targets in {output_file}.')
    return output_file_list


def configure_fields(xml_file_list, output_dir,
                     sync=True, qsub=False,
                     configure_path='/soft/configure/configure',
                     overwrite=False, threads=8,
                     extra_configure_options='',
                     extra_qsub_options='',
                     epoch='2021.5', seed=42):
    """
    Run xml files through configure tool to place fibres

    Parameters
    ----------
    xml_file_list : list of str
        A list of input OB XML files.
    output_dir : str
        Name of the directory which will contains the output XML files.
    qsub : bool, optional
        If running on the herts cluster submit the jobs to the queue.
    sync : bool, optional
        If running on the herts cluster wait until jobs finished before
        returning.
    extra_configure_options : str, optional
        Extra options to be passed to configure.
    extra_qsub_options : str, optional
        Extra options to be passed to qsub on herts cluster.
    overwrite : bool, optional
        Overwrite the output xml file.

    Returns
    -------
    output_file_list : list of str
        A list with the output XML files.
    """

    output_file_list = []
    job_id_list = []
    for xml_file in xml_file_list:

        # Check that the input XML exists and is a file

        assert os.path.isfile(xml_file)

        # Choose the output filename depending on the input filename

        input_basename_wo_ext = os.path.splitext(os.path.basename(xml_file))[0]

        if (input_basename_wo_ext.endswith('-configured') or
                input_basename_wo_ext.endswith('-')):
            output_basename_wo_ext = input_basename_wo_ext + 'configured'
        else:
            output_basename_wo_ext = input_basename_wo_ext + '-configured'

        output_file = os.path.join(output_dir, output_basename_wo_ext + '.xml')

        # Save the output filename for the result

        output_file_list.append(output_file)

        # If the output file already exists, delete it or continue with the next
        # one

        if os.path.exists(output_file):
            if overwrite == True:
                logging.info('Removing previous file: {}'.format(output_file))
                os.remove(output_file)
            else:
                logging.info(
                    'Skipping file {} as its output already exists: {}'.format(
                        xml_file, output_file))
                continue

        command = '{} --gui 0 '.format(configure_path)
        command += '--epoch {} '.format(epoch)
        command += '--seed {} '.format(seed)
        command += '--threads {} '.format(threads)
        command += '--field {} '.format(os.path.abspath(xml_file))
        command += '--output {} '.format(os.path.abspath(output_file))
        command += extra_configure_options

        if qsub:
            # Construct qsub command to submit job
            job_name = input_basename_wo_ext
            command = 'echo "{}" | qsub -l pmem=16gb -l '.format(command)
            command += 'walltime=12:00:00 -l nodes=1:ppn={} '.format(threads)
            command += '-o {}/{}.stdout '.format(output_dir,
                                                 output_basename_wo_ext)
            command += '-e {}/{}.stderr '.format(output_dir,
                                                 output_basename_wo_ext)
            command += '-N {} {} -'.format(job_name, extra_qsub_options)
            logging.info('Running command: {}'.format(command))
            job_id = _run_command(command)
            job_id_list.append(job_id)

        else:
            logging.info('Running command: {}'.format(command))
            _run_command(command)

    if sync and qsub and len(job_id_list) > 0:
        jobs_names = ':'.join(job_id_list)
        command = 'echo "echo Done" | qsub  -W depend=afterany:{} '.format(jobs_names)
        command += '-o /dev/null -e /dev/null -N sync'
        logging.info('Running command: {}'.format(command))
        sync_job = subprocess.check_output(command, shell=True).decode(
            "utf-8").strip()
        command = 'qstat -f {} | grep job_state'.format(sync_job)
        state = _run_command(command)
        while 'H' in state:
            time.sleep(10)
            state = _run_command(command)

    return output_file_list


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Run XML files through configure tool to place fibres')

    parser.add_argument('--xml_file_list', nargs='+',
                        help="""The list of xml files""")

    parser.add_argument('--configure_path',
                        default='/soft/configure/configure',
                        help="""Path to configure executable""")

    parser.add_argument('--outdir', dest='output_dir', default='output',
                        help="""name of the directory which will contain the
                        output XML files""")

    parser.add_argument('--epoch', default='2021.5',
                        help='Epoch of observation')

    parser.add_argument('--seed', default=42, type=int,
                        help='Random seed passed to configure')

    parser.add_argument('--threads', default=0, type=int,
                        help='Number of threads to run configure with. '
                             'By default will use all available cores.')

    parser.add_argument('--extra_configure_options', default='',
                        help='extra command line options to be passed to '
                             'configure (enclose in quotes)')

    parser.add_argument('--qsub', default='auto',
                        choices=['auto', 'yes', 'no'],
                        help='Submit jobs using qsub i.e. you are running on '
                             'the qsub cluster - in which case you should '
                             'typically be running this script on the headnode')

    parser.add_argument('--sync', action='store_true',
                        help='If submitting with qsubm should the script wait '
                             'for the configure tasks to finish before returning')

    parser.add_argument('--overwrite', dest='overwrite', action='store_true',
                        help='overwrite the output files')

    parser.add_argument('--log_level', default='info',
                        choices=['debug', 'info', 'warning', 'error'],
                        help='the level for the logging messages')

    parser.add_argument('--multistage', default='-1', nargs='+', type=float,
                        help='Run configure multiple times, each time freezing the '
                             'previously allocated fibres. The list of numbers are '
                             'the boundaries of targprio i.e. with --multistage 6 2 '
                             'configure will be run 3 times. First fibres will be '
                             'allocated to the targprio>=6 targets, configure will '
                             'be run a second time allocating spare fibres to targprio>=2, '
                             'and a final time for any remaining fibres.')

    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper()))

    if not os.path.exists(args.output_dir):
        logging.info('Creating the output directory')
        os.makedirs(args.output_dir)

    if args.qsub == 'auto':
        qsub = _is_tool('qsub')
    elif args.qsub == 'yes':
        qsub = True
    else:
        qsub = False

    if args.threads == 0:
        if qsub:
            threads = 8  # default of 8 threads on herts cluster
        else:
            threads = multiprocessing.cpu_count()
    else:
        threads = args.threads

    if args.multistage[0] <= 0:
        # Single stage configure
        configure_fields(args.xml_file_list, args.output_dir,
                         epoch=args.epoch,
                         sync=args.sync,
                         qsub=qsub,
                         configure_path=args.configure_path,
                         overwrite=args.overwrite,
                         seed=args.seed,
                         threads=threads,
                         extra_configure_options=args.extra_configure_options)
    else:
        # multistage stage configure

        assert all(prio > 0 for prio in args.multistage), \
            'All your multistage targprio boundaries should be > 0'

        targ_prio_boundaries = args.multistage + [-1]  # We give the last stage a negative targprio so nothing gets filtered

        # for the first run we use --sky_search=0
        extra_configure_options = args.extra_configure_options + ' --sky_search=0'
        output_dir = args.output_dir

        intermediate_post_configure_dir = os.path.join(args.output_dir, 'stage-0-empty')
        os.makedirs(intermediate_post_configure_dir, exist_ok=True)
        intermediate_post_configured_files = _copy_empty_xmls(args.xml_file_list, intermediate_post_configure_dir)

        for stage, targprio_boundary in enumerate(targ_prio_boundaries):
            intermediate_pre_configure_dir = os.path.join(args.output_dir, 'stage-{}-pre-configure'.format(stage))
            os.makedirs(intermediate_pre_configure_dir, exist_ok=True)

            intermediate_pre_configure_files = _filter_xmls_by_targprio(args.xml_file_list,
                                                                        intermediate_post_configured_files,
                                                                        intermediate_pre_configure_dir,
                                                                        targprio_boundary)

            if targprio_boundary < 0:
                # Final configuration
                output_dir = args.output_dir
            else:
                output_dir = os.path.join(args.output_dir, 'stage-{}-post-configure'.format(stage))
                os.makedirs(output_dir, exist_ok=True)

            intermediate_post_configured_files = configure_fields(intermediate_pre_configure_files, output_dir,
                                                                  epoch=args.epoch,
                                                                  sync=args.sync,
                                                                  qsub=qsub,
                                                                  configure_path=args.configure_path,
                                                                  overwrite=args.overwrite,
                                                                  seed=args.seed,
                                                                  threads=threads,
                                                                  extra_configure_options=args.extra_configure_options)
            # for all apart from the first run we use --preallocate-guide=0
            extra_configure_options = args.extra_configure_options + ' --preallocate-guide=0'
