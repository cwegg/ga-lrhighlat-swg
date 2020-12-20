#!/usr/bin/env python3
import time
import os.path
import argparse
import logging
import subprocess

def configure_fields(xml_file_list, output_dir,
                     sync=True, herts=False,
                     configure_path='/soft/configure/configure',
                     overwrite=False, threads=8,
                     extra_configure_options='',
                     extra_qsub_options='',
                     epoch='2021.5'):
    """
    Run xml files through configure tool to place fibres

    Parameters
    ----------
    xml_file_list : list of str
        A list of input OB XML files.
    output_dir : str
        Name of the directory which will contains the output XML files.
    herts : bool, optional
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
    job_id_list =[]
    for xml_file in xml_file_list:

        # Check that the input XML exists and is a file

        assert os.path.isfile(xml_file)

        # Choose the output filename depedending on the input filename

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
        command += '--threads {} '.format(threads)
        command += '--field {} '.format(os.path.abspath(xml_file))
        command += '--output {}'.format(os.path.abspath(output_file))
        command += extra_configure_options

        if herts:
            # Construct qsub command to submit job
            job_name = input_basename_wo_ext
            command = 'echo "{}" | qsub -l pmem=8gb -l '.format(command)
            command += 'walltime=12:00:00 -l nodes=1:ppn={} '.format(threads)
            command += '-o {}/{}.stdout '.format(output_dir,
                                                 output_basename_wo_ext)
            command += '-e {}/{}.stderr '.format(output_dir,
                                                 output_basename_wo_ext)
            command += '-N {} {} -'.format(job_name,extra_qsub_options)
            logging.info('Running command: {}'.format(command))
            job_id = subprocess.check_output(command, shell=True).decode(
                "utf-8").strip()
            job_id_list.append(job_id)

        else:
            logging.info('Running command: {}'.format(command))
            os.system(command)

    if sync and herts and len(job_id_list) > 0:
        jobs_names = ':'.join(job_id_list)
        command = 'echo "echo Done" | qsub  -W depend=afterany:{}'.format(jobs_names)
        logging.info('Running command: {}'.format(command))
        sync_job = subprocess.check_output(command, shell=True).decode(
            "utf-8").strip()
        command = 'qstat -f {} | grep job_state'.format(sync_job)
        state = subprocess.check_output(command, shell=True).decode(
            "utf-8").strip()
        while 'H' in state:
            time.sleep(10)
            state = subprocess.check_output(command, shell=True).decode("utf-8") .strip()

    return output_file_list


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Run XML files through configure tool to place fibres')

    parser.add_argument('xml_file_list',nargs='+',
                        help="""The list of xml files""")

    parser.add_argument('--configure_path',
                        default='/soft/configure/configure',
                        help="""Path to configure executable""")

    parser.add_argument('--outdir', dest='output_dir', default='output',
                        help="""name of the directory which will containe the
                        output XML files""")

    parser.add_argument('--epoch',
                        help='Epoch of observation')

    parser.add_argument('--herts', action='store_true',
                        help='Submit jobs to herts cluster - you should '
                             'typically be running this script on the headnode')

    parser.add_argument('--sync', action='store_true',
                        help='Should the script wait for the configure task '
                             'to finish before returning')

    parser.add_argument('--overwrite', dest='overwrite', action='store_true',
                        help='overwrite the output files')

    parser.add_argument('--log_level', default='info',
                        choices=['debug', 'info', 'warning', 'error'],
                        help='the level for the logging messages')

    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper()))

    if not os.path.exists(args.output_dir):
        logging.info('Creating the output directory')
        os.makedirs(args.output_dir)

    configure_fields(args.xml_file_list, args.output_dir,
                     epoch=args.epoch,
                     sync=args.sync,
                     herts=args.herts,
                     configure_path=args.configure_path,
                     overwrite=args.overwrite)
