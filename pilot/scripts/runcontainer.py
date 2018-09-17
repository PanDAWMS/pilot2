#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Based on runGen (T. Maeno)
#
# Authors:
# - Alessandra Forti, alessandra.forti@cern.ch, 2018

import os
import time
import argparse
import logging
import urllib
import ast
import subprocess

print "=== start ==="
print time.ctime()

# Example of prun command to process
# prun --exec 'pwd && ls -l %IN %OUT' --outDS user.aforti.test$a \
#      --outputs out.dat --site=ANALY_MANC_SL7 --noBuild \
#      --inDS user.aforti.test5_out.dat.197875784


def main():

    """ Main function of run_container """
    sc = singularity_command()
    run_container(sc)
    rename_ouput()


def singularity_command():

    # Options for the command line string have default values or are mandatory

    # Base singularity command
    singularity_base = 'singularity exec -C'

    # If Cvmfs add that to bind_paths
    cvmfs = ''

    if args.ctr_cvmfs:
        cvmfs = '-B /cvmfs:/cvmfs'

    # Add Command it need to remove the
    command = urllib.unquote(args.ctr_cmd)

    singularity_cmd = "%s --pwd %s -B $PWD:%s %s %s %s" % \
                      (singularity_base,
                       args.ctr_workdir,
                       args.ctr_datadir,
                       cvmfs,
                       args.ctr_image,
                       command)

    print singularity_cmd
    return singularity_cmd


def run_container(cmd=''):

    # Just in case singularity is not the only container we run
    # os.system going to be deprecated (?) but subprocess requires more
    # thinking to handle $PWD. Later if this doesn't end in tears.
    os.system(cmd)


def input():

    # place holder function. ATM if the file is there and the application knows
    # about it this function isn't needed. However even without directIn and
    # without PoolFileCatalogs, I expect the task input to be split by the
    # system and the application not knowing what is there. args.data_dir
    # should be prepended to the files at the very basic and the user
    # analysis should accept input as an argument.
    logging.info("Input files %s" % args.input_files)


def rename_ouput():

    # Rename the output files. No need to move them to currentDir
    # because we are already there. PFC and jobreport.json at a
    # later stage all jobs I checked have them empty anyway
    for old_name, new_name in args.output_files.iteritems():
        # archive *
        if old_name.find('*') != -1:
            subprocess.check_output(['tar', 'cvfz', new_name, old_name])
        else:
            subprocess.check_output(['mv', new_name, old_name])


if __name__ == "__main__":

    arg_parser = argparse.ArgumentParser()

    # Required arguments
    required = arg_parser.add_argument_group('required arguments')

    # Command to execute
    required.add_argument('-p',
                          dest='ctr_cmd',
                          required=True,
                          help='Command to execute in the container')

    # Container output dataset
    required.add_argument('-o',
                          dest='output_files',
                          type=ast.literal_eval,
                          required=True,
                          help='Output files')

    # Container Image to use
    required.add_argument('--containerImage',
                          dest='ctr_image',
                          required=True,
                          help='Image path in CVMFS or on docker')

    # Optional arguments

    # Container input dataset
    arg_parser.add_argument('-i',
                            dest='input_files',
                            type=ast.literal_eval,
                            default="",
                            help='Input files')

    # Container data directory
    arg_parser.add_argument('--containerDataDir',
                            dest='ctr_datadir',
                            default="/data",
                            help='Change directory where input, output \
                                  and log files should be stored. \
                                  Default: /data')

    # Container workdir
    arg_parser.add_argument('--containerWorkDir',
                            dest='ctr_workdir',
                            default="/",
                            help='Change workdir inside the container. \
                                  Default: /')

    # Container cvmfs
    arg_parser.add_argument('--containerCvmfs',
                            dest='ctr_cvmfs',
                            action='store_true',
                            default=False,
                            help='Mount /cvmfs. Default false')

    # Container proxy
    arg_parser.add_argument('--containerX509',
                            dest='ctr_x509',
                            action='store_true',
                            default=False,
                            help='Set the X509_USER_PROXY \
                                  and X509_CA_CERTDIR. Default: false')

    # Container environment vars
    arg_parser.add_argument('--containerEnv',
                            dest='ctr_env',
                            default="",
                            help='Container environment variables')

    # Container environment vars
    arg_parser.add_argument('--debug',
                            dest='debug',
                            action='store_true',
                            default=False,
                            help='Enable debug mode for logging messages')

    args, unknown = arg_parser.parse_known_args()

    # Setup the logging level
    format_str = '%(asctime)s | %(levelname)-8s | %(message)s'
    if args.debug:
        logging.basicConfig(filename='run_container.log',
                            level=logging.DEBUG,
                            format=format_str)
    else:
        logging.basicConfig(filename='run_container.log',
                            level=logging.INFO,
                            format=format_str)

    logging.info("Following arguments are unknown or unsupported %s" % unknown)

    main()
