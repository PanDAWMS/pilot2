#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Danila Oleynik, danila.oleynik@cern.ch, 2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2019

# Note: The Pilot 2 utilities to provide MPI related functionality through mpi4py
# Required for HPC workflow where Pilot 2 acts like an MPI application

import logging
try:
    logger = logging.getLogger(__name__)
except Exception:
    logger = None


def get_ranks_info():
    """
    Return current MPI rank and number of ranks
    None, None - if MPI environment is not available

    :return: rank, max_rank
    """

    rank = None
    max_rank = None
    try:
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()
        max_rank = comm.Get_size()
    except ImportError:
        if logger:
            logger.info("mpi4py not found")
        else:
            print("mpi4py not found")
    return rank, max_rank
