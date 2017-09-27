#!/bin/bash

ABSOLUTE_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
FILE=${ABSOLUTE_PATH}/EVNT.08716373._000060.pool.root.1

if [ -f $FILE ]; then
    echo "File $FILE exists."
else
    export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase
    source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh
    localSetupRucioClients
    export export RUCIO_ACCOUNT=wguan

    rucio download -d $ABSOLUTE_PATH mc15_13TeV:EVNT.08716373._000060.pool.root.1
fi
