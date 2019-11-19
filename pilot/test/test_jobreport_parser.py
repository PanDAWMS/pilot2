#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch

import unittest
import json

from pilot.user.atlas.common import parse_jobreport_data


class TestUtils(unittest.TestCase):
    """
    Unit tests for utils functions.
    """

    def setUp(self):
        # skip tests if running on a Mac -- Macs don't have /proc
        #self.mac = False
        # if os.environ.get('MACOSX') == 'true':
        #    self.mac = True

        #from pilot.info import infosys
        # infosys.init("AGLT2_TEST-condor")
        pass

    def test_failed_jobreport(self):
        """
        ..

        :return: (assertion)
        """

        report = """
        {
  "cmdLine": "'/ccs/proj/csc108/AtlasReleases/21.0.15/AtlasOffline/21.0.15/InstallArea/x86_64-slc6-gcc49-opt/share/Sim_tf.py'""" \
            + """ '--inputEVNTFile=EVNT.13276676._000045.pool.root.1' '--maxEvents=50' '--postInclude' 'default:RecJobTransforms/UseFrontier.py' """ \
            + """'--preExec' """ \
            + """'EVNTtoHITS:simFlags.SimBarcodeOffset.set_Value_and_Lock(200000)' 'EVNTtoHITS:simFlags.TRTRangeCut=30.0;simFlags.TightMuonStepping=True' """ \
            + """'--preInclude' 'EVNTtoHITS:SimulationJobOptions/preInclude.BeamPipeKill.py,SimulationJobOptions/preInclude.FrozenShowersFCalOnly.py' """ \
            + """'--skipEvents=9250' '--firstEvent=449251' '--outputHITSFile=HITS.13287270._195329.pool.root.1' '--physicsList=FTFP_BERT_ATL_VALIDATION' """ \
            + """'--randomSeed=8986' '--conditionsTag' 'default:OFLCOND-MC16-SDR-14' '--geometryVersion=default:ATLAS-R2-2016-01-00-01_VALIDATION' """ \
            + """'--runNumber=364177' '--AMITag=s3126' '--DataRunNumber=284500' '--simulator=FullG4' '--truthStrategy=MC15aPlus'",
  "created": "2018-03-04T09:33:09",
  "executor": [
    {
      "errMsg": null,
      "exeConfig": {
        "script": "athena.py",
        "substep": "sim"
      },
      "name": "EVNTtoHITS",
      "rc": -1,
      "statusOK": false,
      "validation": false
    }
  ],
  "exitAcronym": "TRF_EXEC_VALIDATION_FAIL",
  "exitCode": 66,
  "exitMsg": "File EVNT.13276676._000045.pool.root.1 did not pass corruption test",
  "exitMsgExtra": "",
  "files": {
    "input": [
      {
        "dataset": null,
        "nentries": null,
        "subFiles": [
          {
            "file_guid": null,
            "name": "EVNT.13276676._000045.pool.root.1"
          }
        ],
        "type": "EVNT"
      }
    ],
    "output": [
      {
        "argName": "outputHITSFile",
        "dataset": null,
        "subFiles": [
          {
            "file_guid": null,
            "file_size": null,
            "name": "HITS.13287270._195329.pool.root.1",
            "nentries": null
          }
        ],
        "type": "HITS"
      }
    ]
  },
  "name": "Sim_tf",
  "reportVersion": "2.0.7",
  "resource": {
    "executor": {
      "EVNTtoHITS": {
        "cpuTime": null,
        "postExe": {
          "cpuTime": null,
          "wallTime": null
        },
        "preExe": {
          "cpuTime": null,
          "wallTime": null
        },
        "total": {
          "cpuTime": null,
          "wallTime": null
        },
        "validation": {
          "cpuTime": null,
          "wallTime": null
        },
        "wallTime": null
      }
    },
    "machine": {
      "cpu_family": "21",
      "linux_distribution": [
        "SUSE Linux Enterprise Server ",
        "11",
        "x86_64"
      ],
      "model": "1",
      "model_name": "AMD Opteron(TM) Processor 6274",
      "node": "nid15407",
      "platform": "Linux-3.0.101-0.46.1_1.0502.8871-cray_gem_c-x86_64-with-SuSE-11-x86_64"
    },
    "transform": {
      "cpuTime": 2,
      "cpuTimeTotal": 0,
      "externalCpuTime": 0,
      "trfPredata": null,
      "wallTime": 0
    }
  }
}
        """
        report_data = json.loads(report)
        print((json.dumps(parse_jobreport_data(report_data), sort_keys=True, indent=2)))

    def test_successful_jobreport(self):
        """
        ..

        :return: (assertion)
        """

        report = """
{
  "cmdLine": "'/cvmfs/atlas.cern.ch/repo/sw/software/21.0/AtlasOffline/21.0.15/InstallArea/x86_64-slc6-gcc49-opt/share/Sim_tf.py'""" \
            + """'--inputEVNTFile=EVNT.08847247._002212.pool.root.1' ",
  "executor": [
    {
      "asetup": null,
      "errMsg": "",
      "exeConfig": {
        "inputs": [
          "EVNT"
        ],
        "outputs": [
          "HITS"
        ],
        "script": "athena.py",
        "substep": "sim"
      },
      "logfileReport": {
        "countSummary": {
          "CATASTROPHE": 0,
          "CRITICAL": 0,
          "DEBUG": 0,
          "ERROR": 0,
          "FATAL": 0,
          "IGNORED": 0,
          "INFO": 11748,
          "UNKNOWN": 270546,
          "VERBOSE": 0,
          "WARNING": 169
        },
        "details": {
          "WARNING": [
            {
              "count": 1,
              "firstLine": 10898,
              "message": "-------- WWWW ------- G4Exception-START -------- WWWW -------\\n""" \
            + """*** G4Exception : GeomNav1002\\nissued by : G4Navigator::ComputeStep()\\nTrack stuck or not moving.\\n""" \
            + """Track stuck, not moving for 10 steps\\nin volume -LArMgr::LAr::EMEC::Neg::InnerWheel::Lead- at point (-509.71,377.051,-3897.05)\\n""" \
            + """direction: (0.259253,0.546694,-0.796186).\\nPotential geometry or navigation problem !\\n""" \
            + """Trying pushing it of 1e-07 mm ...Potential overlap in geometry!\\n\\n*** This is just a warning message. ***\\n""" \
            + """-------- WWWW -------- G4Exception-END --------- WWWW -------"
            } ]
        }
      },
      "metaData": {},
      "name": "EVNTtoHITS",
      "rc": 0,
      "statusOK": true,
      "validation": true
    }
  ],
  "exitAcronym": "OK",
  "exitCode": 0,
  "exitMsg": "OK",
  "exitMsgExtra": "",
  "files": {
    "input": [
      {
        "dataset": null,
        "nentries": 5000,
        "subFiles": [
          {
            "file_guid": "A7AD81C0-B1B0-D341-A229-D8E7B066EF08",
            "name": "EVNT.08847247._002212.pool.root.1"
          }
        ],
        "type": "EVNT"
      }
    ],
    "output": [
      {
        "argName": "outputHITSFile",
        "dataset": null,
        "subFiles": [
          {
            "file_guid": "9051A804-2965-9346-9ED7-64435E702DFC",
            "file_size": 156541511,
            "name": "HITS.12039077._013137.pool.root.1",
            "nentries": 250
          }
        ],
        "type": "HITS"
      }
    ]
  },
  "name": "Sim_tf",
  "reportVersion": "2.0.7",
  "resource": {
    "dbDataTotal": 4284032,
    "dbTimeTotal": 9.74,
    "executor": {
      "EVNTtoHITS": {
        "cpuTime": 243302,
        "cpuTimePerWorker": 1900.796875,
        "dbData": 4251604,
        "dbTime": 7.29,
        "memory": {
          "Avg": {
            "avgPSS": 6536986,
            "avgRSS": 129252048,
            "avgSwap": 0,
            "avgVMEM": 179846244,
            "rateRBYTES": 165880,
            "rateRCHAR": 450275,
            "rateWBYTES": 70567,
            "rateWCHAR": 83780
          },
          "Max": {
            "maxPSS": 12599089,
            "maxRSS": 252762140,
            "maxSwap": 0,
            "maxVMEM": 349917308,
            "totRBYTES": 676790272,
            "totRCHAR": 1837125486,
            "totWBYTES": 287916032,
            "totWCHAR": 341826635
          }
        },
        "mpworkers": 128,
        "nevents": 250,
        "postExe": {
          "cpuTime": 1036,
          "wallTime": 306
        },
        "preExe": {
          "cpuTime": 0,
          "wallTime": 0
        },
        "total": {
          "cpuTime": 244337,
          "wallTime": 5917
        },
        "validation": {
          "cpuTime": 0,
          "wallTime": 13
        },
        "wallTime": 5598
      },
      "HITSMergeAthenaMP0": {
        "cpuTime": 298,
        "dbData": 32428,
        "dbTime": 2.45,
        "memory": {
          "Avg": {
            "avgPSS": 574755,
            "avgRSS": 722394,
            "avgSwap": 0,
            "avgVMEM": 1366161,
            "rateRBYTES": 13980,
            "rateRCHAR": 1754533,
            "rateWBYTES": 413340,
            "rateWCHAR": 439546
          },
          "Max": {
            "maxPSS": 802907,
            "maxRSS": 1156932,
            "maxSwap": 0,
            "maxVMEM": 2418240,
            "totRBYTES": 4194304,
            "totRCHAR": 526360001,
            "totWBYTES": 124002304,
            "totWCHAR": 131863967
          }
        },
        "postExe": {
          "cpuTime": 1,
          "wallTime": 1
        },
        "preExe": {
          "cpuTime": 0,
          "wallTime": 3
        },
        "total": {
          "cpuTime": 299,
          "wallTime": 305
        },
        "validation": {
          "cpuTime": 0,
          "wallTime": 1
        },
        "wallTime": 301
      }
    },
    "machine": {
      "cpu_family": "6",
      "linux_distribution": [
        "CentOS",
        "6.9",
        "Final"
      ],
      "model": "87",
      "model_name": "Intel(R) Xeon Phi(TM) CPU 7230 @ 1.30GHz",
      "node": "nid00056",
      "platform": "Linux-4.4.49-92.11.1_3.0-cray_ari_c-x86_64-with-centos-6.9-Final"
    },
    "transform": {
      "cpuEfficiency": 0.32,
      "cpuPWEfficiency": 0.4074,
      "cpuTime": 226,
      "cpuTimeTotal": 244636,
      "externalCpuTime": 738,
      "processedEvents": 250,
      "trfPredata": null,
      "wallTime": 5952
    }
  }
}
        """
        report_data = json.loads(report)
        print((json.dumps(parse_jobreport_data(report_data), sort_keys=True, indent=2)))


if __name__ == '__main__':
    unittest.main()
