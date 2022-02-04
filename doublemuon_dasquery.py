#!/usr/bin/env python3
#
# Retrieve file info from CMS DAS
#
# Dietrich Liko

import subprocess
import sys
import yaml
import json

with open("doublemuon.yaml", "r") as inp:
    data = yaml.safe_load(inp)

for name, files in data.items():
    with open(f"{name}.files.txt", "w") as txt:
        for dasname in files:
            cmd = [
                "/cvmfs/cms.cern.ch/common/dasgoclient",
                "--json",
                f"--query=file dataset={dasname}",
            ]
            stdout = subprocess.check_output(cmd)  # noqa:S603
            cnt = 0
            for item in json.loads(stdout):
                for file_item in item["file"]:
                    print(file_item["name"], file=txt)
                    cnt += 1
            print(f"{cnt} files for {dasname}")
