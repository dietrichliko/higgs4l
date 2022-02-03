#!/usr/bin/env python3

import fnmatch
import os

EOSPATH = "/eos/vbc/experiments/cms"

for name in os.listdir():
    if not fnmatch.fnmatch(name, "*.files.txt"):
        continue
    with open(name, "r") as inp:
        pathnames = inp.readlines()

    for pname in pathnames:
        path = EOSPATH + pname.rstrip()
        if not os.path.exists(path):
            print(path)
