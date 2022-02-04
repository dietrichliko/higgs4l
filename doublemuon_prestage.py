#!/usr/bin/env python3
#
# Sample analysis of H-> ZZ* -> 4mu
#
# Based on various examples. Includes prestageing for CLIP
#
#
# Dietrich Liko

import argparse
import concurrent.futures
import getpass
import logging
import os
import subprocess
from typing import Generator, List

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s -  %(message)s",
    datefmt="%y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

MAX_WORKERS = 4
PATH_STAGE = f"/scratch-cbe/users/{getpass.getuser()}"
BASE_URL = "root://eos.grid.vbc.ac.at/"
BACKUP_URL = "root://xrootd-cms.infn.it/"

NAMES = [
    "DoubleMuon2016.files.txt",
    "DoubleMuon2017.files.txt",
    "DoubleMuon2018.files.txt",
]


def all_files(max: int = -1) -> Generator[str, None, None]:
    """Generator for all file names.

    Arguments:
        max: run on max files, negative for all
    """
    cnt = 0
    for name in NAMES:
        with open(name, "r") as inp:
            for line in inp:
                cnt += 1
                if max > 0 and cnt > max:
                    return
                else:
                    yield line[:-1]


def stage_file(lfn: str) -> str:
    """Stage a file from EOS to scratch.

    Arguments:
        lfn: logical file name (/store/... )
    """
    path = PATH_STAGE + lfn

    if not os.path.exists(path):
        url = BASE_URL + lfn
        cmd = ["/usr/bin/xrdcp", "-np", "-s", url, path]
        try:
            log.info("Staging %s", lfn)
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError:
            url = BACKUP_URL + lfn
            cmd = ["/usr/bin/xrdcp", "-np", "-s", url, path]
            log.warning("AAA Staging %s", lfn)
            subprocess.run(cmd, check=True)

    log.debug("Done %s", lfn)

    return path


def stage_all_files(small: bool) -> List[str]:
    """Stage all files by running MAX_WORKERS tasks."""
    if small:
        max = 10
        log.info("Only %d files will be staged", max)
    else:
        max = -1

    all_path = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for path in executor.map(stage_file, all_files(max)):
            all_path.append(path)

    log.info("Number of files: %d", len(all_path))
    return all_path


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Prestage the CMS double muon data")
    parser.add_argument(
        "--small", action="store_true", default=False, help="Run only on 10 files"
    )
    parser.add_argument(
        "--debug", action="store_true", default=False, help="Enable debug output"
    )
    args = parser.parse_args()

    if args.debug:
        log.setLevel(logging.DEBUG)

    log.info("Prestage the CMS double muon data")

    stage_all_files(args.small)
