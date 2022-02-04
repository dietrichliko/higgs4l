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
import time
from typing import Any, Dict, Generator, List, Tuple

import ROOT  # type: ignore
import yaml

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
        print(f"Stageing ... {lfn}")
        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError:
            url = BACKUP_URL + lfn
            cmd = ["/usr/bin/xrdcp", "-np", "-s", url, path]
            print(f"AAA Stageing ... {lfn}")
            subprocess.run(cmd, check=True)

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
        for path in executor.map(stage_file, all_files(small)):
            all_path.append(path)

    return all_path


def higgs4mu(df) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Analysis H->ZZ*->4mu."""
    counters: Dict[str, Any] = {}

    log.info("Setting up the 4 muon analysis")
    # Good Muons

    df = (
        df.Define(
            "GoodMuon",
            "Muon_pt > 10. && Muon_mediumPromptId && Muon_pfRelIso03_all < 0.3",
        )
        .Define("nGoodMuon", "std::count(GoodMuon.begin(), GoodMuon.end(), true)")
        .Define("GoodMuon_pt", "Muon_pt[GoodMuon]")
        .Define("GoodMuon_eta", "Muon_eta[GoodMuon]")
        .Define("GoodMuon_phi", "Muon_phi[GoodMuon]")
        .Define("GoodMuon_mass", "Muon_mass[GoodMuon]")
        .Define("GoodMuon_pdgId", "Muon_pdgId[GoodMuon]")
        .Define("GoodMuon_charge", "Muon_charge[GoodMuon]")
        .Define(
            "nGoodMuon_pos",
            "std::count(GoodMuon_pdgId.begin(), GoodMuon_pdgId.end(), 13)",
        )
        .Define(
            "nGoodMuon_neg",
            "std::count(GoodMuon_pdgId.begin(), GoodMuon_pdgId.end(), -13)",
        )
    )

    # 4 Muon Events
    df_4mu = (
        df.Filter(
            "nGoodMuon_pos == 2 && nGoodMuon_neg == 2", "Events with 2 lepton pairs"
        )
        .Define(
            "zz_idx",
            "FindZZ(GoodMuon_pt,GoodMuon_eta,GoodMuon_phi,GoodMuon_mass,GoodMuon_charge)",
        )
        .Define(
            "z_mass",
            "ZZInvMass(zz_idx,GoodMuon_pt,GoodMuon_eta,GoodMuon_phi,GoodMuon_mass)",
        )
        .Define("z1_mass", "z_mass[0]")
        .Define("z2_mass", "z_mass[1]")
        .Define(
            "higgs_mass",
            "HiggsInvMass(GoodMuon_pt,GoodMuon_eta,GoodMuon_phi,GoodMuon_mass)",
        )
    )

    df_4mu_sel = df_4mu.Filter(
        "z_mass[0] > 40. && z_mass[0] < 120.", "First Z between 40 and 120 GeV"
    ).Filter("z_mass[1] > 12 && z_mass[1] < 200.", "Second Z between 12 and 200 GeV")

    return {
        "df": df,
        "df_4mu": df_4mu,
        "df_4mu_sel": df_4mu_sel,
    }, counters


def book_histos(dfs: Dict[str, Any], histos_file: str) -> List[Any]:
    """Book histograms for the dataframes."""
    log.info("Booking histograms")
    with open(histos_file, "r") as inp:
        defs = yaml.safe_load(inp)

    histos = []
    for item in defs:
        df = item["DataFrame"]
        for h1d in item["Histo1D"]:
            name = h1d["name"]
            title = h1d.get("title", name)
            bins = h1d.get("bins")
            var = h1d.get("var", name)
            log.debug(f"Booking %s for %s", name, df)
            histos.append(dfs[df].Histo1D((name, title, *bins), var))

    log.info("Number of histograms: %d", len(histos))
    return histos


if __name__ == "__main__":

    # init ROOT
    ROOT.gROOT.SetBatch(True)
    ROOT.PyConfig.IgnoreCommandLineOptions = True

    parser = argparse.ArgumentParser(
        description="Sample analysis of H-> ZZ* -> 4mu using Dataframes"
    )
    parser.add_argument(
        "--small", action="store_true", default=False, help="Run only on 10 files"
    )
    parser.add_argument(
        "--debug", action="store_true", default=False, help="Enable verbose logging"
    )
    parser.add_argument(
        "--threads", type=int, default=0, help="Number of threads (default: 0)"
    )
    parser.add_argument(
        "-o",
        "--output",
        default="doublemuon.root",
        help="Histogram output (default: doublemuon.root)",
    )
    parser.add_argument(
        "--histos",
        default="doublemuon.histos.yaml",
        help="Histogram definition (default doublemuon.histos.yaml)",
    )
    args = parser.parse_args()

    if args.debug:
        log.setLevel(logging.DEBUG)

    log.info("Sample analysis of H-> ZZ* -> 4mu using Dataframes")

    # enable ROOT multithreading
    if args.threads >= 0:
        ROOT.EnableImplicitMT(args.threads)
    log.info("ROOT Threadpool size is %d", ROOT.GetThreadPoolSize())

    # Load C++
    script_dir = os.path.dirname(os.path.realpath(__file__))
    ROOT.gInterpreter.Declare(f'#include "{script_dir}/doublemuon_inc.h"')

    # stage all files and create chain
    paths = stage_all_files(args.small)
    chain = ROOT.TChain("Events")
    for path in paths:
        chain.Add(path)

    # Dataframe
    df = ROOT.RDataFrame(chain)

    events = df.Count()

    dfs, counters_4mu = higgs4mu(df)

    histos = book_histos(dfs, args.histos)

    log.info("Starting analysis")
    time_start = time.time()
    ROOT.RDF.RunGraphs(list(counters_4mu.values()) + histos)

    df.Report().Print()

    for name, counter in counters_4mu.items():
        print(f"{name}: {counter.GetValue()}")

    time_total = time.time() - time_start

    events_total = events.GetValue()

    rate = events_total / time_total

    log.info(
        "Events: %d, Time: %d secs, Rate %.2f MHz", events_total, time_total, rate / 1e6
    )

    log.info("Writeing 5s", args.output)
    out = ROOT.TFile(args.output, "RECREATE")
    for hist in histos:
        hist.Write()
    out.Close()
