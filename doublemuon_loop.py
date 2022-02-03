#!/usr/bin/env python3

import concurrent.futures
import getpass
import os
import subprocess
import time
from typing import Any, Dict, Generator, List, Tuple

import ROOT  # type: ignore
import yaml

MAX_WORKERS = 4
PATH_STAGE = f"/scratch-cbe/users/{getpass.getuser()}"
BASE_URL = "root://eos.grid.vbc.ac.at/"
BACKUP_URL = "root://xrootd-cms.infn.it/"

NAMES = [
    "DoubleMuon2016.files.txt",
    "DoubleMuon2017.files.txt",
    "DoubleMuon2018.files.txt",
]


def all_files() -> Generator[str, None, None]:

    for name in NAMES:
        with open(name, "r") as inp:
            for line in inp:
                yield line[:-1]


def stage_file(lfn: str) -> str:

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


def stage_all_files() -> List[str]:

    all_path = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for path in executor.map(stage_file, all_files()):
            all_path.append(path)

    return all_path


def higgs4mu(df) -> Tuple[Dict[str, Any], Dict[str, Any]]:

    counters = {}

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
        .Define("GoodMuon_pdgId", "Muon_pdgId[GoodMuon]")
        .Define(
            "nGoodMuon_pos",
            "std::count(GoodMuon_pdgId.begin(), GoodMuon_pdgId.end(), 13)",
        )
        .Define(
            "nGoodMuon_neg",
            "std::count(GoodMuon_pdgId.begin(), GoodMuon_pdgId.end(), -13)",
        )
    )

    df_4mu = df.Filter("nGoodMuon == 4")
    counters["events_4l"] = df_4mu.Count()
    df_4mu_ok = df_4mu.Filter("nGoodMuon_pos == 2 && nGoodMuon_pos == 2")
    counters["events_4l_sign"] = df_4mu_ok.Count()

    return {
        "df": df,
        "df_4mu": df_4mu,
        "df_4mu_ok": df_4mu_ok,
    }, counters


def book_histos(dfs: Dict[str, Any], histos_file: str) -> Dict[str, Any]:

    with open(histos_file, "r") as inp:
        defs = yaml.safe_load(inp)

    histos = {}
    for item in defs:
        df = item["DataFrame"]
        for h1d in item["Histo1D"]:
            name = h1d["name"]
            title = h1d.get("title", name)
            bins = h1d.get("bins")
            var = h1d.get("var", name)
            histos[name] = dfs[df].Histo1D((name, title, *bins), var)

    return histos


if __name__ == "__main__":

    ROOT.gROOT.SetBatch(True)
    ROOT.EnableImplicitMT()
    ROOT.PyConfig.IgnoreCommandLineOptions = True

    paths = stage_all_files()

    chain = ROOT.TChain("Events")
    for path in paths:
        chain.Add(path)

    time_start = time.time()

    h_nMuon = ROOT.TH1F("nMuon", "# of Muons", 21, -0.5, 20.5)
    events_total = 0
    for event in chain:
        events_total += 1
        h_nMuon.Fill(event.nMuon)

    time_total = time.time() - time_start

    rate = events_total / time_total

    print(f"Events: {events_total} ({rate/1E6:.2f}MHz)")
