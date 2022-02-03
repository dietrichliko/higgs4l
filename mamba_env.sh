#!/bin/sh

mamba create -y -n higgs4l -c conda-forge python=3.8 root pyyaml black isort mypy flake8 flake8-bugbear
