# Simple Higgs Plot

Plots the mass distribution for H->ZZ*->4mu.

## Installation

The program requires at least [python 3.8](http://python.org) and [ROOT 6.24](http://root.cern.ch) 
In addition an installed CMS Computing environment is required, in particular the Grid UI und 
CVMFS (Both available at CLIP).

### Conda Bootstrap environment

As CLIP has no python3 installed, the recommended way is to use a bootstrap [conda](http://anaconda.org) environment. The suggested environment contains some useful tools. In case you prefer, feel free to 
define your own. 

Add to```.bashrc```
```bash
# Conda setup
eval "$('/software/2020/software/anaconda3/2019.10/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
conda activate /groups/hephy/cms/dietrich.liko/conda/envs/my-base
```
and login again.


### Using CMSSW

As a relative modern toolchain is required, currently the release ```CMSSW_12_3_X_2022-02-02-1100```
is suggested. 

Create the environment
```bash
cmsrel CMSSW_12_3_X_2022-02-02-1100
cd CMS_12_3_X/src
cmsenv
```
and checkout the repository
```bash
git clone git@github.com:dietrichliko/higgs4l.git
```

### Installation using Conda and Mamba

An alternative is to setup the full stack in a separate ```conda``` environment. It is 
suggested to move your private conda environments from the home area to the group area 
by creating a  corresponding ```.condarc```. Adapt the directory paths to your needs. 
It is suggested to move your private conda environments from the home area to the group area by creating a 
corresponding ```.condarc```. Adapt the directory paths to your needs. 

```text
auto_activate_base: false
pip_interop_enabled: True
envs_dirs:
  - /groups/hephy/cms/dietrich.liko/conda/envs
pkgs_dirs:
  - /groups/hephy/cms/dietrich.liko/conda/pkgs
```

The bootstrap conda environment contains [mamba](https://github.com/mamba-org/mamba), a faster implementation
of conda install recommended for the use with ```conda-forge```.

Create an environment for the Higgs analysis
```bash
conda activate my-base
mamba create -y -n higgs4l -c conda-forge python=3.8 root pyyaml
conda deactivate
```

Enable the environment enable it with ```conda```
```bash
conda activate higgs4l
```

and checkout the repository
```bash
git clone git@github.com:dietrichliko/higgs4l.git
```
### Running the program

Check the state of the CLIP scratch area. The full doubleMuon datasets requires about 850GB.
```bash
df -h /scratch/cbe
```

Pre-stage the data can be done from the login node
```bash
doublemuon_prestage.py
```

As the analysis program fills all CPU available, it is good practice to allocate a worker node. Please
keep in mind that allocating a full node is relatively expensive and keep its usage to the required
minimum.
```
srun --time=00:30:00 --nodes=1 --pty /bin/bash
./doublemuon.py 
```
