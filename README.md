# geovaex
## What is geovaex
geovaex is an extension of vaex to analyze spatial data.

## Installing
Using virtual environment:
```
$ conda create -n your-environment-name ipykernel
```
```
$ conda activate your-environment-name
```
First, install GDAL:
```
$ conda install -c conda-forge gdal=3.1.2
```
and finally install geovaex:
```
$ pip install git+https://github.com/OpertusMundi/geovaex.git
```
You can add the created environment in jupyter notebook running:
```
$ python -m ipykernel install --user --name=your-environment-name
```
## Usage
See the included notebook.
