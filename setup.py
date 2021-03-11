import setuptools
from importlib.machinery import SourceFileLoader
import os

with open("README.md", "r") as fh:
    long_description = fh.read()

dirname = os.path.dirname(__file__)
path_version = os.path.join(dirname, "geovaex/_version.py")
version = SourceFileLoader('version', path_version).load_module()

setuptools.setup(
    name="geovaex",
    version=version.__version__,
    author="Pantelis Mitropoulos",
    author_email="pmitropoulos@getmap.gr",
    description="Geospatial extension for vaex",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/OpertusMundi/geovaex.git",
    packages=setuptools.find_packages(),
    install_requires=[
        'pygeos>=0.8.0,<1.0.0',
        'pyarrow>=3.0.0,<3.0.1',
        'gdal>=3.0.2,<3.2.0',
        'numpy>=1.18.4,<1.18.5',
        'vaex>=3.0.0,<3.0.1',
        'pyproj>=2.6.0,<2.7.0'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)