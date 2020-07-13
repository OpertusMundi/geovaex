import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="geovaex",
    version="0.0.1",
    author="Pantelis Mitropoulos",
    author_email="pmitropoulos@getmap.gr",
    description="Geospatial extension for vaex",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/OpertusMundi/geovaex.git",
    packages=setuptools.find_packages(),
    install_requires=[
        'pygeos>=0.7.1,<0.7.2',
        'pyarrow>=0.17.1,<0.17.2',
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