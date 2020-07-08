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
	url="https://gitlab.com/pmitropoulos/geovaex",
	packages=setuptools.find_packages(),
	install_requires=[
		'pygeos>=0.7.1,<0.7.2',
		'pyarrow>=0.17.1,<0.17.2',
		'osgeo>=3.0.4,<3.0.5',
		'numpy>=1.18.4,<1.18.5',
		'vaex>=3.0.0,<3.0.1'
	],
	classifiers=[
		"Programming Language :: Python :: 3",
		"License :: OSI Approved :: MIT License",
		"Operating System :: OS Independent",
	],
	python_requires='>=3.7',
)