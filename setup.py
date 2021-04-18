from setuptools import setup, find_packages

required = [
    "pandas==0.25.3",
    "datapackage-pipelines==2.2.6",
    # "datapackage-pipelines @ git+https://github.com/frictionlessdata/datapackage-pipelines.git@d78d1391adf6470ca484303e512e038f7dc57483",
    "pyparsing==2.2.0",
    "dataflows==0.2.11",
    "tabulator==1.53.5",
    # "tabulator @ git+https://github.com/BCODMO/tabulator-py.git@master",
    "tableschema==1.16.4",
    "goodtables==2.5.0",
    "python-dateutil==2.8.0",
]


setup(
    name="bcodmo_frictionless",
    version="v2.10.2",
    description="BCODMO Custom Processors and Checks",
    author="BCODMO",
    author_email="conrad.schloer@gmail.com",
    url="https://github.com/bcodmo/bcodmo_frictionless",
    packages=find_packages(),
    install_requires=required,
    include_package_data=True,
)
