from setuptools import setup, find_packages

required = [
    "pandas==0.25.3",
    # "datapackage-pipelines==v2.1.10",
    "datapackage-pipelines @ git+https://github.com/frictionlessdata/datapackage-pipelines.git@d78d1391adf6470ca484303e512e038f7dc57483",
    "pyparsing==2.2.0",
    "dataflows==0.1.1",
    "tabulator==1.52.3",
    "tableschema==1.15.0",
]


setup(
    name="bcodmo_processors",
    version="v2.0.4",
    description="BCODMO Custom Processors",
    author="BCODMO",
    author_email="conrad.schloer@gmail.com",
    url="https://github.com/bcodmo/bcodmo_processors",
    packages=find_packages(),
    install_requires=required,
    include_package_data=True,
)
