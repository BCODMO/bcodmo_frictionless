from setuptools import setup, find_packages

required = [
    "pandas==0.25.3",
    "datapackage-pipelines==2.1.10",
    "pyparsing==2.2.0",
    "dataflows==0.0.71",
    "tabulator==1.46.0",
    "tableschema==1.15.0",
]


setup(
    name="bcodmo_processors",
    version="v2.0.0",
    description="BCODMO Custom Processors",
    author="BCODMO",
    author_email="conrad.schloer@gmail.com",
    url="https://github.com/bcodmo/bcodmo_processors",
    packages=find_packages(),
    install_requires=required,
    include_package_data=True,
)
