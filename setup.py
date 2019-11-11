from setuptools import setup, find_packages

required = [
    'datapackage-pipelines==2.1.10',
    'pyparsing==2.2.0',
    'dataflows @ git+https://git@github.com/BCODMO/dataflows.git@master',
    'tabulator==1.24.2',
    'pandas',

]


setup(
    name='bcodmo_processors',
    version='v1.0.5',
    description='BCODMO Custom Processors',
    author='BCODMO',
    author_email='conrad.schloer@gmail.com',
    url='https://github.com/bcodmo/bcodmo_processors',
    packages=find_packages(),
    install_requires=required,
    include_package_data=True,
)

