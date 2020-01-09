from setuptools import setup, find_packages

required = [
    'datapackage-pipelines==2.1.10',
    'pyparsing==2.2.0',
    'dataflows==0.0.65',
    'tabulator @ git+https://git@github.com/BCODMO/tabulator-py.git@lw-399-generalized-number-format',
    'pandas',

]


setup(
    name='bcodmo_processors',
    version='v0.0.1',
    description='BCODMO Custom Processors',
    author='BCODMO',
    author_email='conrad.schloer@gmail.com',
    url='https://github.com/bcodmo/bcodmo_processors',
    packages=find_packages(),
    install_requires=required,
    include_package_data=True,
)

