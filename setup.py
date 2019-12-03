from setuptools import setup, find_packages

required = [
    'datapackage-pipelines==2.1.10',
    'pyparsing==2.2.0',
    'dataflows @ git+https://git@github.com/datahq/dataflows.git@efcfc0dd438125d1e75ae86a683685dad2e474d5',
    'tabulator==1.31.0',
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

