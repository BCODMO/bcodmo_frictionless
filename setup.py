from setuptools import setup, find_packages

required = [
    'datapackage-pipelines',
    'pyparsing',
    'dataflows',
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

