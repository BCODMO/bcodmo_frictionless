# bcodmo_processors
Custom datapackage-pipelines processors for BCODMO

To run the dpp command locally using the custom processors located in this repository, simply clone this reposistory and add the environment variable DPP_PROCESSOR_PATH.
If this repository is located at $PROCESSOR_REPO, the environment variable will be $GENERATOR_REPO/bcodmo_processors.

You can add environment variables manually using export DPP_PROCESSOR_PATH=$PUT_PATH_HERE or you can place all of your environment variables in a .env file and run the following commands:
```
set -a
source .env
```

Now when using dpp it will first look inside this repository when resolving processors.


If you want to get rid of the bcodmo_pipeline_processors prefix you can instead set DPP_PROCESSOR_PATH to $GENERATOR_REPO/bcodmo_processors/bcodmo_pipeline_processors.

## The BCODMO Processor Library

See https://github.com/frictionlessdata/datapackage-pipelines for documentation for standard processors. 

### ***`bcodmo_pipeline_processors.load`***

Loads data into the package. Similar to the standard processor load.

_Parameters_:

- All parameters from the [standard processor load](https://github.com/frictionlessdata/datapackage-pipelines#load)
- `missing_values` - a list of values that are interpretated as missing data (nd) values. Defaults to `['']`
- `input_seperator` - the string used to separate values in the `from` and `name` parameters for loading multiple resources with one processor. Defaults to `','`
- `remove_empty_rows` - a boolean determining if empty rows (rows where all values are the empty string or None) are deleted. Defaults to false

_Other differences from the standard load_:

- additional bcodmo-fixedwidth parser that takes in `width` and `infer` parameters
- if `name` is left empty the resource name will default to `res{n}` where n is the number of resources.

See standard processor for examples.

