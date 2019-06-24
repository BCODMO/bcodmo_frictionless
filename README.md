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

### ***`bcodmo_pipeline_processors.concatenate`***

Concatenate a number of streamed resouces into a single resource. Similar to the standard processor concatenate.

_Parameters_:

- All parameters from the [standard processor concatenate](https://github.com/frictionlessdata/datapackage-pipelines#concatenate)
- `include_source_name` - whether or not a source name should be included as a field in the resulting resource. Can be one of False, 'resource', 'path' or 'file'.
    - 'resource' will add the resource name as a field
    - 'path' will add the path or url that was originally used to load the resource
    - 'file' will add the filename from the original resource
- `source_field_name` - the name of the new field created by include_source_name. Required if include_source_name is not False

See standard processor for examples.

### ***`bcodmo_pipeline_processors.dump_to_path`***

Dump data to a path. Similar to the standard processor dump_to_path.

_Parameters_:

- All parameters from the [standard processor dump_to_path](https://github.com/frictionlessdata/datapackage-pipelines#dump_to_path)
- `save_pipeline_spec` - whether or not the pipeline_spec.yaml file should also be saved in dump_to_path. Note that the entire pipeline's pipeline-spec.yaml file will be saved, regardless of where in the pipeline the dump_to_path processor lives.

_Other differences from the standard load_:

- an attempt is made to change file permissions to 775 
- carriage return \r at line endings are removed

See standard processor for examples.

### ***`bcodmo_pipeline_processors.boolean_add_computed_field`***

Add a field computed using boolean values.

_Parameters_:

- `fields` - a list of new_fields
  - `functions` - a list of functions for the new field
    - `boolean`- boolean string for this function. See notes for details on boolean string
    - `value` - the value to set if the boolean string evaluates as true
  - `target` - the name of the new field to be created

_Notes_:

- boolean string is made up of an number of conditions and boolean comparisons. Conditions are made up of comparison term, operator, comparision term. A comparison term can be a date, a number, a variable (contained within curly braces {}), a string (contained within single quotes '') or null (can be one of None, NONE, null or NULL). An operator can be one of >=, >, <=, <, != or ==. Boolean comparison can be any one of AND, and, &&, OR, or, ||.
  - For example:
    - {lat} > 50 && {depth} != NULL
    - {species} == 's. pecies' OR {species} == NULL
- functions are evaluated in the order they are passed in. So if function 0 and function 3 both evaluate as true for row 30, the value in function 3 will show up in row 30.

### ***`bcodmo_pipeline_processors.convert_date`***

Convert any number of fields containing date information into a single date field with display format and timezone options.

_Parameters_:

- `fields` - a list of new_fields
  - `output_field` - the name of the output field
  - `output_format` - the python datetime format string for the output field
  - `input_type` - the input field type. One of 'python' or 'excel'. If 'python', evaluate the input field/fields using python format strings. If 'excel', only take in a single input field and evaluate it as an excel date serial number.
  - `input_field` - a single input field. Only use if `input_type` is 'excel'. Depecrated if `input_type` is 'python'. 

   ***`the rest of the parameters are only relevant if input_type is 'python'`***
  - `inputs` - a list of input fields
    - `field` - the input field name
    - `format` - the format of this input field
  - `input_timezone` - the timezone to be passed to the datestring. Required if `input_format` does not have timezone information and timezone is used in the output (either through '%Z' in `output_format` or a value in `output_timezone`), otherwise optional
  - `input_timezone_utc_offset` - UTC offset in seconds. Optional
  - `output_timezone` - the output timezone
  - `output_timezone_utc_offset` - UTC offset for the output timezone. Optional
  - `input_format` - deprecated, for use with `input_field` if `input_type` is 'python'


_Notes_:

- 

