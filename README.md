# bcodmo_processors
Custom datapackage-pipelines processors for BCODMO

To run the dpp command locally using the custom processors located in this repository, simply clone this reposistory and add the environment variable DPP_PROCESSOR_PATH.
If this repository is located at $PROCESSOR_REPO, the environment variable will be $PROCESSOR_REPO/bcodmo_processors.

You can add environment variables manually using export DPP_PROCESSOR_PATH=$PUT_PATH_HERE or you can place all of your environment variables in a .env file and run the following commands:
```
set -a
source .env
```

Now when using dpp it will first look inside this repository when resolving processors.


If you want to get rid of the bcodmo_pipeline_processors prefix you can instead set DPP_PROCESSOR_PATH to $PROCESSOR_REPO/bcodmo_processors/bcodmo_pipeline_processors.

## The BCODMO Processor Library

See https://github.com/frictionlessdata/datapackage-pipelines for documentation for standard processors. 

### ***`bcodmo_pipeline_processors.load`***

Loads data into the package. Similar to the standard processor load.

_Parameters_:

- All parameters from the [standard processor load](https://github.com/frictionlessdata/datapackage-pipelines#load)
- `missing_values` - a list of values that are interpretated as missing data (nd) values. Defaults to `['']`
- `input_seperator` - the string used to separate values in the `from` and `name` parameters for loading multiple resources with one processor. Defaults to `','`
- `remove_empty_rows` - a boolean determining if empty rows (rows where all values are the empty string or None) are deleted. Defaults to false
- `sheet_regex` - a boolean determining if the sheet name from an xlsx/xls file should be processed with a regular expression

_Other differences from the standard load_:

- `from` and `name` can be a delimeter seperated list of sources/resource names.
- additional bcodmo-fixedwidth parser that takes in `width` and `infer` parameters
- if `name` is left empty the resource name will default to `res{n}` where n is the number of resources.
- if `sheet_regex` is used `name` will be ignored and the sheet will be the resource name, unless there are multiple `from` values, in which case the name will be `{resource_name}-{sheet_name}`
- `sheet_regex` can only be used with local paths

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

_Other differences from the standard dump_to_path_:

- an attempt is made to change file permissions to 775 
- carriage return \r at line endings are removed

See standard processor for examples.

### ***`bcodmo_pipeline_processors.boolean_add_computed_field`***

Add a field computed using boolean values.

_Parameters_:

- `resources` - a list of resources to perform this operation on
- `fields` - a list of new_fields
  - `functions` - a list of functions for the new field
    - `boolean`- boolean string for this function. See notes for details on boolean string
    - `value` - the value to set if the boolean string evaluates as true
    - `math_operation` - a boolean to determine whether or not the value should be evaluated as a mathematical expression.
  - `target` - the name of the new field to be created
  - `type` - the type of the new field to be created

_Notes_:

- boolean string is made up of an number of conditions and boolean comparisons. Conditions are made up of comparison term, operator, comparision term. A comparison term can be a date, a number, a variable (contained within curly braces {}), a regular expression (contained within re''), a string (contained within single quotes ''), LINE_NUMBER, or null (can be one of None, NONE, null or NULL). An operator can be one of >=, >, <=, <, != or ==. Boolean comparison can be any one of AND, and, &&, OR, or, ||. All terms and operators must be seperated by spaces.
  - For example:
    - {lat} > 50 && {depth} != NULL
    - {species} == 's. pecies' OR {species} == NULL
- functions are evaluated in the order they are passed in. So if function 0 and function 3 both evaluate as true for row 30, the value in function 3 will show up in row 30.
- Regular expression can only be used with the == and != operators and must be compared to a string.
- Use curly braces {} to match field names in the row
- If `math_operation` is set to true, the operators +, -, *, / and ^ can be used to set the value to the result of a mathematical operation. Order of operations are as expected and parentheses can be used to group operations.
- Values will be interpreted based on the type. If a field of type string looks like '5313' it will not equal the number 5313, but rather only the string '5313'.

### ***`bcodmo_pipeline_processors.boolean_filter_rows`***

Filter rows with a boolean statement

_Parameters_:

- `resources` - a list of resources to perform this operation on
- `boolean_statement` - a single boolean statement. Only rows that pass the statement will be kept. `See boolean_add_computed_field` for details on boolean syntax.

_Notes_:


### ***`bcodmo_pipeline_processors.convert_date`***

Convert any number of fields containing date information into a single date field with display format and timezone options.

_Parameters_:

- `resources` - a list of resources to perform this operation on
- `fields` - a list of new_fields
  - `output_field` - the name of the output field
  - `output_format` - the python datetime format string for the output field
  - `input_type` - the input field type. One of 'python' or 'excel'. If 'python', evaluate the input field/fields using python format strings. If 'excel', only take in a single input field and evaluate it as an excel date serial number.
  - `input_field` - a single input field. Only use if `input_type` is 'excel'. Depecrated if `input_type` is 'python'. 
- `boolean_statement` - a single boolean statement. Only rows that pass the statement will be impacted. See `boolean_add_computed_field` for details on boolean syntax.

   ***the rest of the parameters are only relevant if input_type is 'python'***
  - `inputs` - a list of input fields
    - `field` - the input field name
    - `format` - the format of this input field
  - `input_timezone` - the timezone to be passed to the datestring. Required if `input_format` does not have timezone information and timezone is used in the output (either through '%Z' in `output_format` or a value in `output_timezone`), otherwise optional
  - `input_timezone_utc_offset` - UTC offset in seconds. Optional
  - `output_timezone` - the output timezone
  - `output_timezone_utc_offset` - UTC offset for the output timezone. Optional
  - `input_format` - deprecated, for use with `input_field` if `input_type` is 'python'


_Notes_:

- The output type is string until the [date type dump_to_path issue](https://github.com/BCODMO/frictionless-usecases/issues/19) is resolved 
- If the `output_field` already exists in the schema, the existing values will be overwritten

### ***`bcodmo_pipeline_processors.convert_to_decimal_degrees`***

Convert a single field containing coordinate information from degrees-minutes-seconds or degrees-decimal_minutes to decimal_degrees.

_Parameters_:

- `resources` - a list of resources to perform this operation on
- `fields` - a list of new_fields
  - `input_field` - the name of the input field 
  - `output_field` - the name of the output field
  - `input_format` - the input format. One of 'degrees-minutes-seconds' or 'degrees-decimal_minutes'
  - `pattern` - the pattern for the input field. See notes for details
  - `directional` - the directional of the coordinate. Must be one of 'N', 'E', 'S', 'W'
- `boolean_statement` - a single boolean statement. Only rows that pass the statement will be impacted. See `boolean_add_computed_field` for details on boolean syntax.


_Notes_:

- `pattern` is made up of [python regular expression named groups](https://docs.python.org/3/howto/regex.html#non-capturing-and-named-groups). The possible group names are 'directional', 'degrees', 'minutes', 'seconds', and 'decimal_minutes'. If the `input_format` is 'degrees-minutes-seconds' the groups 'degrees', 'minutes' and 'seconds' are required. If the `input_format` is 'degrees-decimal_minutes' the groups 'degrees' and 'decimal_minutes' are required. The 'directional'
  group is always optional.
- if 'directional' is passed in both through the `pattern` and the `directional` parameter, the `directional` parameter takes precendence.

### ***`bcodmo_pipeline_processors.remove_resources`***

Remove any number of resources from the pipeline

_Parameters_:

- `resources` - the resources to remove 


### ***`bcodmo_pipeline_processors.rename_fields`***

Rename any number of fields 

_Parameters_:

- `resources` - a list of resources to perform this operation on
- `fields` - a list of fields
    - `old_field` - the name of the field before it is renamed
    - `new_field` - the new name for the field


_Notes_:

- if `new_field` already exists as a field in the resource an error will be thrown


### ***`bcodmo_pipeline_processors.rename_fields_regex`***

Rename any number of fields using a regular expression

_Parameters_:

- `resources` - a list of resources to perform this operation on
- `fields` - a list of fields to perform this operation on
- `pattern` - regular expression patterns to be usedd
    - `find` - the find pattern for the old field name
    - `replace` - the replace pattern for the new field name 


_Notes_:

- if the field name created by `replace` already exists in the resource an error will be thrown
- regular expressions are always python regular expressions

### ***`bcodmo_pipeline_processors.rename_resource`***

Rename a resource

_Parameters_:

-`old_resource` - the old name of the resource
-`new_resource` - the new name of the resource


### ***`bcodmo_pipeline_processors.reorder_fields`***

Rename any number of fields using a regular expression

_Parameters_:

- `resources` - a list of resources to perform this operation on
- `fields` - the new order of fields 

_Notes_:

- if a field does not exist in the resource an error will be thrown
- if the number of passed in fields does not match the number of fields in the resource an error will be thrown


### ***`bcodmo_pipeline_processors.round_fields`***

Round any number of fields

_Parameters_:

- `resources` - a list of resources to perform this operation on
- `fields` - a list of fields to perform this operation on 
    - `name` - the name of the field to round
    - `digits` - the number of digits to round the field to
    - `preserve_trailing_zeros` - whether trailing zeros should be preserved
    - `maximum_precision` - whether values with precision lower than digits should be rounded
    - `convert_to_integer` - whether the field should be converted to an integer
- `boolean_statement` - a single boolean statement. Only rows that pass the statement will be impacted. See `boolean_add_computed_field` for details on boolean syntax.

_Notes_:

- As of v1.0.3, round_fields works on ONLY number types. 
- if attempted to use on an incorrect field type an error will be thrown
- `convert_to_integer` parameter will only work if digits is set to 0


### ***`bcodmo_pipeline_processors.split_column`***

Split a field into any number of other fields

_Parameters_:

- `resources` - a list of resources to perform this operation on
- `fields` - a list of fields to perform this operation on 
    - `input_field` - the name of the field to split 
    - `output_fields` - the names of the output fields
    - `pattern` - the pattern to match the input_field. Use python regular expression matches (denoted by parentheses) to capture values for `output_fields` 
- `delete_input` - whether the `input_field` should be deleted after the `output_fields` are created
- `boolean_statement` - a single boolean statement. Only rows that pass the statement will be impacted. See `boolean_add_computed_field` for details on boolean syntax.

_Notes_:

- all new fields will be typed as strings 
- the number of matches in `pattern` must equal the number of output fields in `output_fields`

### ***`bcodmo_pipeline_processors.find_replace`***

Find and replace regular expression within a field. Same as standard processor other than boolean.

_Parameters_:

- All parameters from the [standard processor find_replace](https://github.com/frictionlessdata/datapackage-pipelines#find_replace)
- `boolean_statement` - a single boolean statement. Only rows that pass the statement will be impacted. See `boolean_add_computed_field` for details on boolean syntax.

_Notes_:
