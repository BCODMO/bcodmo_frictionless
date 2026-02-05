# bcodmo_frictionless

Custom dataflows processors and goodtables checks for BCO-DMO.

To run the dpp command locally using the custom processors located in this repository, clone this repository and set the environment variable `DPP_PROCESSOR_PATH` to `$REPO_PATH/bcodmo_frictionless/bcodmo_pipeline_processors`.

```bash
export DPP_PROCESSOR_PATH=/path/to/bcodmo_frictionless/bcodmo_frictionless/bcodmo_pipeline_processors
```

## The BCO-DMO Processor Library

See https://github.com/frictionlessdata/datapackage-pipelines for documentation on standard processors.

---

## Loading and Output Processors

### **`bcodmo_pipeline_processors.load`**

Loads data into the package.

**Parameters:**

- `from` - source URL(s) or path(s), can be comma-separated for multiple sources
- `name` - resource name(s), required (comma-separated if multiple sources)
- `use_filename` - use the filename as the resource name instead of `name`
- `input_separator` - separator for `from` and `name` parameters (default: `,`)
- `input_path_pattern` - treat `from` as a glob pattern to match multiple files
- `remove_empty_rows` - remove rows where all values are empty (default: `true`)
- `missing_values` - list of values to interpret as missing data (default: `['']`)
- `sheet` - sheet name/number for Excel files
- `sheet_regex` - treat `sheet` as a regex pattern to match multiple sheets
- `sheet_separator` - separator for multiple sheet names in `sheet`
- `format` - file format (supports `bcodmo-fixedwidth`, `bcodmo-regex-csv`)
- `recursion_limit` - override Python's recursion limit

**Fixed-width format parameters** (when `format` is `bcodmo-fixedwidth`):

- `width` - column width
- `infer` - infer width automatically
- `parse_seabird_header` - parse a `.cnv` seabird file header
- `seabird_capture_skipped_rows` - list of `{column_name, regex}` to capture data from skipped rows
- `seabird_capture_skipped_rows_join` - join multiple matches (default: `true`)
- `seabird_capture_skipped_rows_join_string` - join string (default: `;`)
- `fixedwidth_sample_size` - rows to sample for width inference

---

### **`bcodmo_pipeline_processors.concatenate`**

Concatenates multiple resources into a single resource.

**Parameters:**

- `sources` - list of resource names to concatenate
- `target` - target resource configuration
  - `name` - name of the concatenated resource (default: `concat`)
  - `path` - output path
- `fields` - mapping of target field names to source field names
- `include_source_names` - list of source identifiers to add as columns
  - `type` - one of `resource`, `path`, or `file`
  - `column_name` - name of the new column
- `missing_values` - list of missing value indicators

---

### **`bcodmo_pipeline_processors.dump_to_path`**

Dumps data to a local filesystem path.

**Parameters:**

- `out-path` - output directory (default: `.`)
- `save_pipeline_spec` - save the pipeline-spec.yaml file
- `pipeline_spec` - pipeline spec content to save
- `data_manager` - object with `name` and `orcid` keys for the data manager

**Notes:**

- Attempts to set file permissions to 775
- Removes carriage return (`\r`) from line endings

---

### **`bcodmo_pipeline_processors.dump_to_s3`**

Dumps data to an S3-compatible storage.

**Parameters:**

- `bucket_name` - S3 bucket name
- `prefix` - path prefix within the bucket
- `format` - output format (default: `csv`)
- `save_pipeline_spec` - save the pipeline-spec.yaml file
- `pipeline_spec` - pipeline spec content to save
- `data_manager` - object with `name` and `orcid` keys
- `use_titles` - use field titles instead of names in output
- `temporal_format_property` - property name to use for temporal field formats
- `delete` - delete existing files at prefix before dumping
- `limit_yield` - limit number of rows yielded downstream
- `dump_unique_lat_lon` - create a separate file with unique lat/lon pairs

**Environment variables:**

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `LAMINAR_S3_HOST` - S3 endpoint URL

---

## Field Manipulation Processors

### **`bcodmo_pipeline_processors.rename_fields`**

Renames fields.

**Parameters:**

- `resources` - list of resources to operate on
- `fields` - list of field mappings
  - `old_field` - current field name
  - `new_field` - new field name

---

### **`bcodmo_pipeline_processors.rename_fields_regex`**

Renames fields using regular expressions.

**Parameters:**

- `resources` - list of resources to operate on
- `fields` - list of field names to rename
- `pattern` - regex pattern
  - `find` - regex pattern to match
  - `replace` - replacement pattern

---

### **`bcodmo_pipeline_processors.reorder_fields`**

Reorders fields in a resource.

**Parameters:**

- `resources` - list of resources to operate on
- `fields` - list of field names in the desired order (must include all fields)

---

### **`bcodmo_pipeline_processors.update_fields`**

Updates field metadata in the schema.

**Parameters:**

- `resources` - list of resources to operate on
- `fields` - object mapping field names to metadata properties to update

---

### **`bcodmo_pipeline_processors.set_types`**

Sets field types and options.

**Parameters:**

- `resources` - list of resources to operate on
- `types` - object mapping field names to type options
- `regex` - treat field names as regex patterns (default: `true`)

---

### **`bcodmo_pipeline_processors.add_schema_metadata`**

Adds metadata to the resource schema.

**Parameters:**

- `resources` - list of resources to operate on
- Any additional parameters are added as schema metadata

---

## Data Transformation Processors

### **`bcodmo_pipeline_processors.boolean_add_computed_field`**

Adds computed fields using boolean conditions.

**Parameters:**

- `resources` - list of resources to operate on
- `fields` - list of new fields to create
  - `target` - name of the new field
  - `type` - data type of the new field
  - `functions` - list of conditions and values
    - `boolean` - boolean expression (see Boolean Syntax below)
    - `value` - value to set when condition is true (supports `{field_name}` substitution)
    - `math_operation` - if `true`, evaluate `value` as a math expression
    - `always_run` - if `true`, skip the boolean check

**Notes:**

- Functions are evaluated in order; later matches override earlier ones
- Supports datetime, date, and time output types

---

### **`bcodmo_pipeline_processors.boolean_filter_rows`**

Filters rows based on a boolean condition.

**Parameters:**

- `resources` - list of resources to operate on
- `boolean_statement` - boolean expression; only rows that pass are kept

---

### **`bcodmo_pipeline_processors.find_replace`**

Finds and replaces text using regular expressions.

**Parameters:**

- `resources` - list of resources to operate on
- `fields` - list of fields to process
  - `name` - field name
  - `patterns` - list of find/replace patterns
    - `find` - regex pattern to find
    - `replace` - replacement string
    - `replace_function` - one of `string`, `uppercase`, `lowercase` (default: `string`)
    - `replace_missing_values` - apply to missing values (default: `false`)
- `boolean_statement` - optional condition for which rows to process

---

### **`bcodmo_pipeline_processors.string_format`**

Formats strings using Python string formatting.

**Parameters:**

- `resources` - list of resources to operate on
- `fields` - list of format operations
  - `output_field` - name of the output field
  - `input_string` - Python format string (e.g., `{0:03d}-{1}`)
  - `input_fields` - list of field names to use as format arguments
- `boolean_statement` - optional condition for which rows to process

**Notes:**

- Field types matter: use `{0:03d}` for integers, `{0:03f}` for floats

---

### **`bcodmo_pipeline_processors.split_column`**

Splits a field into multiple fields.

**Parameters:**

- `resources` - list of resources to operate on
- `fields` - list of split operations
  - `input_field` - field to split
  - `output_fields` - list of output field names
  - `pattern` - regex with capture groups (mutually exclusive with `delimiter`)
  - `delimiter` - regex delimiter to split on (mutually exclusive with `pattern`)
  - `preserve_metadata` - copy bcodmo metadata from input field
- `delete_input` - delete the input field after splitting
- `boolean_statement` - optional condition for which rows to process

---

### **`bcodmo_pipeline_processors.edit_cells`**

Edits specific cells by row number.

**Parameters:**

- `resources` - list of resources to operate on
- `edited` - object mapping row numbers to edits
  - Each value is a list of `{field, value}` objects

---

### **`bcodmo_pipeline_processors.extract_nonnumeric`**

Extracts non-numeric values from fields into new fields.

**Parameters:**

- `resources` - list of resources to operate on
- `fields` - list of field names to process
- `suffix` - suffix for new field names (default: `_`)
- `preserve_metadata` - copy bcodmo metadata to new fields
- `boolean_statement` - optional condition for which rows to process

**Notes:**

- Non-numeric values are moved to the new field, and the original is set to null

---

### **`bcodmo_pipeline_processors.round_fields`**

Rounds numeric fields.

**Parameters:**

- `resources` - list of resources to operate on
- `fields` - list of fields to round
  - `name` - field name
  - `digits` - decimal places
  - `preserve_trailing_zeros` - keep trailing zeros
  - `maximum_precision` - only round values with precision >= digits
  - `convert_to_integer` - convert to integer (only when `digits` is 0)
- `boolean_statement` - optional condition for which rows to process

---

### **`bcodmo_pipeline_processors.convert_units`**

Converts values between units.

**Parameters:**

- `resources` - list of resources to operate on
- `fields` - list of conversions
  - `name` - field name
  - `conversion` - conversion function: `feet_to_meter`, `fathom_to_meter`, `inch_to_cm`, `mile_to_km`
  - `preserve_field` - keep the original field
  - `new_field_name` - name for the converted field (required if `preserve_field` is true)
  - `preserve_metadata` - copy bcodmo metadata to new field

---

## Date and Coordinate Processors

### **`bcodmo_pipeline_processors.convert_date`**

Converts date/time fields between formats.

**Parameters:**

- `resources` - list of resources to operate on
- `fields` - list of conversions
  - `output_field` - name of the output field
  - `output_format` - Python datetime format string
  - `output_type` - one of `datetime`, `date`, `time`, `string` (default: `datetime`)
  - `input_type` - one of `python`, `excel`, `matlab`, `decimalDay`, `decimalYear`
  - `preserve_metadata` - copy bcodmo metadata from input field

  **For `python` input_type:**
  - `inputs` - list of input fields
    - `field` - field name
    - `format` - Python datetime format
  - `input_timezone` - input timezone
  - `input_timezone_utc_offset` - UTC offset in hours
  - `output_timezone` - output timezone
  - `output_timezone_utc_offset` - UTC offset in hours
  - `year` - override year value

  **For `excel`/`matlab` input_type:**
  - `input_field` - single input field

  **For `decimalDay` input_type:**
  - `input_field` - single input field
  - `year` - year value (required)

  **For `decimalYear` input_type:**
  - `input_field` - single input field
  - `decimal_year_start_day` - start day (0 or 1, required)

- `boolean_statement` - optional condition for which rows to process

---

### **`bcodmo_pipeline_processors.convert_to_decimal_degrees`**

Converts coordinates to decimal degrees.

**Parameters:**

- `resources` - list of resources to operate on
- `fields` - list of conversions
  - `input_field` - input field name
  - `output_field` - output field name
  - `format` - one of `degrees-minutes-seconds` or `degrees-decimal_minutes`
  - `pattern` - regex with named groups: `degrees`, `minutes`, `seconds`, `decimal_minutes`, `directional`
  - `directional` - compass direction (`N`, `E`, `S`, `W`) if not in pattern
  - `handle_out_of_bounds` - handle values outside normal ranges
  - `preserve_metadata` - copy bcodmo metadata from input field
- `boolean_statement` - optional condition for which rows to process

---

## Resource Management Processors

### **`bcodmo_pipeline_processors.remove_resources`**

Removes resources from the pipeline.

**Parameters:**

- `resources` - list of resource names to remove

---

### **`bcodmo_pipeline_processors.rename_resource`**

Renames a resource.

**Parameters:**

- `old_resource` - current resource name
- `new_resource` - new resource name

---

### **`bcodmo_pipeline_processors.join`**

Joins two resources together.

**Parameters:**

- `source` - source resource configuration
  - `name` - source resource name
  - `key` - join key field(s) or key template
  - `delete` - delete source after join (default: `false`)
- `target` - target resource configuration
  - `name` - target resource name
  - `key` - join key field(s) or key template
- `fields` - object mapping target field names to source field specs
  - `name` - source field name
  - `aggregate` - aggregation function: `sum`, `avg`, `median`, `max`, `min`, `first`, `last`, `count`, `any`, `set`, `array`, `counters`
- `mode` - join mode: `inner`, `half-outer`, `full-outer` (default: `half-outer`)

---

## Standard Dataflows Processors

These processors are from the standard [dataflows](https://github.com/datahq/dataflows) library.

### **`duplicate`**

Duplicates a resource within the package.

**Parameters:**

- `source` - name of the resource to duplicate (default: first resource)
- `target-name` - name for the duplicated resource (default: `{source}_copy`)
- `target-path` - path for the duplicated resource (default: `{target-name}.csv`)
- `duplicate_to_end` - place duplicate at end of package instead of after source

---

### **`update_package`**

Updates package-level metadata.

**Parameters:**

- Any key-value pairs to add/update in the package descriptor (except `resources`)

---

### **`update_resource`**

Updates resource-level metadata.

**Parameters:**

- `resources` - list of resources to operate on
- `metadata` - object of key-value pairs to add/update in the resource descriptor

---

### **`delete_fields`**

Removes fields from resources.

**Parameters:**

- `resources` - list of resources to operate on
- `fields` - list of field names to delete
- `regex` - treat field names as regex patterns (default: `true`)

---

### **`sort`**

Sorts rows by field values.

**Parameters:**

- `resources` - list of resources to operate on
- `sort-by` - field name, format string (e.g., `{field1}{field2}`), or callable
- `reverse` - sort in descending order (default: `false`)

**Notes:**

- Numeric fields are sorted numerically
- Supports multi-field sorting via format strings

---

### **`add_computed_field`**

Adds computed fields using predefined operations.

**Parameters:**

- `resources` - list of resources to operate on
- `fields` - list of field definitions
  - `target` - name of the new field (or object with `name` and `type`)
  - `operation` - one of the operations below
  - `source` - list of source field names (for most operations)
  - `with` - additional parameter for some operations

**Operations:**

- `sum` - sum of source field values
- `avg` - average of source field values
- `max` - maximum of source field values
- `min` - minimum of source field values
- `multiply` - product of source field values
- `constant` - constant value (specified in `with`)
- `join` - join source values with separator (specified in `with`)
- `format` - format string using row values (specified in `with`, e.g., `{field1}-{field2}`)

---

### **`unpivot`**

Transforms columns into rows (wide to long format).

**Parameters:**

- `resources` - list of resources to operate on
- `unpivot` - list of field specifications to unpivot
  - `name` - field name or regex pattern
  - `keys` - object mapping extra key field names to values (can use regex backreferences)
- `extraKeyFields` - list of new key field definitions (with `name` and `type`)
- `extraValueField` - definition for the value field (with `name` and `type`)
- `regex` - treat field names as regex patterns (default: `true`)

**Example:**

```yaml
unpivot:
  - name: "value_\\d+"
    keys:
      year: "\\1"
extraKeyFields:
  - name: year
    type: integer
extraValueField:
  name: value
  type: number
```

---

### **`join`**

Joins two resources together (standard dataflows version).

**Parameters:**

- `source` - source resource configuration
  - `name` - source resource name
  - `key` - join key field(s) or key template
  - `delete` - delete source after join (default: `true`)
- `target` - target resource configuration
  - `name` - target resource name
  - `key` - join key field(s) or key template
- `fields` - object mapping target field names to source field specs
  - `name` - source field name
  - `aggregate` - aggregation function: `sum`, `avg`, `median`, `max`, `min`, `first`, `last`, `count`, `any`, `set`, `array`, `counters`
- `mode` - join mode: `inner`, `half-outer`, `full-outer` (default: `half-outer`)

**Notes:**

- Source resource must appear before target in the package
- Use `*` in fields to include all source fields

---

## Boolean Syntax

Many processors support a `boolean_statement` parameter for conditional processing.

**Comparison terms:**

- Numbers: `50`, `3.14`, `-10`
- Strings: `'value'` (single quotes)
- Field references: `{field_name}` (curly braces)
- Dates: `2023-01-15`, `2023/01/15`
- Regular expressions: `re'pattern'`
- Null values: `null`, `NULL`, `None`, `NONE`
- Row number: `LINE_NUMBER`, `ROW_NUMBER`

**Operators:**

- Comparison: `==`, `!=`, `>`, `>=`, `<`, `<=`
- Boolean: `AND`, `and`, `&&`, `OR`, `or`, `||`

**Examples:**

```
{lat} > 50 && {depth} != NULL
{species} == 's. pecies' OR {species} == NULL
{value} == re'^[A-Z]+'
LINE_NUMBER > 10
```

**Notes:**

- All terms and operators must be separated by spaces
- Regular expressions can only be compared with `==` or `!=` against strings
- Values are compared based on their type (string `'5313'` does not equal number `5313`)

---

## Math Expression Syntax

The `boolean_add_computed_field` processor supports math expressions when `math_operation` is true.

**Operators:** `+`, `-`, `*`, `/`, `^` (exponent)

**Example:**

```yaml
value: "{field1} + {field2} * 2"
math_operation: true
```
