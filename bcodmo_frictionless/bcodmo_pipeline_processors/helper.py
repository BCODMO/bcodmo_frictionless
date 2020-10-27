def get_missing_values(res):
    return res.descriptor.get(
        'schema', {},
    ).get(
        'missingValues', ['']
    )

