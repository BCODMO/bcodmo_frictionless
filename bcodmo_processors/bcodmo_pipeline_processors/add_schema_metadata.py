from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, res_iter = ingest()
resources = parameters.pop('resources')
if datapackage is None:
    raise Exception('Cannot update schema metadata for an empty datapackage')
else:
    for resourceName in resources:
        for resource in datapackage['resources']:
            if resource['name'] == resourceName:
                resource['schema'].update(parameters)

spew(datapackage, res_iter)
