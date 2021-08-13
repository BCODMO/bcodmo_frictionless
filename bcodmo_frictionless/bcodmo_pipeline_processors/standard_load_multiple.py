from dataflows import load as standard_load
from datapackage import Package


class standard_load_multiple(standard_load):
    def __init__(
        self,
        load_sources,
        names,
        sheets=None,
        **options,
    ):
        super(standard_load_multiple, self).__init__("", **options)
        self.load_sources = load_sources
        self.names = names
        self.sheets = sheets
        print(self.sheets)
        print(options)

    def _set_individual(self, i):
        load_source = self.load_sources[i]
        name = self.names[i]

        self.load_source = load_source
        self.name = name

        if self.sheets:
            sheet = self.sheets[i]
            self.options["sheet"] = sheet
        else:
            self.options["sheet"] = ""

    def process_datapackage(self, dp: Package):
        new_dp = dp
        for i in range(len(self.load_sources)):
            # Set the proper variables for this individual resource
            self._set_individual(i)

            new_dp = super(standard_load_multiple, self).process_datapackage(new_dp)

        new_dp.descriptor["resources"] = self.resource_descriptors
        return new_dp
