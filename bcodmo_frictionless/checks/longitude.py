from goodtables import check, Error
from decimal import Decimal

# Module API


@check("longitude-bounds", type="custom", context="body")
class LongitudeBounds(object):

    # Public

    def __init__(self, constraint, **options):
        self.__constraint = constraint

    def check_row(self, cells):
        print("CECHKING LON BOUNDS")
        errors = []
        for cell in cells:

            row_number = cell.get("row-number")
            if cell["header"] == self.__constraint:
                field = cell["field"]
                if field.descriptor["type"] not in ["integer", "number"]:
                    # Ignore latitude/longitude columns that are not properly formatted as numbers
                    # TODO maybe add a warning?
                    # TODO ask data managers
                    continue
                # Check constraint
                message = None
                value = cell["value"]
                if value and value not in field.missing_values:

                    try:
                        assert type(value) in [int, Decimal, float]
                    except AssertionError:
                        message = f"Longitude column {self.__constraint} at row {row_number} is not a number"

                    if not message:
                        try:
                            assert cell["value"] > -180 and cell["value"] < 180
                        except AssertionError:
                            message = f"Longitude column {self.__constraint} at row {row_number} is not between -180 and 180"

                if message:
                    error = Error(
                        "longitude-bounds",
                        row_number=row_number,
                        message=message,
                        cell=cell,
                    )

                    errors.append(error)
        return errors
