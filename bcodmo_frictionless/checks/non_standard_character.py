from goodtables import check, Error
from decimal import Decimal

# Module API


@check("non-standard-character", type="custom", context="body")
class NonStandardCharacter(object):

    # Public

    def __init__(self, **options):
        pass

    def check_row(self, cells):
        errors = []
        for cell in cells:

            row_number = cell.get("row-number")
            field = cell["field"]
            if field.descriptor["type"] != "string":
                continue
            # Check constraint
            message = None
            whitespace = False
            value = cell["value"]
            if value and (
                (not hasattr(field, "missingvalue"))
                or value not in field.missing_values
            ):
                for c in value:
                    v = ord(c)
                    if v > 128:
                        message = f"Non standard character {v} found"
                        if v == 133 or v == 160:
                            whitespace = True
                            message = f"Non standard whitespace character {v} found. Try finding and replacing all whitespace characters with \\s+"
                        break

            if message:
                error = Error(
                    "non-standard-whitespace-character"
                    if whitespace
                    else "non-standard-character",
                    row_number=row_number,
                    message=message,
                    cell=cell,
                )

                errors.append(error)
        return errors
