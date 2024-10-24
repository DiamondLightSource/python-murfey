from pydantic import ValidationError
from pydantic.fields import UndefinedType
from rich.pretty import pprint
from rich.prompt import Prompt

from murfey.util.config import MachineConfig


def run():
    new_config = {}
    for k, field in MachineConfig.__fields__.items():
        pprint(field.name)
        pprint(field.field_info.description)
        if isinstance(field.field_info.default, UndefinedType):
            value = Prompt.ask("Please provide a value")
        else:
            value = Prompt.ask(
                "Please provide a value", default=field.field_info.default
            )
        new_config[k] = value

        try:
            MachineConfig.validate(new_config)
        except ValidationError as exc:
            for ve in exc.errors():
                if ve["type"] != "value_error.missing":
                    print("Validation failed")
