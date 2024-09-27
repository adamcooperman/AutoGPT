from enum import Enum
from typing import Any

from backend.data.block import Block, BlockCategory, BlockOutput, BlockSchema
from backend.data.model import SchemaField


class ComparisonOperator(Enum):
    EQUAL = "=="
    NOT_EQUAL = "!="
    GREATER_THAN = ">"
    LESS_THAN = "<"
    GREATER_THAN_OR_EQUAL = ">="
    LESS_THAN_OR_EQUAL = "<="
    
class ConditionBlock(Block):
    class Input(BlockSchema):
        input_object: Any = SchemaField(
            description="Enter the input object or list for comparison",
            placeholder="For example: {'value': 10} or [{'value': 10}, {'value': 20}]",
        )
        value1_key: str = SchemaField(
            description="Enter the key for the first value (if input is a dict or list of dicts)",
            placeholder="For example: 'value'",
            default=None,
        )
        value2_key: str = SchemaField(
            description="Enter the key for the second value (if input is a dict or list of dicts)",
            placeholder="For example: 'value'",
            default=None,
        )
        value1_index: int = SchemaField(
            description="Enter the index for the first value (if input is a list)",
            placeholder="For example: 0",
            default=None,
        )
        value2_index: int = SchemaField(
            description="Enter the index for the second value (if input is a list)",
            placeholder="For example: 1",
            default=None,
        )
        operator: ComparisonOperator = SchemaField(
            description="Choose the comparison operator",
            placeholder="Select an operator",
        )
        yes_value: Any = SchemaField(
            description="(Optional) Value to output if the condition is true. If not provided, the input object will be used.",
            placeholder="Leave empty to use input object, or enter a specific value",
            default=None,
        )
        no_value: Any = SchemaField(
            description="(Optional) Value to output if the condition is false. If not provided, the input object will be used.",
            placeholder="Leave empty to use input object, or enter a specific value",
            default=None,
        )
        passthrough: bool = SchemaField(
            description="Whether to pass through the entire input object based on the condition result",
            default=False,
        )

    class Output(BlockSchema):
        result: bool = SchemaField(
            description="The result of the condition evaluation (True or False)"
        )
        output: Any = SchemaField(
            description="The output value based on the condition result"
        )

    def __init__(self):
        super().__init__(
            id="74346365-854c-4012-ab74-8d1430334c09",
            input_schema=ConditionBlock.Input,
            output_schema=ConditionBlock.Output,
            description="Handles conditional logic based on comparison operators with passthrough functionality",
            categories={BlockCategory.LOGIC},
            test_input={
                "input_object": {"value1": 10, "value2": 5},
                "value1_key": "value1",
                "value2_key": "value2",
                "operator": ComparisonOperator.GREATER_THAN.value,
                "yes_value": "Greater",
                "no_value": "Not greater",
                "passthrough": False,
            },
            test_output=[
                ("result", True),
                ("output", "Greater"),
            ],
        )

    def run(self, input_data: Input, **kwargs) -> BlockOutput:
        input_object = input_data.input_object
        operator = input_data.operator
        passthrough = input_data.passthrough

        def get_value(obj, key, index):
            if isinstance(obj, dict):
                return obj.get(key)
            elif isinstance(obj, list):
                if index is not None and 0 <= index < len(obj):
                    return obj[index]
                elif key is not None:
                    return obj[0].get(key) if isinstance(obj[0], dict) else None
            return obj

        if isinstance(input_object, list):
            value1 = get_value(input_object, input_data.value1_key, input_data.value1_index)
            value2 = get_value(input_object, input_data.value2_key, input_data.value2_index)
        else:
            value1 = get_value(input_object, input_data.value1_key, None)
            value2 = get_value(input_object, input_data.value2_key, None)

        comparison_funcs = {
            ComparisonOperator.EQUAL: lambda a, b: a == b,
            ComparisonOperator.NOT_EQUAL: lambda a, b: a != b,
            ComparisonOperator.GREATER_THAN: lambda a, b: a > b,
            ComparisonOperator.LESS_THAN: lambda a, b: a < b,
            ComparisonOperator.GREATER_THAN_OR_EQUAL: lambda a, b: a >= b,
            ComparisonOperator.LESS_THAN_OR_EQUAL: lambda a, b: a <= b,
        }

        try:
            result = comparison_funcs[operator](value1, value2)
            yield "result", result

            if result:
                output = input_object if passthrough else (input_data.yes_value if input_data.yes_value is not None else value1)
            else:
                output = input_object if passthrough else (input_data.no_value if input_data.no_value is not None else value1)

            yield "output", output

        except Exception:
            yield "result", None
            yield "output", None