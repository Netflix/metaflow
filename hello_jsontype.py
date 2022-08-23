from metaflow import FlowSpec, step, Parameter, JSONType


class HelloJSONType(FlowSpec):
    """
    A flow for testing ability to handle JSONType objects.

    """

    str_to_list = Parameter("str_to_list",
                            type=JSONType,
                            default="[1, 2, 3]")
    str_to_dict = Parameter("str_to_dict",
                            type=JSONType,
                            default='{"one": 1, "two": 2, "three": 3}')
    list_param = Parameter("list_param",
                           type=JSONType,
                           default="[1, 2, 3]")
    dict_param = Parameter("dict_param",
                           type=JSONType,
                           default='{"one": 1, "two": 2, "nine": 9}')

    @step
    def start(self):
        """
        This is the 'start' step. All flows must have a step named 'start' that
        is the first step in the flow.

        """
        print("HelloJSONType is starting.")
        self.next(self.hello)

    @step
    def hello(self):
        """
        A step to print out JSONType objects.

        """
        print("Ready to begin testing JSONType object params")
        print(f"str_to_list - type: {type(self.str_to_list)}, data: {self.str_to_list}")
        print(f"str_to_dict - type: {type(self.str_to_dict)}, data: {self.str_to_dict}")
        print(f"list_param - type: {type(self.list_param)}, data: {self.list_param}")
        print(f"dict_param - type: {type(self.dict_param)}, data: {self.dict_param}")
        
        self.next(self.end)

    @step
    def end(self):
        """
        This is the 'end' step.

        """
        print("HelloJSONType is all done.")


if __name__ == "__main__":
    HelloJSONType()
