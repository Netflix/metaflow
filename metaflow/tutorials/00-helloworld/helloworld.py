from metaflow import FlowSpec, step, S3, tracing
import json


class HelloFlow(FlowSpec):
    """
    A flow where Metaflow prints 'Hi'.

    Run this flow to validate that Metaflow is installed correctly.

    """

    @step
    def start(self):
        """
        This is the 'start' step. All flows must have a step named 'start' that
        is the first step in the flow.

        """
        print("HelloFlow is starting.")


        with S3(run=self) as s3:
            message = json.dumps({'message': 'hello world!'})
            url = s3.put('example_object', message)
            print("Message saved at", url)
        self.next(self.hello)

    @step
    def hello(self):
        """
        A step for metaflow to introduce itself.

        """
        print("Metaflow says: Hi!")
        with S3(run=self) as s3:
            s3obj = s3.get('example_object')
            print("Object found at", s3obj.url)
            print("Message:", json.loads(s3obj.text))
        self.next(self.end)

    @step
    def end(self):
        """
        This is the 'end' step. All flows must have an 'end' step, which is the
        last step in the flow.

        """
        print("HelloFlow is all done.")


if __name__ == "__main__":
    HelloFlow()
