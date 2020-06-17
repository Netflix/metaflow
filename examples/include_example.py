from metaflow import FlowSpec, IncludeFile, step

class IncludeTest(FlowSpec):

    my_file = IncludeFile(
        'my_file', required=True, is_text=True, help='Included file',
        default='./myfile.txt')

    @step
    def start(self):
        print("I see that file contains %s" % self.my_file)
        self.file_content = self.my_file
        self.next(self.end)

    @step
    def end(self):
        print("All done")

if __name__ == "__main__":
    IncludeTest()
