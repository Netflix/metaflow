from metaflow import FlowSpec, step, project, conda, Task, pypi


class ComplexDAGFlow(FlowSpec):
    @step
    def start(self):
        self.split_start = [1, 2, 3]
        self.my_output = []
        print("My output is: ", self.my_output)
        self.next(self.step_a, foreach="split_start")

    @step
    def step_a(self):
        self.split_a = [4, 5]
        self.my_output = self.my_output + [self.input]
        print("My output is: ", self.my_output)
        self.next(self.step_b, foreach="split_a")

    @step
    def step_b(self):
        self.split_b = [6, 7, 8]
        self.my_output = self.my_output + [self.input]
        print("My output is: ", self.my_output)
        self.next(self.step_c, foreach="split_b")

    @conda(libraries={"numpy": "2.1.1"})
    @step
    def step_c(self):
        import numpy as np

        self.np_version = np.__version__
        print(f"numpy version: {self.np_version}")
        self.my_output = self.my_output + [self.input] + [9, 10]
        print("My output is: ", self.my_output)
        self.next(self.step_d)

    @step
    def step_d(self, inputs):
        self.my_output = sorted([inp.my_output for inp in inputs])[0]
        print("My output is: ", self.my_output)
        self.next(self.step_e)

    @step
    def step_e(self):
        print(f"I am step E. Input is: {self.input}")
        self.split_e = [9, 10]
        print("My output is: ", self.my_output)
        self.next(self.step_f, foreach="split_e")

    @step
    def step_f(self):
        self.my_output = self.my_output + [self.input]
        print("My output is: ", self.my_output)
        self.next(self.step_g)

    @step
    def step_g(self):
        print("My output is: ", self.my_output)
        self.next(self.step_h)

    @step
    def step_h(self, inputs):
        self.my_output = sorted([inp.my_output for inp in inputs])[0]
        print("My output is: ", self.my_output)
        self.next(self.step_i)

    @step
    def step_i(self, inputs):
        self.my_output = sorted([inp.my_output for inp in inputs])[0]
        print("My output is: ", self.my_output)
        self.next(self.step_j)

    @step
    def step_j(self):
        print("My output is: ", self.my_output)
        self.next(self.step_k, self.step_l)

    @step
    def step_k(self):
        self.my_output = self.my_output + [11]
        print("My output is: ", self.my_output)
        self.next(self.step_m)

    @step
    def step_l(self):
        print(f"I am step L. Input is: {self.input}")
        self.my_output = self.my_output + [12]
        print("My output is: ", self.my_output)
        self.next(self.step_m)

    @conda(libraries={"scikit-learn": ""})
    @step
    def step_m(self, inputs):
        import sklearn

        self.sklearn_version = sklearn.__version__
        self.my_output = sorted([inp.my_output for inp in inputs])[0]
        print("My output is: ", self.my_output)
        self.next(self.step_n)

    @step
    def step_n(self, inputs):
        self.my_output = sorted([inp.my_output for inp in inputs])[0]
        print("My output is: ", self.my_output)
        self.next(self.end)

    @step
    def end(self):
        self.my_output = self.my_output + [13]
        print("My output is: ", self.my_output)
        print("Flow is complete!")


if __name__ == "__main__":
    ComplexDAGFlow()
