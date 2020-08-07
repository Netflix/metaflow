from metaflow import FlowSpec, step

class ComplexFlow(FlowSpec):

    @step
    def start(self):
        """
        This is the 'start' step. All flows must have a step named 'start' that
        is the first step in the flow.

        """
        print("START step:\n We are now inside the START step - Hello World!\n")
        self.next(self.br1, self.br2)

    @step
    def br1(self):
        """
        A step for metaflow to introduce itself.

        """
        print("\n\n_______________________________________")
        print(" BR1 -> BR11 and BR12 ")
        print("\n\n_______________________________________")
        self.next(self.br11, self.br12)

    @step
    def br2(self):
        """
        A step for metaflow to introduce itself.

        """
        print("\n\n_______________________________________")
        print(" BR2 -> BR21 and BR22 ")
        print("___________________________________________")
        self.next(self.br21, self.br22)

    @step
    def br11(self):
        """
        A step for metaflow to introduce itself.

        """
        print("\n\n_______________________________________")
        print(" BR11 -> JOIN1")
        print("\n\n_______________________________________")
        self.next(self.join1)

    @step
    def br12(self):
        """
        A step for metaflow to introduce itself.

        """
        print("\n\n_______________________________________")
        print(" BR 12 -> JOIN1 ")
        print("___________________________________________")
        self.next(self.join1)

    @step
    def br21(self):
        """
        A step for metaflow to introduce itself.

        """
        print("\n\n_______________________________________")
        print(" BR21 -> JOIN2")
        print("\n\n_______________________________________")
        self.next(self.join2)

    @step
    def br22(self):
        """
        A step for metaflow to introduce itself.

        """
        print("\n\n_______________________________________")
        print(" BR22 -> JOIN2 ")
        print("___________________________________________")
        self.next(self.join2)

    @step
    def join1(self, br1_inp):
        """
        A step for metaflow to introduce itself.

        """
        print("\n\n_______________________________________")
        print(" BR11 & BR12 -> JOIN1 ")
        self.var1 = 200
        print("Inside join1; var = ", self.var1)
        print("___________________________________________")
        self.next(self.join3)

    @step
    def join2(self, br2_inp):
        """
        A step for metaflow to introduce itself.

        """
        print("\n\n_______________________________________")
        print(" BR21 & BR22 -> JOIN2 ")
        self.var2 = 100
        print("Inside join2; var = ", self.var2)
        print("___________________________________________")
        self.next(self.join3)

    @step
    def join3(self, br_inp):
        """
        A step for metaflow to introduce itself.

        """
        print("\n\n_______________________________________")
        print(" JOIN1 & JOIN2 -> JOIN3")
        print("Verifying all input paths are correct by accessing same variable from"
              "both branches of the join...")
        print("JOIN1 var = ", br_inp.join1.var1)
        print("JOIN2 var = ", br_inp.join2.var2)
        print("___________________________________________")
        self.next(self.end)

    @step
    def end(self):
        """
        This is the 'end' step. All flows must have an 'end' step, which is the
        last step in the flow.

        """
        print("\n\n_______________________________________")
        print("END step:\n We have reached the END step!\n")
        print("HelloFlow is all done.")
        print("___________________________________________")

if __name__ == '__main__':
    ComplexFlow()
