from metaflow import Config, FlowSpec, step


class ConfigRootTypeFlow(FlowSpec):
    cfg = Config("cfg")
    barecfg = Config("barecfg", default_value="", plain=True)

    @step
    def start(self):
        print("BareCFG: ", self.barecfg)
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ConfigRootTypeFlow()
