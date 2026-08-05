"""Flow for testing mutator overrides of late-attached @kubernetes."""

from metaflow import FlowSpec, StepMutator, step


class kubernetes_override(StepMutator):
    def init(self, *args, **kwargs):
        self.deco_kwargs = kwargs

    def mutate(self, mutable_step):
        for deco_name, _, _, attributes in mutable_step.decorator_specs:
            if deco_name == "kubernetes":
                attributes.update(self.deco_kwargs)
                mutable_step.add_decorator(
                    deco_type=deco_name,
                    deco_kwargs=attributes,
                    duplicates=mutable_step.OVERRIDE,
                )


class LateAttachedKubernetesMutatorFlow(FlowSpec):
    @kubernetes_override(cpu=2, memory=8192)
    @step
    def start(self):
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    LateAttachedKubernetesMutatorFlow()
