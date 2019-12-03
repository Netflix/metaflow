from metaflow_test import MetaflowTest, ExpectationFailed, steps

class CurrentSingletonTest(MetaflowTest):
    """
    Test that the current singleton returns the right values
    """
    PRIORITY = 1

    @steps(0, ['start'])
    def step_start(self):
        from uuid import uuid4
        from metaflow import current
        self.flow_names = {current.flow_name}
        self.run_ids = {current.run_id}
        self.origin_run_ids = {current.origin_run_id}
        self.steps = {current.step_name}
        self.step_name = current.step_name
        self.namespaces = {current.namespace}
        self.usernames = {current.username}
        self.uuid = str(uuid4())
        self.task_data = {current.pathspec: self.uuid}

    @steps(1, ['join'])
    def step_join(self):
        from uuid import uuid4
        from metaflow import current

        # merge all incoming branches
        # join step needs to reassign all artifacts.
        from itertools import chain
        self.flow_names = set(chain(*(i.flow_names for i in inputs)))
        self.run_ids = set(chain(*(i.run_ids for i in inputs)))
        self.origin_run_ids = set(chain(*(i.origin_run_ids for i in inputs)))
        self.steps = set(chain(*(i.steps for i in inputs)))
        self.namespaces = set(chain(*(i.namespaces for i in inputs)))
        self.usernames = set(chain(*(i.usernames for i in inputs)))
        self.task_data = {}
        for i in inputs:
            self.task_data.update(i.task_data)

        # add data for the join step
        self.step_name = current.step_name
        self.flow_names.add(current.flow_name)
        self.run_ids.add(current.run_id)
        self.origin_run_ids.add(current.origin_run_id)
        self.namespaces.add(current.namespace)
        self.usernames.add(current.username)
        self.steps.add(current.step_name)
        self.uuid = str(uuid4())
        self.task_data[current.pathspec] = self.uuid

    @steps(2, ['all'])
    def step_all(self):
        from uuid import uuid4
        from metaflow import current

        self.flow_names.add(current.flow_name)
        self.run_ids.add(current.run_id)
        self.origin_run_ids.add(current.origin_run_id)
        self.namespaces.add(current.namespace)
        self.usernames.add(current.username)
        self.step_name = current.step_name
        self.steps.add(current.step_name)
        self.uuid = str(uuid4())
        self.task_data[current.pathspec] = self.uuid

    def check_results(self, flow, checker):
        run = checker.get_run()
        if run is None:
            # very basic sanity check for CLI
            for step in flow:
                checker.assert_artifact(step.name, 'step_name', step.name)
        else:
            from metaflow import Task
            task_data = run.data.task_data
            for pathspec, uuid in task_data.items():
                assert_equals(Task(pathspec).data.uuid, uuid)
            for step in run:
                for task in step:
                    assert_equals(task.data.step_name, step.id)
                    pathspec = '/'.join(task.pathspec.split('/')[-4:])
                    assert_equals(task.data.uuid, task_data[pathspec])
            assert_equals(run.data.flow_names, {run.parent.id})
            assert_equals(run.data.run_ids, {run.id})
            assert_equals(run.data.origin_run_ids, {None})
            assert_equals(run.data.namespaces, {'user:tester'})
            assert_equals(run.data.usernames, {'tester'})
