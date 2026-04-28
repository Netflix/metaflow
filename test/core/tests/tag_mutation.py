# -*- coding: utf-8 -*-
import pytest
from metaflow_test import FlowDefinition, ExpectationFailed, steps


class TagMutation(FlowDefinition):
    """
    Test that tag mutation works
    """

    PRIORITY = 2
    SKIP_GRAPHS = [
        "simple_switch",
        "nested_switch",
        "branch_in_switch",
        "foreach_in_switch",
        "switch_in_branch",
        "switch_in_foreach",
        "recursive_switch",
        "recursive_switch_inside_foreach",
    ]
    HEADER = "@project(name='tag_mutation')"

    @steps(1, ["all"])
    def step_all(self):
        from metaflow import current, Task

        run = Task(current.pathspec).parent.parent
        for i in range(7):
            tag = str(i)
            run.add_tag(tag)
            assert tag in run.user_tags
            run.remove_tag(tag)
            assert tag not in run.user_tags

    def check_results(self, flow, checker):
        import random

        system_tags = checker.get_system_tags()
        assert (
            system_tags
        ), "Expect at least one system tag for an effective set of checks"
        some_existing_system_tags = random.sample(
            list(system_tags), min(len(system_tags) // 2, 1)
        )

        # Verify that trying to add a tag that already exists as a system tag is OK (only non system tags get added)
        checker.add_tags(["tag_along", *some_existing_system_tags])
        assert "tag_along" in checker.get_user_tags()
        assert len(set(some_existing_system_tags) & checker.get_user_tags()) == 0

        # Verify that trying to remove a tag that already exists as a system tag fails (all or nothing)
        with pytest.raises(Exception):
            checker.remove_tags(["tag_along", *some_existing_system_tags])
        assert "tag_along" in checker.get_user_tags()
        checker.remove_tag("tag_along")
        assert "tag_along" not in checker.get_user_tags()

        # Verify "remove, then add" behavior of replace_tags
        checker.add_tags(["AAA", "BBB"])
        assert "AAA" in checker.get_user_tags() and "BBB" in checker.get_user_tags()
        checker.replace_tags(["AAA", "BBB"], ["BBB", "CCC"])
        assert "AAA" not in checker.get_user_tags()
        assert "BBB" in checker.get_user_tags()
        assert "CCC" in checker.get_user_tags()

        # Verify UTF-8 support for tags
        checker.add_tags(["FeatEng1", "FeatEng2", "新想法"])
        assert "FeatEng1" in checker.get_user_tags()
        assert "FeatEng2" in checker.get_user_tags()
        assert "新想法" in checker.get_user_tags()

        checker.remove_tags(["新想法", "FeatEng1"])
        assert "FeatEng1" not in checker.get_user_tags()
        assert "FeatEng2" in checker.get_user_tags()
        assert "新想法" not in checker.get_user_tags()

        # try empty str as tag - should fail
        with pytest.raises(Exception):
            checker.add_tag("")
        assert "" not in checker.get_user_tags()

        # try adding a tag that is too long - should fail
        with pytest.raises(Exception):
            checker.add_tag("a" * 600)
        assert ("a" * 600) not in checker.get_user_tags()

        # try adding a tag made up of random bytes
        random_bytes = bytes(random.getrandbits(8) for _ in range(64))
        with pytest.raises(Exception):
            checker.add_tag(random_bytes)
        assert random_bytes not in checker.get_user_tags()

        # TODO add test for "too many tags", pending metadata service support (it depends on existing tags as well)

        # try int as tag - should fail
        with pytest.raises(Exception):
            checker.remove_tag(4)
        assert 4 not in checker.get_user_tags()

        # try to replace nothing with nothing - should fail
        with pytest.raises(Exception):
            checker.replace_tags([], [])

        # these check actions do not work for CliCheck. As of 6/3/2022, the only other
        # checker is MetadataCheck. But we write the code like this to force consideration
        # if/when we add the third checker.
        if checker.__class__.__name__ != "CliCheck":
            # Verify task tags do not diverge
            run = checker.get_run()
            assert run.end_task.tags == run.tags

            # Validate deprecated functionality (maintained for backwards compatibility
            # until usage migrated off)
            # When that happens, these test cases may be removed.
            checker.add_tag(["whoop", "eee"])
            assert "whoop" in checker.get_user_tags()
            assert "eee" in checker.get_user_tags()

            checker.replace_tag(["whoop", "eee"], ["woo", "hoo"])
            assert "whoop" not in checker.get_user_tags()
            assert "eee" not in checker.get_user_tags()
            assert "woo" in checker.get_user_tags()
            assert "hoo" in checker.get_user_tags()

            checker.remove_tag(["woo", "hoo"])
            assert "woo" not in checker.get_user_tags()
            assert "hoo" not in checker.get_user_tags()
