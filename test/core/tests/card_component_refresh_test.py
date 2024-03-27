from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag


class CardComponentRefreshTest(MetaflowTest):
    """
    This test will validates the card component API based for runtime updates.
    """

    PRIORITY = 3

    @tag('environment(vars={"METAFLOW_CARD_NO_WARNING": "True"})')
    @tag('card(type="test_component_refresh_card", id="refresh_card", save_errors=False)')  # fmt: skip
    @steps(
        0,
        [
            "singleton-start",
            "sigleton-end",
            "singleton",
            "foreach-split-small",
            "foreach-inner-small",
            "foreach-join-small",
            "split-and",
            "single-branch-split",
            "join-and",
            "parallel-step",
        ],
    )
    def step_start(self):
        import random
        import string

        def _create_random_strings(char_len):
            return "".join(random.choice(string.ascii_letters) for i in range(char_len))

        def _array_is_a_subset(arr1, arr2):
            return set(arr1).issubset(set(arr2))

        def create_random_string_array(size=10):
            return [_create_random_strings(10) for i in range(size)]

        from metaflow import current
        from metaflow.plugins.cards.card_client import Card
        from metaflow.plugins.cards.card_modules.test_cards import (
            _component_values_to_hash,
            TestJSONComponent,
        )
        import random
        import time

        possible_reload_tokens = []
        make_reload_token = lambda a1, a2: "runtime-%s" % _component_values_to_hash(
            {"random_key_1": {"abc": a1}, "random_key_2": {"abc": a2}}
        )
        component_1_arr = create_random_string_array(5)
        # Calling the first refresh should trigger a render of the card.
        current.card.append(
            TestJSONComponent({"abc": component_1_arr}), id="component_1"
        )
        component_2_arr = create_random_string_array(5)
        inscope_component = TestJSONComponent({"abc": component_2_arr})
        current.card.append(inscope_component)
        current.card.refresh()
        # sleep for a little bit because the card refresh is async.
        # This feels a little hacky but need better ideas on how to test this
        # when async processes may write cards/data in a "best-effort" manner.
        # The `try_to_get_card` function will keep retrying to get a card until a
        # timeout value is reached. After which the function will throw a `TimeoutError`.
        _reload_tok = make_reload_token(component_1_arr, component_2_arr)
        card = try_to_get_card(id="refresh_card")
        assert_equals(isinstance(card, Card), True)

        sleep_between_refreshes = 2  # Set based on the RUNTIME_CARD_MIN_REFRESH_INTERVAL which acts as a rate-limit to what is refreshed.

        card_html = card.get()
        possible_reload_tokens.append(_reload_tok)
        # The reload token for card type `test_component_refresh_card` contains a hash of the component values.
        # The first assertion will check if this reload token exists is set to what we expect in the HTML page.
        assert_equals(_reload_tok in card_html, True)

        card_data = None
        for i in range(5):
            # We need to test the following :
            #   1. We can call `inscope_component.update()` with new data and it will be reflected in the card.
            #   2. `current.card.components["component1"].update()` and it will be reflected in the card.
            # How do we test it :
            #   1. Add new values to the components that have been created.
            #   2. Since the card is calculating the reload token based on the hash of the value, we verify that dataupdates have the same reload token or any of the possible reload tokens.
            #   3. We also verify that the card_data contains the `data` key that has the lastest information updated for `component_1`
            component_2_arr.append(_create_random_strings(10))
            component_1_arr.append(_create_random_strings(10))

            inscope_component.update({"abc": component_2_arr})
            current.card.components["component_1"].update({"abc": component_1_arr})
            _reload_tok = make_reload_token(component_1_arr, component_2_arr)
            current.card.refresh()

            possible_reload_tokens.append(_reload_tok)
            card_data = card.get_data()
            if card_data is not None:
                assert_equals(card_data["reload_token"] in possible_reload_tokens, True)
                assert_equals(
                    _array_is_a_subset(
                        card_data["data"]["component_1"]["abc"], component_1_arr
                    ),
                    True,
                )
            time.sleep(sleep_between_refreshes)

        assert_equals(card_data is not None, True)
        self.final_data = component_1_arr
        # setting step name here helps us figure out what steps should be validated by the checker
        self.step_name = current.step_name

    @steps(1, ["all"])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        def _array_is_a_subset(arr1, arr2):
            return set(arr1).issubset(set(arr2))

        if checker.__class__.__name__ != "MetadataCheck":
            return
        run = checker.get_run()
        for step in flow:
            meta_check_dict = checker.artifact_dict_if_exists(step.name, "final_data")
            # Which ever steps ran the actual card testing code
            # contains the `final_data` attribute and the `step_name` attribute.
            # If these exist then we can succesfully validate the card data since it is meant to exist.
            step_done_check_dict = checker.artifact_dict_if_exists(
                step.name, "step_name"
            )
            for task_id in step_done_check_dict:
                if (
                    len(step_done_check_dict[task_id]) == 0
                    or step_done_check_dict[task_id]["step_name"] != step.name
                ):
                    print(
                        "Skipping task pathspec %s" % run[step.name][task_id].pathspec
                    )
                    continue
                # If the `step_name` attribute was set then surely `final_data` will also be set;
                data_obj = meta_check_dict[task_id]["final_data"]
                card_present, card_data = checker.get_card_data(
                    step.name,
                    task_id,
                    "test_component_refresh_card",
                    card_id="refresh_card",
                )
                assert_equals(card_present, True)
                data_has_latest_artifact = _array_is_a_subset(
                    data_obj, card_data["data"]["component_1"]["abc"]
                )
                assert_equals(data_has_latest_artifact, True)
                print(
                    "Succesfully validated task pathspec %s"
                    % run[step.name][task_id].pathspec
                )
