from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag


class CardWithRefreshTest(MetaflowTest):
    """
    This test Does few checks that the core user interfaces are working :
    1. It validates we can call `current.card.refresh` without any errors.
    2. It validates if the data updates that are getting shipped are correct.

    How will it do it :

    1. In step code:
        1. We create a random array of strings.
        2. We call `current.card.refresh` with the array.
        3. We check if the card is present given that we have called referesh and the card
            should have reached the backend in some short period of time
        4. Keep adding new data to the array and keep calling refresh.
        5. The data-update that got shipped should *atleast* be a subset the actual data present in the runtime code.
    2. In check_results:
        1. We check if the data that got shipped can be access post task completion
        2. We check if the data that got shipped is a subset of the actual data created during the runtime code.
    """

    PRIORITY = 3

    @tag('environment(vars={"METAFLOW_CARD_NO_WARNING": "True"})')
    @tag('card(type="test_refresh_card", id="refresh_card")')
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

        from metaflow import current
        from metaflow.cards import get_cards
        from metaflow.plugins.cards.card_client import Card
        import random
        import time

        start_arr = [_create_random_strings(10) for i in range(5)]
        # Calling the first refresh should trigger a render of the card.
        current.card.refresh({"arr": start_arr})
        # sleep for a little bit because the card refresh is async.
        # This feels a little hacky but need better ideas on how to test this
        # when async processes may write cards/data in a "best-effort" manner.
        # The `try_to_get_card` function will keep retrying to get a card until a
        # timeout value is reached. After which the function will throw a `TimeoutError`.
        card = try_to_get_card(id="refresh_card")
        assert_equals(isinstance(card, Card), True)

        sleep_between_refreshes = 4  # Set based on the RUNTIME_CARD_MIN_REFRESH_INTERVAL which acts as a rate-limit to what is refreshed.

        # Now we check if the refresh interface is working as expected from data updates.
        card_data = None
        for i in range(5):
            # We need to put sleep statements because:
            #   1. the update to cards is run via async processes
            #   2. card refreshes are rate-limited by RUNTIME_CARD_MIN_REFRESH_INTERVAL so we can't validate with each update.
            # There by there is no consistent way to know from during user-code when a data update
            # actually got shiped.
            start_arr.append(_create_random_strings(10))
            current.card.refresh({"arr": start_arr})
            # We call the `card.get_data` interface to validate the data is available in the card.
            # This is a private interface and should not be used by users but is used by internal services.
            card_data = card.get_data()
            if card_data is not None:
                # Assert that data is atleast subset of what we sent to the datastore.
                assert_equals(
                    _array_is_a_subset(card_data["data"]["user"]["arr"], start_arr),
                    True,
                )
                # The `TestRefreshCard.refresh(task, data)` method returns the `data` object as a pass through.
                # This test will also serve a purpose of ensuring that any changes to these keys are
                # caught by the test framework. The minimum subset should be present and grown as
                # need requires.
                # We first check the keys created by the refresh-JSON created in the `card_cli.py`
                top_level_keys = set(["data", "reload_token"])
                assert_equals(top_level_keys.issubset(set(card_data.keys())), True)
                # We then check the keys returned from the `current.card._get_latest_data` which is the
                # `data` parameter in the `MetaflowCard.refresh ` method.
                required_data_keys = set(
                    ["mode", "component_update_ts", "components", "render_seq", "user"]
                )
                assert_equals(
                    required_data_keys.issubset(set(card_data["data"].keys())), True
                )

            time.sleep(sleep_between_refreshes)

        assert_equals(card_data is not None, True)
        self.final_data = {"arr": start_arr}
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
                    step.name, task_id, "test_refresh_card", card_id="refresh_card"
                )
                assert_equals(card_present, True)
                data_has_latest_artifact = _array_is_a_subset(
                    data_obj["arr"], card_data["data"]["user"]["arr"]
                )
                assert_equals(data_has_latest_artifact, True)
                print(
                    "Succesfully validated task pathspec %s"
                    % run[step.name][task_id].pathspec
                )
