from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag

class CardImportTest(MetaflowTest):
    """
    Test that checks if the card decorator stores the information as intended
    """
    PRIORITY = 2 

    @tag('card(type="mock_card")')
    @steps(0, ['start'],)
    def step_start(self):
        self.data = 'abc'

    @steps(1, ['all'])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        run = checker.get_run()
        if run:
            from hashlib import sha1
            import os
            import errno
            card_stored = False
            CARD_DIRECTORY_NAME='mf.cards'
            for step in run:
                for task in step:
                    if step.id == 'start':
                        stored_path = os.path.join('.metaflow',CARD_DIRECTORY_NAME,flow.name,'runs',run.id,'tasks',task.id,'cards')
                        try:
                            os.stat(stored_path)
                            card_stored=True
                        except OSError as e:
                            if e.errno == errno.ENOENT:
                                pass
                            else:
                                raise
            assert_equals(True, card_stored)

    
