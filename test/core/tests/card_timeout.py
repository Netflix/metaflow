from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag

class CardTimeoutTest(MetaflowTest):
    """
    Test that checks if the card decorator works as intended with the timeout decorator. 
    # todo: Set timeout in the card arguement 
    # todo: timeout decorator doesn't timeout for cards. We use the arguement in the card_decorator. 
    """
    PRIORITY = 2 

    @tag('timeout(seconds=10)')
    @tag('card(type="timeout_card",args={"timeout":30})')
    @steps(0, ['start',],)
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
            assert_equals(False, card_stored)

    
