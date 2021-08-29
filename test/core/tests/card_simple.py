from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag

class CardDecoratorBasicTest(MetaflowTest):
    """
    Test that checks if the card decorator stores the information as intended for a built in card
    """
    PRIORITY = 3

    @tag('card(type="basic")')
    @steps(0, ['start'],)
    def step_start(self):
        self.data = 'abc'
    
    @tag('card(type="basic")')
    @steps(0, ['foreach-nested-inner'],)
    def step_foreach_inner(self):
        self.data = 'bcd'
    
    @tag('card(type="basic")')
    @steps(1, ['join'],)
    def step_join(self):
        self.data = 'jkl'
    
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

    
