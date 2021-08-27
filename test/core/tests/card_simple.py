from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag

class CardDecoratorBasicTest(MetaflowTest):
    """
    Test that checks if the card decorator stores the information as intended
    """
    PRIORITY = 2 

    @tag('card(type="basic")')
    @steps(0, ['singleton-start', 'foreach-inner'], required=True)
    def step_sleep(self):
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
                    content_str = '\n'.join([
                        "<p>%s : %s</p>" % (key,value.data)
                        for key,value in task.data._artifacts.items()
                    ])
                    html_str="""
                    <html>
                    <head>
                    </head>
                    <body>
                    %s
                    </body>
                    </html>
                    """ % (content_str)                    
                    file_name = 'basic-%s.html'%(sha1(bytes(str(html_str).encode("utf-8"))).hexdigest())
                    stored_path = os.path.join(CARD_DIRECTORY_NAME,run.id,'runs',step.id,'tasks',task.id,'cards',file_name)
                    card_stored=True
                    try:
                        os.stat(stored_path)
                    except OSError as e:
                        if e.errno == errno.ENOENT:
                            pass
                        else:
                            raise
            assert_equals(True, card_stored)

    
