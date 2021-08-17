from .card import MetaflowCard

class BasicCard(MetaflowCard):

    name='basic'

    def render(self, task):
        mustache = self._get_mustache()
        content_str = '\n'.join([
            "<p>%s : %s</p>" % (key,value.data)
            for key,value in task.data._artifacts.items()
        ])
        TEMPLATE = """
        <html>
        <head>
        </head>
        <body>
        %s
        </body>
        </html>
        """ % (content_str)
        return mustache.render(TEMPLATE)
