from .card import MetaflowCard

class BasicCard(MetaflowCard):

    name='basic'

    def render(self, task):
        mustache = self._get_mustache()
        content_str = '\n'.join([
            f"<p>{key} : {value.data}</p>" 
            for key,value in task.data._artifacts.items()
        ])
        TEMPLATE = f"""
        <html>
        <head>
        </head>
        <body>
        {content_str}
        </body>
        </html>
        """
        return mustache.render(TEMPLATE)
