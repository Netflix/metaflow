from . import Renderer

class BasicRenderer(Renderer):

    TYPE='basic'

    def render(self, task):
        datastore_info = task.artifacts
        mustache = self._get_mustache()
        content_str = '\n'.join([f"<p>{key} : {value}</p>" for key,value in datastore_info.items()])
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
