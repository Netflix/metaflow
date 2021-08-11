from . import Renderer

class BasicRenderer(Renderer):

    TYPE='basic'

    def render(self, task_datastore):
        datastore_info = task_datastore.to_dict()
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
