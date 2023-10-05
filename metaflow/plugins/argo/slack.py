class Slack(object):
    def __init__(flow_name, metaflow_ui_url=None, argo_ui_url=None):
        self.flow_name = flow_name
        self.metaflow_ui_url = metaflow_ui_url
        self.argo_ui_url = argo_ui_url

    def success_block(self):
        {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "%s/argo-{{workflow.name}}" % self.flow_name,
                        "emoji": true,
                    },
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*Status*: :rotating_light: failed",
                    },
                },
                {"type": "divider"},
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "See details of the run at <https://google.com|Metaflow UI>",
                    },
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "See details of the execution at <https://google.com|Argo UI>",
                    },
                },
            ]
        }
