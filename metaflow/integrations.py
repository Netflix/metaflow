# This file can contain "shortcuts" to other parts of Metaflow (integrations)

# This is an alternative to providing an extension package where you would define
# these aliases in the toplevel file.

# It follows a similar pattern to plugins so that the these integration aliases can be
# turned on and off and avoid exposing things that are not necessarily needed/wanted.

from metaflow.extension_support.integrations import process_integration_aliases

# To enable an alias `metaflow.integrations.get_s3_client` to
# `metaflow.plugins.aws.aws_client.get_aws_client`, use the following:
#
# ALIASES_DESC = [("get_s3_client", ".plugins.aws.aws_client.get_aws_client")]
#
# ALIASES_DESC is a list of tuples:
#  - name: name of the integration alias
#  - obj: object it points to
#
ALIASES_DESC = [("ArgoEvent", ".plugins.argo.argo_events.ArgoEvent")]

# Aliases can be enabled or disabled through configuration or extensions:
#  - ENABLED_INTEGRATION_ALIAS: list of alias names to enable.
#  - TOGGLE_INTEGRATION_ALIAS: if ENABLED_INTEGRATION_ALIAS is not set anywhere
#    (environment variable, configuration or extensions), list of integration aliases
#    to toggle (+<name> or <name> enables, -<name> disables) to build
#    ENABLED_INTEGRATION_ALIAS from the concatenation of the names in
#    ALIASES_DESC (concatenation of the names here as well as in the extensions).

# Keep this line and make sure ALIASES_DESC is above this line.
process_integration_aliases(globals())
