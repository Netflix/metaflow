"""This modules provides a helper function for the conda plugin."""


try:
    from urlparse import urlparse
except:  # noqa E722
    from urllib.parse import urlparse


class PluginHelper:
    """A plugin helper class."""

    @staticmethod
    def sanitize_conda_package_url(url, token_ref="t"):  # noqa S107
        """Sanitize conda private channels.

        This function removes `username`, `password` and `token`
        if they exist from the url.

        Possible conda private channels configuration:
            - https://user:password@private.com:port/<channel>
            - https://private.com:port/t/<token>/<channel>
            - https://private.com:port/t/<token>/<channel>/label/<labelname>

        :param url: channel url
        :type url: Union[str, ParseResult]
        :param token_ref: token name in the  url path
        :type token_ref: str
        :return: the sanitized url
        :type : Union[str, ParseResult]
        """
        sanitized_url = url
        if url:
            # parse the url (code should be py2, py3 compatible)
            c_url = urlparse(url) if isinstance(url, str) else url
            # remove the username/password from the url 'netloc' field
            # use the _replace() method as described here:
            # https://docs.python.org/3/library/urllib.parse.html#urllib.parse.urlparse
            if c_url.password:
                c_url = c_url._replace(
                    netloc=c_url.netloc.replace(
                        "{}:{}@".format(c_url.username, c_url.password), "", 1
                    )
                )
            # remove the token, if it exists, from the url 'path' field
            if (
                token_ref
                and c_url.path.startswith("/{}/".format(token_ref))
                and len(c_url.path.lstrip("/").split("/")) > 1
            ):
                c_url = c_url._replace(
                    path="/".join([""] + c_url.path.lstrip("/").split("/")[2:])
                )
            sanitized_url = (
                c_url.geturl() if isinstance(url, str) else c_url  # noqa SC200
            )
        return sanitized_url
