from kfp.components import func_to_container_op


@func_to_container_op
def exit_handler(
    flow_name: str,
    status: str,
    kfp_run_id: str,
    notify_variables: dict,
):
    """
    The environment variables that this depends on:
        METAFLOW_NOTIFY_ON_SUCCESS
        METAFLOW_NOTIFY_ON_ERROR
        METAFLOW_NOTIFY_EMAIL_SMTP_HOST
        METAFLOW_NOTIFY_EMAIL_SMTP_PORT
        METAFLOW_NOTIFY_EMAIL_FROM
        K8S_CLUSTER_ENV
        POD_NAMESPACE
        ARGO_WORKFLOW_NAME
        METAFLOW_NOTIFY_EMAIL_BODY
    """
    import os

    def get_env(name, default=None) -> str:
        return notify_variables.get(name, os.environ.get(name, default=default))

    def email_notify(send_to):
        import smtplib
        import posixpath
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        from email.utils import formatdate

        smtp_host = get_env("METAFLOW_NOTIFY_EMAIL_SMTP_HOST")
        smtp_port = int(get_env("METAFLOW_NOTIFY_EMAIL_SMTP_PORT"))
        email_from = get_env("METAFLOW_NOTIFY_EMAIL_FROM")
        cluster_env = get_env("K8S_CLUSTER_ENV", "")

        msg = MIMEMultipart(mime_subtype="mixed")
        msg["Subject"] = f"Flow {flow_name} {status} on {cluster_env}"
        msg["From"] = email_from
        msg["To"] = send_to
        msg["Date"] = formatdate(localtime=True)

        kfp_run_url = posixpath.join(
            get_env("KFP_RUN_URL_PREFIX", ""),
            "_/pipeline/#/runs/details",
            kfp_run_id,
        )

        pod_namespace = get_env("POD_NAMESPACE", "")
        argo_workflow_name = get_env("ARGO_WORKFLOW_NAME", "")
        email_body = get_env("METAFLOW_NOTIFY_EMAIL_BODY", "")
        body = (
            f"status = {status} <br/>\n"
            f"{kfp_run_url} <br/>\n"
            f"Metaflow RunId = kfp-{kfp_run_id} <br/>\n"
            f"argo -n {pod_namespace} get {argo_workflow_name} <br/>"
            "<br/>"
            f"{email_body}"
        )
        mime_text = MIMEText(body, "html")
        msg.attach(mime_text)

        s = smtplib.SMTP(smtp_host, smtp_port)
        s.sendmail(email_from, send_to, msg.as_string())
        s.quit()
        print(msg)

    notify_on_error = get_env("METAFLOW_NOTIFY_ON_ERROR")
    notify_on_success = get_env("METAFLOW_NOTIFY_ON_SUCCESS")

    print(f"Flow completed with status={status}")
    if notify_on_error and status == "Failed":
        email_notify(notify_on_error)
    elif notify_on_success and status == "Succeeded":
        email_notify(notify_on_success)
    else:
        print("No notification is necessary!")
