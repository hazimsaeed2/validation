# Databricks notebook source
# MAGIC %pip install msal

# COMMAND ----------

# DBTITLE 1,Import Required Libraries
import json
import base64
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime
import msal
import requests
import os

# COMMAND ----------

# DBTITLE 1,Initialize Widgets for Parameters
# These can be passed from the calling notebook or job
dbutils.widgets.text("tenant_id", "", "Azure AD Tenant ID")
dbutils.widgets.text("client_id", "", "Application Client ID")
dbutils.widgets.text("client_secret", "", "Client Secret")
dbutils.widgets.text("sender_mailbox", "svc_dbx_notification@bjs.com", "Sender Mailbox")
dbutils.widgets.text("use_secrets", "true", "Use Databricks Secrets")
dbutils.widgets.text("secret_scope", "digital_email_notifications_vault", "Secret Scope Name")

# COMMAND ----------

# DBTITLE 1,MS Graph Email Sender Class
class MSGraphEmailSender:
    """
    Email sender using Microsoft Graph API with MSAL authentication.
    Databricks-optimized version with secrets support.
    """

    def __init__(self):
        """Initialize using widgets or Databricks secrets."""
        use_secrets = dbutils.widgets.get("use_secrets").lower() == "true"

        if use_secrets:
            # Get from Databricks secrets
            print('Using dbx secrets...')
            secret_scope        = dbutils.widgets.get("secret_scope")
            self.tenant_id      = dbutils.secrets.get(scope=secret_scope, key="tenant_id")
            self.client_id      = dbutils.secrets.get(scope=secret_scope, key="client_id")
            self.client_secret  = dbutils.secrets.get(scope=secret_scope, key="client_secret")
            self.sender_mailbox = dbutils.widgets.get("sender_mailbox")

        else:
            # Get from widgets (for testing only - never use in production!)
            self.tenant_id      = dbutils.widgets.get("tenant_id")
            self.client_id      = dbutils.widgets.get("client_id")
            self.client_secret  = dbutils.widgets.get("client_secret")
            self.sender_mailbox = dbutils.widgets.get("sender_mailbox")

        # Initialize MSAL app
        self.authority = f"https://login.microsoftonline.com/{self.tenant_id}"
        self.app = msal.ConfidentialClientApplication(
            authority=self.authority,
            client_id=self.client_id,
            client_credential=self.client_secret
        )

        # Graph API endpoint
        self.graph_endpoint = "https://graph.microsoft.com/v1.0"

        # Token cache
        self._token = None

        print(f"Email sender initialized for mailbox: {self.sender_mailbox}")

    def _acquire_token(self) -> str:
        """Acquire access token using MSAL."""
        if self._token and 'access_token' in self._token:
            return self._token['access_token']

        scopes = ["https://graph.microsoft.com/.default"]

        result = self.app.acquire_token_silent(scopes, account=None)
        if not result:
            result = self.app.acquire_token_for_client(scopes=scopes)

        if "access_token" in result:
            self._token = result
            return result['access_token']
        else:
            error_msg = f"Token acquisition failed: {result.get('error')}"
            raise Exception(error_msg)

    def send_email(
        self,
        to_recipients: List[str],
        subject: str,
        body: str,
        cc_recipients: List[str] = None,
        bcc_recipients: List[str] = None,
        is_html: bool = False,
        importance: str = "normal",
        save_to_sent: bool = True,
        attachment_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """Send email via Microsoft Graph API, optionally with an attachment from a volume path."""
        try:
            access_token = self._acquire_token()

            # Build message
            message = {
                "subject": subject,
                "body": {
                    "contentType": "HTML" if is_html else "Text",
                    "content": body
                },
                "toRecipients": [{"emailAddress": {"address": email}} for email in to_recipients],
                "importance": importance
            }

            if cc_recipients:
                message["ccRecipients"] = [{"emailAddress": {"address": email}} for email in cc_recipients]
            if bcc_recipients:
                message["bccRecipients"] = [{"emailAddress": {"address": email}} for email in bcc_recipients]

            # Handle attachment if provided
            attachments = []
            if attachment_path:
                if not os.path.isfile(attachment_path):
                    return {"success": False, "error": f"Attachment file not found: {attachment_path}"}
                filename = os.path.basename(attachment_path)
                with open(attachment_path, "rb") as f:
                    file_bytes = f.read()
                encoded_content = base64.b64encode(file_bytes).decode("utf-8")
                # Microsoft Graph expects attachments as dicts in the 'attachments' array
                attachments.append({
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": filename,
                    "contentBytes": encoded_content
                })
            if attachments:
                message["attachments"] = attachments

            request_body = {
                "message": message,
                "saveToSentItems": save_to_sent
            }

            # Send email
            send_url = f"{self.graph_endpoint}/users/{self.sender_mailbox}/sendMail"
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json"
            }

            response = requests.post(send_url, headers=headers, json=request_body, timeout=30)

            if response.status_code == 202:
                return {"success": True, "message": "Email sent successfully"}
            else:
                return {
                    "success": False,
                    "error": f"Failed with status {response.status_code}",
                    "details": response.text
                }

        except Exception as e:
            return {"success": False, "error": str(e)}

# COMMAND ----------

# DBTITLE 1,Global Email Sender Instance
# Initialize once and reuse
email_sender = None

def get_email_sender():
    """Get or create email sender instance."""
    global email_sender
    if email_sender is None:
        email_sender = MSGraphEmailSender()
    return email_sender

# COMMAND ----------

# DBTITLE 1,Pipeline Notification Functions
from typing import List, Dict, Any, Optional
from datetime import datetime

def send_success_notification(
    pipeline_name: str,
    environment: str,
    recipients: List[str],
    metrics: Dict[str, Any] = None,
    duration_seconds: int = None,
    attachment_path: Optional[str] = None
) -> bool:
    """Send success notification for pipeline completion."""

    sender = get_email_sender()

    # Build email body
    html_body = f"""
    <html>
    <head>
        <style>
            .success {{ color: #28a745; }}
            .metric {{ background: #f8f9fa; padding: 5px; margin: 5px 0; }}
        </style>
    </head>
    <body>
        <h2 class="success">✅ Pipeline Completed Successfully</h2>
        <p><strong>Pipeline:</strong> {pipeline_name}</p>
        <p><strong>Environment:</strong> {environment.upper()}</p>
        <p><strong>Timestamp:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    """

    if duration_seconds:
        html_body += f"<p><strong>Duration:</strong> {duration_seconds} seconds</p>"

    if metrics:
        html_body += "<h3>Metrics:</h3>"
        for key, value in metrics.items():
            html_body += f'<div class="metric"><strong>{key}:</strong> {value}</div>'

    html_body += """
        <hr>
        <p style="color: #666; font-size: 12px;">Automated notification from Databricks EDP Framework</p>
    </body>
    </html>
    """

    result = sender.send_email(
        to_recipients=recipients,
        subject=f"{environment.upper()} - {pipeline_name} Success",
        body=html_body,
        is_html=True,
        attachment_path=attachment_path
    )

    return result["success"]

def send_failure_notification(
    pipeline_name: str,
    environment: str,
    recipients: List[str],
    error_message: str,
    stack_trace: str = None,
    attachment_path: Optional[str] = None
) -> bool:
    """Send failure notification for pipeline errors."""

    sender = get_email_sender()

    # Build email body
    html_body = f"""
    <html>
    <head>
        <style>
            .failure {{ color: #dc3545; }}
            .error {{ background: #ffebee; padding: 10px; margin: 10px 0; font-family: monospace; }}
        </style>
    </head>
    <body>
        <h2 class="failure">Pipeline Failed</h2>
        <p><strong>Pipeline:</strong> {pipeline_name}</p>
        <p><strong>Environment:</strong> {environment.upper()}</p>
        <p><strong>Timestamp:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>

        <h3>Error Details:</h3>
        <div class="error">{error_message}</div>
    """

    if stack_trace:
        html_body += f"""
        <h3>Stack Trace:</h3>
        <div class="error"><pre>{stack_trace}</pre></div>
        """

    html_body += """
        <hr>
        <p style="color: #666; font-size: 12px;">Automated notification from Databricks EDP Framework</p>
    </body>
    </html>
    """

    result = sender.send_email(
        to_recipients=recipients,
        subject=f"{environment.upper()} - {pipeline_name} Failed",
        body=html_body,
        is_html=True,
        importance="high",
        attachment_path=attachment_path
    )

    return result["success"]

def send_dq_warning_notification(
    pipeline_name: str,
    environment: str,
    recipients: List[str],
    dq_issues: List[Dict[str, Any]],
    attachment_path: Optional[str] = None
) -> bool:
    """Send data quality warning notification."""

    sender = get_email_sender()

    # Build email body
    html_body = f"""
    <html>
    <head>
        <style>
            .warning {{ color: #ffc107; }}
            table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
        </style>
    </head>
    <body>
        <h2 class="warning">⚠️ Data Quality Issues Detected</h2>
        <p><strong>Pipeline:</strong> {pipeline_name}</p>
        <p><strong>Environment:</strong> {environment.upper()}</p>
        <p><strong>Timestamp:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>

        <h3>Issues Found:</h3>
        <table>
            <tr>
                <th>Check Name</th>
                <th>Table</th>
                <th>Severity</th>
                <th>Details</th>
            </tr>
    """

    for issue in dq_issues:
        html_body += f"""
            <tr>
                <td>{issue.get('check_name', 'N/A')}</td>
                <td>{issue.get('table', 'N/A')}</td>
                <td>{issue.get('severity', 'N/A')}</td>
                <td>{issue.get('details', 'N/A')}</td>
            </tr>
        """

    html_body += """
        </table>
        <hr>
        <p style="color: #666; font-size: 12px;">Automated notification from Databricks EDP Framework</p>
    </body>
    </html>
    """

    result = sender.send_email(
        to_recipients=recipients,
        subject=f"{environment.upper()} - {pipeline_name} DQ Warning",
        body=html_body,
        is_html=True,
        attachment_path=attachment_path
    )

    return result["success"]

# COMMAND ----------

def send_custom_email(
    pipeline_name: str,
    environment: str,
    recipients: List[str],
    html_body: str,
    subject: str,
    is_html: bool,
    attachment_path: str = None

) -> bool:

    sender = get_email_sender()
   
    result = sender.send_email(
        to_recipients= recipients,
        subject=subject,
        body=html_body,
        is_html=is_html,
        attachment_path=attachment_path
    )
    if result["success"]:
        print("Email with attachment sent successfully!")
    else:
        print(f"Failed to send with attachment with \nError: {result.get('error')} and \nDetails:  {result.get('details')}")

    return result 

# COMMAND ----------

# DBTITLE 1,Test Function
def test_email():
    """Test email sending capability."""

    sender = get_email_sender()


    result = sender.send_email(
        to_recipients=["dgarayalde@bjs.com"],
        subject="Test from Databricks Volume",
        body="This email is a test.",
        is_html=False
    )
    if result["success"]:
        print("Test email sent successfully!")
    else:
        print(f"Failed to send: {result.get('error')}")

    return result

# COMMAND ----------

# DBTITLE 1,Export Functions for Use in Other Notebooks
# When this notebook is run via %run, these will be available
__all__ = [
    'MSGraphEmailSender',
    'get_email_sender',
    'send_success_notification',
    'send_failure_notification',
    'send_dq_warning_notification',
    'test_email'
]

print("MS Graph Email Sender loaded successfully!")
print("Available functions: send_success_notification, send_failure_notification, send_dq_warning_notification")

# COMMAND ----------

# test_email()
# send_dq_warning_notification(
#     pipeline_name="test 3",
#     environment="dev",
#     recipients=["dgarayalde@bjs.com"],
#     dq_issues=[{
#         "check_name": "test 3",
#         "table": "test 3",
#         "severity": "test 3",
#         "details": "test 3"
#     }])
