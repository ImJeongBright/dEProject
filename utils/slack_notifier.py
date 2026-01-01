import requests
import os
from airflow.models import Variable

def send_slack_alert(context):
    """
    Airflow Task 실패 시 호출되는 Callback 함수
    Slack Webhook을 통해 에러 알림을 전송합니다.
    """
    # 1. Webhook URL 가져오기 (Airflow Variable 혹은 Env Var)
    webhook_url = os.getenv('SLACK_WEBHOOK_URL') or Variable.get("SLACK_WEBHOOK_URL", default_var=None)
    
    if not webhook_url:
        print("Warning: SLACK_WEBHOOK_URL not found. Skipping alert.")
        return

    # 2. Context에서 정보 추출
    ti = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    task_id = ti.task_id
    execution_date = context.get('execution_date').strftime('%Y-%m-%d %H:%M:%S')
    log_url = ti.log_url
    exception = context.get('exception')

    # 3. 메시지 구성 (Slack Block Kit 사용 가능, 여기선 심플하게 text)
    message = {
        "text": ":rotating_light: *Airflow Task Failed* :rotating_light:",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*:rotating_light: Task Failed: {task_id}*"
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*DAG:*\n{dag_id}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Execution Time:*\n{execution_date}"
                    }
                ]
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Exception:*\n```{str(exception)}```"
                }
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "View Logs"
                        },
                        "url": log_url
                    }
                ]
            }
        ]
    }

    # 4. 전송
    try:
        response = requests.post(webhook_url, json=message)
        response.raise_for_status()
        print(f"Sent Slack alert for task {task_id}")
    except Exception as e:
        print(f"Failed to send Slack alert: {e}")
