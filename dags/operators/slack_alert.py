import os

from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator


ENV = os.environ.get('ENVIRONMENT')


class SlackAlert:
    def __init__(self, connection_id='slack_conn'):
        self.connection_id = connection_id

    def alert(self, context):
        message_template = ':red_siren: Task failure in {}: {} - {}'
        message = message_template.format(ENV, context['dag_run'], context['task_instance'])

        slack_operator = SlackWebhookOperator(
            task_id='slack_alert',
            http_conn_id=self.connection_id,
            message=message,
            username='Airflow')

        slack_operator.execute(context)



