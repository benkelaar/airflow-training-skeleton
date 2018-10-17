from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

import os
import tempfile


class HttpToGcsOperator(BaseOperator):
    """
    Calls an endpoint on an HTTP system to execute an action

    :param http_conn_id: The connection to run the operator against
    :type http_conn_id: string
    :param endpoint: The relative part of the full url. (templated)
    :type endpoint: string
    :param gcs_path: The path of the GCS to store the result
    :type gcs_path: string
    """

    template_fields = ('endpoint', 'gcs_path')
    template_ext = ()
    ui_color = '#ff69b4'

    @apply_defaults
    def __init__(self,
                 http_conn_id,
                 endpoint,
                 gcs_path,
                 *args, **kwargs):
        super(HttpToGcsOperator, self).__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint
        self.gcs_path = gcs_path

    def execute(self, context):
        result = HttpHook(http_conn_id=self.http_conn_id, method='GET').run(endpoint=self.endpoint)

        file, path = tempfile.mkstemp()
        try:
            with os.fdopen(file, 'w') as tmp:
                tmp.write(result.text)
                GoogleCloudStorageHook().upload(
                    bucket='airflow-training-simple-dag',
                    object=self.gcs_path,
                    mime_type='application/json',
                    filename=path)
        finally:
            os.remove(path)


