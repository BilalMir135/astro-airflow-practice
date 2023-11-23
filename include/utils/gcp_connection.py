from airflow import settings
from airflow.models import Connection

import json

def add_gcp_connection():
    new_conn = Connection(
            conn_id="gcp",
            conn_type='google_cloud_platform',
    )
    extra_field = {
        "extra__google_cloud_platform__project": "spock-main",
        "extra__google_cloud_platform__key_path": '/usr/local/airflow/include/gcp/service_account.json'
    }

    session = settings.Session()

    #checking if connection exist
    if session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).first():
        my_connection = session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).one()
        my_connection.set_extra(json.dumps(extra_field))
        session.add(my_connection)
        session.commit()
    else: #if it doesn't exit create one
        new_conn.set_extra(json.dumps(extra_field))
        session.add(new_conn)
        session.commit()