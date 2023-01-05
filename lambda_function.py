import json
import os
import redshift_connector

from botocore.exceptions import ClientError
from helpers.log_helper import create_log

logger = create_log('lambda_function')


def lambda_handler(event, context):
    logger.info('Connecting to Redshift')
    try:
        connection = redshift_connector.connect(
            iam=True,
            cluster_identifier=os.environ['REDSHIFT_CLUSTER'],
            database=os.environ['REDSHIFT_DB_NAME'],
            db_user=os.environ['REDSHIFT_DB_USER'],
            region=os.environ['AWS_REGION'],
            access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    except ClientError as e:
        connection = None
        logger.error('Error connecting to database: {}'.format(e))
        raise ReplaceRedshiftDataError(
            'Error connecting to database: {}'.format(e)) from None

    logger.info('Starting transaction')
    try:
        cursor = connection.cursor()
        cursor.execute('BEGIN TRANSACTION;')
        cursor.execute((
            'DELETE FROM {main_table} '
            'USING {staging_table} '
            'WHERE {main_table}.patron_id = {staging_table}.patron_id;'
        ).format(main_table=os.environ['MAIN_TABLE'],
                 staging_table=os.environ['STAGING_TABLE']))
        cursor.execute(
            'INSERT INTO {main_table} SELECT * FROM {staging_table};'.format(
                main_table=os.environ['MAIN_TABLE'],
                staging_table=os.environ['STAGING_TABLE']
            ))
        cursor.execute('DELETE FROM {staging_table};'.format(
            staging_table=os.environ['STAGING_TABLE']))
        cursor.execute('END TRANSACTION;')

        logger.info('Finished transaction')
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Job ran successfully."
            }),
        }
    except Exception as e:
        connection.rollback()
        logger.error('Error executing transaction: {}'.format(e))
        raise ReplaceRedshiftDataError(
            'Error executing queries: {}'.format(e)) from None
    finally:
        logger.info('Closing connections')
        cursor.close()
        connection.close()


class ReplaceRedshiftDataError(Exception):
    def __init__(self, message=None):
        self.message = message
