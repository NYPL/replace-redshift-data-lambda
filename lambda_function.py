import json
import os
import redshift_connector

from botocore.exceptions import ClientError
from nypl_py_utils.classes.kms_client import KmsClient
from nypl_py_utils.functions.log_helper import create_log

logger = create_log('lambda_function')


def lambda_handler(event, context):
    logger.info('Connecting to Redshift')
    kms_client = KmsClient()
    try:
        connection = redshift_connector.connect(
            host=kms_client.decrypt(os.environ['REDSHIFT_DB_HOST']),
            database=os.environ['REDSHIFT_DB_NAME'],
            user=kms_client.decrypt(os.environ['REDSHIFT_DB_USER']),
            password=kms_client.decrypt(os.environ['REDSHIFT_DB_PASSWORD']))
        kms_client.close()
    except ClientError as e:
        connection = None
        kms_client.close()
        logger.error('Error connecting to database: {}'.format(e))
        raise ReplaceRedshiftDataError(
            'Error connecting to database: {}'.format(e)) from None

    logger.info('Starting transaction')
    try:
        cursor = connection.cursor()
        cursor.execute('SELECT COUNT(*) FROM {staging_table};'.format(
            staging_table=os.environ['STAGING_TABLE']))
        total_length = cursor.fetchall()[0][0]
        cursor.execute(
            'SELECT COUNT(DISTINCT patron_id) FROM {staging_table};'.format(
                staging_table=os.environ['STAGING_TABLE']))
        distinct_length = cursor.fetchall()[0][0]
        if total_length != distinct_length:
            logger.error('Number of distinct patrons does not equal total '
                         'number of rows')
            raise ReplaceRedshiftDataError(
                'Number of distinct patrons does not equal total number of '
                'rows') from None

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
        connection.commit()

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
