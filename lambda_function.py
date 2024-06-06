import json
import os

from nypl_py_utils.classes.kms_client import KmsClient
from nypl_py_utils.classes.redshift_client import RedshiftClient
from nypl_py_utils.functions.log_helper import create_log

logger = create_log("lambda_function")

_MAIN_DELETION_QUERY = """
    DELETE FROM {main_table}
    USING {staging_table}
    WHERE {main_table}.patron_id = {staging_table}.patron_id;"""

_DUPLICATE_DELETION_QUERY = """
    DELETE FROM {staging_table}
    WHERE patron_id IN ({duplicate_ids});"""

_DUPLICATES_QUERY = """
    SELECT *
    FROM {staging_table} JOIN
    (
        SELECT patron_id, COUNT(patron_id) AS patron_count
        FROM {staging_table}
        GROUP BY patron_id
    ) t
    ON t.patron_id = {staging_table}.patron_id
    WHERE patron_count > 1;"""


def lambda_handler(event, context):
    logger.info("Starting lambda processing")
    kms_client = KmsClient()
    redshift_client = RedshiftClient(
        kms_client.decrypt(os.environ["REDSHIFT_DB_HOST"]),
        os.environ["REDSHIFT_DB_NAME"],
        kms_client.decrypt(os.environ["REDSHIFT_DB_USER"]),
        kms_client.decrypt(os.environ["REDSHIFT_DB_PASSWORD"]),
    )
    kms_client.close()
    redshift_client.connect()

    logger.info("Checking for duplicate records")
    raw_duplicates = redshift_client.execute_query(
        _DUPLICATES_QUERY.format(staging_table=os.environ["STAGING_TABLE"])
    )
    unique_map = {}
    for row in raw_duplicates:
        id = row[0]
        if id not in unique_map:
            unique_map[id] = row
        elif unique_map[id] != row:
            logger.error("Duplicate patron ids with different values found")
            raise ReplaceRedshiftDataError(
                "Duplicate patron ids with different values found"
            )
    # If there are duplicate rows, delete all of them and insert each row back
    # individually into the staging table
    if len(unique_map.keys()) > 0:
        duplicate_ids = "'" + "','".join(unique_map.keys()) + "'"
        queries = [
            (
                _DUPLICATE_DELETION_QUERY.format(
                    staging_table=os.environ["STAGING_TABLE"],
                    duplicate_ids=duplicate_ids,
                ),
                None,
            )
        ]

        # Need to include %s for each value that's being inserted. This is
        # len(row)-2 because the row contains two extra fields from the join.
        placeholder_length = len(next(iter(unique_map.values()))) - 2
        placeholder = ", ".join(["%s"] * placeholder_length)
        insert_query = "INSERT INTO {staging_table} VALUES ({placeholder});".format(
            staging_table=os.environ["STAGING_TABLE"], placeholder=placeholder
        )
        for value in unique_map.values():
            queries.append((insert_query, value[:-2]))
        redshift_client.execute_transaction(queries)

    redshift_client.execute_transaction(
        [
            (
                _MAIN_DELETION_QUERY.format(
                    main_table=os.environ["MAIN_TABLE"],
                    staging_table=os.environ["STAGING_TABLE"],
                ),
                None,
            ),
            (
                "INSERT INTO {main_table} SELECT * FROM {staging_table};".format(
                    main_table=os.environ["MAIN_TABLE"],
                    staging_table=os.environ["STAGING_TABLE"],
                ),
                None,
            ),
            (
                "DELETE FROM {staging_table};".format(
                    staging_table=os.environ["STAGING_TABLE"]
                ),
                None,
            ),
        ]
    )
    redshift_client.close_connection()

    logger.info("Finished lambda processing")
    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Job ran successfully."}),
    }


class ReplaceRedshiftDataError(Exception):
    def __init__(self, message=None):
        self.message = message
