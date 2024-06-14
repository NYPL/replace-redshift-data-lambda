import json
import pytest

from copy import deepcopy
from lambda_function import (
    _DUPLICATE_DELETION_QUERY,
    _DUPLICATES_QUERY,
    _MAIN_DELETION_QUERY,
    lambda_handler,
    ReplaceRedshiftDataError,
)
from tests.test_helpers import TestHelpers

_PLACEHOLDER = "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s"

_PRIMARY_REDSHIFT_QUERIES = [
    (
        _MAIN_DELETION_QUERY.format(
            main_table="test_main_table", staging_table="test_staging_table"
        ),
        None,
    ),
    ("INSERT INTO test_main_table SELECT * FROM test_staging_table;", None),
    ("DELETE FROM test_staging_table;", None),
]

_TEST_PATRONS = [
    ["patron1", "address1", "11111", "11111111111", "2024-01-01", "2024-06-01",
     "2024-03-01", 1, 2, "aa", "bb", "patron1", 10],
    ["patron2", "address2", None, None, "2024-02-02", None, None, None, None, None,
     None, "patron2", 20],
]


class TestLambdaFunction:

    @classmethod
    def setup_class(cls):
        TestHelpers.set_env_vars()

    @classmethod
    def teardown_class(cls):
        TestHelpers.clear_env_vars()

    @pytest.fixture
    def test_instance(self, mocker):
        mocker.patch("lambda_function.create_log")
        mock_kms_client = mocker.MagicMock()
        mock_kms_client.decrypt.return_value = "decrypted"
        mocker.patch("lambda_function.KmsClient", return_value=mock_kms_client)

    def get_mock_redshift_client(self, mocker, response):
        mock_redshift_client = mocker.MagicMock()
        mock_redshift_client.execute_query.return_value = response
        mocker.patch(
            "lambda_function.RedshiftClient", return_value=mock_redshift_client
        )
        return mock_redshift_client

    def test_lambda_handler_no_duplicates(self, test_instance, mocker):
        mock_redshift_client = self.get_mock_redshift_client(mocker, [])

        assert lambda_handler(None, None) == {
            "statusCode": 200,
            "body": json.dumps({"message": "Job ran successfully."}),
        }

        mock_redshift_client.connect.assert_called_once()
        mock_redshift_client.execute_query.assert_called_once_with(
            _DUPLICATES_QUERY.format(staging_table="test_staging_table")
        )
        mock_redshift_client.execute_transaction.assert_called_once_with(
            _PRIMARY_REDSHIFT_QUERIES
        )
        mock_redshift_client.close_connection.assert_called_once()

    def test_lambda_handler_exact_duplicates(self, test_instance, mocker):
        EXACT_DUPLICATE_PATRONS = _TEST_PATRONS + _TEST_PATRONS
        mock_redshift_client = self.get_mock_redshift_client(
            mocker, EXACT_DUPLICATE_PATRONS
        )

        assert lambda_handler(None, None) == {
            "statusCode": 200,
            "body": json.dumps({"message": "Job ran successfully."}),
        }

        mock_redshift_client.connect.assert_called_once()
        mock_redshift_client.execute_query.assert_called_once_with(
            _DUPLICATES_QUERY.format(staging_table="test_staging_table")
        )
        mock_redshift_client.execute_transaction.assert_has_calls(
            [
                mocker.call(
                    [
                        (
                            _DUPLICATE_DELETION_QUERY.format(
                                staging_table="test_staging_table",
                                duplicate_ids="'patron1','patron2'",
                            ),
                            None,
                        ),
                        (
                            f"INSERT INTO test_staging_table VALUES ({_PLACEHOLDER});",
                            [v[:-2] for v in _TEST_PATRONS],
                        ),
                    ]
                ),
                mocker.call(_PRIMARY_REDSHIFT_QUERIES),
            ]
        )
        mock_redshift_client.close_connection.assert_called_once()

    def test_lambda_handler_inexact_duplicates(self, test_instance, mocker):
        INEXACT_DUPLICATE_PATRONS = deepcopy(_TEST_PATRONS) + deepcopy(_TEST_PATRONS)
        INEXACT_DUPLICATE_PATRONS[-1][1] = "different_address"
        mock_redshift_client = self.get_mock_redshift_client(
            mocker, INEXACT_DUPLICATE_PATRONS
        )

        with pytest.raises(ReplaceRedshiftDataError) as e:
            lambda_handler(None, None)

        assert "Duplicate patron ids with different values found" in e.value.message
        mock_redshift_client.connect.assert_called_once()
        mock_redshift_client.execute_query.assert_called_once_with(
            _DUPLICATES_QUERY.format(staging_table="test_staging_table")
        )
        mock_redshift_client.execute_transaction.assert_not_called()
