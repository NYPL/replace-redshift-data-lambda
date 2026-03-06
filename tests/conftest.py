import os
import pytest

TEST_ENV_VARS = {
    "REDSHIFT_DB_NAME": "test_db",
    "REDSHIFT_DB_HOST": "test_redshift_host",
    "REDSHIFT_DB_USER": "test_redshift_user",
    "REDSHIFT_DB_PASSWORD": "test_redshift_password",
    "STAGING_TABLE": "test_staging_table",
    "MAIN_TABLE": "test_main_table",
}


@pytest.fixture(scope="session", autouse=True)
def tests_setup_and_teardown():
    # Will be executed before the first test
    os.environ.update(TEST_ENV_VARS)

    yield

    # Will execute after final test
    for os_config in TEST_ENV_VARS.keys():
        del os.environ[os_config]