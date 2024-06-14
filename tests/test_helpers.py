import os


class TestHelpers:
    ENV_VARS = {
        "REDSHIFT_DB_NAME": "test_db",
        "REDSHIFT_DB_HOST": "test_redshift_host",
        "REDSHIFT_DB_USER": "test_redshift_user",
        "REDSHIFT_DB_PASSWORD": "test_redshift_password",
        "STAGING_TABLE": "test_staging_table",
        "MAIN_TABLE": "test_main_table",
    }

    @classmethod
    def set_env_vars(cls):
        for key, value in cls.ENV_VARS.items():
            os.environ[key] = value

    @classmethod
    def clear_env_vars(cls):
        for key in cls.ENV_VARS.keys():
            if key in os.environ:
                del os.environ[key]
