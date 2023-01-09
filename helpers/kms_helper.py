import boto3
import os

from base64 import b64decode
from binascii import Error as base64Error
from botocore.exceptions import ClientError
from helpers.log_helper import create_log

logger = create_log('kms_helper')


def decrypt(encrypted_text):
    try:
        kms_client = boto3.client(
            'kms', region_name=os.environ.get('AWS_REGION', 'us-east-1'))
        decoded_text = b64decode(encrypted_text)
        return kms_client.decrypt(CiphertextBlob=decoded_text)[
            'Plaintext'].decode('utf-8')
    except (ClientError, base64Error, TypeError) as e:
        logger.error('Could not decrypt \'{val}\': {err}'.format(
            val=encrypted_text, err=e))
        raise KmsHelperError('Could not decrypt \'{val}\': {err}'.format(
            val=encrypted_text, err=e)) from None


class KmsHelperError(Exception):
    def __init__(self, message=None):
        self.message = message
