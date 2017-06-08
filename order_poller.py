from __future__ import print_function

import boto3
import os, time
import base64
from base64 import b64decode

import httplib2
from apiclient import discovery
from apiclient import errors
from oauth2client.service_account import ServiceAccountCredentials

from mysql.connector import connect

SCOPES = 'https://www.googleapis.com/auth/gmail.readonly'
CLIENT_EMAIL = 'tanya@fusspotandfoodie.com'

# Decrypt code should run once and variables stored outside of the function
# handler so that these are decrypted once per container
HOST = boto3.client('kms').decrypt(CiphertextBlob=b64decode(os.environ['host']))['Plaintext']
USER = boto3.client('kms').decrypt(CiphertextBlob=b64decode(os.environ['user']))['Plaintext']
PASSWORD = boto3.client('kms').decrypt(CiphertextBlob=b64decode(os.environ['password']))['Plaintext']
DATABASE = boto3.client('kms').decrypt(CiphertextBlob=b64decode(os.environ['database']))['Plaintext']
JSON_KEY = boto3.client('kms').decrypt(CiphertextBlob=b64decode(os.environ['json_key']))['Plaintext']


def run(json_input, context):
    config = {
        'user': USER,
        'password': PASSWORD,
        'host': HOST,
        'database': DATABASE,
        'raise_on_warnings': True,
        'use_pure': False,
    }

    cnx = connect(**config)
    cursor = cnx.cursor()

    q = "select value from calc_parameters where name='order_poller_last_polled'"
    cursor.execute(q)

    last_polled = None
    for (value,) in cursor:
        last_polled = int(value)

    print('last_polled={val}'.format(val=last_polled))

    messages = poll(last_polled)
    for message in messages:
        persist(cursor, message)

    if not messages:
        print('No messages')

    next_poll = int(time.time())
    print('next_poll={val}'.format(val=next_poll))

    q = "update calc_parameters set value={val} where name='order_poller_last_polled'".format(val=next_poll)
    cursor.execute(q)

    cnx.commit()

    cursor.close()
    cnx.close()


def poll(last_polled):
    """
    Poll Gmail inbox for new orders
    """
    try:
        json_data = JSON_KEY.replace('\n', '')
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(json_data, scopes=SCOPES)
        delegated_credentials = credentials.create_delegated(CLIENT_EMAIL)

        http = delegated_credentials.authorize(httplib2.Http())
        service = discovery.build('gmail', 'v1', http=http)

        query = 'from:no-reply-commerce@wix.com AND after:{last}'.format(last=int(last_polled))
        response = service.users().messages().list(userId=CLIENT_EMAIL, q=query).execute()

        message_ids = []
        if 'messages' in response:
            message_ids.extend(response['messages'])

        while 'nextPageToken' in response:
            page_token = response['nextPageToken']
            response = service.users().messages().list(userId=CLIENT_EMAIL, q=query,
                                                       pageToken=page_token).execute()

        messages = []
        for msg_ids in message_ids:
            msg_id = msg_ids['id']
            message = service.users().messages().get(userId=CLIENT_EMAIL, id=msg_id, format='raw').execute()
            msg_snippet = message['snippet']
            msg_raw = base64.urlsafe_b64decode(message['raw'].encode('ASCII'))

            messages.append((msg_id, msg_snippet, msg_raw))
        return messages

    except errors.HttpError, error:
        print('An error occurred: %s' % error)


def persist(cursor, message):
    """
    Persist raw messages in database
    """
    msg_id, msg_snippet, msg_raw = message
    print('msg_id={id}'.format(id=msg_id))
    received_ts = '2017-05-22 10:15:32'
    q = "insert into order_poller (received_ts, message_id, message_snippet, message_raw, is_captured) " \
        "values (%s, %s, %s, %s, %s)"
    data = (received_ts, msg_id, msg_snippet, msg_raw, 0)

    cursor.execute(q, data)


if __name__ == '__main__':
    run(json_input=None, context=None)
