import sys, os

from unittest import mock

from app import hello, csv
import db, db2csv



SAMPLE_EVENT = {
    "event_ts": "1610429825.000500",
    "item": {
        "channel": "G01K6DQ7TJ4",
        "ts": "1610428341.000200",
        "type": "message"
    },
    "item_user": "U01DQLUCR71",
    "key": "G01K6DQ7TJ4:1610428341.000200:U01DQLUCR71:sunglasses",
    "reaction": "sunglasses",
    "type": "reaction_added",
    "user": "U01DQLUCR71"
}


def test_create_table():
    rv = db.create_table()
    assert ('CREATING', 'CREATING') == rv


def test_put_event():
    rv = db.put_event(SAMPLE_EVENT)
    assert 200 == rv['ResponseMetadata']['HTTPStatusCode']


def test_put_attendance():
    rv = db.put_attendance(SAMPLE_EVENT, channel_id='G01K6DQ7TJ4')
    assert "SUCCESS" == rv


def test_db2csv():
    rv = db2csv.db2csv()
    assert 'user_id,username,z20210112\nU01DQLUCR71,test_user,14:37:05\n' == rv

def test_delete_table():
    rv = db.delete_table()
    assert ('DELETING', 'DELETING') == rv