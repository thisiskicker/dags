# -*- coding: utf-8 -*-
import pandas
import pysftp


def insert_mongo():
    with sftp.open("file.csv") as f:
        df = pandas.read_csv(f)
        db.collection.insert_many(df.to_dict('records'))
