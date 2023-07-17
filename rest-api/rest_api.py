from flask import Flask
import psycopg2

from dotenv import load_dotenv
import os
import time

import pandas as pd
import numpy as np

from sklearn.preprocessing import StandardScaler

import tensorflow as tf
from tensorflow import keras

import joblib


app = Flask(__name__)

@app.route('/')
def hello():
    return 'Hello, World!'

@app.route('/pwd')
def pwd():
    return os.getcwd()

@app.route('/printdir')
def printdir():
    return os.listdir()


def load_scaler():
    scalerx = joblib.load("../models/scalerx.save")
    scalery = joblib.load("../models/scalery.save")
    return scalerx, scalery

def load_model():
    model = keras.models.load_model("../models/nn_model.h5")
    return model

def connect_to_db():
    # load dotenv
    load_dotenv()

    # connect to database
    conn = psycopg2.connect(
        host=os.getenv("HOST"),
        database=os.getenv("DBNAME"),
        user=os.getenv("DBUSER"),
        password=os.getenv("PASS"),
        port=os.getenv("PORT")
    )

    # create cursor
    cursor = conn.cursor()
    return cursor

def get_today():
    today = time.strftime("%Y-%m-%d")
    return today

def get_data(cursor, symbol):
    query = """
                SELECT * FROM one_hour_candle
                WHERE symbol = %s AND bucket >= NOW() - INTERVAL '40 days'
                ORDER BY bucket;
    """

    cursor.execute(query, (symbol,))
    results = cursor.fetchall()
    return results

def transform_data(results):
    ts = pd.DataFrame(results, columns=["bucket", "symbol", "open", "high", "low", "close", "volume"])
    ts["bucket"] = pd.to_datetime(ts["bucket"])
    ts.set_index("bucket", inplace=True)
    return ts

def rolling_window(ts):
    time_steps_past = 6*24 # 6 days * 24 hours
    X = []

    for i in range(time_steps_past, len(ts)):
        X.append(ts[i-time_steps_past:i])

    X = np.array(X)
    return X

@app.route('/api/predict_AAPL_tomorrow')
def predict_tomorrow():
    # load scaler
    scalerx, scalery = load_scaler()

    # load model
    model = load_model()

    # connect to database
    cursor = connect_to_db()

    # get data
    results = get_data(cursor, "AAPL")

    # transform data
    ts = transform_data(results)

    # create dataset X and y, rolling window of 7 days
    df = ts['close']
    X = rolling_window(df)

    # scale
    X1 = scalerx.transform(X)

    # predict
    y_pred = model.predict(X)

    # inverse transform
    y_pred = scalery.inverse_transform(y_pred)

    # return last prediction as string
    return str(y_pred[-1][0])


if __name__ == "__main__":
    app.run()
