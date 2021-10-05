import os
import uuid
from flask import Flask, jsonify, Response
import psycopg2
from psycopg2.errors import SerializationFailure


app = Flask(__name__)

conn = psycopg2.connect(
    database=os.environ.get("DB_NAME"),
    user=os.environ.get("DB_USER"),
    password=os.environ.get("DB_PASSWORD"),
    sslmode=os.environ.get("DB_SSL"),
    host=os.environ.get("DB_HOST"),
    port=os.environ.get("DB_PORT"),
)


@app.post('/accounts/insert/<balance>')
def insert_account(balance: int):
    _id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute("INSERT INTO accounts (id, balance) VALUES (%s, %s)", (_id, balance))
    conn.commit()
    return jsonify({'id': _id})


@app.post('/accounts/read/<key>')
def read_account(key: str):
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM accounts WHERE id = %s", (key, ))
        account = cur.fetchall()[0]
    return jsonify({'account': account})


@app.post('/accounts/update/<key>/<balance>')
def update_account(key: str, balance: int):
    with conn.cursor() as cur:
        cur.execute("UPSERT INTO accounts (id, balance) VALUES (%s, %s)", (key, balance))
    conn.commit()
    return jsonify({'id': key})


@app.post('/accounts/transfer/<out_key>/<in_key>/<amount>')
def transfer_account(out_key: str, in_key: str, amount: int):
    with conn.cursor() as cur:

        # Check the current balance.
        cur.execute("SELECT balance FROM accounts WHERE id = %s", (out_key,))
        from_balance = cur.fetchone()[0]
        if from_balance < int(amount):
            raise RuntimeError(
                f"Insufficient funds in {out_key}: have {from_balance}, need {amount}"
            )

        # Perform the transfer.
        cur.execute(
            "UPDATE accounts SET balance = balance - %s WHERE id = %s", (amount, out_key)
        )
        cur.execute(
            "UPDATE accounts SET balance = balance + %s WHERE id = %s", (amount, in_key)
        )
    try:
        conn.commit()
    except SerializationFailure:
        conn.rollback()
    return Response('Success', status=200)


if __name__ == '__main__':
    app.run(debug=False)
