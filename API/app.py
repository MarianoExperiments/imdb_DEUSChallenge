from flask import Flask, jsonify, request
import psycopg2

app = Flask(__name__)

def get_db_connection():
    conn = psycopg2.connect(
        dbname='challenge',
        user='admin',
        password='admin',
        host='postgres'
    )
    return conn

@app.route('/test', methods=['GET'])
def get_test():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM public.dag")
    test_result = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(test_result)

if __name__ == '__main__':
    app.run(host='0.0.0.0')
