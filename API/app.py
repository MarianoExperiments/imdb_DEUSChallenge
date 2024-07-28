from flask import Flask, jsonify, request
import psycopg2

app = Flask(__name__)

def get_db_connection():
    conn = psycopg2.connect(
        host="postgres",
        database="challenge",
        user="admin",
        password="admin"
    )
    return conn

@app.route('/professionals', methods=['GET'])
def get_professionals():
    profession = request.args.get('profession', 'actor')  # Default to 'actor'
    if profession not in ['actor', 'actress']:
        return jsonify({"error": "Invalid profession. Choose 'actor' or 'actress'."}), 400

    query = """
        SELECT
            nb."primaryName" AS name,
            AVG(tr."averageRating") AS score,
            COUNT(DISTINCT tp."tconst") AS number_of_titles_as_principal,
            SUM(tb."runtimeMinutes") AS total_runtime_minutes
        FROM
            imdb.name_basics AS nb
        JOIN
            imdb.title_principals AS tp ON nb."tconst" = tp."nconst"
        JOIN
            imdb.title_basics AS tb ON tp."tconst" = tb."tconst"
        JOIN
            imdb.title_ratings AS tr ON tb."tconst" = tr."tconst"
        WHERE
            tp."category" = %s
            AND tb."runtimeMinutes" IS NOT NULL
        GROUP BY
            nb."primaryName"
        # HAVING
        #     SUM(tb."runtimeMinutes") IS NOT NULL
        ORDER BY
            score DESC;
    """

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(query, (profession,))
    results = cur.fetchall()
    cur.close()
    conn.close()

    # Format the results into a list of dictionaries
    professionals = []
    for row in results:
        professional = {
            "name": row[0],
            "score": row[1],
            "number_of_titles_as_principal": row[2]
        }
        if row[3] is not None:
            professional["total_runtime_minutes"] = row[3]
        professionals.append(professional)

    return jsonify(professionals)

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
