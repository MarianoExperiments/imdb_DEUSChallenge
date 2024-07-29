from flask import Flask, jsonify, request
import psycopg2
from flask_restx import Api, Resource

app = Flask(__name__)

api = Api(app, version='1.0', title='Professionals API', description='API for professionals information')

def get_db_connection():
    conn = psycopg2.connect(
        host="postgres",
        database="challenge",
        user="admin",
        password="admin"
    )
    return conn

@api.route('/professionals_list')
class ProfessionalsList(Resource):
    @api.doc(params={'profession': 'Choose "actor" or "actress" (default: "actor")'})
    def get(self):
        profession = request.args.get('profession', 'actor')  # Default to 'actor'
        if profession not in ['actor', 'actress']:
            return jsonify({"error": "Invalid profession. Choose 'actor' or 'actress'."}), 400

        query = """
            WITH nb AS(
                SELECT "primaryName" AS name, nconst FROM imdb.name_basics 
            ),
            tp AS(
                SELECT nconst, tconst, category FROM imdb.title_principals WHERE category=%s
            ),
            tb AS(
                SELECT tconst, "runtimeMinutes" FROM imdb.title_basics WHERE "runtimeMinutes" IS NOT NULL
            ),
            tr AS(
                SELECT tconst, "averageRating" FROM imdb.title_ratings
            )
            SELECT 
	            nb.name,
	            AVG(tr."averageRating") AS score,
	            COUNT(DISTINCT tp.tconst) AS number_of_titles_as_principal,
	            SUM(tb."runtimeMinutes") AS total_runtime_minutes
            FROM
	            nb
            JOIN
	            tp ON nb.nconst = tp.nconst
            JOIN
	            tb ON tp.tconst = tb.tconst
            JOIN
	            tr ON tb.tconst = tr.tconst
            GROUP BY
	            nb.name
            ORDER BY
	            score DESC
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
