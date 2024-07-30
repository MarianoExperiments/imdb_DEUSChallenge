from flask import Flask, jsonify, request
import psycopg2
from flask_restx import Api, Resource
import logging

app = Flask(__name__)
logging.basicConfig(level=logging.DEBUG)

api = Api(app, version='1.0', title='Professionals API', description='API for professionals information')

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host="postgres",
            database="challenge",
            user="admin",
            password="admin"
        )
        return conn
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        return None

@api.route('/professionals_list')
class ProfessionalsList(Resource):
    @api.doc(params={'profession': 'Choose "actor" or "actress" (default: "actor")', "limit": "Limit the operation because the API CAN'T handle big amounts of data (default: 1000)"})
    def get(self):
        profession = request.args.get('profession', 'actor')  # Default to 'actor'
        limit = request.args.get('limit', 1000)  # Default to 'actor'
        if profession not in ['actor', 'actress']:
            return jsonify({"error": "Invalid profession. Choose 'actor' or 'actress'."}), 400

        query = """
            SELECT 
	            name,
	            AVG("averageRating") AS score,
	            COUNT(DISTINCT tconst) AS number_of_titles_as_principal,
	            SUM("runtimeMinutes") AS total_runtime_minutes
            FROM
	            imdb.actor_movie_details
            WHERE
                category = %s
            GROUP BY
	            name
            ORDER BY
	            score DESC
            LIMIT %s
        """

        conn = get_db_connection()
        if conn is None:
            return jsonify({"error": "Unable to connect to the database"}), 500
        
        try:
            cur = conn.cursor()
            cur.execute(query, (profession,limit,))
            results = cur.fetchall()
            cur.close()
            conn.close()
        except Exception as e:
            logging.error(f"Error executing query: {e}")
            return jsonify({"error": "Error executing query"}), 500

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
