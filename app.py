from flask import Flask, jsonify, request, render_template  # <-- Aggiunto render_template
from pymongo import MongoClient

app = Flask(__name__)

# Connessione a MongoDB
MONGO_URI = "mongodb://localhost:27017/"
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["mountain_rescue_db"]
collection = db["telemetry_stream"]

@app.route("/", methods=["GET"])
def home():
    # Ora restituisce la dashboard HTML interattiva invece del JSON grezzo!
    return render_template("index.html")

@app.route("/api/hikers/latest", methods=["GET"])
def get_latest_hikers():
    # ... (il codice di questa funzione rimane identico a prima) ...
    pipeline = [
        {"$sort": {"timestamp": -1}},
        {
            "$group": {
                "_id": "$hiker_id",
                "latest_timestamp": {"$first": "$timestamp"},
                "location": {"$first": "$location"},
                "metrics": {"$first": "$metrics"}
            }
        }
    ]
    results = list(collection.aggregate(pipeline))
    for r in results:
        r["hiker_id"] = r.pop("_id")
    return jsonify(results), 200

@app.route("/api/hikers/nearby", methods=["GET"])
def get_nearby_hikers():
    # ... (il codice di questa funzione rimane identico a prima) ...
    try:
        lat = float(request.args.get("lat", 46.3782))
        lon = float(request.args.get("lon", 13.8366))
        max_distance = float(request.args.get("max_distance", 5000))
    except ValueError:
        return jsonify({"error": "Parametri non validi"}), 400

    query = {
        "location": {
            "$near": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [lon, lat]
                },
                "$maxDistance": max_distance
            }
        }
    }
    results = list(collection.find(query, {"_id": 0}).limit(50))
    return jsonify({
        "search_center": {"lat": lat, "lon": lon},
        "max_distance_m": max_distance,
        "results_found": len(results),
        "data": results
    }), 200

if __name__ == "__main__":
    app.run(debug=True, port=5000)