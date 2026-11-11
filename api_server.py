"""
=====================================================================
API SERVEUR — DASHBOARD WEB DE SUPERVISION SEHO / JANITZA
=====================================================================
A lancer EN PARALLELE de surveillance_janitza.py (qui ecrit
statut_actuel.json + la base surveillance.db).

Ce serveur :
  - Lit le snapshot JSON temps reel (statut_actuel.json / /dev/shm)
  - Interroge la base SQLite (lecture seule, mode WAL -> pas de blocage)
  - Calcule l'historique de courant/puissance pour les graphes
  - Calcule l'energie cumulee (totale + aujourd'hui) par phase
  - Sert la page dashboard.html sur http://<IP_DU_PI>:5000/

Lancement :
    pip install flask
    python3 api_server.py
=====================================================================
"""

import json
import os
import sqlite3
from datetime import datetime, date, timedelta

from flask import Flask, jsonify, send_from_directory

# --- Doit correspondre a surveillance_janitza.py -----------------------
DOSSIER_BASE = os.path.dirname(os.path.abspath(__file__))
FICHIER_DB   = os.path.join(DOSSIER_BASE, "surveillance.db")
DOSSIER_LIVE = "/dev/shm" if os.path.isdir("/dev/shm") else os.path.join(DOSSIER_BASE, "logs")
FICHIER_JSON_LIVE = os.path.join(DOSSIER_LIVE, "statut_actuel.json")
FICHIER_JSON_PRED = os.path.join(DOSSIER_LIVE, "predictions_actuelles.json")

TENSION_NOMINALE_V = 228.0

app = Flask(__name__)


def _connexion_db():
    conn = sqlite3.connect(f"file:{FICHIER_DB}?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


def _lire_json(chemin):
    try:
        with open(chemin, "r") as f:
            return json.load(f)
    except Exception:
        return None


@app.route("/api/status")
def api_status():
    """Snapshot temps reel : courants, puissances, energie, sante, alertes."""
    snap = _lire_json(FICHIER_JSON_LIVE)
    if snap is None:
        return jsonify({"erreur": "snapshot indisponible — le script de surveillance tourne-t-il ?"}), 503
    return jsonify(snap)


@app.route("/api/predictions")
def api_predictions():
    pred = _lire_json(FICHIER_JSON_PRED)
    return jsonify(pred or {})


@app.route("/api/historique/courant")
def api_historique_courant():
    """Derniers echantillons de courant (table mesures) pour le graphe live."""
    try:
        conn = _connexion_db()
        cur = conn.execute(
            "SELECT horodatage, I1_A, I2_A, I3_A FROM mesures "
            "ORDER BY id DESC LIMIT 300"
        )
        lignes = [dict(r) for r in cur.fetchall()]
        lignes.reverse()
        conn.close()
        return jsonify(lignes)
    except Exception as e:
        return jsonify({"erreur": str(e)}), 500


@app.route("/api/historique/puissance_horaire")
def api_puissance_horaire():
    """
    Puissance moyenne + energie par heure glissante sur les dernieres 24h,
    calculee a partir des moyennes de bloc (table blocs, un point / 10 min).
    """
    try:
        conn = _connexion_db()
        depuis = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
        cur = conn.execute(
            "SELECT debut, I1_moy_A, I2_moy_A, I3_moy_A FROM blocs "
            "WHERE debut >= ? ORDER BY id ASC",
            (depuis,)
        )
        points = []
        for r in cur.fetchall():
            i1, i2, i3 = r["I1_moy_A"] or 0, r["I2_moy_A"] or 0, r["I3_moy_A"] or 0
            p_kw = round(TENSION_NOMINALE_V * (i1 + i2 + i3) / 1000.0, 3)
            points.append({"horodatage": r["debut"], "puissance_kW": p_kw})
        conn.close()
        return jsonify(points)
    except Exception as e:
        return jsonify({"erreur": str(e)}), 500


@app.route("/api/energie")
def api_energie():
    """
    Energie cumulee (depuis toujours) + energie des 14 derniers jours,
    a partir de rapport_journalier (rempli une fois par jour) complete
    par la journee en cours issue du snapshot live.
    """
    try:
        conn = _connexion_db()
        cur = conn.execute(
            "SELECT date, energie_jour_kWh FROM rapport_journalier "
            "ORDER BY date DESC LIMIT 14"
        )
        historique = [dict(r) for r in cur.fetchall()]
        historique.reverse()
        conn.close()
    except Exception:
        historique = []

    snap = _lire_json(FICHIER_JSON_LIVE) or {}
    energie_live = (snap.get("energie") or {})
    aujourdhui = date.today().strftime("%Y-%m-%d")

    # Remplace/ajoute la journee en cours par la valeur live (plus fraiche
    # que rapport_journalier, qui n'est ecrit qu'au changement de jour).
    historique = [h for h in historique if h["date"] != aujourdhui]
    historique.append({
        "date": aujourdhui,
        "energie_jour_kWh": energie_live.get("jour_kWh", {}).get("totale", 0.0),
    })

    return jsonify({
        "cumulee_kWh": energie_live.get("cumulee_kWh"),
        "jour_kWh": energie_live.get("jour_kWh"),
        "historique_14j": historique,
    })


@app.route("/api/alertes")
def api_alertes():
    try:
        conn = _connexion_db()
        cur = conn.execute(
            "SELECT horodatage, type_alerte, phase, message, statut FROM alertes "
            "ORDER BY id DESC LIMIT 30"
        )
        lignes = [dict(r) for r in cur.fetchall()]
        conn.close()
        return jsonify(lignes)
    except Exception as e:
        return jsonify({"erreur": str(e)}), 500


@app.route("/api/sante")
def api_sante():
    """Derniers indices de sante par phase + MTBF."""
    try:
        conn = _connexion_db()
        resultat = {}
        for phase in ("I1", "I2", "I3"):
            cur = conn.execute(
                "SELECT indice_sante, categorie, mtbf_estime_h, nb_pannes_historique, "
                "heures_cumulees, cycles_cumules FROM indice_sante "
                "WHERE phase = ? ORDER BY id DESC LIMIT 1",
                (phase,)
            )
            row = cur.fetchone()
            resultat[phase] = dict(row) if row else None
        conn.close()
        return jsonify(resultat)
    except Exception as e:
        return jsonify({"erreur": str(e)}), 500


def recuperer_dernieres_predictions():
    """
    Retourne, pour chacune des 3 phases (I1/I2/I3), la derniere ligne
    generee dans la table 'predictions' (module predictif du script de
    surveillance). Une seule requete SQL : jointure entre la table et
    le MAX(id) par phase, ce qui donne directement les 3 lignes les
    plus recentes groupees par phase (MAX(id) est plus fiable que
    MAX(horodatage) en cas d'egalite de timestamp a la seconde pres).
    """
    try:
        conn = _connexion_db()
        cur = conn.execute("""
            SELECT p.phase, p.horodatage, p.nb_blocs_utilises, p.pente_A_par_bloc,
                   p.r2, p.I_actuel_estime_A, p.tendance_significative,
                   p.heures_avant_panne, p.message
            FROM predictions p
            INNER JOIN (
                SELECT phase, MAX(id) AS max_id
                FROM predictions
                GROUP BY phase
            ) dernier ON p.phase = dernier.phase AND p.id = dernier.max_id
        """)
        resultat = {row["phase"]: dict(row) for row in cur.fetchall()}
        conn.close()
        return resultat
    except Exception as e:
        return {"erreur": str(e)}


@app.route("/api/predictions_db")
def api_predictions_db():
    """Derniere prediction (table SQLite 'predictions') pour I1/I2/I3 —
    alimente le panneau 'Indice de sante & MTBF' du dashboard."""
    return jsonify(recuperer_dernieres_predictions())


@app.route("/")
def dashboard():
    return send_from_directory(DOSSIER_BASE, "dashboard.html")


if __name__ == "__main__":
    print(f"Base SQLite     : {FICHIER_DB}")
    print(f"Snapshot live   : {FICHIER_JSON_LIVE}")
    print("Dashboard sur   : http://0.0.0.0:5000/  (ou http://<IP_DU_RASPBERRY>:5000/)")
    app.run(host="0.0.0.0", port=5000, debug=False)
