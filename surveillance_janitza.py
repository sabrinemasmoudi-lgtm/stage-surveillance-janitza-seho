"""
=====================================================================
SYSTEME DE SURVEILLANCE — RESISTANCES DE PRECHAUFFAGE TRIPHASEES
Janitza UMG 96RM-E-RCM + Raspberry Pi 4/5 — Modbus TCP
Machine SEHO MWS 2300 (2340-LRT) — Marquardt Tunisie
=====================================================================
Fonctions :
  - Acquisition continue (2s), filtrage SSR PAR PHASE
  - Detection de panne franche + desequilibre inter-phases (avec phase
    associee stockee en base, pour un calcul MTBF fiable)
  - Matrice de signature (ratios mutuels I1/I2/I3)
  - Module predictif : tendance lineaire + estimation temps avant panne
  - Indice de sante combine (0-100%) par phase (usage + cycles + tendance)
  - MTBF estime par phase (heures cumulees / nombre de pannes reelles)
  - Score de sante GLOBAL machine (combine les 3 phases)
  - Watchdog de communication Janitza (alerte a 10s puis 30s sans reponse)
  - Rapport journalier automatique (production, arret, cycles, alertes)
  - Puissance ESTIMEE (tension nominale fixe 228V, pas de cable tension)
  - Journalisation SQLite (mode WAL — lectures concurrentes sans blocage)
  - Export JSON temps reel (statut_actuel.json) pour l'application mobile
  - Export JSON predictif (predictions_actuelles.json) a chaque bloc
  - Notifications push Firebase — OPTIONNEL, desactive proprement si absent
=====================================================================
A LANCER EN PARALLELE DE : api_server.py (serveur Flask pour le mobile)
=====================================================================
"""

import json
import logging
import os
import sqlite3
import struct
import time
from dataclasses import dataclass, field
from datetime import datetime, date
from statistics import mean

from pymodbus.client import ModbusTcpClient

# =====================================================================
# CONFIGURATION RESEAU & JANITZA
# =====================================================================
IP_JANITZA               = "192.168.1.100"
PORT_MODBUS              = 502
DEVICE_ID                = 1

INTERVALLE_LECTURE_S     = 2       # secondes entre chaque mesure
DUREE_BLOC_S             = 600     # 10 minutes par bloc d'analyse

# --- Seuils de detection ---------------------------------------------
SEUIL_PANNE_A            = 2.0     # A — en dessous : phase coupee/disjonctee
SEUIL_MACHINE_ARRET_A    = 1.5     # A — sous ce seuil sur les 3 phases : machine a l'arret
SEUIL_DESEQUILIBRE_PCT   = 30.0    # % — asymetrie structurelle bac a etain
DESEQUILIBRE_NOMINAL_PCT = 24.7    # % — ecart structurel NORMAL mesure sur E1/E2/E3

# --- Relais statiques (SSR) — anti-fausses-alertes -------------------
SEUIL_SSR_ETEINT_A       = 0.5
MIN_ECHANTILLONS_VALIDES = 30

# --- Module predictif — tendance lineaire -----------------------------
FENETRE_PREDICTION_BLOCS = 18
MIN_BLOCS_PREDICTION     = 6
R2_MIN                   = 0.50
PENTE_SEUIL_A_PAR_BLOC   = -0.05
SEUIL_ALERTE_HEURES      = 24.0

# --- Indice de sante combine (par phase) -------------------------------
DUREE_VIE_HEURES_THEORIQUE = 20000
CYCLES_VIE_THEORIQUE       = 100000
POIDS_USAGE_HEURES   = 0.4
POIDS_CYCLES         = 0.3
POIDS_TENDANCE       = 0.3
FENETRE_SANTE_HEURES = 168.0
SEUIL_SANTE_BON        = 70.0
SEUIL_SANTE_SURVEILLER = 40.0

# --- Watchdog de communication Janitza ---------------------------------
SEUIL_COMM_PARTIELLE_S = 10.0
SEUIL_COMM_TOTALE_S    = 30.0

# --- Mesure de tension NON CABLEE — choix hardware volontaire ----------
TENSION_NOMINALE_V = 228.0

# --- Chemins fichiers ---------------------------------------------------
DOSSIER_BASE = os.path.dirname(os.path.abspath(__file__))
DOSSIER_LOGS = os.path.join(DOSSIER_BASE, "logs")
FICHIER_DB   = os.path.join(DOSSIER_BASE, "surveillance.db")

DOSSIER_LIVE = "/dev/shm" if os.path.isdir("/dev/shm") else DOSSIER_LOGS
FICHIER_JSON_LIVE = os.path.join(DOSSIER_LIVE, "statut_actuel.json")
FICHIER_JSON_PRED = os.path.join(DOSSIER_LIVE, "predictions_actuelles.json")

# --- Registres Modbus Janitza UMG 96RM-E-RCM --------------------------
REG_I1, REG_I2, REG_I3 = 19012, 19014, 19016

REG_TC = {
    "I1_primaire": 10, "I1_secondaire": 12,
    "I2_primaire": 18, "I2_secondaire": 20,
    "I3_primaire": 26, "I3_secondaire": 28,
}
RAPPORT_TC_PRIMAIRE, RAPPORT_TC_SECONDAIRE = 200, 1

# =====================================================================
# LOGGING
# =====================================================================
os.makedirs(DOSSIER_LOGS, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(DOSSIER_LOGS, "surveillance.log")),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("surveillance")


# =====================================================================
# NOTIFICATIONS PUSH (Firebase Cloud Messaging) — OPTIONNEL
# =====================================================================
FIREBASE_ACTIF = False
TOKEN_APPAREIL_MOBILE = "A_REMPLACER_PAR_LE_TOKEN_FCM_DU_TELEPHONE"

try:
    import firebase_admin
    from firebase_admin import credentials, messaging

    _chemin_cle = os.path.join(DOSSIER_BASE, "serviceAccountKey.json")
    if os.path.exists(_chemin_cle):
        _cred = credentials.Certificate(_chemin_cle)
        firebase_admin.initialize_app(_cred)
        FIREBASE_ACTIF = True
        log.info("Firebase Cloud Messaging initialise — notifications push actives.")
    else:
        log.warning(
            "serviceAccountKey.json introuvable — notifications push "
            "desactivees (fonctionnement normal sans elles)."
        )
except ImportError:
    log.warning("Module firebase_admin non installe — notifications push desactivees.")
except Exception as e:
    log.error(f"Erreur d'initialisation Firebase : {e}")


def envoyer_notification_push(titre: str, corps: str):
    if not FIREBASE_ACTIF:
        return
    if TOKEN_APPAREIL_MOBILE.startswith("A_REMPLACER"):
        log.warning("Token FCM non configure — notification ignoree.")
        return
    try:
        message = messaging.Message(
            notification=messaging.Notification(title=titre, body=corps),
            token=TOKEN_APPAREIL_MOBILE,
        )
        messaging.send(message)
        log.info(f"[PUSH] Notification envoyee : {titre}")
    except Exception as e:
        log.error(f"[PUSH] Echec envoi notification : {e}")


# =====================================================================
# BASE DE DONNÉES SQLITE
# =====================================================================
class BaseDonnees:
    def __init__(self, chemin: str):
        self.chemin = chemin
        self.conn = sqlite3.connect(chemin, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._creer_tables()
        log.info(f"Base SQLite ouverte (mode WAL) : {chemin}")

    def _creer_tables(self):
        c = self.conn.cursor()

        c.execute("""
            CREATE TABLE IF NOT EXISTS mesures (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                horodatage  TEXT    NOT NULL,
                I1_A        REAL,
                I2_A        REAL,
                I3_A        REAL
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS blocs (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                debut            TEXT NOT NULL,
                fin              TEXT NOT NULL,
                nb_ech_I1        INTEGER,
                nb_ech_I2        INTEGER,
                nb_ech_I3        INTEGER,
                I1_moy_A         REAL,
                I2_moy_A         REAL,
                I3_moy_A         REAL,
                M11 REAL, M12 REAL, M13 REAL,
                M21 REAL, M22 REAL, M23 REAL,
                M31 REAL, M32 REAL, M33 REAL,
                nb_alertes       INTEGER DEFAULT 0
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS alertes (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                horodatage         TEXT NOT NULL,
                type_alerte        TEXT NOT NULL,
                phase              TEXT,
                message            TEXT NOT NULL,
                statut             TEXT DEFAULT 'ACTIF',
                date_acquittement  TEXT
            )
        """)
        try:
            c.execute("ALTER TABLE alertes ADD COLUMN phase TEXT")
        except sqlite3.OperationalError:
            pass

        c.execute("""
            CREATE TABLE IF NOT EXISTS predictions (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                horodatage          TEXT NOT NULL,
                phase               TEXT NOT NULL,
                nb_blocs_utilises   INTEGER,
                pente_A_par_bloc    REAL,
                r2                  REAL,
                I_actuel_estime_A   REAL,
                tendance_significative INTEGER,
                heures_avant_panne  REAL,
                message             TEXT
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS etat_resistances (
                phase            TEXT PRIMARY KEY,
                heures_cumulees  REAL    DEFAULT 0,
                cycles_cumules   INTEGER DEFAULT 0,
                energie_kWh      REAL    DEFAULT 0,
                derniere_maj     TEXT
            )
        """)
        try:
            c.execute("ALTER TABLE etat_resistances ADD COLUMN energie_kWh REAL DEFAULT 0")
        except sqlite3.OperationalError:
            pass

        c.execute("""
            CREATE TABLE IF NOT EXISTS indice_sante (
                id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                horodatage            TEXT NOT NULL,
                phase                 TEXT NOT NULL,
                heures_cumulees       REAL,
                cycles_cumules        INTEGER,
                composant_usage       REAL,
                composant_cycles      REAL,
                composant_tendance    REAL,
                indice_sante          REAL,
                categorie             TEXT,
                mtbf_estime_h         REAL,
                nb_pannes_historique  INTEGER
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS machine_sante (
                id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                horodatage           TEXT NOT NULL,
                score_global         REAL,
                categorie            TEXT,
                phase_la_plus_faible TEXT
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS rapport_journalier (
                id                       INTEGER PRIMARY KEY AUTOINCREMENT,
                date                     TEXT NOT NULL,
                temps_production_h       REAL,
                temps_arret_h            REAL,
                temps_chauffe_moyen_min  REAL,
                cycles_I1                INTEGER,
                cycles_I2                INTEGER,
                cycles_I3                INTEGER,
                nb_alertes               INTEGER,
                heures_cumulees_I1       REAL,
                heures_cumulees_I2       REAL,
                heures_cumulees_I3       REAL,
                energie_jour_kWh         REAL,
                energie_cumulee_kWh      REAL,
                score_sante_machine      REAL,
                categorie_sante_machine  TEXT
            )
        """)
        for col in ("energie_jour_kWh REAL DEFAULT 0", "energie_cumulee_kWh REAL DEFAULT 0"):
            try:
                c.execute(f"ALTER TABLE rapport_journalier ADD COLUMN {col}")
            except sqlite3.OperationalError:
                pass

        self.conn.commit()
        log.info(
            "Tables SQLite OK : mesures | blocs | alertes | predictions | "
            "etat_resistances | indice_sante | machine_sante | rapport_journalier"
        )

    def inserer_mesure(self, i1, i2, i3):
        self.conn.execute(
            "INSERT INTO mesures (horodatage, I1_A, I2_A, I3_A) VALUES (?,?,?,?)",
            (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), i1, i2, i3)
        )
        self.conn.commit()

    def inserer_bloc(self, debut, fin, nb_ech, moyennes, matrice, nb_alertes):
        def v(i, j):
            try: return matrice[i][j]
            except (IndexError, TypeError): return None

        cur = self.conn.execute("""
            INSERT INTO blocs (
                debut, fin, nb_ech_I1, nb_ech_I2, nb_ech_I3,
                I1_moy_A, I2_moy_A, I3_moy_A,
                M11, M12, M13,
                M21, M22, M23,
                M31, M32, M33,
                nb_alertes
            ) VALUES (?,?,?,?,?, ?,?,?, ?,?,?, ?,?,?, ?,?,?, ?)
        """, (
            debut.strftime("%Y-%m-%d %H:%M:%S"),
            fin.strftime("%Y-%m-%d %H:%M:%S"),
            nb_ech["I1"], nb_ech["I2"], nb_ech["I3"],
            moyennes["I1_moy"], moyennes["I2_moy"], moyennes["I3_moy"],
            v(0,0), v(0,1), v(0,2),
            v(1,0), v(1,1), v(1,2),
            v(2,0), v(2,1), v(2,2),
            nb_alertes,
        ))
        self.conn.commit()
        return cur.lastrowid

    def inserer_alertes(self, alertes: list):
        if not alertes:
            return
        h = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for a in alertes:
            self.conn.execute(
                "INSERT INTO alertes (horodatage, type_alerte, phase, message) "
                "VALUES (?,?,?,?)",
                (h, a["type"], a["phase"], a["message"])
            )
            envoyer_notification_push(f"Alerte SEHO — {a['type']} ({a['phase']})", a["message"])
        self.conn.commit()

    def inserer_alerte_predictive(self, phase: str, message: str):
        h = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.conn.execute(
            "INSERT INTO alertes (horodatage, type_alerte, phase, message) VALUES (?,?,?,?)",
            (h, "PREDICTIF", phase, message)
        )
        self.conn.commit()
        envoyer_notification_push("Prediction SEHO — Maintenance preventive", message)

    def inserer_alerte_communication(self, message: str, resolu: bool = False):
        h = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        type_alerte = "COMMUNICATION_RETABLIE" if resolu else "COMMUNICATION"
        self.conn.execute(
            "INSERT INTO alertes (horodatage, type_alerte, phase, message) VALUES (?,?,?,?)",
            (h, type_alerte, None, message)
        )
        self.conn.commit()
        titre = "Communication retablie" if resolu else "COMMUNICATION PERDUE"
        envoyer_notification_push(f"SEHO — {titre}", message)

    def compter_pannes_phase(self, phase: str) -> int:
        cur = self.conn.execute(
            "SELECT COUNT(*) FROM alertes WHERE type_alerte = 'PANNE' AND phase = ?",
            (phase,)
        )
        return cur.fetchone()[0]

    def compter_alertes_actives(self) -> int:
        cur = self.conn.execute("SELECT COUNT(*) FROM alertes WHERE statut = 'ACTIF'")
        return cur.fetchone()[0]

    def compter_alertes_jour(self, date_str: str) -> int:
        cur = self.conn.execute(
            "SELECT COUNT(*) FROM alertes WHERE substr(horodatage,1,10) = ?",
            (date_str,)
        )
        return cur.fetchone()[0]

    def inserer_prediction(self, phase, nb_blocs, pente, r2, i_actuel,
                           significative, heures_avant_panne, message):
        self.conn.execute("""
            INSERT INTO predictions (
                horodatage, phase, nb_blocs_utilises,
                pente_A_par_bloc, r2, I_actuel_estime_A,
                tendance_significative, heures_avant_panne, message
            ) VALUES (?,?,?, ?,?,?, ?,?,?)
        """, (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            phase, nb_blocs, pente, r2, i_actuel,
            1 if significative else 0, heures_avant_panne, message
        ))
        self.conn.commit()

    def recuperer_historique_phase(self, phase: str, n: int) -> list:
        colonne = f"{phase}_moy_A"
        cur = self.conn.execute(
            f"SELECT {colonne} FROM blocs WHERE {colonne} IS NOT NULL "
            f"ORDER BY id DESC LIMIT ?",
            (n,)
        )
        valeurs = [row[0] for row in cur.fetchall()]
        valeurs.reverse()
        return valeurs

    def charger_etat_resistances(self) -> dict:
        cur = self.conn.execute(
            "SELECT phase, heures_cumulees, cycles_cumules, energie_kWh FROM etat_resistances"
        )
        etat = {}
        for phase, heures, cycles, energie in cur.fetchall():
            etat[phase] = {"heures": heures, "cycles": cycles, "energie": energie or 0.0}
        return etat

    def sauvegarder_etat_resistance(self, phase: str, heures: float, cycles: int, energie_kWh: float = 0.0):
        self.conn.execute("""
            INSERT INTO etat_resistances (phase, heures_cumulees, cycles_cumules, energie_kWh, derniere_maj)
            VALUES (?,?,?,?,?)
            ON CONFLICT(phase) DO UPDATE SET
                heures_cumulees = excluded.heures_cumulees,
                cycles_cumules  = excluded.cycles_cumules,
                energie_kWh     = excluded.energie_kWh,
                derniere_maj    = excluded.derniere_maj
        """, (phase, heures, cycles, energie_kWh, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        self.conn.commit()

    def inserer_indice_sante(self, phase, heures, cycles, detail, mtbf_h, nb_pannes):
        self.conn.execute("""
            INSERT INTO indice_sante (
                horodatage, phase, heures_cumulees, cycles_cumules,
                composant_usage, composant_cycles, composant_tendance,
                indice_sante, categorie, mtbf_estime_h, nb_pannes_historique
            ) VALUES (?,?,?,?, ?,?,?, ?,?, ?,?)
        """, (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            phase, heures, cycles,
            detail["composant_usage"], detail["composant_cycles"],
            detail["composant_tendance"], detail["indice_sante"],
            detail["categorie"], mtbf_h, nb_pannes,
        ))
        self.conn.commit()

    def inserer_score_global(self, detail: dict):
        self.conn.execute("""
            INSERT INTO machine_sante (
                horodatage, score_global, categorie, phase_la_plus_faible
            ) VALUES (?,?,?,?)
        """, (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            detail["score_global"], detail["categorie"], detail["phase_la_plus_faible"],
        ))
        self.conn.commit()

    def inserer_rapport_journalier(self, r: dict):
        self.conn.execute("""
            INSERT INTO rapport_journalier (
                date, temps_production_h, temps_arret_h, temps_chauffe_moyen_min,
                cycles_I1, cycles_I2, cycles_I3, nb_alertes,
                heures_cumulees_I1, heures_cumulees_I2, heures_cumulees_I3,
                energie_jour_kWh, energie_cumulee_kWh,
                score_sante_machine, categorie_sante_machine
            ) VALUES (?,?,?,?, ?,?,?,?, ?,?,?, ?,?, ?,?)
        """, (
            r["date"], r["temps_production_h"], r["temps_arret_h"],
            r["temps_chauffe_moyen_min"],
            r["cycles_jour"]["I1"], r["cycles_jour"]["I2"], r["cycles_jour"]["I3"],
            r["nb_alertes"],
            r["heures_cumulees"]["I1"], r["heures_cumulees"]["I2"], r["heures_cumulees"]["I3"],
            r["energie_jour_kWh"], r["energie_cumulee_kWh"],
            r["score_sante_machine"], r["categorie_sante_machine"],
        ))
        self.conn.commit()

    def fermer(self):
        self.conn.close()
        log.info("Base SQLite fermee proprement.")


# =====================================================================
# WATCHDOG DE COMMUNICATION JANITZA
# =====================================================================
class WatchdogCommunication:
    def __init__(self):
        self.alerte_partielle_envoyee = False
        self.alerte_totale_envoyee = False

    def verifier(self, temps_sans_reponse: float, db: BaseDonnees) -> str:
        if temps_sans_reponse >= SEUIL_COMM_TOTALE_S:
            if not self.alerte_totale_envoyee:
                message = (
                    f"COMMUNICATION PERDUE avec le Janitza — "
                    f"{temps_sans_reponse:.0f}s sans reponse "
                    f"(seuil {SEUIL_COMM_TOTALE_S:.0f}s)"
                )
                log.error(message)
                db.inserer_alerte_communication(message, resolu=False)
                self.alerte_totale_envoyee = True
                self.alerte_partielle_envoyee = True
            return "PERDUE"

        elif temps_sans_reponse >= SEUIL_COMM_PARTIELLE_S:
            if not self.alerte_partielle_envoyee:
                log.warning(
                    f"Communication instable avec le Janitza — "
                    f"{temps_sans_reponse:.0f}s sans reponse"
                )
                self.alerte_partielle_envoyee = True
            return "INSTABLE"

        else:
            if self.alerte_partielle_envoyee or self.alerte_totale_envoyee:
                log.info("Communication retablie avec le Janitza.")
                if self.alerte_totale_envoyee:
                    db.inserer_alerte_communication(
                        "Communication retablie avec le Janitza apres coupure.",
                        resolu=True
                    )
                self.alerte_partielle_envoyee = False
                self.alerte_totale_envoyee = False
            return "OK"


# =====================================================================
# SUIVI D'USURE — heures cumulees + cycles ON/OFF (par phase)
# =====================================================================
class SuiviUsure:
    def __init__(self, db: BaseDonnees):
        self.db = db
        etat = db.charger_etat_resistances()
        self.heures = {p: etat.get(p, {}).get("heures", 0.0) for p in ("I1", "I2", "I3")}
        self.cycles = {p: etat.get(p, {}).get("cycles", 0) for p in ("I1", "I2", "I3")}
        self.energie_kWh = {p: etat.get(p, {}).get("energie", 0.0) for p in ("I1", "I2", "I3")}
        self.etat_precedent = {p: None for p in ("I1", "I2", "I3")}

        for p in ("I1", "I2", "I3"):
            log.info(
                f"Etat initial {p} : {self.heures[p]:.2f}h cumulees, "
                f"{self.cycles[p]} cycles cumules, "
                f"{self.energie_kWh[p]:.2f} kWh cumules (charge depuis la base)"
            )

    def mettre_a_jour(self, i1, i2, i3, dt_s: float):
        for phase, valeur in (("I1", i1), ("I2", i2), ("I3", i3)):
            if valeur is None:
                continue
            actif = valeur >= SEUIL_SSR_ETEINT_A
            if actif:
                self.heures[phase] += dt_s / 3600.0
            # Energie active (kWh) : integration trapezoidale simple P.dt
            puissance_w = TENSION_NOMINALE_V * valeur
            self.energie_kWh[phase] += (puissance_w * dt_s) / 3600.0 / 1000.0
            precedent = self.etat_precedent[phase]
            if precedent is False and actif:
                self.cycles[phase] += 1
            self.etat_precedent[phase] = actif

    def energie_totale_kWh(self) -> float:
        return sum(self.energie_kWh.values())

    def sauvegarder(self):
        for phase in ("I1", "I2", "I3"):
            self.db.sauvegarder_etat_resistance(
                phase, self.heures[phase], self.cycles[phase], self.energie_kWh[phase]
            )


# =====================================================================
# SUIVI JOURNALIER — production / arret / cycles / alertes du jour
# =====================================================================
class SuiviJournalier:
    def __init__(self, usure: SuiviUsure):
        self.jour = date.today()
        self.nb_blocs_production = 0
        self.nb_blocs_arret = 0
        self.total_echantillons_chauffe = 0
        self.nb_blocs_jour = 0
        self.cycles_debut_jour = dict(usure.cycles)
        self.energie_debut_jour = dict(usure.energie_kWh)

    def enregistrer_bloc(self, bloc_vide: bool, nb_ech: dict):
        self.nb_blocs_jour += 1
        if bloc_vide:
            self.nb_blocs_arret += 1
        else:
            self.nb_blocs_production += 1
            self.total_echantillons_chauffe += sum(nb_ech.values())

    def jour_a_change(self) -> bool:
        return date.today() != self.jour

    def energie_jour_kWh(self, usure: SuiviUsure) -> dict:
        return {
            p: round(usure.energie_kWh[p] - self.energie_debut_jour.get(p, usure.energie_kWh[p]), 3)
            for p in ("I1", "I2", "I3")
        }

    def generer_rapport(self, nb_alertes_jour: int, usure: SuiviUsure, score_global):
        temps_production_h = round(self.nb_blocs_production * DUREE_BLOC_S / 3600, 2)
        temps_arret_h = round(self.nb_blocs_arret * DUREE_BLOC_S / 3600, 2)

        if self.nb_blocs_jour > 0:
            temps_chauffe_moyen_min = round(
                (self.total_echantillons_chauffe / 3 * INTERVALLE_LECTURE_S / 60)
                / self.nb_blocs_jour, 2
            )
        else:
            temps_chauffe_moyen_min = 0.0

        cycles_jour = {
            p: usure.cycles[p] - self.cycles_debut_jour.get(p, usure.cycles[p])
            for p in ("I1", "I2", "I3")
        }

        energie_jour = self.energie_jour_kWh(usure)

        return {
            "date": self.jour.strftime("%Y-%m-%d"),
            "temps_production_h": temps_production_h,
            "temps_arret_h": temps_arret_h,
            "temps_chauffe_moyen_min": temps_chauffe_moyen_min,
            "cycles_jour": cycles_jour,
            "nb_alertes": nb_alertes_jour,
            "heures_cumulees": {p: round(usure.heures[p], 1) for p in ("I1", "I2", "I3")},
            "energie_jour_kWh": round(sum(energie_jour.values()), 3),
            "energie_cumulee_kWh": round(usure.energie_totale_kWh(), 3),
            "score_sante_machine": score_global["score_global"] if score_global else None,
            "categorie_sante_machine": score_global["categorie"] if score_global else "N/A",
        }

    def reinitialiser(self, usure: SuiviUsure):
        self.jour = date.today()
        self.nb_blocs_production = 0
        self.nb_blocs_arret = 0
        self.total_echantillons_chauffe = 0
        self.nb_blocs_jour = 0
        self.cycles_debut_jour = dict(usure.cycles)
        self.energie_debut_jour = dict(usure.energie_kWh)


def afficher_rapport_journalier(r: dict):
    print("\n" + "-" * 40)
    print(f"  RAPPORT JOURNALIER — {r['date']}")
    print("-" * 40)
    print(f"  Production          : {r['temps_production_h']:.1f} h")
    print(f"  Temps d'arret        : {r['temps_arret_h']:.1f} h")
    print(f"  Chauffe moyen/bloc   : {r['temps_chauffe_moyen_min']:.1f} min")
    print(f"  Cycles (I1/I2/I3)    : "
          f"{r['cycles_jour']['I1']} / {r['cycles_jour']['I2']} / {r['cycles_jour']['I3']}")
    print(f"  Alertes              : {r['nb_alertes']}")
    print(f"  Energie du jour      : {r['energie_jour_kWh']:.2f} kWh")
    print(f"  Energie cumulee      : {r['energie_cumulee_kWh']:.2f} kWh")
    if r["score_sante_machine"] is not None:
        print(f"  Sante machine        : {r['score_sante_machine']:.1f}% "
              f"({r['categorie_sante_machine']})")
    else:
        print(f"  Sante machine        : N/A (donnees insuffisantes)")
    print("-" * 40 + "\n")
    log.info(f"Rapport journalier genere pour {r['date']} : {r}")


# =====================================================================
# INDICE DE SANTE COMBINE (par phase) + MTBF
# =====================================================================
def calculer_indice_sante(heures_cumulees: float, cycles_cumules: int, tendance_result):
    composant_usage = max(0.0, 100.0 * (1 - heures_cumulees / DUREE_VIE_HEURES_THEORIQUE))
    composant_cycles = max(0.0, 100.0 * (1 - cycles_cumules / CYCLES_VIE_THEORIQUE))

    if (tendance_result and tendance_result.get("significative")
            and tendance_result.get("heures_avant_panne") is not None):
        ratio = min(tendance_result["heures_avant_panne"] / FENETRE_SANTE_HEURES, 1.0)
        composant_tendance = 100.0 * max(ratio, 0.0)
    else:
        composant_tendance = 100.0

    indice = (
        POIDS_USAGE_HEURES * composant_usage
        + POIDS_CYCLES * composant_cycles
        + POIDS_TENDANCE * composant_tendance
    )
    indice = max(0.0, min(100.0, indice))

    if indice >= SEUIL_SANTE_BON:
        categorie = "BON"
    elif indice >= SEUIL_SANTE_SURVEILLER:
        categorie = "A SURVEILLER"
    else:
        categorie = "CRITIQUE — planifier remplacement"

    return {
        "composant_usage": round(composant_usage, 1),
        "composant_cycles": round(composant_cycles, 1),
        "composant_tendance": round(composant_tendance, 1),
        "indice_sante": round(indice, 1),
        "categorie": categorie,
    }


def calculer_mtbf(db: BaseDonnees, phase: str, heures_cumulees: float):
    nb_pannes = db.compter_pannes_phase(phase)
    if nb_pannes == 0:
        return None, 0
    return round(heures_cumulees / nb_pannes, 1), nb_pannes


def calculer_score_global_machine(sante_par_phase: dict):
    valides = {p: d for p, d in sante_par_phase.items() if d}
    if not valides:
        return None

    indices = [d["indice_sante"] for d in valides.values()]
    score = round(mean(indices), 1)

    if any(d["categorie"].startswith("CRITIQUE") for d in valides.values()):
        categorie = "CRITIQUE — planifier remplacement"
    elif score >= SEUIL_SANTE_BON:
        categorie = "BON"
    elif score >= SEUIL_SANTE_SURVEILLER:
        categorie = "A SURVEILLER"
    else:
        categorie = "CRITIQUE — planifier remplacement"

    phase_la_plus_faible = min(valides, key=lambda p: valides[p]["indice_sante"])

    return {
        "score_global": score,
        "categorie": categorie,
        "phase_la_plus_faible": phase_la_plus_faible,
    }


# =====================================================================
# MODULE PREDICTIF — regression lineaire (moindres carres, sans numpy)
# =====================================================================
def regression_lineaire(valeurs: list):
    n = len(valeurs)
    if n < 2:
        return None

    xs = list(range(n))
    sx  = sum(xs)
    sy  = sum(valeurs)
    sxx = sum(x * x for x in xs)
    sxy = sum(x * y for x, y in zip(xs, valeurs))

    denom = n * sxx - sx * sx
    if denom == 0:
        return None

    pente     = (n * sxy - sx * sy) / denom
    intercept = (sy - pente * sx) / n

    y_moy = sy / n
    ss_tot = sum((y - y_moy) ** 2 for y in valeurs)
    if ss_tot == 0:
        r2 = 1.0 if pente == 0 else 0.0
    else:
        ss_res = sum((y - (intercept + pente * x)) ** 2 for x, y in zip(xs, valeurs))
        r2 = 1 - ss_res / ss_tot

    return pente, intercept, r2


def analyser_tendance_phase(db: BaseDonnees, phase: str):
    valeurs = db.recuperer_historique_phase(phase, FENETRE_PREDICTION_BLOCS)
    if len(valeurs) < MIN_BLOCS_PREDICTION:
        return None

    resultat = regression_lineaire(valeurs)
    if resultat is None:
        return None
    pente, intercept, r2 = resultat

    n = len(valeurs)
    i_actuel_estime = intercept + pente * (n - 1)

    significative = (pente <= PENTE_SEUIL_A_PAR_BLOC) and (r2 >= R2_MIN)

    heures_avant_panne = None
    if significative and i_actuel_estime > SEUIL_PANNE_A:
        blocs_avant_panne = (SEUIL_PANNE_A - i_actuel_estime) / pente
        heures_avant_panne = round(blocs_avant_panne * (DUREE_BLOC_S / 3600), 1)
    elif significative and i_actuel_estime <= SEUIL_PANNE_A:
        heures_avant_panne = 0.0

    return {
        "phase": phase,
        "nb_blocs": n,
        "pente": round(pente, 4),
        "r2": round(r2, 3),
        "i_actuel_estime": round(i_actuel_estime, 3),
        "significative": significative,
        "heures_avant_panne": heures_avant_panne,
    }


def evaluer_predictions_et_sante(db: BaseDonnees, usure: SuiviUsure, cache_partage: dict):
    export_predictions = {
        "horodatage": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "phases": {}
    }
    sante_calculee = {}

    for phase in ("I1", "I2", "I3"):
        tendance = analyser_tendance_phase(db, phase)

        if tendance is not None:
            if tendance["significative"]:
                message = (
                    f"Tendance a la baisse detectee sur {phase} : "
                    f"pente {tendance['pente']} A/bloc (R2={tendance['r2']}), "
                    f"courant actuel estime {tendance['i_actuel_estime']} A"
                )
                if tendance["heures_avant_panne"] is not None:
                    message += (
                        f" — panne franche estimee dans "
                        f"{tendance['heures_avant_panne']:.1f} h si la tendance se maintient"
                    )
                statut_export = "DEGRADATION"
            else:
                message = (
                    f"{phase} stable — pas de derive significative "
                    f"(pente {tendance['pente']} A/bloc, R2={tendance['r2']})"
                )
                statut_export = "SAIN"

            db.inserer_prediction(
                phase, tendance["nb_blocs"], tendance["pente"], tendance["r2"],
                tendance["i_actuel_estime"], tendance["significative"],
                tendance["heures_avant_panne"], message
            )
            print(f"  [PREDICTIF] {message}")
            log.info(f"[PREDICTIF] {message}")

            if (tendance["significative"]
                    and tendance["heures_avant_panne"] is not None
                    and tendance["heures_avant_panne"] < SEUIL_ALERTE_HEURES):
                alerte = (
                    f"RISQUE DE PANNE — {phase} : degradation continue detectee, "
                    f"panne franche estimee dans {tendance['heures_avant_panne']:.1f}h "
                    f"(pente {tendance['pente']} A/bloc, R2={tendance['r2']}) "
                    f"— planifier une intervention preventive"
                )
                print(f"  *** ALERTE PREDICTIVE : {alerte}")
                log.warning(f"ALERTE PREDICTIVE — {alerte}")
                db.inserer_alerte_predictive(phase, alerte)

            export_predictions["phases"][phase] = {
                "status": statut_export,
                "heures_avant_panne": tendance["heures_avant_panne"],
                "pente_A_par_bloc": tendance["pente"],
                "r2": tendance["r2"],
                "message": message,
            }
        else:
            print(f"  [PREDICTIF] {phase} — historique insuffisant "
                  f"({MIN_BLOCS_PREDICTION} blocs minimum requis)")
            export_predictions["phases"][phase] = {
                "status": "INITIALISATION",
                "heures_avant_panne": None,
                "message": "Historique insuffisant pour une tendance fiable",
            }

        detail = calculer_indice_sante(usure.heures[phase], usure.cycles[phase], tendance)
        mtbf_h, nb_pannes = calculer_mtbf(db, phase, usure.heures[phase])
        db.inserer_indice_sante(
            phase, usure.heures[phase], usure.cycles[phase], detail, mtbf_h, nb_pannes
        )

        mtbf_txt = (
            f"MTBF estime {mtbf_h:.0f}h (base sur {nb_pannes} panne(s))"
            if mtbf_h is not None
            else "MTBF non calculable (aucune panne confirmee jusqu'ici — fiabilite excellente)"
        )

        print(
            f"  [SANTE]     {phase} — indice {detail['indice_sante']:>5.1f}% "
            f"({detail['categorie']})  "
            f"[usage={detail['composant_usage']:.0f}%  "
            f"cycles={detail['composant_cycles']:.0f}%  "
            f"tendance={detail['composant_tendance']:.0f}%]  "
            f"— {usure.heures[phase]:.1f}h cumulees, {usure.cycles[phase]} cycles"
        )
        print(f"  [MTBF]      {phase} — {mtbf_txt}")

        if detail["categorie"] != "BON":
            log.warning(
                f"[SANTE] {phase} : indice {detail['indice_sante']}% "
                f"({detail['categorie']})"
            )

        export_predictions["phases"][phase]["indice_sante"] = detail["indice_sante"]
        export_predictions["phases"][phase]["categorie_sante"] = detail["categorie"]
        export_predictions["phases"][phase]["mtbf_estime_h"] = mtbf_h
        export_predictions["phases"][phase]["nb_pannes_historique"] = nb_pannes

        cache_partage["sante"][phase] = detail
        sante_calculee[phase] = detail

    score_global = calculer_score_global_machine(sante_calculee)
    if score_global:
        db.inserer_score_global(score_global)
        print(
            f"\n  [MACHINE]   SEHO MWS2300 — score global "
            f"{score_global['score_global']:.1f}% ({score_global['categorie']}) "
            f"— phase la plus faible : {score_global['phase_la_plus_faible']}"
        )
        if score_global["categorie"] != "BON":
            log.warning(
                f"[MACHINE] Score global {score_global['score_global']}% "
                f"({score_global['categorie']})"
            )
        export_predictions["score_global_machine"] = score_global
    cache_partage["score_global"] = score_global

    try:
        with open(FICHIER_JSON_PRED, "w") as f:
            json.dump(export_predictions, f, indent=2)
    except Exception as e:
        log.error(f"Impossible d'ecrire le snapshot predictif : {e}")

    return export_predictions


# =====================================================================
# COUCHE MODBUS
# =====================================================================
class JanitzaClient:
    def __init__(self, ip: str, port: int, device_id: int):
        self.ip = ip
        self.port = port
        self.device_id = device_id
        self.client = ModbusTcpClient(ip, port=port)
        self.dernier_succes = time.time()
        # Compat pymodbus : le mot-cle d'identification de l'esclave a change
        # plusieurs fois selon les versions ("unit" -> "slave" -> "device_id").
        # L'introspection de signature (inspect.signature) s'est averee peu
        # fiable sur certaines versions (le mot-cle detecte n'est pas toujours
        # celui reellement accepte a l'execution). On determine donc le bon
        # mot-cle en l'essayant reellement au premier appel Modbus, avec repli
        # automatique sur les autres candidats en cas de TypeError.
        self._id_kwarg = None  # determine au premier appel reussi

    def _appeler_avec_id(self, methode, *args, **kwargs):
        """
        Appelle une methode pymodbus (read_holding_registers / write_registers)
        en essayant les differents mots-cles d'identification d'esclave connus,
        jusqu'a trouver celui accepte par la version installee. Une fois trouve,
        il est memorise pour tous les appels suivants.
        """
        candidats = [self._id_kwarg] if self._id_kwarg else ["device_id", "slave", "unit", None]
        derniere_erreur = None
        for kw in candidats:
            try:
                if kw is None:
                    resultat = methode(*args, **kwargs)
                else:
                    resultat = methode(*args, **kwargs, **{kw: self.device_id})
                if self._id_kwarg is None:
                    self._id_kwarg = kw
                    log.info(f"pymodbus : mot-cle d'identification esclave detecte = '{kw}'")
                return resultat
            except TypeError as e:
                derniere_erreur = e
                continue
        raise derniere_erreur

    def connecter(self) -> bool:
        ok = self.client.connect()
        if ok:
            log.info(f"Connexion Modbus etablie vers {self.ip}:{self.port}")
            self.dernier_succes = time.time()
        else:
            log.error(f"Echec de connexion vers {self.ip}:{self.port}")
        return ok

    def _assurer_connexion(self):
        if not self.client.is_socket_open():
            log.warning("Connexion perdue — tentative de reconnexion...")
            self.client.close()
            time.sleep(1)
            self.connecter()

    def lire_float(self, registre: int):
        self._assurer_connexion()
        try:
            r = self._appeler_avec_id(
                self.client.read_holding_registers, registre, count=2
            )
            if r.isError():
                log.warning(f"Erreur lecture registre {registre} : {r}")
                return None
            raw = struct.pack(">HH", r.registers[0], r.registers[1])
            valeur = round(struct.unpack(">f", raw)[0], 4)
            self.dernier_succes = time.time()
            return valeur
        except Exception as e:
            log.warning(f"Exception registre {registre} : {type(e).__name__}: {e}")
            return None

    def temps_sans_reponse(self) -> float:
        return time.time() - self.dernier_succes

    def ecrire_float(self, registre: int, valeur: float) -> bool:
        self._assurer_connexion()
        try:
            raw = struct.pack(">f", float(valeur))
            r0, r1 = struct.unpack(">HH", raw)
            result = self._appeler_avec_id(
                self.client.write_registers, registre, [r0, r1]
            )
            if result.isError():
                log.error(f"Erreur ecriture reg {registre}={valeur} : {result}")
                return False
            self.dernier_succes = time.time()
            return True
        except Exception as e:
            log.error(f"Exception ecriture reg {registre} : {type(e).__name__}: {e}")
            return False

    def fermer(self):
        self.client.close()
        log.info("Connexion Modbus fermee.")


def configurer_rapport_tc(janitza: JanitzaClient) -> bool:
    """
    Configure le rapport TC (200A/1A) sur les 3 voies de courant.

    IMPORTANT — protection de l'EEPROM/Flash du Janitza : cette fonction
    est appelee a CHAQUE demarrage du script (y compris les redemarrages
    automatiques via systemd apres coupure de courant). La memoire non
    volatile du Janitza a un nombre de cycles d'ecriture limite ; il ne
    faut donc JAMAIS ecrire un registre si sa valeur est deja correcte.
    Strategie : LIRE chaque registre d'abord, ne l'ECRIRE que si sa
    valeur differe de la valeur attendue (tolerance 0.01). En usage
    normal (redemarrages frequents, valeur deja bonne), aucune ecriture
    n'est effectuee — seules des lectures, illimitees sur l'EEPROM.
    """
    log.info(f"Verification du rapport TC {RAPPORT_TC_PRIMAIRE}A/{RAPPORT_TC_SECONDAIRE}A ...")
    valeurs = {
        REG_TC["I1_primaire"]:   RAPPORT_TC_PRIMAIRE,
        REG_TC["I1_secondaire"]: RAPPORT_TC_SECONDAIRE,
        REG_TC["I2_primaire"]:   RAPPORT_TC_PRIMAIRE,
        REG_TC["I2_secondaire"]: RAPPORT_TC_SECONDAIRE,
        REG_TC["I3_primaire"]:   RAPPORT_TC_PRIMAIRE,
        REG_TC["I3_secondaire"]: RAPPORT_TC_SECONDAIRE,
    }

    tout_ok = True
    nb_deja_corrects = 0
    nb_ecritures = 0

    for reg, attendu in valeurs.items():
        lu = janitza.lire_float(reg)

        if lu is not None and abs(lu - attendu) <= 0.01:
            # Deja a la bonne valeur -> AUCUNE ecriture (protege l'EEPROM)
            nb_deja_corrects += 1
            continue

        # Valeur incorrecte ou illisible -> ecriture necessaire
        log.info(f"Registre {reg} : lu={lu}, attendu={attendu} — ecriture requise")
        if not janitza.ecrire_float(reg, attendu):
            log.error(f"Echec d'ecriture du registre {reg} = {attendu}")
            tout_ok = False
            continue
        nb_ecritures += 1

        # Relecture de verification apres ecriture
        relu = janitza.lire_float(reg)
        if relu is None or abs(relu - attendu) > 0.01:
            log.error(f"Verification post-ecriture echouee — reg {reg} : attendu {attendu}, lu {relu}")
            tout_ok = False

    if tout_ok:
        if nb_ecritures == 0:
            log.info(
                f"Rapport TC deja correct sur les 6 registres — "
                f"aucune ecriture EEPROM effectuee."
            )
        else:
            log.info(
                f"Rapport TC configure : {nb_ecritures} registre(s) corrige(s), "
                f"{nb_deja_corrects} deja correct(s)."
            )
    else:
        log.error("Configuration TC incomplete — voir logs ci-dessus.")

    return tout_ok


# =====================================================================
# STRUCTURES DE DONNÉES
# =====================================================================
@dataclass
class BlocMesure:
    debut: datetime = field(default_factory=datetime.now)
    i1: list = field(default_factory=list)
    i2: list = field(default_factory=list)
    i3: list = field(default_factory=list)

    def ajouter(self, i1, i2, i3):
        if i1 is None or i2 is None or i3 is None:
            return
        if i1 >= SEUIL_SSR_ETEINT_A:
            self.i1.append(i1)
        if i2 >= SEUIL_SSR_ETEINT_A:
            self.i2.append(i2)
        if i3 >= SEUIL_SSR_ETEINT_A:
            self.i3.append(i3)

    def est_vide(self) -> bool:
        return not (self.i1 or self.i2 or self.i3)

    def nb_echantillons(self) -> dict:
        return {"I1": len(self.i1), "I2": len(self.i2), "I3": len(self.i3)}

    def phases_valides(self) -> dict:
        n = self.nb_echantillons()
        return {
            "I1": n["I1"] >= MIN_ECHANTILLONS_VALIDES,
            "I2": n["I2"] >= MIN_ECHANTILLONS_VALIDES,
            "I3": n["I3"] >= MIN_ECHANTILLONS_VALIDES,
        }


# =====================================================================
# ANALYSE (detection ponctuelle — panne franche / desequilibre)
# =====================================================================
def calculer_moyennes(bloc: BlocMesure) -> dict:
    valides = bloc.phases_valides()
    return {
        "I1_moy": round(mean(bloc.i1), 3) if valides["I1"] else None,
        "I2_moy": round(mean(bloc.i2), 3) if valides["I2"] else None,
        "I3_moy": round(mean(bloc.i3), 3) if valides["I3"] else None,
    }


def detecter_panne_franche(moyennes: dict) -> list:
    alertes = []
    for phase in ("I1", "I2", "I3"):
        val = moyennes[f"{phase}_moy"]
        if val is not None and val < SEUIL_PANNE_A:
            alertes.append({
                "phase": phase,
                "type": "PANNE",
                "message": (
                    f"PANNE FRANCHE — {phase} = {val:.3f} A "
                    f"(< seuil {SEUIL_PANNE_A} A pendant les cycles de chauffe active) "
                    f"— resistance probablement coupee"
                ),
            })
    return alertes


def detecter_desequilibre(moyennes: dict) -> list:
    valeurs = [moyennes[f"{p}_moy"] for p in ("I1", "I2", "I3")
               if moyennes[f"{p}_moy"] is not None]
    if len(valeurs) < 3:
        return []
    moy = mean(valeurs)
    if moy == 0:
        return []
    alertes = []
    for phase in ("I1", "I2", "I3"):
        val = moyennes[f"{phase}_moy"]
        if val is None:
            continue
        ecart = abs(val - moy) / moy * 100
        if ecart > SEUIL_DESEQUILIBRE_PCT:
            alertes.append({
                "phase": phase,
                "type": "DESEQUILIBRE",
                "message": (
                    f"DESEQUILIBRE — {phase} = {val:.3f} A, "
                    f"ecart {ecart:.1f}% vs moyenne {moy:.3f} A "
                    f"(seuil {SEUIL_DESEQUILIBRE_PCT}%) — degradation asymetrique possible"
                ),
            })
    return alertes


def calculer_matrice_signature(moyennes: dict) -> list:
    valeurs = [moyennes["I1_moy"], moyennes["I2_moy"], moyennes["I3_moy"]]
    matrice = []
    for i in range(3):
        ligne = []
        for j in range(3):
            vi, vj = valeurs[i], valeurs[j]
            if vi is None or vj is None or vj == 0:
                ligne.append(None)
            else:
                ligne.append(round(vi / vj, 4))
        matrice.append(ligne)
    return matrice


def calculer_equilibre_pct(moyennes: dict):
    valeurs = [moyennes[f"{p}_moy"] for p in ("I1", "I2", "I3")
               if moyennes[f"{p}_moy"] is not None]
    if len(valeurs) < 3:
        return None
    moy = mean(valeurs)
    if moy == 0:
        return None
    ecart_max = max(abs(v - moy) / moy * 100 for v in valeurs)

    if ecart_max <= DESEQUILIBRE_NOMINAL_PCT:
        return 100.0
    plage = SEUIL_DESEQUILIBRE_PCT - DESEQUILIBRE_NOMINAL_PCT
    if plage <= 0:
        return 0.0
    ratio = (ecart_max - DESEQUILIBRE_NOMINAL_PCT) / plage
    return round(max(0.0, 100.0 * (1 - ratio)), 1)


# =====================================================================
# ESTIMATION DE PUISSANCE (tension nominale fixe, pas de mesure reelle)
# =====================================================================
def calculer_puissance_estimee(i1, i2, i3):
    p1 = round(TENSION_NOMINALE_V * i1, 1) if i1 is not None else None
    p2 = round(TENSION_NOMINALE_V * i2, 1) if i2 is not None else None
    p3 = round(TENSION_NOMINALE_V * i3, 1) if i3 is not None else None
    composantes = [p for p in (p1, p2, p3) if p is not None]
    p_totale = round(sum(composantes), 1) if composantes else None
    return p1, p2, p3, p_totale


# =====================================================================
# EXPORT JSON TEMPS REEL (pour api_server.py / application mobile)
# =====================================================================
def ecrire_snapshot_live(cache_partage: dict, i1, i2, i3, statut_communication: str,
                          usure: "SuiviUsure" = None, suivi_jour: "SuiviJournalier" = None):
    machine_arretee = all(
        x is not None and x < SEUIL_MACHINE_ARRET_A for x in (i1, i2, i3)
    )

    p1, p2, p3, p_totale = calculer_puissance_estimee(i1, i2, i3)

    score_global = cache_partage.get("score_global")

    energie = None
    if usure is not None:
        energie_jour = suivi_jour.energie_jour_kWh(usure) if suivi_jour else {}
        energie = {
            "cumulee_kWh": {
                "L1": round(usure.energie_kWh["I1"], 3),
                "L2": round(usure.energie_kWh["I2"], 3),
                "L3": round(usure.energie_kWh["I3"], 3),
                "totale": round(usure.energie_totale_kWh(), 3),
            },
            "jour_kWh": {
                "L1": energie_jour.get("I1", 0.0),
                "L2": energie_jour.get("I2", 0.0),
                "L3": energie_jour.get("I3", 0.0),
                "totale": round(sum(energie_jour.values()), 3) if energie_jour else 0.0,
            },
        }

    snapshot = {
        "horodatage": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "statut_machine": "ARRET" if machine_arretee else "PRODUCTION_ACTIVE",
        "statut_communication": statut_communication,
        "puissance_totale_kW": round(p_totale / 1000.0, 2) if p_totale is not None else None,
        "methode_puissance": "estimee (tension nominale fixe 228V, non mesuree)",
        "equilibre_phases_pct": cache_partage.get("equilibre_pct"),
        "mesures": {
            "L1": {"courant_A": i1, "tension_V": TENSION_NOMINALE_V, "puissance_estimee_W": p1},
            "L2": {"courant_A": i2, "tension_V": TENSION_NOMINALE_V, "puissance_estimee_W": p2},
            "L3": {"courant_A": i3, "tension_V": TENSION_NOMINALE_V, "puissance_estimee_W": p3},
        },
        "energie": energie,
        "sante": {
            phase: {
                "indice": cache_partage["sante"][phase]["indice_sante"],
                "categorie": cache_partage["sante"][phase]["categorie"],
            }
            for phase in ("I1", "I2", "I3")
            if cache_partage["sante"].get(phase)
        },
        "sante_machine": score_global if score_global else None,
        "nb_alertes_actives": cache_partage.get("nb_alertes_actives", 0),
    }

    try:
        with open(FICHIER_JSON_LIVE, "w") as f:
            json.dump(snapshot, f, indent=2)
    except Exception as e:
        log.error(f"Impossible d'ecrire le snapshot live JSON : {e}")


# =====================================================================
# AFFICHAGE CONSOLE
# =====================================================================
def fmt(v, dec=3):
    return f"{v:.{dec}f}" if v is not None else "  N/A"


def afficher_rapport_bloc(bloc, moyennes, matrice, alertes, equilibre_pct):
    fin = datetime.now()
    n = bloc.nb_echantillons()
    valides = bloc.phases_valides()

    print("\n" + "=" * 74)
    print(f"  BLOC D'ANALYSE — "
          f"{bloc.debut.strftime('%d/%m/%Y %H:%M:%S')} -> {fin.strftime('%H:%M:%S')}")
    print(f"  Echant. chauffe active : I1={n['I1']:>3} ({'OK' if valides['I1'] else 'insuffisant'})  "
          f"I2={n['I2']:>3} ({'OK' if valides['I2'] else 'insuffisant'})  "
          f"I3={n['I3']:>3} ({'OK' if valides['I3'] else 'insuffisant'})")
    print(f"  Stockage : SQLite -> {FICHIER_DB}")
    print("=" * 74)
    print(f"  Moyennes (chauffe active uniquement)  "
          f"I1 = {fmt(moyennes['I1_moy'])} A   "
          f"I2 = {fmt(moyennes['I2_moy'])} A   "
          f"I3 = {fmt(moyennes['I3_moy'])} A")
    _mp1, _mp2, _mp3, _mp_tot = calculer_puissance_estimee(
        moyennes['I1_moy'], moyennes['I2_moy'], moyennes['I3_moy']
    )
    if _mp_tot is not None:
        print(f"  Puissance estimee (moyenne du bloc)   : {_mp_tot/1000:.2f} kW")
    if equilibre_pct is not None:
        print(f"  Equilibre des phases (calibre) : {equilibre_pct:.1f}%")
    print("-" * 74)
    print("  Matrice de signature (Ii / Ij) :")
    print("             I1          I2          I3")
    for i, label in enumerate(["I1", "I2", "I3"]):
        ligne = "   ".join(fmt(matrice[i][j], 4).rjust(9) for j in range(3))
        print(f"    {label}      {ligne}")
    print("-" * 74)
    if alertes:
        for a in alertes:
            print(f"  *** ALERTE : {a['message']}")
    else:
        print("  OK — Aucune alerte ponctuelle — systeme dans les tolerances.")
    print("-" * 74)
    print("  Analyse predictive, MTBF, indice de sante et score machine :")


def afficher_statut_live(cycle: int, i1, i2, i3, ecoule: float, statut_comm: str,
                          usure: "SuiviUsure" = None):
    reste = int(DUREE_BLOC_S - ecoule)
    alerte_comm = "" if statut_comm == "OK" else f"  [COMM:{statut_comm}]"

    _, _, _, p_totale = calculer_puissance_estimee(i1, i2, i3)
    txt_puissance = f"P={p_totale/1000:.2f} kW  " if p_totale is not None else "P=—  "

    txt_energie = ""
    if usure is not None:
        txt_energie = f"E_cumul={usure.energie_totale_kWh():.2f} kWh  "

    print(
        f"  [{datetime.now().strftime('%H:%M:%S')}] "
        f"Cycle #{cycle:>5}  "
        f"I1={fmt(i1)} A  I2={fmt(i2)} A  I3={fmt(i3)} A  "
        f"{txt_puissance}{txt_energie}"
        f"(prochain bloc dans {reste}s){alerte_comm}",
        end="\r"
    )


# =====================================================================
# BOUCLE PRINCIPALE
# =====================================================================
def main():
    db = BaseDonnees(FICHIER_DB)
    usure = SuiviUsure(db)
    watchdog = WatchdogCommunication()
    suivi_jour = SuiviJournalier(usure)

    janitza = JanitzaClient(IP_JANITZA, PORT_MODBUS, DEVICE_ID)
    if not janitza.connecter():
        log.error("Arret — connexion Janitza impossible.")
        db.fermer()
        return

    if not configurer_rapport_tc(janitza):
        log.warning("TC incomplet — mesures courant possiblement incorrectes.")

    log.info(
        f"Surveillance demarree — bloc {DUREE_BLOC_S}s, "
        f"echantillonnage {INTERVALLE_LECTURE_S}s"
    )
    log.info(
        f"Seuil panne : {SEUIL_PANNE_A} A  |  "
        f"Seuil desequilibre : {SEUIL_DESEQUILIBRE_PCT}%  |  "
        f"Seuil SSR eteint : {SEUIL_SSR_ETEINT_A} A"
    )
    log.info(
        f"Module predictif : fenetre {FENETRE_PREDICTION_BLOCS} blocs "
        f"({FENETRE_PREDICTION_BLOCS * DUREE_BLOC_S / 3600:.1f}h) | "
        f"R2 min {R2_MIN} | seuil alerte {SEUIL_ALERTE_HEURES}h"
    )
    log.info(
        f"Watchdog communication : avertissement {SEUIL_COMM_PARTIELLE_S:.0f}s, "
        f"alerte {SEUIL_COMM_TOTALE_S:.0f}s"
    )
    log.info(f"JSON temps reel ecrit dans : {DOSSIER_LIVE}")
    log.info(f"Base SQLite : {FICHIER_DB}")
    log.info(
        f"Tension NON MESUREE (pas de cables tension cables) — "
        f"puissance estimee avec tension nominale fixe {TENSION_NOMINALE_V} V"
    )
    print("\n  Supervision en cours — Ctrl+C pour arreter\n")

    bloc  = BlocMesure()
    cycle = 0

    cache_partage = {
        "sante": {"I1": None, "I2": None, "I3": None},
        "equilibre_pct": None,
        "score_global": None,
        "nb_alertes_actives": db.compter_alertes_actives(),
    }

    try:
        while True:
            cycle += 1

            i1 = janitza.lire_float(REG_I1)
            i2 = janitza.lire_float(REG_I2)
            i3 = janitza.lire_float(REG_I3)

            statut_comm = watchdog.verifier(janitza.temps_sans_reponse(), db)

            db.inserer_mesure(i1, i2, i3)
            bloc.ajouter(i1, i2, i3)
            usure.mettre_a_jour(i1, i2, i3, INTERVALLE_LECTURE_S)

            ecrire_snapshot_live(cache_partage, i1, i2, i3, statut_comm, usure, suivi_jour)

            ecoule = (datetime.now() - bloc.debut).total_seconds()
            afficher_statut_live(cycle, i1, i2, i3, ecoule, statut_comm, usure)

            if ecoule >= DUREE_BLOC_S:
                print()
                n = bloc.nb_echantillons()

                if bloc.est_vide():
                    log.warning(
                        "Bloc termine : resistances restees completement "
                        "eteintes (regulation SSR ou machine a l'arret). "
                        "Pas d'analyse."
                    )
                else:
                    moyennes = calculer_moyennes(bloc)
                    matrice  = calculer_matrice_signature(moyennes)
                    alertes  = (detecter_panne_franche(moyennes)
                                + detecter_desequilibre(moyennes))
                    equilibre_pct = calculer_equilibre_pct(moyennes)

                    afficher_rapport_bloc(bloc, moyennes, matrice, alertes, equilibre_pct)

                    db.inserer_bloc(
                        bloc.debut, datetime.now(),
                        n, moyennes, matrice, len(alertes)
                    )
                    db.inserer_alertes(alertes)

                    for a in alertes:
                        log.warning(a["message"])

                    valides = bloc.phases_valides()
                    for phase, ok in valides.items():
                        if not ok and n[phase] > 0:
                            log.info(
                                f"Phase {phase} : {n[phase]} echantillons de chauffe "
                                f"(< {MIN_ECHANTILLONS_VALIDES} requis) — "
                                f"moyenne non calculee"
                            )

                    evaluer_predictions_et_sante(db, usure, cache_partage)

                    cache_partage["equilibre_pct"] = equilibre_pct
                    cache_partage["nb_alertes_actives"] = db.compter_alertes_actives()

                    print("=" * 74 + "\n")

                suivi_jour.enregistrer_bloc(bloc.est_vide(), n)
                if suivi_jour.jour_a_change():
                    nb_alertes_jour = db.compter_alertes_jour(
                        suivi_jour.jour.strftime("%Y-%m-%d")
                    )
                    rapport = suivi_jour.generer_rapport(
                        nb_alertes_jour, usure, cache_partage.get("score_global")
                    )
                    db.inserer_rapport_journalier(rapport)
                    afficher_rapport_journalier(rapport)
                    suivi_jour.reinitialiser(usure)

                usure.sauvegarder()
                bloc = BlocMesure()

            time.sleep(INTERVALLE_LECTURE_S)

    except KeyboardInterrupt:
        print()
        log.info("Arret demande par l'utilisateur (Ctrl+C).")

    except Exception as e:
        log.exception(f"Erreur inattendue : {e}")

    finally:
        usure.sauvegarder()
        janitza.fermer()
        db.fermer()
        log.info(f"Base SQLite disponible : {FICHIER_DB}")
        log.info("Ouvrir avec DB Browser for SQLite.")


if __name__ == "__main__":
    main()
