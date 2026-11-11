# 🔌 Surveillance Industrielle — Janitza UMG 96RM & SEHO MWS 2300

> **Stage Ingénieur** — Système de surveillance en temps réel des résistances de préchauffage triphasées sur machine SEHO MWS 2300, via compteur Janitza UMG 96RM-E-RCM et Raspberry Pi.

![Python](https://img.shields.io/badge/Python-3.9+-blue?logo=python&logoColor=white)
![Raspberry Pi](https://img.shields.io/badge/Raspberry%20Pi-4/5-C51A4A?logo=raspberrypi&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-API-000000?logo=flask&logoColor=white)
![Modbus](https://img.shields.io/badge/Modbus-TCP-green)
![SQLite](https://img.shields.io/badge/SQLite-Database-003B57?logo=sqlite&logoColor=white)

---

## 📋 Description

Ce projet implémente un **système de supervision industrielle complet** pour surveiller les résistances de préchauffage triphasées de la machine **SEHO MWS 2300 (2340-LRT)** chez **Marquardt Tunisie**. Il utilise le compteur d'énergie **Janitza UMG 96RM-E-RCM** communiquant en **Modbus TCP** avec un **Raspberry Pi**.

### Fonctionnalités principales

- 📊 **Acquisition continue** (toutes les 2s) des courants triphasés via Modbus TCP
- 🔍 **Détection de pannes franches** et **déséquilibre inter-phases**
- 📈 **Module prédictif** : tendance linéaire + estimation du temps avant panne
- 🏥 **Indice de santé** combiné (0-100%) par phase et global machine
- ⏱️ **MTBF estimé** par phase (heures cumulées / pannes réelles)
- 🔔 **Watchdog de communication** Janitza (alertes à 10s et 30s)
- 📋 **Rapport journalier automatique** (production, arrêt, cycles, alertes)
- 📱 **Export JSON temps réel** pour application mobile
- 🌐 **Dashboard web** avec graphiques interactifs
- 🔔 **Notifications push Firebase** (optionnel)

## 🏗️ Architecture

```
┌─────────────────────────────────┐
│    Machine SEHO MWS 2300        │
│    Résistances Triphasées       │
│         (3 phases)              │
└──────────────┬──────────────────┘
               │ Courant
               ▼
┌─────────────────────────────────┐
│  Janitza UMG 96RM-E-RCM        │
│  Compteur d'énergie             │
│  Modbus TCP (192.168.1.100:502) │
└──────────────┬──────────────────┘
               │ Ethernet
               ▼
┌─────────────────────────────────┐
│       Raspberry Pi 4/5          │
│  ┌───────────────────────────┐  │
│  │ surveillance_janitza.py   │  │
│  │ - Acquisition Modbus      │  │
│  │ - Détection pannes        │  │
│  │ - Prédiction tendances    │  │
│  │ - Export JSON              │  │
│  └─────────┬─────────────────┘  │
│  ┌─────────▼─────────────────┐  │
│  │ api_server.py             │  │
│  │ - API REST Flask          │  │
│  │ - Dashboard Web           │  │
│  │ - Historique & graphiques │  │
│  └───────────────────────────┘  │
└─────────────────────────────────┘
         │              │
    SQLite DB      Dashboard Web
  surveillance.db   :5000/
```

## 📁 Structure du Projet

```
stage-surveillance-janitza-seho/
├── surveillance_janitza.py      # Script principal d'acquisition & analyse
├── api_server.py                # Serveur Flask (API + dashboard web)
├── dashboard.html               # Interface web de supervision
├── screenshots/                 # Captures d'écran du système
├── docs/
│   └── rapport_sabrine_masmoudi.pdf  # Rapport de stage complet
├── .gitignore
└── README.md
```

## 🚀 Installation

### Prérequis

- Raspberry Pi 4 ou 5
- Python 3.9+
- Connexion Ethernet vers le Janitza UMG 96RM
- Accès réseau au compteur sur `192.168.1.100:502`

### Installation des dépendances

```bash
pip install flask pymodbus
```

### Configuration

Éditez les paramètres réseau dans `surveillance_janitza.py` :

```python
IP_JANITZA          = "192.168.1.100"   # Adresse IP du compteur
PORT_MODBUS         = 502               # Port Modbus TCP
DEVICE_ID           = 1                 # ID Modbus du device
INTERVALLE_LECTURE_S = 2                # Intervalle de mesure (secondes)
```

### Lancement

**Terminal 1** — Acquisition et analyse :
```bash
python surveillance_janitza.py
```

**Terminal 2** — Serveur web (dashboard) :
```bash
python api_server.py
```

Le dashboard sera accessible sur `http://<IP_DU_PI>:5000/`

## 📊 Dashboard Web

Le dashboard inclut :
- Courants triphasés en temps réel (I1, I2, I3)
- Puissance estimée par phase
- Indice de santé global de la machine
- Historique des alertes et pannes
- Tendances prédictives

## 🔬 Algorithmes

| Fonctionnalité | Description |
|----------------|-------------|
| **Détection panne franche** | Seuil de courant minimal par phase |
| **Déséquilibre inter-phases** | Matrice de signature (ratios mutuels I1/I2/I3) |
| **Prédiction** | Tendance linéaire + estimation temps avant seuil critique |
| **Indice de santé** | Score combiné 0-100% (usage + cycles + tendance) par phase |
| **MTBF** | Heures cumulées / nombre de pannes réelles par phase |

## 📄 Documentation

- [Rapport de stage complet (PDF)](./docs/rapport_sabrine_masmoudi.pdf)

## 🛠️ Technologies

- **Langage** : Python 3.9+
- **Plateforme** : Raspberry Pi 4/5
- **Communication** : Modbus TCP (pymodbus)
- **Base de données** : SQLite (mode WAL)
- **API / Web** : Flask
- **Compteur** : Janitza UMG 96RM-E-RCM
- **Machine** : SEHO MWS 2300 (2340-LRT)
- **Entreprise** : Marquardt Tunisie

## 👤 Auteur

**Sabrine Masmoudi**  
- GitHub : [@sabrinemasmoudi-lgtm](https://github.com/sabrinemasmoudi-lgtm)

## 📜 Licence

Ce projet est réalisé dans le cadre d'un stage ingénieur chez Marquardt Tunisie.
