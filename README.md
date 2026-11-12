# Spark Log Streaming — Pipeline temps réel de traitement de logs

Projet académique de **Big Data** implémentant un pipeline complet d'ingestion, de traitement et de visualisation de logs en temps réel, à l'aide d'Apache Kafka, Apache Spark Structured Streaming, HDFS, PostgreSQL et Grafana — le tout orchestré avec Docker Compose.

---

## Table des matières

1. [Architecture globale](#architecture-globale)
2. [Stack technique](#stack-technique)
3. [Structure du projet](#structure-du-projet)
4. [Infrastructure Docker](#infrastructure-docker)
5. [Producers Kafka](#producers-kafka)
6. [Traitement Spark (Scala)](#traitement-spark-scala)
7. [Démarrage rapide](#démarrage-rapide)
8. [Interfaces web](#interfaces-web)

---

## Architecture globale

```
┌─────────────────────────────────────────────────────────────────┐
│                        PRODUCERS (Python)                       │
│  web_producer.py     auth_producer.py   microservices_producer  │
│  (JSON / HTTP logs)  (Syslog / SSH)     (Apache Common Log)     │
└───────────────────────────┬─────────────────────────────────────┘
                            │  Kafka Topics
                            │  web-logs | auth-logs | microservices-logs
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                   APACHE KAFKA + ZOOKEEPER                      │
└───────────────────────────┬─────────────────────────────────────┘
                            │  Structured Streaming
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              SPARK MASTER + 2 WORKERS  (Scala)                  │
│                                                                 │
│  LogParser       → Parsing multi-format + Géoloc + UA          │
│  Aggregations    → Métriques par fenêtre temporelle (1 min)     │
│  AlertSystem     → Détection anomalies en temps réel           │
│  GrafanaSink     → Écriture PostgreSQL (métriques + alertes)    │
│  StorageHDFS     → Persistance Parquet sur HDFS                 │
│  SparkSQLQueries → Requêtes analytiques batch sur HDFS         │
└──────────┬──────────────────────────────┬───────────────────────┘
           │                              │
           ▼                              ▼
┌─────────────────┐             ┌──────────────────────┐
│  HDFS           │             │  POSTGRESQL           │
│  (Parquet,      │             │  metrics_per_minute   │
│   partitionné   │             │  alerts               │
│   par date/src) │             └──────────┬───────────┘
└─────────────────┘                        │
                                           ▼
                                  ┌─────────────────┐
                                  │  GRAFANA         │
                                  │  (Dashboards)    │
                                  └─────────────────┘
```

---

## Stack technique

| Composant | Technologie | Version |
|---|---|---|
| Streaming | Apache Spark Structured Streaming | 4.0.2 |
| Langage Spark | Scala | 2.13.16 |
| Message broker | Apache Kafka (Confluent) | 7.6.9 |
| Coordination | Apache Zookeeper | 7.6.9 |
| Stockage distribué | Hadoop HDFS | 3.2.1 |
| Base de données | PostgreSQL | 15 |
| Visualisation | Grafana | latest |
| Producers | Python + kafka-python | — |
| Géolocalisation | MaxMind GeoLite2 | — |
| User-Agent parsing | ua-parser (uap-java) | 1.6.1 |
| Build | sbt + sbt-assembly | — |
| Orchestration | Docker Compose | — |

---

## Structure du projet

```
spark-log-streaming-project/
├── docker-compose.yml              # Infrastructure complète (10 services)
├── run.ps1                         # Script de déploiement Spark (PowerShell)
├── data/                           # Données MongoDB + GeoLite2 DB
│   └── GeoLite2-City.mmdb
├── producers/                      # Générateurs de logs Python
│   ├── web_producer.py             # Logs HTTP au format JSON
│   ├── auth_producer.py            # Logs SSH/auth au format Syslog
│   └── microservices_producer.py   # Logs Apache Common Log
└── spark-streaming/                # Application Spark (Scala / sbt)
    ├── build.sbt
    └── src/main/scala/projet/
        ├── LogParser.scala         # Parsing + enrichissement (géoloc, UA)
        ├── Aggregations.scala      # Métriques temps réel par fenêtre
        ├── AlertSystem.scala       # Système d'alertes
        ├── GrafanaSink.scala       # Écriture PostgreSQL → Grafana
        ├── StorageHDFS.scala       # Persistance Parquet sur HDFS
        └── SparkSQLQueries.scala   # Requêtes analytiques batch
```

---

## Infrastructure Docker

L'ensemble du cluster est défini dans un seul fichier `docker-compose.yml` et démarre en une commande. Les 10 services partagent le réseau `logs_stream_network`.

### Services et ports exposés

| Service | Image | Port(s) | Rôle |
|---|---|---|---|
| **zookeeper** | confluentinc/cp-zookeeper:7.6.9 | `2181` | Coordination Kafka |
| **kafka** | confluentinc/cp-kafka:7.6.9 | `9092` | Broker de messages |
| **namenode** | bde2020/hadoop-namenode:2.0.0-hadoop3.2.1 | `9870`, `9000` | HDFS master + API |
| **datanode** | bde2020/hadoop-datanode:2.0.0-hadoop3.2.1 | — | Stockage HDFS |
| **spark-master** | apache/spark:4.0.2-scala2.13-java17 | `8080`, `7077` | Spark UI + cluster |
| **spark-worker-1** | apache/spark:4.0.2-scala2.13-java17 | — | Worker Spark |
| **spark-worker-2** | apache/spark:4.0.2-scala2.13-java17 | — | Worker Spark |
| **postgres** | postgres:15 | `5432` | Stockage métriques/alertes |
| **grafana** | grafana/grafana:latest | `3001` | Visualisation |

### Volumes persistants

```yaml
volumes:
  namenode_data:   # Métadonnées HDFS
  datanode_data:   # Blocs HDFS
  postgres_data:   # Tables PostgreSQL
  grafana_data:    # Dashboards Grafana
```

### Configuration Kafka

Kafka expose deux listeners pour gérer les deux cas d'usage :
- `PLAINTEXT://kafka:29092` — communication inter-conteneurs (Spark → Kafka)
- `PLAINTEXT_HOST://localhost:9092` — accès depuis la machine hôte (producers Python)

```yaml
KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:9092
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
```

### Topics Kafka utilisés

| Topic | Format | Producer |
|---|---|---|
| `web-logs` | JSON | `web_producer.py` |
| `auth-logs` | Syslog | `auth_producer.py` |
| `microservices-logs` | Apache Common Log | `microservices_producer.py` |

---

## Producers Kafka

Trois producteurs Python simulent du trafic réel en continu (1 log toutes les 500 ms).

### `web_producer.py` — Logs HTTP (JSON)
Publie sur `web-logs` des événements structurés avec niveau (INFO/WARN/ERROR pondérés 70/20/10), IP, User-Agent, temps de réponse et code HTTP.

```json
{
  "timestamp": "2026-11-12 14:32:01",
  "level": "INFO",
  "source": "web",
  "message": "Request to /api/users",
  "ip": "10.0.0.2",
  "user_agent": "Mozilla/5.0",
  "response_time": 342,
  "status_code": 200
}
```

### `auth_producer.py` — Logs SSH/Auth (Syslog)
Publie sur `auth-logs` des événements d'authentification (succès, échec, sudo) au format Syslog standard, avec pondération réaliste (50 % succès, 30 % échecs).

```
2026 Nov 12 14:32:01 server01 sshd[4821]: Failed password for user root from 192.168.1.1
```

### `microservices_producer.py` — Logs HTTP (Apache Common Log)
Publie sur `microservices-logs` des logs au format Apache Combined Log, incluant méthode HTTP, endpoint, code statut et temps de réponse.

```
10.0.0.2 - john [12/Nov/2026:14:32:01 +0000] "GET /api/orders HTTP/1.1" 200 512
```

---

## Traitement Spark (Scala)

Toutes les applications Spark lisent les trois topics Kafka simultanément, parsent chaque format, et construisent un DataFrame unifié avec les colonnes : `timestamp`, `level`, `source`, `message`, `ip`, `user_agent`, `response_time`, `status_code`.

### `LogParser` — Parsing & enrichissement
Point d'entrée principal. Parse les trois formats (JSON, Apache, Syslog) puis enrichit chaque log avec :
- **Géolocalisation** via MaxMind GeoLite2 (UDF Scala → `"Paris, France"`)
- **Parsing User-Agent** via uap-java (UDF Scala → `"Chrome / Windows"`)

### `Aggregations` — Métriques par fenêtre temporelle
Calcule toutes les minutes, par source :
- Nombre de requêtes (`requests_per_minute`)
- Nombre d'erreurs (`errors_per_minute`)
- Taux d'erreur en % (`error_rate_pct`)
- Latence min/moy/max en ms (`latency`)

Les checkpoints sont stockés sur HDFS (`hdfs://namenode:9000/checkpoints/aggregations/`).

### `AlertSystem` — Détection d'anomalies
Déclenche des alertes en streaming selon quatre seuils :

| Alerte | Seuil | Condition |
|---|---|---|
| Taux d'erreur élevé | ≥ 30 % d'erreurs/minute | toutes sources |
| Latence critique | ≥ 1 000 ms (avg/min) | web + microservices |
| Intrusion SSH | ≥ 5 échecs `Failed password` /min par IP | auth |
| Pic de charge | ≥ 100 requêtes/minute | toutes sources |

### `GrafanaSink` — Écriture PostgreSQL
Utilise `foreachBatch` pour écrire par micro-batch vers deux tables PostgreSQL :
- **`metrics_per_minute`** : métriques agrégées (total, erreurs, warnings, taux d'erreur, latence moy)
- **`alerts`** : alertes déclenchées avec type, source, message et valeur

Grafana se connecte ensuite directement à PostgreSQL pour visualiser ces données.

### `StorageHDFS` — Persistance Parquet
Écrit les logs parsés en format **Parquet** sur HDFS, partitionné par `date` et `source` :
```
hdfs://namenode:9000/logs/parquet/
  └── date=2026-11-12/
        ├── source=web/
        ├── source=auth/
        └── source=microservices/
```

### `SparkSQLQueries` — Analytique batch
Lit les fichiers Parquet stockés sur HDFS et exécute 8 requêtes analytiques SQL :
1. Volume total de logs par source
2. Nombre et % d'erreurs par source
3. Temps de réponse moyen par endpoint
4. Distribution des codes HTTP
5. Top IPs les plus actives
6. Activité par heure de la journée
7. Top 10 des minutes avec le plus de tentatives d'intrusion
8. Performance globale par source (taux d'erreur, latence)

---

## Démarrage rapide

### Prérequis
- Docker Desktop
- Python 3.x + `pip install kafka-python`
- sbt (pour builder le JAR Scala)

### 1. Démarrer l'infrastructure

```bash
docker-compose up -d
```

Vérifier que tous les services sont `healthy` :

```bash
docker-compose ps
```

### 2. Builder le JAR Spark

```bash
cd spark-streaming
sbt assembly
```

Le JAR est généré dans `target/scala-2.13/log-streaming-assembly-1.0.jar`.

### 3. Lancer les producers

```bash
# Dans trois terminaux séparés
python producers/web_producer.py
python producers/auth_producer.py
python producers/microservices_producer.py
```

### 4. Soumettre une application Spark

```powershell
# Depuis la racine du projet (PowerShell)
.\run.ps1 -class projet.LogParser
.\run.ps1 -class projet.Aggregations
.\run.ps1 -class projet.AlertSystem
.\run.ps1 -class projet.GrafanaSink
.\run.ps1 -class projet.StorageHDFS
.\run.ps1 -class projet.SparkSQLQueries   # après avoir stocké des logs HDFS
```

Le script `run.ps1` copie automatiquement le JAR dans les trois conteneurs Spark avant de soumettre le job.

### 5. Arrêter l'infrastructure

```bash
docker-compose down
# Pour supprimer aussi les volumes (données) :
docker-compose down -v
```

---

## Interfaces web

| Interface | URL | Credentials |
|---|---|---|
| Spark Master UI | http://localhost:8080 | — |
| HDFS NameNode UI | http://localhost:9870 | — |
| Grafana | http://localhost:3001 | admin / admin |
| PostgreSQL | localhost:5432 | spark / spark123 |

### Configurer Grafana
1. Ouvrir http://localhost:3001
2. Ajouter une datasource **PostgreSQL** : host `postgres:5432`, database `logs_db`, user `spark`, password `spark123`
3. Créer des dashboards sur les tables `metrics_per_minute` et `alerts`
