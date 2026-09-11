# 🎧 SpotifyAzureProject

Pipeline de données de bout en bout sur **Microsoft Azure**, simulant l'ingestion, le chargement incrémental (CDC) et la transformation de données de streaming musical de type *Spotify*, selon une **architecture Medallion (Bronze → Silver → Gold)**.

![Azure](https://img.shields.io/badge/Microsoft%20Azure-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Azure Data Factory](https://img.shields.io/badge/Azure%20Data%20Factory-0078D4?style=for-the-badge&logo=azuredataengineering&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=microsoftsqlserver&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=for-the-badge&logo=delta&logoColor=white)

---

## 📋 Sommaire

- [Description](#-description)
- [Architecture globale](#️-architecture-globale)
- [Technologies utilisées](#️-technologies-utilisées)
- [Modèle de données (schéma en étoile)](#️-modèle-de-données-schéma-en-étoile)
- [Structure du dépôt](#-structure-du-dépôt)
- [Fonctionnement détaillé du pipeline](#-fonctionnement-détaillé-du-pipeline)
- [Captures d'écran du projet](#-captures-décran-du-projet)
- [Comment reproduire ce projet](#-comment-reproduire-ce-projet)
- [Auteur](#-auteur)

---

## 📖 Description

Ce projet met en place un **pipeline ETL/ELT complet** pour un cas d'usage de streaming musical (façon Spotify) :

1. Les données transactionnelles (utilisateurs, artistes, morceaux, écoutes) sont stockées dans une base **Azure SQL Database**, modélisée en **schéma en étoile**.
2. **Azure Data Factory (ADF)** orchestre l'extraction **incrémentale** de ces données (via un mécanisme de **CDC — Change Data Capture — basé sur des colonnes de date/heure**) et les dépose au format **Parquet** dans un **Azure Data Lake Storage Gen2**.
3. **Azure Databricks** (avec **Unity Catalog** et des pipelines **Lakeflow / Delta Live Tables**) reprend ces fichiers pour les transformer selon les trois couches classiques **Bronze → Silver → Gold**, jusqu'à obtenir un modèle analytique final prêt pour la restitution (BI / reporting).
4. Le déploiement des artefacts Databricks est industrialisé via **Databricks Asset Bundles (DAB)**.

L'objectif pédagogique du projet est de démontrer la maîtrise d'un pipeline **Data Engineering moderne sur Azure** : ingestion incrémentale, paramétrage dynamique, architecture Medallion et Lakehouse.

---

## 🏗️ Architecture globale

```mermaid
flowchart LR
    subgraph Source["🗄️ Source"]
        SQL[(Azure SQL Database<br/>spotifyProjectDB)]
    end

    subgraph ADF["🏭 Azure Data Factory"]
        FE["Pipeline ForEach<br/>(incremental_ingestion)"]
        LOOKUP["Lookup: last_cdc"]
        COPY["Copy Data<br/>(requête dynamique CDC)"]
        IF["If Condition<br/>(dataRead > 0)"]
    end

    subgraph Lake["🌊 Azure Data Lake Storage Gen2"]
        BRONZE[("Bronze<br/>fichiers Parquet bruts")]
    end

    subgraph DBX["🧱 Azure Databricks (Unity Catalog)"]
        SILVER[("Silver<br/>données nettoyées")]
        GOLD[("Gold<br/>modèle en étoile final")]
    end

    BI["📊 BI / Reporting"]

    SQL --> LOOKUP --> COPY --> IF
    FE -.contient.-> LOOKUP
    COPY --> BRONZE
    BRONZE --> SILVER --> GOLD --> BI
```

**Principe du chargement incrémental (CDC applicatif) :**
Pour chaque table (`DimUser`, `DimArtist`, `DimTrack`, `DimDate`, `FactStream`), ADF conserve la **dernière valeur de la colonne de suivi** (`updated_at`, `date`, `stream_timestamp`…) dans une table de contrôle. À chaque exécution, seules les lignes **plus récentes** que ce dernier point de contrôle sont extraites, puis le point de contrôle est mis à jour — évitant ainsi de recharger l'intégralité des données à chaque run.

---

## 🛠️ Technologies utilisées

| Catégorie | Outil / Service |
|---|---|
| Base de données source | Azure SQL Database |
| Orchestration & intégration | Azure Data Factory (ForEach, Lookup, Copy Data, If Condition, Set Variable, Script) |
| Stockage Data Lake | Azure Data Lake Storage Gen2 (conteneurs `bronze`, `silver`, `gold`) |
| Traitement Big Data / Lakehouse | Azure Databricks (Unity Catalog, Lakeflow / Delta Live Tables) |
| Déploiement CI/CD Databricks | Databricks Asset Bundles (DAB) |
| Format de fichiers | Parquet / Delta |
| Langage de requêtage | SQL / Expressions dynamiques ADF |

---

## 🗂️ Modèle de données (schéma en étoile)

Le schéma cible (`spotifyProjectDB`, couche **Gold**) suit une modélisation en étoile classique :

- **`DimUser`** — `user_id`, `user_name`, `country`, `subscription_type`, `start_date`, `end_date`, `updated_at`
- **`DimArtist`** — `artist_id`, `artist_name`, `genre`, `country`…
- **`DimTrack`** — `track_id`, `track_name`, `artist_id`, `album_name`, `duration_sec`, `release_date`…
- **`DimDate`** — dimension calendrier
- **`FactStream`** *(table de faits)* — écoutes/streams reliant utilisateurs, morceaux et dates, avec `stream_timestamp`

---

## 📁 Structure du dépôt

```
SpotifyAzureProject/
├── factory/            # Paramètres globaux de la Data Factory
├── linkedService/       # Services liés ADF (azzuresql, dataLake)
├── dataset/             # Datasets ADF (source SQL, Parquet dynamique, JSON…)
├── pipeline/            # Pipelines ADF (incremental_ingestion, ForEach, backfill…)
├── GPROD/.bundle/spotify_dab/prod/  # Databricks Asset Bundle (déploiement des jobs/pipelines Databricks)
├── publish_config.json # Configuration de publication ADF
├── screenshots/         # Captures d'écran illustrant le projet
└── README.md
```

---

## ⚙️ Fonctionnement détaillé du pipeline

### 1. Azure SQL Database — Création du schéma source
Les tables du schéma en étoile (`DimUser`, `DimArtist`, `DimTrack`, `DimDate`, `FactStream`) sont créées via un script DDL exécuté dans l'éditeur de requêtes Azure SQL.

### 2. Services liés (Linked Services)
Deux services liés sont configurés dans ADF :
- `azzuresql` → connexion à **Azure SQL Database**
- `dataLake` → connexion à **Azure Data Lake Storage Gen2**

### 3. Pipeline paramétré avec boucle `ForEach`
Le pipeline `incremental_ingestion` reçoit en paramètre un **tableau JSON** (`loop_input`) décrivant chaque table à traiter :

```json
{
  "schema": "dbo",
  "table": "DimUser",
  "cdc_column": "updated_at",
  "from_date": ""
}
```

Une activité **ForEach** itère sur ce tableau et exécute, pour chaque table, la séquence suivante :

1. **`last_cdc` (Lookup)** — récupère la dernière valeur de checkpoint enregistrée pour la table.
2. **`current` (Set Variable)** — capture l'horodatage courant (`@utcNow()`), qui servira de nouveau checkpoint.
3. **`AzureSqlToLake` (Copy Data)** — exécute une requête **dynamique** construite à partir des paramètres du pipeline :
   ```sql
   SELECT * FROM @{pipeline().parameters.schema}.@{pipeline().parameters.table}
   WHERE @{pipeline().parameters.cdc_column} > '@{activity('last_cdc').output.value[0].cdc_timestamp}'
   ```
   Le résultat est déposé dans le conteneur **`bronze`** du Data Lake, au format Parquet, avec un nom de fichier dynamique : `@concat(pipeline().parameters.table, '_', variables('current'))`.
4. **`If incrementalData` (If Condition)** — vérifie si des lignes ont été lues (`@greater(activity('AzureSqlToLake').output.dataRead, 0)`) :
   - **Vrai** → une activité **Script** récupère le `MAX(cdc_column)` réellement chargé, puis une activité **Copy Data** met à jour la table de contrôle (`update_last_cdc`) avec ce nouveau point de repère.
   - **Faux** → le fichier Parquet vide généré est supprimé (`DeleteEmptyFile`), aucun checkpoint n'est modifié.

Un pipeline complémentaire gère le **premier chargement / backfilling** (chargement complet initial) avant le passage en mode incrémental.

### 4. Azure Databricks — Architecture Medallion
Trois **external locations** sont déclarées dans **Unity Catalog**, pointant vers les conteneurs ADLS Gen2 : `bronze`, `silver`, `gold`.

Un pipeline **Lakeflow / Delta Live Tables** (de type *ETL pipeline*) lit les fichiers de la couche **Silver** (ex. `silver.fact_stream`), les transforme via des tables de streaming intermédiaires (`fact_stream_stg`) puis alimente les tables finales de la couche **Gold** (ex. `gold.dim_track`, `gold.fact_stream`), directement interrogeables en SQL via le Catalog Explorer.

### 5. Déploiement
Le dossier `GPROD/.bundle/spotify_dab/prod` correspond à un **Databricks Asset Bundle**, permettant de déployer les jobs/pipelines Databricks de manière reproductible (`databricks bundle deploy`).

---

## 🖼️ Captures d'écran du projet

### 1️⃣ Création de la base de données et du schéma en étoile
Script DDL créant les tables `DimUser`, `DimArtist`, `DimTrack`, `DimDate` et `FactStream` dans Azure SQL Database.

![Création des tables SQL](screenshots/01-creation-tables-sql.png)

Les tables apparaissent ensuite dans l'explorateur de la base :

![Tables créées](screenshots/02-tables-creees.png)

### 2️⃣ Configuration des services liés Azure Data Factory
Connexion vers **Azure SQL Database** et **Azure Data Lake Storage Gen2**.

![Services liés ADF](screenshots/03-linked-services-adf.png)

### 3️⃣ Pipeline principal avec boucle `ForEach`
Le pipeline parcourt dynamiquement la liste des tables à charger.

![Pipeline ForEach](screenshots/05-pipeline-foreach-loop.png)

Paramètre `loop_input` transmis à la boucle :

![Paramètre loop_input](screenshots/06-parametre-loop-input.png)
![Expression Items du ForEach](screenshots/07-foreach-items-expression.png)

### 4️⃣ Enchaînement Lookup → Copy Data → If Condition
Détail des activités internes de la boucle `ForEach`.

![Lookup, Copy Data, If Condition](screenshots/08-lookup-copy-if-condition.png)

Variable `current` capturant l'horodatage du run (`@utcNow()`) :

![Variable current](screenshots/04-variable-current-utcnow.png)

### 5️⃣ Requête d'extraction incrémentale dynamique
Construction dynamique de la requête SQL à partir des paramètres `schema`, `table` et `cdc_column`.

![Requête incrémentale dynamique](screenshots/10-requete-incrementale-dynamique.png)
![Configuration de la source dynamique](screenshots/09-source-dynamique-config.png)

Configuration du fichier de sortie (sink) et de son nom dynamique dans le conteneur `bronze` :

![Configuration du sink dynamique](screenshots/11-sink-dynamique-config.png)
![Nom de fichier dynamique](screenshots/12-sink-nom-fichier-dynamique.png)

### 6️⃣ Condition `If` — présence de nouvelles données
Vérifie si des lignes ont été chargées (`dataRead > 0`) afin de décider de la suite du traitement.

![Condition If dataRead](screenshots/13-condition-if-dataread.png)

**Cas vrai** — récupération et mise à jour du dernier checkpoint CDC :

![Script de calcul du max CDC](screenshots/14-script-max-cdc-config.png)
![Requête du max CDC](screenshots/15-script-max-cdc-query.png)
![Configuration source - mise à jour du CDC](screenshots/16-update-cdc-source-config.png)
![Requête de mise à jour du CDC](screenshots/17-update-cdc-source-query.png)
![Configuration sink - mise à jour du CDC](screenshots/18-update-cdc-sink-config.png)

**Cas faux** — suppression du fichier vide généré :

![Suppression du fichier vide](screenshots/19-delete-empty-file-config.png)

### 7️⃣ Premier chargement (backfilling)
Requête utilisée pour le chargement initial complet, avant le passage en mode incrémental.

![Requête de backfilling](screenshots/20-backfill-first-load-query.png)

### 8️⃣ Suivi d'exécution du pipeline
Résultat d'une exécution complète : toutes les activités se terminent avec succès (`Succeeded`).

![Exécution du pipeline ForEach](screenshots/21-execution-pipeline-foreach.png)

### 9️⃣ Databricks — Architecture Medallion (Unity Catalog)
Déclaration des trois zones externes **bronze**, **silver** et **gold** pointant vers le Data Lake.

![Unity Catalog - Bronze/Silver/Gold](screenshots/22-databricks-unity-catalog-medallion.png)

### 🔟 Exécution du pipeline Databricks (Lakeflow / DLT)
Transformation Silver → Gold via des tables de streaming (`fact_stream_stg` → `fact_stream`).

![Exécution du pipeline Databricks](screenshots/23-databricks-pipeline-execution.png)

### 1️⃣1️⃣ Table finale dans la couche Gold
Interrogation SQL de la table `gold.dim_track`, prête pour l'analyse.

![Table Gold dim_track](screenshots/24-gold-table-dim-track.png)

---

## 🚀 Comment reproduire ce projet

1. **Créer les ressources Azure** : un groupe de ressources, une Azure SQL Database, un compte Azure Data Lake Storage Gen2 (conteneurs `bronze`, `silver`, `gold`) et une instance Azure Data Factory.
2. **Exécuter le script DDL** pour créer les tables du schéma en étoile dans Azure SQL Database.
3. **Configurer les services liés** dans ADF vers la base SQL et le Data Lake.
4. **Importer les pipelines** du dossier [`pipeline/`](./pipeline) dans ADF (ou les reconstruire manuellement avec l'activité `ForEach` décrite ci-dessus).
5. **Créer un espace Azure Databricks** avec Unity Catalog activé, puis déclarer les *external locations* `bronze`, `silver`, `gold`.
6. **Déployer les artefacts Databricks** via Databricks Asset Bundles :
   ```bash
   databricks bundle deploy -t prod
   databricks bundle run -t prod
   ```
7. **Déclencher le pipeline ADF** (manuellement ou via un trigger planifié) puis **lancer le pipeline Databricks** pour propager les données jusqu'à la couche Gold.

---

## 👤 Auteur

**Ilyass R’baai**
École Marocaine des Sciences de l'Ingénieur 
🔗 
R’BAAI Ilyass

---

⭐ N'hésitez pas à mettre une étoile sur le dépôt si ce projet vous a été utile !
