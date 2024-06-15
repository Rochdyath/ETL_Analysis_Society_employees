# ETL_Analysis_Society_employees
Description


# Intructions d'installation
Prenez soin de suivre toutes ces étapes pour le bon fonctionnement du projet

## Créer l'environnement
Tout d'abord cloner le repo et déplacez vous dans le repo
```bash
    git clone git@github.com:EPITECH-BENIN/ETL_Analysis_Society_employees.git
    cd ETL_Analysis_Society_employees
```
Ensuite créez et activez votre environnement python
```bash
    python3 -m venv .venv
    source .venv/bin/activate
```

## Installer les dépendances
### Apache airflow
Initialiser les variables d'environnement
```bash
    export AIRFLOW_HOME="$(pwd)"
    AIRFLOW_VERSION=2.9.1
    PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
```
Installer airflow
```bash
    pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```
Migrer la base de donnée
```bash
    airflow db migrate
```
Créer un utilisateur ayant le rôle admin. N'oubliez pas de changez les valeurs username, firstname, lastname et email. Un mot de passe vous sera demander après l'exécution de cette commande. Vous aurez besoin de ce mot de passe et de l'username pour accéder à votre profil aiflow
```bash
    airflow users create \
        --username your_username \
        --firstname your_firstname \
        --lastname your_lastname \
        --role Admin \
        --email your_email@gmail.com
```
Pour plus de détails sur l'installation de airflow, vous pouvez visitez la [documentation officielle de apache airflow](https://airflow.apache.org/docs/apache-airflow/stable/start.html)

### Apache pyspark
```bash
    pip install pyspark
    pip install pyspark[sql]
    pip install pyspark[pandas_on_spark] plotly
    pip install pyspark[connect]
```
Pour plus de détails sur l'installation de pyspark, vous pouvez visitez la [documentation officielle de apache spark](https://spark.apache.org/docs/latest/api/python/getting_started/install.html)

### Openpyxl
```bash
    pip install openpyxl
```


# Lancement du programme
Lancer le serveur sur le port 9696. Vous pouvez changer le port si vous le souhaitez
```bash
    airflow webserver --port 9696
```
Dans un autre terminal, exécutez les commandes suivantes pour lancer le scheduler. Assurez-vous d'être dans votre environnement. Si ce n'est pas le cas, activez le.
```bash
    export AIRFLOW_HOME="$(pwd)"
    airflow scheduler
```

Rendez vous ensuite sur https://localhost:9696 et connectez vous à l'interface airflow. Activez ensuite les DAGs society_etl et society_analysis. Lancez ensuite le DAG society_etl. Si il réussi le second DAG sera automatiquement lancer.

**:warning:Attention:warning:**: Si vous ne retrouvez pas ces DAGs, patientez en rafraîchissant la page. Si vous patientez assez longtemps sans résultats, exécutez la commande ci - dessous et reprenez les étapes d'installation de apache airflow plus haut.
```bash
    rm -rf logs airflow.cfg airflow.db webserver_config.py
```

# Description des DAGs
## ETL: society_etl
Le DAG society_etl comporte 4 tâches:
* **extraction**: Récupère les fichiers .csv dans le dossier data sous forme de dataframe pyspark. Il retourne ensuite ces dataframes sous forme de dictionaires
* **transform**: Join les dataframes retournés par extraction selon les relation entre les colonnes en un seul dataframe qu'il retourne ensuite.
* **load**: Cré un dossier dest_data si il n'existe pas et sauvegarde le dataframe retourné par transform dans un fichier society.csv
* **run_society_analysis_dag**: Lance le dag society_analysis qui est décris en Partie 2

## Traitement: society_analysis
Le DAG society_analysis comporte 5 tâches:
* **get_budget_by_dept**: Calcule le budget par département et retourne le résultat sous forme de dataframe pyspark
* **get_salary_by_dept**: Calcule le salaire moyen par département et retourne le résultat sous forme de dataframe pyspark
* **get_gender_by_dept**: Calcule le nombre de femme et d'homme par département et retourne le résultat sous forme de dataframe pyspark
* **save_gender_salary_repartition**: Calcule le salaire moyen des hommes d'une part et des femmes de l'autre. On regroupe ensuite le tout en seul dataframe qui est sauvegardé dans le fichier society_analysis.xlsx dans le dossier dest_data. Le résultat se présente sur la feuille: gender_salary_repartition
* **save_dept_analysis**: Join les résultats de get_budget_by_dept, get_salary_by_dept et get_gender_by_dept et le sauvegarde également dans le fichier society_analysis.xlsx. Le résultat se présente sur la feuille: department_analysis

**Bonne compréhension** :tada::smile::tada: