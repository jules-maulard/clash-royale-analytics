# Clash Royale Match-Up Analysis (Hadoop)

## Description
Projet d'analyse statistique distribuée (Hadoop MapReduce) visant à vérifier l'aléatoire du matchmaking de Clash Royale sur 35 millions de parties en comparant fréquences observées et théoriques.

## Structure
* **src/** : Code Java Hadoop (MapReduce).
* **analysis/** : Scripts Python pour l'analyse des résultats et visualisations.
* **benchmarks/** : Scripts et logs pour l'analyse de performance.
* **sample_match.json** : Exemple de structure de données brute.

## Utilisation

### 1. Compilation
```bash
mvn clean package
scp ./target/clash-royale-analytics-0.0.1.jar lsd:.
```

### 2. Exécution (Sur le cluster LSD)
```bash
ssh lsd
kinit
```

Syntaxe : yarn jar JAR INPUT OUTPUT
```bash    
yarn jar clash-royale-analytics-0.0.1.jar /user/auber/data_ple/clash_royale/raw_data_100K.json clash-100k-8 2>&1 | tee bench_100k_8.log
```

### 3. Récupération des résultats

Logs de benchmarks
```bash
scp lsd:bench* benchmarks/
```
    
Données CSV pour analyse
```bash
scp lsd:clash-*.csv analysis/data/
```
## Datasets (HDFS)
Chemin racine : /user/auber/data_ple/clash_royale/

* raw_data_100K.json
* raw_data_200K.json
* raw_data_500K.json
* raw_data_1M.json
* raw_data_10M.json
* raw_data.json (Full ~35M)

## Convention de nommage (Output)
Format : clash-TAILLE-ARCHETYPE-OPTIONS

* **TAILLE** : Identifiant du dataset (ex: 100k, 1M).
* **ARCHETYPE** : Taille min des sous-decks (ex: 8).
* **OPTIONS** :
  * (Vide) : Run complet standard.
  * noCombiner : Désactive le combiner.
  * clean | graph | stats : Lance uniquement une étape spécifique.