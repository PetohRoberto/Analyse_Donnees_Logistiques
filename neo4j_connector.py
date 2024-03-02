

from neo4j import GraphDatabase
from datetime import datetime
import pandas as pd
from PyQt5.QtWidgets import QFileDialog


class Neo4jConnector:

    def __init__(self):
        self._driver = None

        self.make_joins_query = ()
        self.sim_column_query = ("""


            MATCH (s1:Source)-[:POSSEDE]->(t1:Table)-[:CONTIENT]->(c1:Colonne)-[:EFFECTUE]->(a1:Analyse)
            MATCH (s2:Source)-[:POSSEDE]->(t2:Table)-[:CONTIENT]->(c2:Colonne)-[:EFFECTUE]->(a2:Analyse)
            WHERE toLower(c1.nom) = toLower(c2.nom) AND s1 < s2  
                                 
            MERGE (t1)-[:LIE]->(integre:INTEGRATION{type:$type_integration})<-[:LIE]-(t2)
            MERGE (c1)-[:CORRESPOND]->(integre)<-[:CORRESPOND]-(c2)
            SET c1.table_provenance = t1.nom
            SET c2.table_provenance = t2.nom
                                 
            WITH s1, t1, c1, a1, s2, t2, c2, a2, integre
            MATCH (t1)-[contient1:CONTIENT]->(c1)
            MATCH (t2)-[contient2:CONTIENT]->(c2)
            DELETE contient1, contient2

            RETURN s1, t1, a1, s2, t2, a2, integre, COUNT(c1.nom) AS total
                """)
        

    def close(self):
         if self._driver is not None:
            self._driver.close()

    def connect(self):
        uri = "bolt://localhost:7687/test"
        user = "neo4j"
        password = "LogisticData"
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def integrate_by_name(self):
        type_integration = "integration par nom"
        with self._driver.session() as session:
                # Begin a transaction
                with session.begin_transaction() as tx:
                    result = tx.run((self.sim_column_query), type_integration=type_integration)
                    records = list(result)
                    total = sum(record['total'] for record in records) if records else 0

        print("integration par nom terminée")
        print(f"Total de {total} paire(s) de clonnes intégrées")
        return total
                    
                    
    def store_in_db(self, file_name, file_path, results_df, sheet_name=None):
        if not (results_df.empty and file_name.empty and results_df.empty):
            with self._driver.session() as session:
                # Begin a transaction
                with session.begin_transaction() as tx:
                    # Créer le nœud Source
                    source = file_name
                    if sheet_name != None and file_path.lower().endswith(('.xls', '.xlsx')):
                        table = sheet_name
                    else:
                        table = file_name

                    source_query = (
                        "MERGE (s:Source {nom: $source_name, chemin: $file_path})"
                        )
                    tx.run(source_query, source_name=source, file_path=file_path)

                    # Créer la relation POSSEDE s'il n'existe pas
                    posede_query = (
                    "MATCH (s:Source {nom: $source_name, chemin: $file_path}) "
                    "MERGE (s)-[:POSSEDE]->(t:Table {nom: $table_name})"
                    )

                    tx.run(posede_query, source_name=source, file_path=file_path, table_name=table)


                    # Parcourir les résultats et les stocker dans Neo4j
                    #for _, row in results_df.iterrows():
                    column = results_df['Colonne'][0]
                    print(f"SAVE -- nom : {column} - type : {type(column)}")
                    all_resultats = results_df['Resultats']

                    # Créer la relation CONTIENT
                    contient_query = (
                        "MATCH (s:Source{nom:$source_name})-[:POSSEDE]->(t:Table {nom: $table_name})"
                        "MERGE (t)-[:CONTIENT]->(c:Colonne {nom: $column_name})"
                    )
                    tx.run(contient_query, source_name=source, table_name=table, column_name=column)

                   # Create the relationship EFFECTUE query
                    effectue_query = (
                        " MATCH (s:Source {nom: $source_name})-[:POSSEDE]->(t:Table {nom: $table_name})-[:CONTIENT]->(c:Colonne {nom: $column_name}) "
                        " MERGE (c)-[:EFFECTUE]->(a:Analyse) "
                        " SET "
                    )

                    # Add dynamic SET clause based on resultats DataFrame
                    for resultats in all_resultats:
                        for result in resultats:
                            # Enclose both parameter name and value in backticks
                            parameter_name = f"`{result[0]}`"
                            parameter_value = result[1]
                            effectue_query += f"a.{parameter_name} = '{parameter_value}', "

                        # Remove the trailing comma
                        effectue_query = effectue_query.rstrip(', ')

                    # Run the Cypher query
                    tx.run(effectue_query, source_name=source, table_name=table, column_name=column)

                    profilage_query = (
                        "MATCH (s:Source {nom: $source_name, chemin: $file_path})-[:ACCEDE]->(p:Profilage {source: $source_profilage}) "
                        f"RETURN COUNT(*) AS count"
                    )
                    verification = tx.run(profilage_query, source_name=source, file_path=file_path, source_profilage=source)
                    
                    if(verification.single()["count"] == 0):
                        # Créer le nœud Profilage (avec dateCreation) et la relation ACCEDE
                        accede_query = (
                            "MATCH (s:Source {nom: $source_name, chemin: $file_path})-[:POSSEDE]->(t:Table {nom: $table_name}) "
                            f"MATCH (t)-[:CONTIENT]->(c:Colonne) "
                            f"MATCH (c)-[:EFFECTUE]->(a:Analyse) "
                            "MERGE (p:Profilage {source: $source_profilage, date_creation: $date_creation}) "
                            f"MERGE (s)-[:ACCEDE]->(p)"
                        )   
                        tx.run(accede_query, source_name=source, file_path=file_path, table_name=table, source_profilage=source, date_creation=datetime.now().strftime("%d/%m/%Y_%H:%M:%S"))
    
                    # Créer la relation STOCKE + modifier la date de MAJ dans Profilage
                    stocke_query = (
                        "MATCH (s:Source {nom: $source_name, chemin: $file_path})-[:POSSEDE]->(t:Table {nom: $table_name}) "
                        f"MATCH (t)-[:CONTIENT]->(c:Colonne) "
                        f"MATCH (c)-[:EFFECTUE]->(a:Analyse) "
                        "MATCH (s)-[:ACCEDE]->(p:Profilage {source: $source_profilage})"
                        f"MERGE (a)-[:STOCKE]->(p)"
                        f"SET p.date_maj= $date_maj"
                    )
                    tx.run(stocke_query, source_name=source, file_path=file_path, table_name=table, source_profilage=source, date_maj=datetime.now().strftime("%d/%m/%Y_%H:%M:%S"))
            self.make_joins()

    def make_joins(self):
        column_query = """
            MATCH (s1:Source)-[:POSSEDE]->(t1:Table)-[:CONTIENT]->(c1:Colonne)
            MATCH (s2:Source)-[:POSSEDE]->(t2:Table)-[:CONTIENT]->(c2:Colonne)
            WHERE s1=s2 AND c1.nom = c2.nom AND t1 < t2

            MERGE (c1)-[:UNI]-(c2)
        """
        with self._driver.session() as session:
                with session.begin_transaction() as tx:
                    (tx.run(column_query))
                

    def integrate_by_correspondance(self, parent):
        # Chemin vers votre fichier CSV
        options = QFileDialog.Options()
        options |= QFileDialog.DontUseNativeDialog
        file_path, _ = QFileDialog.getOpenFileName(parent, "Choisir un fichier", "", "Fichiers CSV (*.csv)", options=options)

        if file_path:
            try:
                # Charger les données du fichier en utilisant pandas
                if file_path.lower().endswith('.csv'):
                    try:
                        df_correspond = pd.read_csv(file_path, encoding='utf-8')
                    except UnicodeDecodeError:
                        df_correspond = pd.read_csv(file_path, encoding='ISO-8859-1')
                    
                    for _, row in df_correspond.iterrows():
                        source1 = row['source1']
                        column1 = row['column1']
                        source2 = row['source2']
                        column2 = row['column2']

                    type_integration="integration par table de correspondance"


                    # Créer un lien entre la source et la colonne dans Neo4j
                    with self._driver.session() as session:
                        with session.begin_transaction() as tx:
                            query =  """
                                MATCH (s1:Source {nom: $source1})-[:POSSEDE]->(t1:Table)-[:CONTIENT]->(c1:Colonne {nom: $column1})
                                MATCH (s2:Source {nom: $source2})-[:POSSEDE]->(t2:Table)-[:CONTIENT]->(c2:Colonne {nom: $column2})

                                WHERE s1 <> s2
                                MERGE (t1)-[:LIE]->(integre:INTEGRATION{type:$type_integration})<-[:LIE]-(t2)
                                MERGE (c1)-[:CORRESPOND]->(integre)<-[:CORRESPOND]-(c2)
                                SET c1.table_provenance = t1.nom
                                SET c2.table_provenance = t2.nom

                                WITH s1, t1, c1, a1, s2, t2, c2, a2, integre
                                MATCH (t1)-[contient1:CONTIENT]->(c1)
                                MATCH (t2)-[contient2:CONTIENT]->(c2)
                                DELETE contient1, contient2

                                RETURN s1, t1, a1, s2, t2, a2, integre, COUNT(c1.nom) AS total
                                """
                    
                            result = tx.run((query), source1=source1, column1=column1, source2=source2, column2=column2, type_integration=type_integration)
                            records = list(result)
                            total = sum(record['total'] for record in records) if records else 0


                    print("Integration par correspondance finie")
                    print(f"Total de {total} paire(s) de clonnes intégrées")
                    return total

            except Exception as e:
                # Gérer les erreurs lors du chargement du fichier
                return(str(e))

    

    def integrate_by_analysis_profiling(self):
        type_integration = "integration par Analyse des profils"

        with self._driver.session() as session:
                with session.begin_transaction() as tx:
                    
                    profil_query = ("""
                        MATCH (s1:Source)-[:POSSEDE]->(t1:Table)-[:CONTIENT]->(c1:Colonne)-[:EFFECTUE]->(a1:Analyse)
                        MATCH (s2:Source)-[:POSSEDE]->(t2:Table)-[:CONTIENT]->(c2:Colonne)-[:EFFECTUE]->(a2:Analyse)
                        WHERE a1 <> a2 AND a1.`Valeur la plus fréquente` = a2.`Valeur la plus fréquente` AND
                                    a1.`Valeurs distinctes` = a2.`Valeurs distinctes`  AND s1 <> s2

                        MERGE (t1)-[:LIE]->(integre:INTEGRATION{type:$type_integration})<-[:LIE]-(t2)
                        MERGE (c1)-[:CORRESPOND]->(integre)<-[:CORRESPOND]-(c2)
                        SET c1.table_provenance = t1.nom
                        SET c2.table_provenance = t2.nom

                        WITH s1, t1, c1, a1, s2, t2, c2, a2, integre
                        MATCH (t1)-[contient1:CONTIENT]->(c1)
                        MATCH (t2)-[contient2:CONTIENT]->(c2)
                        DELETE contient1, contient2

                        RETURN s1, t1, a1, s2, t2, a2, integre, COUNT(c1.nom) AS total
                    """)
                    result = tx.run((profil_query), type_integration = type_integration)
                    #total = result.single()['total'] if result and result.single() else 0

                    records = list(result)
                    total = sum(record['total'] for record in records) if records else 0

                    

        print("integration par Analyse profil terminée")
        print(f"Total de {total} paire(s) de clonnes intégrées")
        return total