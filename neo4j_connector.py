

from neo4j import GraphDatabase
from datetime import datetime
import pandas as pd
from PyQt5.QtWidgets import QFileDialog
import numpy as np


class Neo4jConnector:

    def __init__(self):
        self._driver = None

        self.corresp_df = None

    def close(self):
         if self._driver is not None:
            self._driver.close()

    def connect(self):
        uri = "bolt://localhost:7687/test"
        user = "neo4j"
        password = "LogisticData"
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
                    
                    
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


    def store_analysis(self, result_df, sheet_name=None):

        with self._driver.session() as session:
            # Begin a transaction
            with session.begin_transaction() as tx:
                print("start saving in Neo4j ...")


                # scatter plot
                # pair frequency plot
                # triplet frequency plot
                # Kmeans image_path, nombre_clusters, columns, data_clustering['Cluster']
                # Hierarchical clustering image_path, nombre_clusters, columns, data_clustering['Cluster']
                chemin_source = result_df['chemin source']
                if sheet_name != None:
                    table_name = sheet_name
                else:
                    table_name = chemin_source

                all_results = result_df['resultats']
                source_query = (
                        "MERGE (s:Source {nom: $source_name, chemin: $file_path})-[:POSSEDE]->(t:Table{nom: $table})-[:Analyser]->(a:Analyses) "
                        "SET "
                        )
                                    # Add dynamic SET clause based on resultats DataFrame
                for resultats in all_results:
                    for result in resultats:
                        # Enclose both parameter name and value in backticks
                        parameter_name = f"`{result[0]}`"
                        parameter_value = result[1]
                        source_query += f"a.{parameter_name} = '{parameter_value}', "

                    # Remove the trailing comma
                    source_query = source_query.rstrip(', ')
                tx.run(source_query, source_name=chemin_source, file_path=chemin_source, table=table_name)



    def count_pairs(self, df):
        group_columns = ['source1', 'table1', 'table2', 'source2'] # 'source1', 'table1', 'column1', 'source2', 'table2'
        
        # Vérifiez si toutes les colonnes de regroupement existent dans le DataFrame
        if all(col in df.columns for col in group_columns):
            count_table_pairs = df.groupby(group_columns).size().reset_index(name='count')
            count_table_pairs['sorted_tables'] = np.sort(count_table_pairs[group_columns], axis=1).tolist()

            count_table_pairs['sorted_tables'] = count_table_pairs['sorted_tables'].apply('--'.join)
            final_count_table_pairs = count_table_pairs.groupby(['sorted_tables']).agg({'count': 'sum'}).reset_index()

            return final_count_table_pairs
        else:
            print("Certaines colonnes de regroupement n'existent pas dans le DataFrame.")
            return None


    def analyse_for_integration(self, parent):
        total_results = []
        similar_columns = self.get_similar_columns()
        similar_name_df = pd.DataFrame([dict(record) for record in similar_columns])
        sim_name_pairs = self.count_pairs(similar_name_df) 
        total_results.append(sim_name_pairs)  


        corresponding_columns_df = self.get_correspond_columns(parent) 
        if corresponding_columns_df is not None:
            self.corresp_df = corresponding_columns_df
            corresp_pairs = self.count_pairs(corresponding_columns_df)
            total_results.append(corresp_pairs)
        else:
             print("table de correspondace nulle")


        similar_columns_by_analysis = self.get_similar_col_by_analysis()
        sim_analysis_df = pd.DataFrame([dict(record) for record in similar_columns_by_analysis])
        print(sim_analysis_df)
        sim_analysis_pairs = self.count_pairs(sim_analysis_df)
        total_results.append(sim_analysis_pairs)


        # Concaténer les résultats de chaque DataFrame
        total_results = pd.concat(total_results, ignore_index=True)

        # Grouper le DataFrame résultant par les tables triées et effectuer la somme
        final_total_count = total_results.groupby('sorted_tables').agg({'count': 'sum'}).reset_index()
        final_total_count = final_total_count.rename(columns={'sorted_tables': 'table_pair'})

        return final_total_count

    @staticmethod
    def compare_tuple(row, mon_tuple):
        return all(elem in row.values for elem in mon_tuple)
    
    def integrate_tables(self, parent):
        statut = []
        total_pairs = self.analyse_for_integration(parent)
        for pair, count in zip(total_pairs['table_pair'], total_pairs['count']):
            if count < 2:
                statut.append("pas intégré")
            else:
                statut.append("intégré")
                print(pair, count)
                s1, t1, t2, s2 = pair.split('--')
                
                query_name_analyse = """
                    MATCH (s1:Source{nom: $source1})-[:POSSEDE]->(t1:Table{nom: $table1})-[:CONTIENT]->(c1:Colonne)-[:EFFECTUE]->(a1:Analyse)
                    MATCH (s2:Source{nom: $source2})-[:POSSEDE]->(t2:Table{nom : $table2})-[:CONTIENT]->(c2:Colonne)-[:EFFECTUE]->(a2:Analyse)
                    WHERE (toLower(c1.nom) = toLower(c2.nom) AND s1 < s2) OR (a1 <> a2 AND a1.`Valeur la plus fréquente` = a2.`Valeur la plus fréquente` AND
                                a1.`Valeurs distinctes` = a2.`Valeurs distinctes`  AND s1 <> s2)

                    MERGE (t1)-[:INTEGRE]-(t2)
                    MERGE (c1)-[:CORRESPOND]-(c2)
                    """

                correspond_df = self.corresp_df
                tuple_cherche = pair.split('--')
                result = correspond_df[correspond_df.apply(self.compare_tuple, axis=1, mon_tuple=tuple_cherche)]
                for _, res in result.iterrows():
                    c1 = res['column1']
                    c2 = res['column2']

                    query_table_corr = """"
                        MATCH (s1:Source{nom: $source1})-[:POSSEDE]->(t1:Table{nom: $table1})-[:CONTIENT]->(c1:Colonne{nom: $colonne1})
                        MATCH (s2:Source{nom: $source2})-[:POSSEDE]->(t2:Table{nom : $table2})-[:CONTIENT]->(c2:Colonne{nom: $colonne2})

                        MERGE (t1)-[:INTEGRE]-(t2)
                        MERGE (c1)-[:CORRESPOND]-(c2)
                    """

                    with self._driver.session() as session:
                    # Begin a transaction
                        with session.begin_transaction() as tx:
                            tx.run(query_name_analyse, source1=s1, source2=s2, table1=t1, table2=t2)
                            tx.run(query_table_corr, source1=s1, source2=s2, table1=t1, table2=t2, colonne1=c1, colonne2=c2)
                            

        return (total_pairs, statut)

    def get_similar_columns(self):
        # type_integration = "integration par nom"
        print("get sim by names ...")
        query = """
            MATCH (s1:Source)-[:POSSEDE]->(t1:Table)-[:CONTIENT]->(c1:Colonne)-[:EFFECTUE]->(a1:Analyse)
            MATCH (s2:Source)-[:POSSEDE]->(t2:Table)-[:CONTIENT]->(c2:Colonne)-[:EFFECTUE]->(a2:Analyse)
            WHERE toLower(c1.nom) = toLower(c2.nom) AND s1 < s2
            
            RETURN s1.chemin as source1, t1.nom as table1, s2.chemin as source2, t2.nom as table2, c1.com AS colonne_similaire
        """
        # source1', 'table1', 'table2', 'source2
        with self._driver.session() as session:
                # Begin a transaction
                with session.begin_transaction() as tx:
                    result = tx.run(query)
                    records = list(result)
                
                    return records

    def get_correspond_columns(self, parent):
        # Chemin vers votre fichier CSV
        print("get corresp columns ...")
        options = QFileDialog.Options()
        options |= QFileDialog.DontUseNativeDialog
        file_path, _ = QFileDialog.getOpenFileName(parent, "Choisir un fichier", "", "Fichiers CSV (*.csv)", options=options)

        if file_path:
            try:
                # Charger les données du fichier en utilisant pandas
                if file_path.lower().endswith('.csv'):
                    try:
                        df_correspond = pd.read_csv(file_path, encoding='utf-8', sep=';')
                    except UnicodeDecodeError:
                        df_correspond = pd.read_csv(file_path, encoding='ISO-8859-1', sep=';')

                    return df_correspond
            
            except Exception as e:
                # Gérer les erreurs lors du chargement du fichier
                return(str(e))

    
    def get_similar_col_by_analysis(self):
        print("get sim analysis columns...")
        with self._driver.session() as session:
            with session.begin_transaction() as tx:
                
                query ="""
                    MATCH (s1:Source)-[:POSSEDE]->(t1:Table)-[:CONTIENT]->(c1:Colonne)-[:EFFECTUE]->(a1:Analyse)
                    MATCH (s2:Source)-[:POSSEDE]->(t2:Table)-[:CONTIENT]->(c2:Colonne)-[:EFFECTUE]->(a2:Analyse)
                    WHERE a1 <> a2 AND a1.`Valeur la plus fréquente` = a2.`Valeur la plus fréquente` AND
                                a1.`Valeurs distinctes` = a2.`Valeurs distinctes`  AND s1 <> s2
                    RETURN s1.chemin as source1, t1.nom as table1, s2.chemin as source2, t2.nom as table2, c1.com AS colonne_similaire
                            """
                results = tx.run(query)

                return list(results)

    def get_sources(self):
        with self._driver.session() as session:
            with session.begin_transaction() as tx:

                query = """
                    MATCH (s:Source)-[:POSSEDE]->(t:Table)-[:CONTIENT]->(c:Colonne)

                    RETURN s.chemin AS chemin_source, t.nom AS nom_table, COLLECT(c.nom) AS columns
                """

                result = tx.run(query)

                return list(result)
