from PyQt5.QtWidgets import QLabel, QWidget, QVBoxLayout, QPushButton


from neo4j_connector import Neo4jConnector

class IntegrationTab(QWidget):
    def __init__(self):
            super().__init__()
            self.layout = QVBoxLayout(self)

            self.load_button = QPushButton("Analyser pour intégrer les tables")
            self.load_button.clicked.connect(self.integrate_tables)
            self.layout.addWidget(self.load_button)

            self.integrate_tables_label = QLabel()
            self.layout.addWidget(self.integrate_tables_label)


    def integrate_tables(self):
        connector = Neo4jConnector()
        connector.connect()
        total_paires, statuts = connector.integrate_tables(self)

        result_texts = []
        for i, row in total_paires.iterrows():
            result_text = f"{row['total_pairs']} {row['count']}"
            result_texts.append(result_text)

        # Concaténer les statuts à chaque résultat
        result_texts = [result_text + f" {statut}" for result_text, statut in zip(result_texts, statuts)]

        self.integrate_tables_label.setText("\n".join(result_texts))
        

    
