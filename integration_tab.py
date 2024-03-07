from PyQt5.QtWidgets import QLabel, QWidget, QVBoxLayout, QPushButton, QCheckBox


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

            self.sim_nom = QCheckBox("compter les nom similaires")
            self.layout.addWidget(self.sim_nom)
            self.sim_nom.stateChanged.connect(self.handle_operation_checkbox)

            self.sim_corresp = QCheckBox("compter les correspondances")
            self.layout.addWidget(self.sim_corresp)
            self.sim_corresp.stateChanged.connect(self.handle_operation_checkbox)

            self.sim_analyses = QCheckBox("compter les analyses similaires")
            self.layout.addWidget(self.sim_analyses)
            self.sim_analyses.stateChanged.connect(self.handle_operation_checkbox)


    def integrate_tables(self):
        connector = Neo4jConnector()
        connector.connect()

        if not self.sim_nom.isChecked():
             use_name_sim = False

        if not self.sim_corresp.isChecked():
             use_corresp_sim = False

        if not self.sim_analyses.isChecked():
             use_analyse_sim = False

        total_paires, statuts = connector.integrate_tables(self, use_name_sim, use_corresp_sim, use_analyse_sim)

        result_texts = []
        for i, row in total_paires.iterrows():
            print(row['table_pair'])
            result_text = f"{row['table_pair']} {row['count']}"
            result_texts.append(result_text)

        # Concaténer les statuts à chaque résultat
        result_texts = [result_text + f" {statut}" for result_text, statut in zip(result_texts, statuts)]

        self.integrate_tables_label.setText("\n\n".join(result_texts))


    def handle_operation_checkbox(self, state):
        sender = self.sender()
        if state == Qt.Checked:
            print(f"{sender.text()} sélectionné pour la source {self.nom_source}")
        else:
            print(f"{sender.text()} désélectionné pour la source {self.nom_source}")

        

    
