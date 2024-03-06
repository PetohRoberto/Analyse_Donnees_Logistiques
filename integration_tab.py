from PyQt5.QtWidgets import QLabel, QWidget, QVBoxLayout, QPushButton


from neo4j_connector import Neo4jConnector

class IntegrationTab(QWidget):
    def __init__(self):
            super().__init__()
            self.layout = QVBoxLayout(self)

            self.load_button = QPushButton("Integrer les tables")
            self.load_button.clicked.connect(self.integrate_tables)
            self.layout.addWidget(self.load_button)

            self.inetgrate_by_name_label = QLabel()
            self.layout.addWidget(self.inetgrate_by_name_label)
            
            self.load_button2 = QPushButton("Integrer par table de correspondance")
            self.load_button2.clicked.connect(self.integrate_by_correspondance)
            self.layout.addWidget(self.load_button2)

            self.inetgrate_by_corr_label = QLabel()
            self.layout.addWidget(self.inetgrate_by_corr_label)
            
            self.load_button3 = QPushButton("Integrer par analyse de profilage")
            self.load_button3.clicked.connect(self.integrate_by_analysis_profiling)
            self.layout.addWidget(self.load_button3)

            self.inetgrate_by_prof_label = QLabel()
            self.layout.addWidget(self.inetgrate_by_prof_label)

    def integrate_tables(self):
        connector = Neo4jConnector()
        connector.connect()
        total_integration = connector.integrate_tables(self)
        self.inetgrate_by_name_label.setText(f"total paires pour nom : {total_integration} ")
        
    def integrate_by_correspondance(self):
        connector = Neo4jConnector()
        connector.connect()
        total_integration = connector.integrate_by_correspondance(self)
        self.inetgrate_by_corr_label.setText(f"total paires pour correspondance : {total_integration} ")
    
    def integrate_by_analysis_profiling(self):
        connector = Neo4jConnector()
        connector.connect()
        total_integration = connector.integrate_by_analysis_profiling()
        self.inetgrate_by_prof_label.setText(f"total paires pour analyse de profil : {total_integration} ")
        

    
