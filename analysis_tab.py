import os
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime
from PyQt5.QtWidgets import QWidget, QVBoxLayout, QCheckBox, QPushButton, QLabel,  QScrollArea, QInputDialog, QTabWidget
from PyQt5.QtGui import QImage, QPixmap
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from io import BytesIO
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

from neo4j_connector import Neo4jConnector
from source_widget import SourceWidget

class AnalysisTab(QWidget):
    def __init__(self):
        super().__init__()

        self.layout = QVBoxLayout(self)

        scroll_area = QScrollArea(self)
        scroll_area.setWidgetResizable(True)

        # Créez le widget QTabWidget et configurez-le
        self.source_tabs = QTabWidget(scroll_area)

        # Définissez le widget à faire défiler
        scroll_area.setWidget(self.source_tabs)

        # Ajoutez le QScrollArea au layout
        self.layout.addWidget(scroll_area)


        calculate_button = QPushButton("Tout Analyser")
        calculate_button.clicked.connect(self.calcul_tout)
        self.layout.addWidget(calculate_button)


        self.sources = self.get_sources()

        self.show_sources()


    def calcul_tout():
        pass

    def show_sources(self):
        self.source_tabs.clear()
        self.source_widgets = []

        for _, source in self.sources.iterrows():
            
            tab = QWidget()
            tab_layout = QVBoxLayout(tab)

            # Utilisez un QHBoxLayout pour mettre le nom de la colonne horizontalement
            source_widget = SourceWidget(source, self)
            tab_layout.addWidget(source_widget)
            self.source_widgets.append(source_widget)

            # Ajoutez vos fonctionnalités spécifiques pour chaque colonne ici
            nom_source = os.path.basename(source['chemin_source'])
            self.source_tabs.addTab(tab, nom_source)

            # Utilisez le style CSS pour orienter le texte horizontalement
            self.source_tabs.setStyleSheet("QTabBar::tab {writing-mode: horizontal-tb }")
            self.source_tabs.setTabPosition(QTabWidget.West)


    
    def get_sources(self):

        connector = Neo4jConnector()
        connector.connect()
        source = connector.get_sources()
        source_df = pd.DataFrame([dict(rec) for rec in source]) 
        connector.close()

        return source_df
