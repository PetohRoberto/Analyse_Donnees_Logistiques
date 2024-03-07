from PyQt5.QtWidgets import QTabWidget, QWidget, QVBoxLayout, QPushButton, QMessageBox, QComboBox, QInputDialog
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QLabel, QCheckBox
import pandas as pd
import os
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from PyQt5.QtGui import QImage, QPixmap
from io import BytesIO
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import AgglomerativeClustering
from scipy.cluster.hierarchy import dendrogram, linkage
from neo4j_connector import Neo4jConnector


class SourceWidget(QWidget):
    def __init__(self, source, parent):
        super().__init__()

        self.setStyleSheet("""
            ColumnWidget {
                background-color: #F1F1F1; /* Couleur de fond */
                color: #333; /* Couleur du texte */
            }

            QCheckBox {
                color: #555; /* Couleur du texte de la case à cocher */
            }

            /* Ajoutez d'autres sélecteurs pour personnaliser d'autres éléments */
            """)

        # Stockez le nom de la colonne comme un attribut de l'instance
        self.nom_source = os.path.basename(source['chemin_source'])
        self.source = source
        self.parent_tab = parent
        self.histogramme_checkbox = None

        self.layout = QVBoxLayout(self)

        # Bouton pour le nom de la colonne
        source_label = QPushButton(self.nom_source)
        self.layout.addWidget(source_label)

        # Widget pour les sous-onglets
        subtab_widget = QTabWidget(self)
        self.layout.addWidget(subtab_widget)

        operations_tab = QWidget()
        operations_layout = QVBoxLayout(operations_tab)


        # Scatter plot et liste déroulante
        self.scatter_plot = QCheckBox("scatter plot")
        self.layout.addWidget(self.scatter_plot)
        self.scatter_plot.stateChanged.connect(self.handle_operation_checkbox)

        self.col1_pairplot = QComboBox(self)
        self.col2_pairplot = QComboBox(self)
        self.layout.addWidget(self.col1_pairplot)
        self.layout.addWidget(self.col2_pairplot)
        self.col1_pairplot.addItems(source['columns'])
        self.col2_pairplot.addItems(source['columns'])

        self.scatter_plot_chart = QLabel()
        self.layout.addWidget(self.scatter_plot_chart)


        # Analyse des paires de valeurs et liste déroulante
        self.pair_frequency_plot = QCheckBox("Frequence des couple de valeurs")
        self.layout.addWidget(self.pair_frequency_plot)
        self.pair_frequency_plot.stateChanged.connect(self.handle_operation_checkbox)

        self.col1_freq_pairplot = QComboBox(self)
        self.col2_freq_pairplot = QComboBox(self)
        self.layout.addWidget(self.col1_freq_pairplot)
        self.layout.addWidget(self.col2_freq_pairplot)
        self.col1_freq_pairplot.addItems(source['columns'])
        self.col2_freq_pairplot.addItems(source['columns'])

        self.pair_frequency_chart = QLabel()
        self.layout.addWidget(self.pair_frequency_chart)

        # Analyse des paires de triplets et liste déroulante
        self.triplet_frequency_plot = QCheckBox("Frequence des triplets de valeurs")
        self.layout.addWidget(self.triplet_frequency_plot)
        self.triplet_frequency_plot.stateChanged.connect(self.handle_operation_checkbox)

        self.col1_freq_triplet_plot = QComboBox(self)
        self.col2_freq_triplet_plot = QComboBox(self)
        self.col3_freq_triplet_plot = QComboBox(self)
        self.layout.addWidget(self.col1_freq_triplet_plot)
        self.layout.addWidget(self.col2_freq_triplet_plot)
        self.layout.addWidget(self.col3_freq_triplet_plot)
        self.col1_freq_triplet_plot.addItems(source['columns'])
        self.col2_freq_triplet_plot.addItems(source['columns'])
        self.col3_freq_triplet_plot.addItems(source['columns'])

        self.triplet_frequency_chart = QLabel() # 
        self.layout.addWidget(self.triplet_frequency_chart)



        # Algorithmes de clustering
        self.kmeans_clustering = QCheckBox("Clustering avec Kmeans")
        self.kmeans_clustering.stateChanged.connect(self.handle_operation_checkbox)
        self.layout.addWidget(self.kmeans_clustering)
        self.kmeans_pairplot_chart = QLabel()
        self.layout.addWidget(self.kmeans_pairplot_chart)
        self.kmeans_histplot_chart = QLabel()
        self.layout.addWidget(self.kmeans_histplot_chart)

        self.hierarchical_clustering = QCheckBox("Clustering avec méthode hiérachique")
        self.hierarchical_clustering.stateChanged.connect(self.handle_operation_checkbox)
        self.layout.addWidget(self.hierarchical_clustering)
        self.hierarchic_pairplot_chart = QLabel()
        self.layout.addWidget(self.hierarchic_pairplot_chart)
        self.hierarchic_dendogram_chart = QLabel()
        self.layout.addWidget(self.hierarchic_dendogram_chart)


        self.result_tab = []
        self.sheet_name = None

        calculate_button = QPushButton("Analyser")
        calculate_button.clicked.connect(self.calculer)
        self.layout.addWidget(calculate_button)

        save_button = QPushButton("Enregistrer")
        save_button.clicked.connect(self.save_results)
        self.layout.addWidget(save_button)


    def handle_operation_checkbox(self, state):
        sender = self.sender()
        if state == Qt.Checked:
            print(f"{sender.text()} sélectionné pour la source {self.nom_source}")
        else:
            print(f"{sender.text()} désélectionné pour la source {self.nom_source}")


    def calculer(self):
        # Itérez sur les sources et vérifiez les checkboxes sélectionnées
        result_texts = []
        source = self.source
        
        if self.scatter_plot.isChecked():
            self.scatter_plot_chart.clear()
            column_x = self.col1_pairplot.currentText()
            column_y = self.col2_pairplot.currentText()
            chart_path = self.create_scatter_plot_chart(source['chemin_source'], column_x, column_y)

            result_texts.append(("scatter plot", chart_path))

        if self.pair_frequency_plot.isChecked():
            self.pair_frequency_chart.clear()
            column_x = self.col1_freq_pairplot.currentText()
            column_y = self.col2_freq_pairplot.currentText()
            chart_path = self.create_frequency_pair_plot_chart(data_path=source['chemin_source'], x=column_x, y=column_y)

            result_texts.append(("pair frequency plot", chart_path))

        if self.triplet_frequency_plot.isChecked():
            self.triplet_frequency_chart.clear()
            column_x = self.col1_freq_triplet_plot.currentText()
            column_y = self.col2_freq_triplet_plot.currentText()
            column_z = self.col3_freq_triplet_plot.currentText()
            chart_path = self.create_frequency_triplet_plot_chart(data_path=source['chemin_source'], x=column_x, y=column_y, z=column_z)
        
            result_texts.append(("triplet frequency plot", chart_path))

        if self.kmeans_clustering.isChecked():
            self.kmeans_histplot_chart.clear()
            self.kmeans_histplot_chart.clear()
            result = self.create_kmeans_clustering_chart(source['chemin_source'], source['columns'])
            result_texts.append(("Kmeans", result))

        if self.hierarchical_clustering.isChecked():
            self.hierarchic_pairplot_chart.clear()
            self.hierarchic_dendogram_chart.clear()
            results = self.create_hierarchical_clustering_chart(source['chemin_source'], source['columns'])
            result_texts.append(("Hierarchical clustering", results))

        self.result_tab.append([self.source['chemin_source'], result_texts])

        # Mettre à jour le DataFrame des résultats
        self.result_tab = pd.DataFrame(self.result_tab, columns=['chemin source', 'resultats'])

        # scatter
        # pair freq plot
        # triplet freq plot
        # kmeans image_path, nombre_clusters, columns, data_clustering['Cluster']
        # hierarchic image_path, nombre_clusters, columns, data_clustering['Cluster']


    def save_results(self):
        print("valeur : ", self.result_tab)
        print("**************")
        print("type : ",  type(self.result_tab))
        print("............")
        if self.source != None and  len(self.result_tab)>0:
            try:
                connector = Neo4jConnector()
                connector.connect()
                connector.store_analysis(self.result_tab, self.sheet_name)
                connector.close()

                # QMessageBox.information(None, "Success", "Data saved in Neo4j")

                print("saved for : ", self.nom_source)
            except Exception as e:
                # Gérer les erreurs lors du chargement du fichier
                print(str(e))
                

    def load_data(self, file_path):
        print("loading data ...")
        if file_path:
            try:
                # Charger les données du fichier en utilisant pandas
                if file_path.lower().endswith('.csv'):
                    try:
                        data = pd.read_csv(file_path, encoding='utf-8')
                    except UnicodeDecodeError:
                        data = pd.read_csv(file_path, encoding='ISO-8859-1')
                elif file_path.lower().endswith(('.xls', '.xlsx'))  and self.nom_source != self.source['nom_table']:
                        data = pd.read_excel(file_path, sheet_name=self.source['nom_table'])
                        self.sheet_name = self.source['nom_table']

                return data

            except Exception as e:
                # Gérer les erreurs lors du chargement du fichier
                print(str(e))
    
    
    def save_figure(self, chart, save_path='images'):
        if not os.path.exists(save_path):
                os.makedirs(save_path)
        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Définir le nom du fichier en fonction des colonnes, du type de graphique et de la date/heure
        file_name = f"{chart}_{'_'.join(self.nom_source)}_{current_datetime}.png"

        image_path = os.path.join(save_path, file_name)

        try:
            plt.savefig(image_path)
            return image_path
        except Exception as e:
            print("Type de données non adapté pour", chart, ":", e)
            return None

    def add_figure_on_interface(self, figure, chart_layout):
            # Ajuster la taille du graphique pour qu'il s'adapte à l'espace disponible
            figure.tight_layout()

            canvas = FigureCanvas(figure)
            buffer = BytesIO()
            canvas.print_png(buffer)
            buffer.seek(0)

            image = QImage.fromData(buffer.read())
            pixmap = QPixmap.fromImage(image)

            try:
                if chart_layout == "kmeans_histplot_chart":
                    self.kmeans_histplot_chart.setPixmap(pixmap)
                if chart_layout == "kmeans_pairplot_chart":
                    self.kmeans_pairplot_chart.setPixmap(pixmap)
                if chart_layout == "hierarchic_pairplot_chart":
                    self.hierarchic_pairplot_chart.setPixmap(pixmap)
                if chart_layout == "hierarchic_dendogram_chart":
                    self.hierarchic_dendogram_chart.setPixmap(pixmap)
                if chart_layout == "scatter_plot_chart":
                    self.scatter_plot_chart.setPixmap(pixmap)
                if chart_layout == "pair_frequency_chart":
                    self.pair_frequency_chart.setPixmap(pixmap)
                if chart_layout == "triplet_frequency_chart":
                    self.triplet_frequency_chart.setPixmap(pixmap)
            except Exception as e:
                print("erreur pendant l'ajout du graphique sur l'interface : ", e)



    def create_kmeans_clustering_chart(self, data_path, columns):
        data = self.load_data(data_path)
        data_clustering = data[columns].copy()

        # Gérer les valeurs manquantes et normaliser
        data_clustering = data_clustering.fillna(data_clustering.mean())
        scaler = StandardScaler()
        data_normalized = scaler.fit_transform(data_clustering)

        # Appliquer le clustering (K-Means)
        k_value = 3
        kmeans = KMeans(n_clusters=k_value, random_state=42)
        data['Cluster'] = kmeans.fit_predict(data_normalized)

        # Histogramme des clusters
        figure = plt.figure(figsize=(4, 5))
        sns.histplot(data=data, x='Cluster', kde=True)
        plt.title('Distribution des Clusters')
        self.add_figure_on_interface(figure, "kmeans_histplot_chart")
        hisplot_image_path = self.save_figure('kmeans_hisplot')
        plt.close(figure)

        # Pair plot des clusters
        plt.figure(figsize=(4, 5))
        sns.pairplot(data, hue='Cluster', palette='viridis')
        plt.title('Pair Plot des Clusters')
        self.add_figure_on_interface(figure, "kmeans_pairplot_chart")
        pairplot_image_path =  self.save_figure('kmeans_pairplot')
        plt.close(figure)

        image_path = (hisplot_image_path, pairplot_image_path)
        
        return (image_path, k_value, columns, data['Cluster']) 

    
    def create_hierarchical_clustering_chart(self, data_path, columns):

        data = self.load_data(data_path)
        data_clustering = data[columns].copy()

        # Gérer les valeurs manquantes et normaliser
        data_clustering = data_clustering.fillna(data_clustering.mean())
        scaler = StandardScaler()
        data_normalized = scaler.fit_transform(data_clustering)

        # Appliquer le clustering hiérarchique agglomératif
        nombre_clusters = 3  # Modifiez en fonction de votre cas
        agglomerative_clustering = AgglomerativeClustering(n_clusters=nombre_clusters)
        data_clustering['Cluster'] = agglomerative_clustering.fit_predict(data_normalized)

        # Afficher le pairplot avec les clusters colorés
        figure = plt.figure(figsize=(4, 5))
        sns.pairplot(data_clustering, hue='Cluster', palette='viridis')
        plt.suptitle("Pairplot avec Clusters (Clustering Hiérarchique Agglomératif)", y=1.02)
        self.add_figure_on_interface(figure, "hierarchic_pairplot_chart")
        hierachical_image_path = self.save_figure('hierarchical_pairplot')
        plt.close(figure)

        # Afficher la matrice de liaison
        figure = plt.figure(figsize=(4, 5))
        linkage_matrix = linkage(data_normalized, method='ward')
        dendrogram(linkage_matrix)
        plt.title("Dendrogramme")
        self.add_figure_on_interface(figure, "hierarchic_dendogram_chart")
        dendogramme_image_path = self.save_figure('hierarchical_dendogramme')
        plt.close(figure)
        image_path = (dendogramme_image_path, hierachical_image_path)

        return (image_path, nombre_clusters, columns, data_clustering['Cluster'])


    def create_scatter_plot_chart(self, data_path, x, y):
        data = self.load_data(data_path)

        # Créer le scatter plot
        print("calculating the scatter plot")
        figure = plt.figure(figsize=(4, 5))
        plt.scatter(data[x], data[y])
        plt.title(f'Scatter plot de {x} et {y}')
        plt.xlabel(x)
        plt.ylabel(y)
        self.add_figure_on_interface(figure, "scatter_plot_chart")
        image_path = self.save_figure('scatter plot')
        plt.close(figure)

        return image_path
    

    def create_frequency_pair_plot_chart(self, data_path, x, y):
        data = self.load_data(data_path)
        data_for_hist = [str(col1)+','+str(col2) for (col1, col2) in zip(data[x], data[y])]

        if self.is_histogram_relevant(data_for_hist):
            figure = plt.figure(figsize=(4, 5))
            plt.hist(data_for_hist)
            plt.title(f'Frequence des paires ({x}, {y})')
            plt.xlabel(f'({x}, {y})')
            plt.ylabel("frequence")
            plt.xticks(rotation='vertical', fontsize=5)
            plt.tight_layout()
            self.add_figure_on_interface(figure, "pair_frequency_chart")
            image_path = self.save_figure('pair frequency')
            plt.close(figure)

            return image_path
        
        print("Frequency pairplot not relevant : too many disticnt values")
        self.pair_frequency_chart.setText("Frequency pairplot not relevant : too many disticnt values")


    def create_frequency_triplet_plot_chart(self, data_path, x, y, z):
        data = self.load_data(data_path)
        data_for_hist = [str(col1)+','+str(col2) + ', '+str(col3) for (col1, col2, col3) in zip(data[x], data[y], data[z])]

        if self.is_histogram_relevant(data_for_hist):
            figure = plt.figure(figsize=(4, 5))
            plt.hist(data_for_hist)
            plt.title(f'Frequence des triplets ({x}, {y}, {z})')
            plt.xlabel(f'({x}, {y}, {z})')
            plt.ylabel("frequence")
            plt.xticks(rotation='vertical')
            plt.tight_layout()
            self.add_figure_on_interface(figure, "triplet_frequency_chart")
            image_path = self.save_figure('triplet frequency')
            plt.close(figure)

            return image_path
        
        print("Frequency pairplot not relevant : too many disticnt values")
        self.pair_frequency_chart.setText("Frequency pairplot not relevant : too many disticnt values")
    

    def is_histogram_relevant(self, data):
        data = pd.DataFrame(data)
        unique_values_count = data.nunique()[0]
        
        if unique_values_count > 100:
            return False
        if unique_values_count < 2 :
            return False

        return True

        