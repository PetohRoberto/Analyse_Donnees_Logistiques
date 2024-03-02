from PyQt5.QtWidgets import QTabWidget, QWidget, QVBoxLayout, QPushButton


class All_columns_operations(QWidget):
    def __init__(self, parent):
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
        self.parent_tab = parent
        self.current_dataframe = parent.current_dataframe

        self.layout = QVBoxLayout(self)

        # Bouton pour le nom de la colonne
        column_label = QPushButton("all columns")
        self.layout.addWidget(column_label)

        # Widget pour les sous-onglets
        subtab_widget = QTabWidget(self)
        self.layout.addWidget(subtab_widget)

        # Créez un onglet pour les opérations
        operations_tab = QWidget()
        operations_layout = QVBoxLayout(operations_tab)

        # Ajoutez les opérations que vous souhaitez
        # Exemple : boutons pour créer un pairplot et une matrice de corrélation
        pairplot_button = QPushButton("Pairplot")
        pairplot_button.clicked.connect(self.create_pairplot_all_columns)
        operations_layout.addWidget(pairplot_button)

        correlation_matrix_button = QPushButton("Matrice de Corrélation")
        correlation_matrix_button.clicked.connect(self.create_correlation_matrix_all_columns)
        operations_layout.addWidget(correlation_matrix_button)

        # Ajoutez l'onglet des opérations à subtab_widget
        subtab_widget.addTab(operations_tab, "Operations")

    def create_pairplot_all_columns(self):
        print("pairplot")

    def create_correlation_matrix_all_columns(self):
        print("corr")