import sys
from PySide6.QtWidgets import QApplication
from PySide6.QtGui import QPalette, QColor
from PySide6.QtCore import Qt

# Importa a janela principal da sua nova estrutura de UI
from app.ui.main_window import MainWindow

if __name__ == "__main__":
    app = QApplication(sys.argv)
    
    app.setStyle("Fusion")

    # Paleta de cores para o tema claro e profissional
    light_palette = QPalette()
    
    # Cores de fundo e texto gerais
    light_palette.setColor(QPalette.Window, QColor("#F0F0F0")) # Fundo principal cinza bem claro
    light_palette.setColor(QPalette.WindowText, QColor("#333333")) # Texto escuro (não preto)
    
    # Cores para campos de entrada, listas, tabelas
    light_palette.setColor(QPalette.Base, QColor("#FFFFFF")) # Fundo branco para inputs
    light_palette.setColor(QPalette.AlternateBase, QColor("#F5F5F5")) # Cor de linha alternada
    light_palette.setColor(QPalette.Text, QColor("#333333")) # Texto dentro dos inputs
    
    # Cores de botões
    light_palette.setColor(QPalette.Button, QColor("#E0E0E0")) # Botões cinza claro
    light_palette.setColor(QPalette.ButtonText, QColor("#333333")) # Texto dos botões
    
    # Cores de destaque (seleção, links, etc.)
    CORPORATE_GREEN = QColor("#86BC25")
    light_palette.setColor(QPalette.Highlight, CORPORATE_GREEN)
    light_palette.setColor(QPalette.HighlightedText, Qt.white)
    
    # Aplicar a paleta de cores base
    app.setPalette(light_palette)

    # Folha de estilos QSS para refinamentos
    professional_stylesheet = """
        QWidget {
            font-family: Aptos, Segoe UI, Arial, sans-serif; /* Define a fonte com fallbacks */
            font-size: 9pt;
        }
        QGroupBox {
            font-weight: bold;
            font-size: 10pt;
        }
        QPushButton {
            border: 1px solid #B0B0B0;
            border-radius: 4px;
            padding: 6px;
            background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                              stop: 0 #F5F5F5, stop: 1 #E0E0E0);
            min-width: 80px;
        }
        QPushButton:hover {
            background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                              stop: 0 #FFFFFF, stop: 1 #E8E8E8);
        }
        QPushButton:pressed {
            background-color: #D0D0D0;
        }
        QPushButton:disabled {
            background-color: #E0E0E0;
            color: #A0A0A0;
            border-color: #C0C0C0;
        }
        /* Estilo especial para o botão de consolidação */
        #consolidate_button {
            font-weight: bold;
            color: white;
            background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                              stop: 0 #86BC25, stop: 1 #70A01C);
            border-color: #608918;
        }
        #consolidate_button:hover {
            background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                              stop: 0 #92CC2A, stop: 1 #7CB820);
        }
        #consolidate_button:pressed {
            background-color: #70A01C;
        }

        QLineEdit, QListWidget, QTableWidget, QTextEdit {
            border: 1px solid #B0B0B0;
            border-radius: 4px;
            padding: 4px;
            background-color: #FFFFFF;
        }
        QListWidget {
            border: 1px solid #B0B0B0;
            border-radius: 4px;
            padding: 2px;
            background-color: #FFFFFF;
        }
        QListWidget::item:hover {
            background-color: #F0F0F0;
            border-radius: 3px;
        }
        QComboBox {
            border: 1px solid #B0B0B0;
            border-radius: 4px;
            padding: 4px;
            padding-right: 25px;
            background-color: #FFFFFF;
        }
        QComboBox::drop-down {
            subcontrol-origin: padding;
            subcontrol-position: top right;
            width: 25px;
            border-left-width: 1px;
            border-left-color: #B0B0B0;
            border-left-style: solid;
            border-top-right-radius: 3px;
            border-bottom-right-radius: 3px;
        }
        QComboBox::down-arrow{
            image: url(app/down.png);
            width: 14px;
            height: 14px;
        }
        QComboBox::drop-down:hover{
            background-color: #E8E8E8
        }
        QComboBox, QAbstractItemView::item:selected {
            background-color: #FFFFFF;
            color: black;
        }
        QHeaderView::section {
            background-color: #4A5568; /* Cinza-azulado escuro corporativo */
            color: white;
            padding: 4px;
            border: 1px solid #E0E0E0;
            font-weight: bold;
        }
        QProgressBar {
            border: 1px solid #B0B0B0;
            border-radius: 5px;
            text-align: center;
            color: #333333;
        }
        QProgressBar::chunk {
            background-color: #86BC25; /* Verde corporativo */
            border-radius: 4px;
        }
    """
    app.setStyleSheet(professional_stylesheet)
    
    window = MainWindow()
    window.consolidate_button.setObjectName("consolidate_button")
    sys.exit(app.exec())