import sys
import os
import glob
import json
import polars as pl

from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLineEdit,
    QLabel, QListWidget, QComboBox, QProgressBar, QTextEdit, QFileDialog,
    QTabWidget, QTableView, QGroupBox, QStyle, QListWidgetItem, QDialog
)
from PySide6.QtCore import Qt
from PySide6.QtGui import QIcon, QAction, QTextCursor
from PySide6.QtSvgWidgets import QSvgWidget

# Importações da nova estrutura de projeto
from .dialogs import (
    PivotDialog, FilterDialog, HeaderMappingDialog, HelpDialog, SheetSelectionDialog
)
from .models import PolarsTableModel
from ..logic.workers import (
    ConsolidationWorker, SheetLoadingWorker, SheetAnalysisWorker, HeaderAnalysisWorker
)
from ..utils import LogLevel, CONFIG_FILE_NAME, _find_header_row_index, _make_headers_unique


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowIcon(QIcon("app/logo1.png"))
        self.header_mapping = {}
        self.filter_rules = []
        self.pivot_rules = {}
        self.duplicate_key_columns = []
        self.sheet_selection_rules = {}
        self.all_sheets_cache = {}
        menu_bar = self.menuBar()
        
        # Menu "Ajuda"
        help_menu = menu_bar.addMenu("&Ajuda") # O & cria um atalho (Alt+A)
        
        guide_action = QAction("Guia do Usuário...", self)
        guide_action.triggered.connect(self.open_help_dialog)
        help_menu.addAction(guide_action)
        self.setWindowTitle("DataFlow")
        self.setGeometry(100, 100, 1000, 750) 

        self.current_files_paths = {}
        self.output_file_path = "" 
        self.consolidation_thread = None
        self.sheet_loader_thread = None
        self.header_analyzer_thread = None
        self.sheet_analysis_worker = None
        self.is_last_log_progress = False
        self.sheet_selections = {} 
        self.last_used_input_folder = self._load_last_input_folder() # <--- CARREGAR AO INICIAR

        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        # --- 1. Seção de Seleção de Pasta ---
        folder_selection_layout = QHBoxLayout()
        # logo_label = QLabel()
        # pixmap = QPixmap("app/logo.png")
        # logo_label.setPixmap(pixmap.scaled(48, 48, Qt.KeepAspectRatio, Qt.SmoothTransformation))
        logo_widget = QSvgWidget("app/logo2.svg")
        logo_widget.setFixedSize(75, 75)
        folder_selection_layout.addWidget(logo_widget)
        self.folder_path_label = QLabel("Pasta do Projeto:")
        self.folder_path_line_edit = QLineEdit()
        self.folder_path_line_edit.setReadOnly(True)
        # Se uma pasta foi carregada, exibi-la (opcional, ou só usar no diálogo)
        if self.last_used_input_folder:
            self.folder_path_line_edit.setText(self.last_used_input_folder)
            # Poderia até chamar list_files_in_folder aqui se quisesse carregar automaticamente
            # self.list_files_in_folder(self.last_used_input_folder) 
            # Mas vamos manter o clique do usuário para carregar os arquivos.
        self.select_folder_button = QPushButton("Selecionar Pasta...")
        folder_icon = self.style().standardIcon(QStyle.SP_DirIcon)
        self.select_folder_button.setIcon(folder_icon)
        self.select_folder_button.clicked.connect(self.open_folder_dialog)

        # BOTÃO para atualizar
        self.refresh_button = QPushButton("Atualizar")
        refresh_icon = self.style().standardIcon(QStyle.SP_BrowserReload)
        self.refresh_button.setIcon(refresh_icon)
        self.refresh_button.clicked.connect(self.refresh_folder_list)
        self.refresh_button.setEnabled(False)
        self.refresh_button.setToolTip("Atualiza a lista de arquivos da pasta selecionada.")

        # BOTÃO para mapear cabeçalhos
        self.map_headers_button = QPushButton("Analisar/Mapear Cabeçalhos")
        header_icon = self.style().standardIcon(QStyle.SP_FileDialogContentsView)
        self.map_headers_button.setIcon(header_icon)
        self.define_filters_button = QPushButton("Definir Filtros")
        filter_icon = self.style().standardIcon(QStyle.SP_DialogHelpButton)
        self.define_filters_button.setIcon(filter_icon)
        self.define_filters_button.clicked.connect(self.open_filter_dialog)
        self.define_filters_button.setEnabled(False) # Habilitar somente após mapeamento de cabeçalho
        self.define_filters_button.setToolTip("Filtre os valores das colunas e remova dados desnecessários.")
        self.map_headers_button.clicked.connect(self.open_header_mapping_dialog)
        self.map_headers_button.setEnabled(False) # Habilitar após selecionar pasta 
        self.map_headers_button.setToolTip("Agrupe cabeçalhos, analise seus tipos de dados, remova duplicatas e escolha quais serão selecionados.")
        self.sheet_selection_button = QPushButton("Seleção de Abas...")
        sheet_icon = self.style().standardIcon(QStyle.SP_MessageBoxInformation)
        self.sheet_selection_button.setIcon(sheet_icon)
        self.sheet_selection_button.clicked.connect(self.open_sheet_selection_dialog)
        self.sheet_selection_button.setEnabled(False) # Começa desabilitado
        self.sheet_selection_button.setToolTip("Defina regras globais para incluir ou excluir abas em todos os arquivos Excel.")

        self.pivot_button = QPushButton("Criar Tabela de Resumo...")
        pivot_icon = self.style().standardIcon(QStyle.SP_FileDialogEnd)
        self.pivot_button.setIcon(pivot_icon)
        self.pivot_button.clicked.connect(self.open_pivot_dialog)
        self.pivot_button.setEnabled(False)
        self.pivot_button.setToolTip("Cria uma tabela resumo (dinâmica) a partir dos dados consolidados.")
        
        folder_selection_layout.addWidget(logo_widget)
        folder_selection_layout.addWidget(self.folder_path_label)
        folder_selection_layout.addWidget(self.folder_path_line_edit)
        folder_selection_layout.addWidget(self.select_folder_button)
        folder_selection_layout.addWidget(self.refresh_button)
        main_layout.addLayout(folder_selection_layout)

        config_buttons_layout = QHBoxLayout()   
        config_buttons_layout.setContentsMargins(0, 10, 0, 0) # Adiciona um espaçamento superior
        config_buttons_layout.addWidget(self.map_headers_button)
        config_buttons_layout.addWidget(self.sheet_selection_button)
        config_buttons_layout.addWidget(self.define_filters_button)
        config_buttons_layout.addWidget(self.pivot_button)
        config_buttons_layout.addStretch() # Empurra os botões para a esquerda
        main_layout.addLayout(config_buttons_layout)
        # --- Opções de Leitura ---
        self.options_group_box = QGroupBox("Opções de Leitura para .CSV e .TXT")
        options_layout = QHBoxLayout()
        delimiter_label = QLabel("Delimitador: ")
        self.delimiter_combo = QComboBox()
        self.delimiter_combo.addItems([
            "Ponto e Vírgula (;)",
            "Vírgula (,)",
            "Tabulação (Tab)",
            "Pipe (|)",
            "Outro..."
        ])
        self.delimiter_custom_edit = QLineEdit()
        self.delimiter_custom_edit.setPlaceholderText("Digite o delimitador")
        self.delimiter_custom_edit.setFixedWidth(120)
        self.delimiter_custom_edit.setVisible(False) # Começa oculto
        self.delimiter_combo.currentTextChanged.connect(self._on_delimiter_changed)
        options_layout.addWidget(delimiter_label)
        options_layout.addWidget(self.delimiter_combo)
        options_layout.addWidget(self.delimiter_custom_edit)
        options_layout.addStretch() # Empurra tudo para a esquerda
        self.options_group_box.setLayout(options_layout)
        main_layout.addWidget(self.options_group_box)

        # --- 2. Seção Intermediária (Arquivos/Abas e Console) ---
        middle_section_layout = QHBoxLayout()
        left_panel_layout = QVBoxLayout()
        
        self.files_label = QLabel("Arquivos Encontrados (.xlsx, .csv, .xls, .txt):")
        self.files_list_widget = QListWidget()
        self.files_list_widget.currentItemChanged.connect(self.on_file_selected_for_preview)
        
        self.sheets_label = QLabel("Abas do Arquivo Excel Selecionado (marque para incluir):")
        self.sheets_list_widget = QListWidget()
        self.sheets_list_widget.setEnabled(False) 
        # Conectar o sinal itemChanged para quando o estado de um checkbox de aba mudar
        self.sheets_list_widget.itemChanged.connect(self.on_sheet_selection_changed)
        self.sheets_list_widget.currentItemChanged.connect(self.on_sheet_list_item_selected_for_preview)

        left_panel_layout.addWidget(self.files_label)
        left_panel_layout.addWidget(self.files_list_widget)
        left_panel_layout.addWidget(self.sheets_label)
        left_panel_layout.addWidget(self.sheets_list_widget)
        # --- Botões para Marcar/Desmarcar Abas ---
        sheet_actions_layout = QHBoxLayout()
        self.mark_all_sheets_button = QPushButton("Marcar Todas")
        self.mark_all_sheets_button.clicked.connect(self.mark_all_sheets)
        self.unmark_all_sheets_button = QPushButton("Desmarcar Todas")
        self.unmark_all_sheets_button.clicked.connect(self.unmark_all_sheets)
        
        sheet_actions_layout.addWidget(self.mark_all_sheets_button)
        sheet_actions_layout.addWidget(self.unmark_all_sheets_button)
        left_panel_layout.addLayout(sheet_actions_layout)
        
        self.mark_all_sheets_button.setEnabled(False)
        self.unmark_all_sheets_button.setEnabled(False)

        middle_section_layout.addLayout(left_panel_layout)

        # 2.2 Painel Direito (Abas para Console e Pré-visualização)
        right_tab_widget = QTabWidget() 

        # Aba do Console de Log
        log_console_widget = QWidget()
        log_layout = QVBoxLayout(log_console_widget)
        self.log_label = QLabel("Console de Log:") # Pode ser removido se o título da aba for suficiente
        self.log_console_text_edit = QTextEdit()
        self.log_console_text_edit.setReadOnly(True)
        
        log_layout.addWidget(self.log_console_text_edit) # Adiciona diretamente, sem label se preferir
        right_tab_widget.addTab(log_console_widget, "Console de Log")

        # Aba de Pré-visualização de Dados
        preview_widget = QWidget()
        preview_layout = QVBoxLayout(preview_widget)
        self.preview_table_view = QTableView()
        self.preview_table_model = PolarsTableModel() # Instancia nosso modelo customizado
        self.preview_table_view.setModel(self.preview_table_model)
        # self.preview_table_view.setEditTriggers(QTableView.NoEditTriggers) # Desabilitar edição
        self.preview_table_view.setAlternatingRowColors(True)
        preview_layout.addWidget(self.preview_table_view)
        right_tab_widget.addTab(preview_widget, "Pré-visualização de Dados")
        
        middle_section_layout.addWidget(right_tab_widget) # Adiciona o QTabWidget ao layout
        
        middle_section_layout.setStretchFactor(left_panel_layout, 1) 
        middle_section_layout.setStretchFactor(right_tab_widget, 3) # Dar mais espaço para o painel direito
        main_layout.addLayout(middle_section_layout)

        # --- 3. Seção de Configuração de Saída ---
        output_config_layout = QHBoxLayout()
        self.output_name_label = QLabel("Nome do Arquivo de Saída:")
        self.output_name_line_edit = QLineEdit("consolidado") # Nome padrão
        
        self.output_format_label = QLabel("Formato:")
        self.output_format_combo_box = QComboBox()
        self.output_format_combo_box.addItems(["XLSX", "CSV", "Parquet"])
        self.output_format_combo_box.currentTextChanged.connect(self.update_output_filename_extension)
        
        self.save_as_button = QPushButton("Salvar Como...")
        self.save_as_button.clicked.connect(self.open_save_file_dialog)

        output_config_layout.addWidget(self.output_name_label)
        output_config_layout.addWidget(self.output_name_line_edit)
        output_config_layout.addWidget(self.output_format_label)
        output_config_layout.addWidget(self.output_format_combo_box)
        output_config_layout.addWidget(self.save_as_button)
        main_layout.addLayout(output_config_layout)

        # --- 4. Seção de Ação e Progresso ---
        action_progress_layout = QVBoxLayout()
        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        self.progress_bar.setTextVisible(True)

        # self.progress_text_label = QLabel("")
        # self.progress_text_label.setAlignment(Qt.AlignCenter)
        # self.progress_text_label.setVisible(False)

        self.consolidate_button = QPushButton("Iniciar Consolidação")
        self.consolidate_button.setObjectName("consolidate_button")
        self.consolidate_button.clicked.connect(self.start_consolidation) 

        self.cancel_button = QPushButton("Cancelar")
        self.cancel_button.clicked.connect(self.cancel_consolidation)
        self.cancel_button.setVisible(False) 

        buttons_layout = QHBoxLayout() 
        buttons_layout.addWidget(self.consolidate_button)
        buttons_layout.addWidget(self.cancel_button)
        
        # action_progress_layout.addWidget(self.progress_text_label)
        action_progress_layout.addWidget(self.progress_bar)
        action_progress_layout.addLayout(buttons_layout) 
        main_layout.addLayout(action_progress_layout)

        main_layout.setSpacing(15)
        self.show()
        self.log_message("Aplicação iniciada. Selecione uma pasta para começar.", LogLevel.INFO)
        self.update_output_filename_extension(self.output_format_combo_box.currentText())
    
    def open_pivot_dialog(self):
        """Abre o diálogo de configuração da tabela de resumo."""
        if not self.header_mapping:
            self.log_message("Por favor, analise e mapeie os cabeçalhos primeiro.", LogLevel.WARNING)
            return

        # Coleta os nomes e tipos das colunas finais
        final_headers_info = {}
        for map_info in self.header_mapping.values():
            if map_info.get('include'):
                final_name = map_info['final_name']
                type_str = map_info['type_str']
                final_headers_info[final_name] = type_str
        
        all_final_headers = sorted(list(final_headers_info.keys()))
        numeric_types = {"Inteiro", "Decimal (Float)"}
        numeric_headers = sorted([h for h, t in final_headers_info.items() if t in numeric_types])

        if not numeric_headers:
            self.log_message("Nenhuma coluna numérica encontrada no mapeamento para usar nos cálculos.", LogLevel.WARNING)

        dialog = PivotDialog(all_final_headers, numeric_headers, self.pivot_rules, self)
        if dialog.exec() == QDialog.Accepted:
            self.pivot_rules = dialog.get_rules()
            if self.pivot_rules:
                self.log_message("Regras da tabela de resumo foram definidas.", LogLevel.SUCCESS)
            else:
                self.log_message("Regras da tabela de resumo foram limpas.", LogLevel.INFO)

    def open_sheet_selection_dialog(self):
        """Inicia a análise de todas as abas em uma thread e abre o diálogo de seleção ao concluir."""
        if self.sheet_analysis_worker and self.sheet_analysis_worker.isRunning():
            self.log_message("A análise de abas já está em andamento.", LogLevel.INFO)
            return

        excel_files = [path for path in self.current_files_paths.values() if path.lower().endswith((".xlsx", ".xls"))]
        if not excel_files:
            self.log_message("Nenhum arquivo Excel encontrado na pasta para analisar as abas.", LogLevel.WARNING)
            return

        self.log_message("Analisando nomes de todas as abas dos arquivos Excel (em segundo plano)...", LogLevel.INFO)
        self.sheet_selection_button.setEnabled(False) # Desabilita durante a análise

        self.sheet_analysis_worker = SheetAnalysisWorker(excel_files)
        self.sheet_analysis_worker.finished.connect(self.on_sheet_analysis_finished)
        self.sheet_analysis_worker.start()

    def on_sheet_analysis_finished(self, all_sheets_cache, unique_sheet_names, error_message):
        """Chamado quando a SheetAnalysisWorker termina."""
        self.sheet_selection_button.setEnabled(True) # Reabilita o botão
        self.sheet_analysis_worker = None

        if error_message:
            self.log_message(f"Erro durante a análise de abas: {error_message}", LogLevel.ERROR)
            return

        self.log_message(f"Análise de abas concluída. {len(unique_sheet_names)} nomes de abas únicos encontrados.", LogLevel.SUCCESS)
        self.all_sheets_cache = all_sheets_cache # Salva o cache para uso posterior

        # Abre o diálogo com as abas encontradas e as regras que já possam existir
        dialog = SheetSelectionDialog(unique_sheet_names, self.sheet_selection_rules, self)
        if dialog.exec() == QDialog.Accepted:
            self.sheet_selection_rules = dialog.get_rules()
            if self.sheet_selection_rules:
                self.log_message(f"Regra de seleção de abas definida. Modo: {self.sheet_selection_rules['mode']}.", LogLevel.SUCCESS)
            else:
                self.log_message("Regras de seleção global de abas foram limpas.", LogLevel.INFO)

    def open_help_dialog(self):
        """Cria e exibe a janela de ajuda/guia do usuário."""
        # O diálogo já tem o texto, não precisamos passar nada.
        # Passamos 'self' para que o diálogo seja "filho" da janela principal.
        dialog = HelpDialog(self)
        dialog.exec() # .exec() abre o diálogo de forma moda
    
    def _on_delimiter_changed(self, text):
        is_custom = (text == "Outro...")
        self.delimiter_custom_edit.setVisible(is_custom)
    
    def refresh_folder_list(self):
        """Recarrega a lista de arquivos da pasta
        atualmente exibida."""
        current_folder = self.folder_path_line_edit.text()
        if not os.path.isdir(current_folder):
            self.log_message("Nenhuma pasta válida selecionada para atualizar.", LogLevel.WARNING)
            return
        self.log_message(f"Atualizando lista de arquivos para: {current_folder}", LogLevel.INFO)
        self.list_files_in_folder(current_folder)

    def get_selected_delimiter(self):
        try:
            selected = self.delimiter_combo.currentText()
            if selected == "Outro...":
                return self.delimiter_custom_edit.text()
            elif selected == "Tabulação (Tab)":
                return '\t'
            elif "(" in selected and ")" in selected:
                return selected.split("(")[1].replace(")", "")
            else:
                return selected
        except RuntimeError as e:
            if "already deleted" in str(e):
                self.log_message("AVISO: Widget de delimitador foi destruído inesperadamente. Usando ';' como padrão.", LogLevel.WARNING)
                return "|"
            else:
                raise
    
    def mark_all_sheets(self):
        self._set_all_sheets_check_state(Qt.Checked)
    
    def unmark_all_sheets(self):
        self._set_all_sheets_check_state(Qt.Unchecked)
    
    def _set_all_sheets_check_state(self, check_state):
        if not self.sheets_list_widget.isEnabled() or self.sheets_list_widget.count() == 0:
            return
        try:
            self.sheets_list_widget.itemChanged.disconnect(self.on_sheet_selection_changed)
        except RuntimeError:
            pass
        for i in range(self.sheets_list_widget.count()):
            item = self.sheets_list_widget.item(i)
            item.setCheckState(check_state)
            current_file_item = self.files_list_widget.currentItem()
            if current_file_item:
                file_path = self.current_files_paths.get(current_file_item.text())
                if file_path and file_path in self.sheet_selections:
                    self.sheet_selections[file_path][item.text()] = (check_state == Qt.Checked)
        self.sheets_list_widget.itemChanged.connect(self.on_sheet_selection_changed)
        action = "marcadas" if check_state == Qt.Checked else "desmarcadas"
        self.log_message(f'Todas as abas visíveis foram {action}.', LogLevel.INFO)

    def _get_config_path(self):
        """Retorna o caminho para o arquivo de configuração da aplicação."""
        # Salvar na pasta do usuário (mais robusto) ou na pasta da aplicação
        # Usar AppData ou .config no Linux/macOS é o ideal, mas para simplicidade:
        try:
            # Tenta obter o diretório do script
            base_path = os.path.dirname(os.path.abspath(sys.argv[0]))
        except:
            # Fallback para o diretório de trabalho atual se sys.argv[0] não for confiável (ex: PyInstaller one-file)
            base_path = os.getcwd() 
        return os.path.join(base_path, CONFIG_FILE_NAME)

    def _load_last_input_folder(self):
        """Carrega o último caminho da pasta de entrada do arquivo de configuração."""
        config_path = self._get_config_path()
        try:
            if os.path.exists(config_path):
                with open(config_path, 'r', encoding='latin-1') as f:
                    config_data = json.load(f)
                    return config_data.get("last_input_folder")
        except Exception as e:
            # Não precisa ser um erro crítico, apenas logar um aviso
            print(f"Aviso: Não foi possível carregar a configuração: {e}") # Usar print para log antes do logger da GUI estar pronto
        return None

    def _save_last_input_folder(self, folder_path):
        """Salva o caminho da pasta de entrada no arquivo de configuração."""
        config_path = self._get_config_path()
        config_data = {"last_input_folder": folder_path}
        try:
            with open(config_path, 'w', encoding='latin-1') as f:
                json.dump(config_data, f, indent=4)
            # Não precisa logar na GUI cada vez que salva, a menos que queira
            # self.log_message("Diretório padrão salvo.", LogLevel.INFO) 
        except Exception as e:
            self.log_message(f"Erro ao salvar configuração de diretório: {e}", LogLevel.ERROR)


    def open_folder_dialog(self):
        # Usar o último diretório salvo ou o diretório do QLineEdit ou o home do usuário
        start_dir = self.last_used_input_folder or self.folder_path_line_edit.text() or os.path.expanduser("~")
        
        folder_path = QFileDialog.getExistingDirectory(self, "Selecionar Pasta do Projeto", start_dir)
        
        if folder_path:
            self.folder_path_line_edit.setText(folder_path)
            self.log_message(f"Pasta selecionada: {folder_path}", LogLevel.INFO)
            self.list_files_in_folder(folder_path)
            self.last_used_input_folder = folder_path # Atualizar para o próximo uso
            self._save_last_input_folder(folder_path) # <--- SALVAR AO SELECIONAR
        else:
            self.log_message("Seleção de pasta cancelada.", LogLevel.INFO)
    
    # --- Métodos para Mapeamento de Cabeçalhos ---
    def open_header_mapping_dialog(self):
        """Coleta cabeçalhos e abre o diálogo de mapeamento."""
        if not self.current_files_paths:
            self.log_message("Selecione uma pasta e arquivos primeiro.", LogLevel.WARNING)
            return
        
        if self.header_analyzer_thread and self.header_analyzer_thread.isRunning():
            self.log_message("A análise de cabeçalhos já está em andamento.", LogLevel.INFO)
            return # Evita iniciar múltiplas análises

        # self.log_message("Analisando cabeçalhos dos arquivos selecionados...", LogLevel.INFO)
        # unique_headers = set()
        
        # Usar get_files_and_sheets_to_process para saber quais arquivos/abas considerar
        files_and_sheets_config = self.get_files_and_sheets_to_process()
        if not files_and_sheets_config:
            self.log_message("Nenhum arquivo/aba configurado para processamento. Não é possível mapear cabeçalhos.", LogLevel.WARNING)
            return
        
        self.log_message("Analisando cabeçalhos dos arquivos selecionados (em segundo plano)...", LogLevel.INFO)
        self.map_headers_button.setEnabled(False) # Desabilita botão para evitar cliques duplos
        selected_delimiter = self.get_selected_delimiter()
        if not selected_delimiter:
            self.log_message("Análise de cabeçalhos falhou: Delimitador inválido para CSV/TXT.", LogLevel.ERROR)
            self.map_headers_button.setEnabled(True)
            return
        self.define_filters_button.setEnabled(False)
        self.pivot_button.setEnabled(False)

        self.filter_rules.clear()
        self.header_analyzer_thread = HeaderAnalysisWorker(files_and_sheets_config, selected_delimiter)
        self.header_analyzer_thread.finished.connect(self.on_header_analysis_finished)
        self.header_analyzer_thread.progress_log.connect(self.log_message)
        self.header_analyzer_thread.start()

        # =============== DESCARTADA ===============
        # Pequena thread para não travar a GUI ao ler cabeçalhos
        # (Pode ser excessivo para apenas cabeçalhos, mas bom se houver muitos arquivos)
        # Por simplicidade no MVP do mapeamento, vamos fazer síncrono por enquanto.
        # Se ficar lento, podemos mover para uma thread.
        
        # processed_for_headers = 0
        # for file_path, selected_sheets in files_and_sheets_config:
        #     file_name = os.path.basename(file_path)
        #     try:
        #         if file_path.lower().endswith(".csv"):
        #             reader = pl.read_csv_batched(file_path, batch_size=5, infer_schema_length=0, encoding = 'latin-1', separator = ';') 
        #             batches = reader.next_batches(1) 
                    
        #             if batches and len(batches) > 0:
        #                     first_batch = batches[0]
        #                     if first_batch is not None and first_batch.width > 0:
        #                         unique_headers.update(first_batch.columns)
        #             # else: Tratar CSV vazio ou só com cabeçalho (opcional)

        #         elif file_path.lower().endswith((".xlsx", ".xls")) and selected_sheets:
        #             for sheet_name in selected_sheets:
        #                 # Ler apenas a primeira linha da aba
        #                 # df_header = pl.read_excel(file_path, sheet_name=sheet_name, n_rows=1, infer_schema_length=0) # n_rows não é para read_excel
        #                 # Ler e pegar head(0).columns ou head(1).columns
        #                 try:
        #                     df_sample = pl.read_excel(file_path, sheet_name=sheet_name).head(0) # Pega só cabeçalhos
        #                 except pl.exceptions.PolarsError as e_excel:
        #                     error_msg_lower = str(e_excel).lower()
        #                     is_numeric_conversion_error = ('conversion' in error_msg_lower and 'f64' in error_msg_lower or 'float' in error_msg_lower) and ('i64' in error_msg_lower or 'int' in error_msg_lower) and 'failed' in error_msg_lower
        #                     if is_numeric_conversion_error:
        #                         try:
        #                             df_temp = pl.read_excel(source = file_path, sheet_name = sheet_name, infer_schema_length = 0)
        #                             if df_temp is not None:
        #                                 df_sample = df_temp.select([pl.all().cast(pl.String)])
        #                                 # df_preview_sliced = df_original.head(n_rows_to_preview)
        #                             else:
        #                                 df_sample = None
        #                         except Exception as e_retry_excel:
        #                             df_sample = None
        #                     else:
        #                         df_sample = None
        #                 if df_sample.width > 0:
        #                      unique_headers.update(df_sample.columns)
        #         processed_for_headers +=1
        #         # TODO: Adicionar feedback de progresso se for demorado
        #     except Exception as e:
        #         self.log_message(f"Erro ao ler cabeçalhos de '{file_name}' (Aba: {sheet_name if selected_sheets else 'N/A'}): {e}", LogLevel.ERROR)

        # if not unique_headers:
        #     self.log_message("Nenhum cabeçalho encontrado ou erro ao ler todos os cabeçalhos.", LogLevel.WARNING)
        #     return

        # self.log_message(f"Cabeçalhos únicos encontrados: {len(unique_headers)}", LogLevel.SUCCESS)
        
        # # Passar o self.header_mapping existente para o diálogo
        # dialog = HeaderMappingDialog(unique_headers, self, self.header_mapping)
        # if dialog.exec() == QDialog.Accepted: # .exec_() em PyQt5
        #     self.header_mapping = dialog.get_mapping()
        #     self.log_message("Mapeamento de cabeçalhos atualizado.", LogLevel.SUCCESS)
        #     # Logar o mapeamento para depuração (opcional)
        #     # for original, map_info in self.header_mapping.items():
        #     #     self.log_message(f"  '{original}' -> '{map_info['final_name']}' (Incluir: {map_info['include']})", LogLevel.INFO)
        # else:
        #     self.log_message("Mapeamento de cabeçalhos cancelado.", LogLevel.INFO)
    
    def on_header_analysis_finished(self, suggested_groups, error_object):
        """Chamado quando a HeaderAnalysisWorker termina."""
        self.map_headers_button.setEnabled(True) # Reabilita o botão
        if self.header_analyzer_thread: # Garante que a thread exista antes de tentar limpá-la
            self.header_analyzer_thread = None # Limpa a referência da thread

        if error_object:
            if isinstance(error_object, InterruptedError):
                self.log_message(f"Análise de cabeçalhos cancelada: {error_object}", LogLevel.WARNING)
            else:
                self.log_message(f"Erro durante a análise de cabeçalhos: {error_object}", LogLevel.ERROR)
            return

        if not suggested_groups:
            self.log_message("Nenhum cabeçalho encontrado ou erro ao ler todos os cabeçalhos. Verifique os arquivos e seleções de abas.", LogLevel.WARNING)
            return

        self.log_message(f"Análise concluída. Cabeçalhos únicos encontrados: {len(suggested_groups)}", LogLevel.SUCCESS)

        # Passar o self.header_mapping existente para o diálogo
        dialog = HeaderMappingDialog(suggested_groups, self, self.header_mapping, self.duplicate_key_columns)
        if dialog.exec() == QDialog.Accepted:
            self.header_mapping = dialog.get_mapping()
            # --- Salva as colunas para checagem de duplicatas ---
            self.duplicates_config = dialog.get_duplicates_config()
            # self.duplicate_key_columns = dialog.get_duplicate_check_columns()
            self.log_message("Mapeamento de cabeçalhos atualizado.", LogLevel.SUCCESS)
            key_columns = self.duplicates_config.get("key_columns", [])
            if key_columns:
                report_msg = "e um relatório sera gerado" if self.duplicates_config.get("generate_report") else ""
                self.log_message(f"Remoção de duplicatas ativada para as chaves: {', '.join(key_columns)} {report_msg}", LogLevel.INFO)
            self.define_filters_button.setEnabled(True)
            self.pivot_button.setEnabled(True)
            # Opcional: Logar o mapeamento para depuração
            # for original, map_info in self.header_mapping.items():
            #     self.log_message(f"  '{original}' -> '{map_info['final_name']}' (Tipo: {map_info['type_str']}, Incluir: {map_info['include']})", LogLevel.INFO)
        else:
            self.log_message("Mapeamento de cabeçalhos cancelado.", LogLevel.INFO)
    
    def on_file_selected_for_preview(self, current_file_item, previous_file_item):
        """Chamado quando um ARQUIVO é selecionado na lista.
           Também chama o antigo on_file_selected para carregar as abas e suas seleções.
        """
        self.on_file_selected(current_file_item, previous_file_item) # Chama a lógica existente de abas
        
        # Limpar pré-visualização se nenhum item ou arquivo não Excel/CSV
        if not current_file_item:
            self.preview_table_model.clear_data()
            return

        file_name = current_file_item.text()
        file_path = self.current_files_paths.get(file_name)

        if not file_path:
            self.preview_table_model.clear_data()
            return

        if file_path.lower().endswith((".csv", ".txt")):
            self.update_preview(file_path) # Pré-visualiza CSV diretamente
        elif file_path.lower().endswith((".xlsx", ".xls")):
            # Para Excel, a pré-visualização da aba será acionada por:
            # a) on_sheet_loading_finished (que seleciona a primeira aba) -> on_sheet_list_item_selected_for_preview
            # b) clique manual do usuário em uma aba -> on_sheet_list_item_selected_for_preview
            # Podemos limpar a pré-visualização aqui, e ela será preenchida quando as abas carregarem e uma for selecionada.
            self.preview_table_model.clear_data()
        else: # Outros tipos de arquivo (não deve acontecer se o filtro de arquivos estiver correto)
            self.preview_table_model.clear_data()


    def on_sheet_list_item_selected_for_preview(self, current_sheet_item, previous_sheet_item):
        """Chamado quando uma ABA é selecionada na lista de abas."""
        if not current_sheet_item:
            self.preview_table_model.clear_data()
            return

        current_file_item = self.files_list_widget.currentItem()
        if not current_file_item:
            self.preview_table_model.clear_data()
            return

        file_name = current_file_item.text()
        file_path = self.current_files_paths.get(file_name)
        sheet_name = current_sheet_item.text()

        if file_path and sheet_name:
            self.update_preview(file_path, sheet_name)
        else:
            self.preview_table_model.clear_data()


    def update_preview(self, file_path, sheet_name=None, n_rows_to_preview=50):
        self.log_message(f"Gerando pré-visualização para: {os.path.basename(file_path)}" + (f" - Aba: {sheet_name}" if sheet_name else ""), LogLevel.INFO)
        try:
            n_preread_rows = 20
            header_row_index = 0
            df_preview_sliced = None
            
            # 1. Pré-leitura para detectar o cabeçalho
            pre_read_df = None
            delimiter = self.get_selected_delimiter() if file_path.lower().endswith((".csv", ".txt")) else None
            
            if file_path.lower().endswith((".csv", ".txt")):
                if not delimiter:
                    self.log_message("Pré-visualização falhou: Delimitador inválido.", LogLevel.ERROR)
                    return
                pre_read_df = pl.read_csv(source=file_path, has_header=False, n_rows=n_preread_rows, separator=delimiter, encoding='latin-1', ignore_errors=True, infer_schema = False, quote_char = None, truncate_ragged_lines = True)
            elif file_path.lower().endswith((".xlsx", ".xls")) and sheet_name:
                pre_read_df = pl.read_excel(source=file_path, sheet_name=sheet_name, has_header = False).head(n_preread_rows)

            if pre_read_df is not None and not pre_read_df.is_empty():
                header_row_index = _find_header_row_index(pre_read_df, n_preread_rows)

                # 2. Extrair cabeçalhos, dados e renomear (a lógica robusta)
                header_names_raw = [str(h) if h is not None else f"column_{i}" for i, h in enumerate(pre_read_df.row(header_row_index))]
                header_names = _make_headers_unique(header_names_raw)
                data_rows = pre_read_df.slice(offset=header_row_index + 1).head(n_rows_to_preview)
                
                if not data_rows.is_empty():
                    rename_mapping = {old_name: new_name for old_name, new_name in zip(data_rows.columns, header_names)}
                    df_preview_sliced = data_rows.rename(rename_mapping)

            # 3. Carregar os dados no modelo da tabela
            if df_preview_sliced is not None and not df_preview_sliced.is_empty():
                self.preview_table_model.load_data(df_preview_sliced)
                self.log_message(f"Pré-visualização gerada com {df_preview_sliced.height} linhas.", LogLevel.SUCCESS)
            else:
                self.log_message(f"O arquivo/aba {os.path.basename(file_path)} está vazio ou não foi possível ler dados para pré-visualização.", LogLevel.WARNING)
                self.preview_table_model.clear_data()

        except Exception as e:
            self.log_message(f"Erro ao gerar pré-visualização para {os.path.basename(file_path)}: {e}", LogLevel.ERROR)
            self.preview_table_model.clear_data()
    
    def open_filter_dialog(self):
        if not self.header_mapping:
            self.log_message("Por favor, analise e mapeie os cabeçalhos primeiro.", LogLevel.WARNING)
            return
        final_headers = {
            map_info['final_name'] for map_info in self.header_mapping.values() if map_info.get('include')
        }
        if not final_headers:
            self.log_message("Nenhym cabeçalho final encontrado no mapeamento. Impossível definir filtros", LogLevel.WARNING)
            return
        dialog = FilterDialog(list(final_headers), self.filter_rules, self)
        if dialog.exec() == QDialog.Accepted:
            self.filter_rules = dialog.get_filters()
            self.log_message(f"Filtros atualizados. {len(self.filter_rules)} regra(s) ativa(s)", LogLevel.SUCCESS)
        else:
            self.log_message("Definição de filtros cancelada.", LogLevel.INFO)

    def on_file_selected(self, current_file_item, previous_file_item):
        """Chamado quando um arquivo é selecionado na lista de arquivos. Delega o carregamento de abas para uma thread."""
        self.sheets_list_widget.clear()
        self.sheets_list_widget.setEnabled(False)
        self.mark_all_sheets_button.setEnabled(False)
        self.unmark_all_sheets_button.setEnabled(False)
        # self.preview_table_model.clear_data() # A pré-visualização é tratada por on_file_selected_for_preview

        if self.sheet_loader_thread and self.sheet_loader_thread.isRunning():
            self.sheet_loader_thread.stop() # Solicita parada da thread anterior
            # Não esperar aqui para não bloquear a UI, a thread antiga morrerá ou emitirá um resultado que será ignorado

        if not current_file_item:
            return

        selected_file_name = current_file_item.text()
        file_path = self.current_files_paths.get(selected_file_name)

        if not file_path:
            self.log_message(f"Caminho não encontrado para o arquivo: {selected_file_name}", LogLevel.ERROR)
            return

        # CSVs não têm abas, então não iniciar a thread para eles aqui.
        # A lógica de pré-visualização de CSV é tratada em on_file_selected_for_preview
        if file_path.lower().endswith((".xlsx", ".xls")):
            self.log_message(f"Carregando abas para '{selected_file_name}'...", LogLevel.INFO)
            self.sheets_list_widget.setEnabled(False) # Garante que esteja desabilitada
            self.mark_all_sheets_button.setEnabled(False)
            self.unmark_all_sheets_button.setEnabled(False)

            self.sheet_loader_thread = SheetLoadingWorker(file_path)
            self.sheet_loader_thread.finished.connect(self.on_sheet_loading_finished)
            self.sheet_loader_thread.start()
        else: # Arquivo não-Excel (ex: CSV) selecionado
            self.log_message(f"Arquivo selecionado: {selected_file_name} (Não é Excel, sem abas para listar).", LogLevel.INFO)
            # A pré-visualização de CSV será acionada por on_file_selected_for_preview
            # Nenhuma aba para popular, então botões de aba permanecem desabilitados.


    def on_sheet_loading_finished(self, file_path_processed, sheet_names, error_message):
        """Chamado quando a SheetLoadingWorker termina de carregar as abas."""
        # Verificar se o resultado ainda é para o arquivo atualmente selecionado
        current_selected_file_item = self.files_list_widget.currentItem()
        if not current_selected_file_item or self.current_files_paths.get(current_selected_file_item.text()) != file_path_processed:
            self.log_message(f"Resultado do carregamento de abas para '{os.path.basename(file_path_processed)}' ignorado (seleção mudou).", LogLevel.INFO)
            if self.sheet_loader_thread and self.sheet_loader_thread.file_path == file_path_processed:
                self.sheet_loader_thread = None # Limpa a referência da thread que acabou
            return

        selected_file_name = os.path.basename(file_path_processed) # Obter nome do arquivo do caminho processado

        if error_message:
            self.log_message(error_message, LogLevel.ERROR)
            if file_path_processed in self.sheet_selections: # Limpar se deu erro
                del self.sheet_selections[file_path_processed]
            self.sheets_list_widget.clear() # Garante que a lista de abas esteja limpa
            self.sheets_list_widget.setEnabled(False)
            self.mark_all_sheets_button.setEnabled(False)
            self.unmark_all_sheets_button.setEnabled(False)
            if self.sheet_loader_thread and self.sheet_loader_thread.file_path == file_path_processed:
                self.sheet_loader_thread = None
            return

        # Sucesso no carregamento das abas
        self.log_message(f"Abas carregadas para '{selected_file_name}'.", LogLevel.INFO)
        can_manage_sheets = False
        if sheet_names:
            self.sheets_list_widget.setEnabled(True)
            can_manage_sheets = True

            # Desconectar temporariamente o sinal para evitar chamadas recursivas
            try:
                self.sheets_list_widget.itemChanged.disconnect(self.on_sheet_selection_changed)
            except RuntimeError:
                pass

            # Verificar se já temos seleções salvas para este arquivo
            if file_path_processed not in self.sheet_selections:
                self.sheet_selections[file_path_processed] = {name: True for name in sheet_names}

            current_sheet_states = self.sheet_selections[file_path_processed]

            # Adicionar abas que estão no arquivo, mas talvez não em current_sheet_states (se o arquivo mudou)
            for sheet_name in sheet_names:
                if sheet_name not in current_sheet_states:
                    current_sheet_states[sheet_name] = True # Default para incluir novas abas

            # Remover abas de current_sheet_states que não existem mais no arquivo
            # (Isso garante que self.sheet_selections reflita apenas abas existentes)
            # Criar uma cópia das chaves para iterar, pois podemos modificar o dicionário
            for stored_sheet_name in list(current_sheet_states.keys()):
                if stored_sheet_name not in sheet_names:
                    del current_sheet_states[stored_sheet_name]

            self.sheets_list_widget.clear() # Limpar antes de repopular
            for sheet_name in sheet_names: # Iterar sobre as abas reais do arquivo
                item = QListWidgetItem(sheet_name)
                item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
                is_checked = current_sheet_states.get(sheet_name, True) # Default para True
                item.setCheckState(Qt.Checked if is_checked else Qt.Unchecked)
                self.sheets_list_widget.addItem(item)

            log_suffix = " (carregadas do cache/atualizadas)" if any(not s for s in current_sheet_states.values()) else " (todas marcadas por padrão/atualizadas)"
            self.log_message(f"Abas encontradas para '{selected_file_name}': {', '.join(sheet_names)}.{log_suffix}", LogLevel.INFO)

            # Reconectar o sinal
            self.sheets_list_widget.itemChanged.connect(self.on_sheet_selection_changed)

            # Tenta pré-selecionar e pré-visualizar a primeira aba, se houver
            if self.sheets_list_widget.count() > 0:
                first_sheet_item = self.sheets_list_widget.item(0)
                if first_sheet_item:
                    # Se on_file_selected_for_preview também chama on_file_selected,
                    # precisamos garantir que on_file_selected não reinicie a thread de abas desnecessariamente.
                    # No entanto, on_file_selected_for_preview chama on_file_selected PRIMEIRO.
                    # A chamada a setCurrentItem aqui irá disparar on_sheet_list_item_selected_for_preview, que é para a pré-visualização.
                    self.sheets_list_widget.setCurrentItem(first_sheet_item)


        else: # Nenhuma aba encontrada
            self.log_message(f"Nenhuma aba encontrada no arquivo Excel: {selected_file_name}", LogLevel.WARNING)
            if file_path_processed in self.sheet_selections:
                del self.sheet_selections[file_path_processed]
            self.sheets_list_widget.clear()
            self.sheets_list_widget.setEnabled(False)

        self.mark_all_sheets_button.setEnabled(can_manage_sheets)
        self.unmark_all_sheets_button.setEnabled(can_manage_sheets)

        if self.sheet_loader_thread and self.sheet_loader_thread.file_path == file_path_processed:
            self.sheet_loader_thread = None # Limpa a referência da thread que acabou

    def on_sheet_selection_changed(self, item_changed: QListWidgetItem):
        """Chamado quando o estado de um checkbox de aba muda."""
        current_file_item = self.files_list_widget.currentItem()
        if not current_file_item:
            return # Nenhum arquivo Excel selecionado na lista principal

        selected_file_name = current_file_item.text()
        file_path = self.current_files_paths.get(selected_file_name)

        if not file_path or not file_path.lower().endswith((".xlsx", ".xls")):
            return # Não é um arquivo Excel válido ou caminho não encontrado

        sheet_name = item_changed.text()
        is_checked = item_changed.checkState() == Qt.Checked

        # Garantir que a entrada para o arquivo exista em self.sheet_selections
        if file_path not in self.sheet_selections:
            # Isso não deveria acontecer se on_file_selected populou corretamente,
            # mas é uma salvaguarda.
            self.sheet_selections[file_path] = {} 
        
        self.sheet_selections[file_path][sheet_name] = is_checked
        # self.log_message(f"Seleção da aba '{sheet_name}' para '{selected_file_name}' atualizada para: {'Marcada' if is_checked else 'Desmarcada'}", LogLevel.INFO)

    def get_files_and_sheets_to_process(self):
        files_to_process_list = []
        if not self.current_files_paths:
            self.log_message("Nenhuma pasta selecionada ou nenhum arquivo encontrado.", LogLevel.WARNING)
            return None

        for file_name, file_path in self.current_files_paths.items():
            is_excel = file_path.lower().endswith((".xlsx", ".xls"))

            if is_excel:
                selected_sheets_for_file = []
                
                # --- NOVA LÓGICA DE DECISÃO ---
                # Se existem regras globais, use-as.
                if self.sheet_selection_rules and self.all_sheets_cache:
                    file_sheets = self.all_sheets_cache.get(file_path, [])
                    rule_mode = self.sheet_selection_rules.get("mode", "include")
                    rule_names = self.sheet_selection_rules.get("names", set())

                    if rule_mode == "include":
                        selected_sheets_for_file = [s for s in file_sheets if s in rule_names]
                    elif rule_mode == "exclude":
                        selected_sheets_for_file = [s for s in file_sheets if s not in rule_names]
                
                # Senão, use a lógica antiga de seleção manual por arquivo.
                else:
                    if file_path in self.sheet_selections:
                        selected_sheets_for_file = [s for s, checked in self.sheet_selections[file_path].items() if checked]
                    # Se um arquivo nunca foi selecionado, pode não ter entrada. Neste caso, não processará nenhuma aba.
                    # Isso é melhor do que o comportamento antigo que tentava ler todas.
                
                if not selected_sheets_for_file:
                    self.log_message(f"Nenhuma aba selecionada para o arquivo Excel '{file_name}'. Será pulado.", LogLevel.INFO)
                    continue
                
                files_to_process_list.append((file_path, selected_sheets_for_file))

            elif file_path.lower().endswith((".csv", ".txt")):
                files_to_process_list.append((file_path, None))
            else:
                self.log_message(f"Arquivo '{file_name}' não é suportado. Pulando.", LogLevel.WARNING)

        if not files_to_process_list:
            self.log_message("Nenhum arquivo encontrado ou selecionado para processamento após aplicar as regras.", LogLevel.WARNING)
            return None
            
        return files_to_process_list


    def log_message(self, message, level=LogLevel.INFO):
        self.is_last_log_progress = False
        self.log_console_text_edit.append(f"{level.value} {message}")

    def list_files_in_folder(self, folder_path):
        self.files_list_widget.clear()
        self.sheet_selections.clear() 
        self.sheets_list_widget.clear()
        self.sheets_list_widget.setEnabled(False)
        self.current_files_paths.clear()
        self.preview_table_model.clear_data()

        self.refresh_button.setEnabled(False) 
        self.map_headers_button.setEnabled(False)
        self.define_filters_button.setEnabled(False) 
        self.sheet_selection_button.setEnabled(False)
        self.pivot_button.setEnabled(False)
        self.header_mapping.clear()
        self.filter_rules.clear()
        self.pivot_rules.clear()
        self.duplicate_key_columns.clear()
        self.sheet_selection_rules.clear()
        self.all_sheets_cache.clear()


        supported_extensions = ("*.xlsx", "*.csv", "*.xls", "*.txt")
        found_files_paths = []
        try:
            for ext in supported_extensions:
                pattern = os.path.join(folder_path, ext)
                found_files_paths.extend(glob.glob(pattern))
            
            if found_files_paths:
                for full_path in found_files_paths:
                    file_name = os.path.basename(full_path)
                    self.files_list_widget.addItem(file_name)
                    self.current_files_paths[file_name] = full_path
                self.log_message(f"Encontrados {len(found_files_paths)} arquivos na pasta.", LogLevel.SUCCESS)
                self.map_headers_button.setEnabled(True) # HABILITAR AQUI se arquivos forem encontrados
                self.refresh_button.setEnabled(True) # E então ele é habilitado
                excel_files_found = any(f.lower().endswith(('.xlsx', '.xls')) for f in found_files_paths)
                self.sheet_selection_button.setEnabled(excel_files_found)
            else:
                self.log_message("Nenhum arquivo suportado (.xlsx, .csv, .xls, .txt) encontrado na pasta.", LogLevel.WARNING)
                # self.map_headers_button.setEnabled(False) # Já está desabilitado pelo início da função

        except Exception as e:
            self.log_message(f"Erro ao listar arquivos: {e}", LogLevel.ERROR)
            self.current_files_paths.clear()
            # self.map_headers_button.setEnabled(False) # Já está desabilitado
    
    def update_output_filename_extension(self, selected_format):
        current_name = self.output_name_line_edit.text()
        name_part, _ = os.path.splitext(current_name)
        new_extension = ".parquet" if selected_format == "Parquet" else "." + selected_format.lower()
        # new_extension = "." + selected_format.lower()
        self.output_name_line_edit.setText(name_part + new_extension)

    def open_save_file_dialog(self):
        current_folder = self.folder_path_line_edit.text() or os.path.expanduser("~")
        suggested_filename = self.output_name_line_edit.text()
        initial_path = os.path.join(current_folder, suggested_filename)
        selected_format = self.output_format_combo_box.currentText()
        if selected_format == "XLSX":
            filter_str = "Arquivos Excel (*.xlsx)"
        elif selected_format == "CSV":
            filter_str = "Arquivos CSV (*.csv)"
        elif selected_format == "Parquet":
            filter_str = "Arquivos Parquet (*.parquet)"
        else:
            filter_str = "Todos os Arquivos (*)"
        file_path, _ = QFileDialog.getSaveFileName(self, "Salvar Arquivo Consolidado Como...", initial_path, filter_str)
        if file_path:
            self.output_file_path = file_path
            base_name = os.path.basename(file_path)
            self.output_name_line_edit.setText(base_name)
            name_part, ext_part = os.path.splitext(base_name)
            if ext_part.lower() == ".xlsx" and self.output_format_combo_box.currentText() != "XLSX":
                self.output_format_combo_box.setCurrentText("XLSX")
            elif ext_part.lower() == ".csv" and self.output_format_combo_box.currentText() != "CSV":
                self.output_format_combo_box.setCurrentText("CSV")
            elif ext_part.lower() == ".parquet" and self.output_format_combo_box.currentText != "Parquet":
                self.output_format_combo_box.setCurrentText("Parquet")
            self.log_message(f"Arquivo de saída definido como: {file_path}", LogLevel.SUCCESS)
        else:
            self.log_message("Seleção de local para salvar cancelada.", LogLevel.INFO)
    
    def update_progress_text(self, text):
        """Atualiza a última linha do console com o texto de progresso, ou adiciona uma nova linha se a última não
        for de progresso."""
        formatted_text = f"{LogLevel.INFO.value} {text}"
        cursor = self.log_console_text_edit.textCursor()
        if self.is_last_log_progress:
            cursor.movePosition(QTextCursor.End)
            cursor.select(QTextCursor.BlockUnderCursor)
            cursor.removeSelectedText()
        # cursor.insertText(formatted_text)
        cursor.movePosition(QTextCursor.End)
        self.log_console_text_edit.append(formatted_text)
        self.log_console_text_edit.ensureCursorVisible()
        self.is_last_log_progress = True

    def start_consolidation(self):
        if not self.folder_path_line_edit.text():
            self.log_message("Por favor, selecione uma pasta de projeto primeiro.", LogLevel.WARNING)
            return

        if not self.output_file_path:
            self.log_message("Por favor, defina o arquivo de saída usando 'Salvar Como...'.", LogLevel.WARNING)
            return

        files_to_process = self.get_files_and_sheets_to_process()
        if not files_to_process: # Checa se é None ou lista vazia
            self.log_message("Nenhum arquivo ou aba válida selecionada para consolidação.", LogLevel.WARNING)
            return

        self.set_ui_for_processing(True)
        output_format = self.output_format_combo_box.currentText()
        selected_delimiter = self.get_selected_delimiter()
        if not selected_delimiter:
            self.log_message("Delimitador inválido ou não definido para arquivos CSV/TXT.", LogLevel.ERROR)
            self.set_ui_for_processing(False)
            return
        
        
        self.consolidation_thread = ConsolidationWorker(files_to_process, self.output_file_path, output_format, self.header_mapping, self.filter_rules, selected_delimiter, self.pivot_rules, self.duplicates_config)
        self.consolidation_thread.log_message.connect(self.log_message) 
        self.consolidation_thread.progress_updated.connect(self.update_progress_bar)
        self.consolidation_thread.finished.connect(self.on_consolidation_finished)
        self.consolidation_thread.progress_text_updated.connect(self.update_progress_text)
        
        self.consolidation_thread.start()

    def update_progress_bar(self, value):
        self.progress_bar.setValue(value)

    def on_consolidation_finished(self, success, message):
        if success:
            self.log_message(f"Resultado: {message}", LogLevel.SUCCESS)
        else:
            self.log_message(f"Resultado: {message}", LogLevel.ERROR)
        
        self.set_ui_for_processing(False)
        self.consolidation_thread = None 

    def set_ui_for_processing(self, processing):
        not_proc = not processing
        self.select_folder_button.setEnabled(not_proc)
        self.files_list_widget.setEnabled(not_proc)
        
        # Habilitar o botão de mapear apenas se não estiver processando E houver arquivos listados
        self.map_headers_button.setEnabled(not_proc and self.files_list_widget.count() > 0) 

        has_sel_excel_sheets = False
        current_file_item = self.files_list_widget.currentItem()
        if not_proc and current_file_item: # Só verifica se não estiver processando e houver item
            fn = current_file_item.text()
            fp = self.current_files_paths.get(fn)
            if fp and (fp.lower().endswith((".xlsx",".xls"))) and self.sheets_list_widget.count() > 0:
                has_sel_excel_sheets = True
        self.sheets_list_widget.setEnabled(not_proc and has_sel_excel_sheets)
        sheet_buttons_enabled = not_proc and self.sheets_list_widget.isEnabled() and self.sheets_list_widget.count() > 0
        self.mark_all_sheets_button.setEnabled(sheet_buttons_enabled)
        self.unmark_all_sheets_button.setEnabled(sheet_buttons_enabled)
        self.output_name_line_edit.setEnabled(not_proc)
        self.output_format_combo_box.setEnabled(not_proc)
        self.save_as_button.setEnabled(not_proc)
        self.consolidate_button.setVisible(not_proc) 
        self.cancel_button.setVisible(processing)
        # self.progress_bar.setVisible(processing)
        # self.progress_text_label.setVisible(processing)       
        if not_proc:
            self.progress_bar.setValue(0)
        self.is_last_log_progress = False

    def cancel_consolidation(self):
        if self.consolidation_thread and self.consolidation_thread.isRunning():
            self.log_message("Solicitando cancelamento da consolidação...", LogLevel.INFO)
            self.consolidation_thread.stop() 
        else:
            self.log_message("Nenhuma consolidação em andamento para cancelar.", LogLevel.INFO)
            self.set_ui_for_processing(False) # Garante que a UI volte ao normal se o botão for clicado por engano

    def open_folder_dialog(self): # Adicionado para garantir que está presente
        start_dir = self.folder_path_line_edit.text() or os.path.expanduser("~")
        folder_path = QFileDialog.getExistingDirectory(self, "Selecionar Pasta do Projeto", start_dir)
        if folder_path:
            self.folder_path_line_edit.setText(folder_path)
            self.log_message(f"Pasta selecionada: {folder_path}", LogLevel.INFO)
            self.list_files_in_folder(folder_path)
        else:
            self.log_message("Seleção de pasta cancelada.", LogLevel.INFO)

    def closeEvent(self, event):
        if self.consolidation_thread and self.consolidation_thread.isRunning():
            self.log_message("Fechando aplicação, parando consolidação em andamento...", LogLevel.INFO)
            self.consolidation_thread.stop()
            self.consolidation_thread.wait() 

        if self.sheet_loader_thread and self.sheet_loader_thread.isRunning(): # Adicionado
            self.log_message("Fechando aplicação, parando carregamento de abas...", LogLevel.INFO)
            self.sheet_loader_thread.stop()
            self.sheet_loader_thread.wait() # Espera a thread finalizar
        
        if self.header_analyzer_thread and self.header_analyzer_thread.isRunning(): # Adicionado
            self.log_message("Fechando aplicação, parando análise de cabeçalhos...", LogLevel.INFO)
            self.header_analyzer_thread.stop()
            self.header_analyzer_thread.wait()

        event.accept()
