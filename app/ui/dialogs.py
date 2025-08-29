import os
from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QPushButton, QLineEdit, QLabel,
    QListWidget, QListWidgetItem, QComboBox, QTextEdit, QDialogButtonBox,
    QTableWidget, QTableWidgetItem, QCheckBox, QHeaderView, QScrollArea,
    QGroupBox, QAbstractItemView, QInputDialog, QRadioButton, QWidget
)
from PySide6.QtCore import Qt

# Importa as constantes do módulo de utilitários
from ..utils import DATA_TYPES_OPTIONS, OPERATOR_OPTIONS, OPERATORS_NO_VALUE

class PivotDialog(QDialog):
    """Um diálogo para configurar a operação de tabela dinâmica (pivot)."""
    def __init__(self, all_headers, numeric_headers, existing_rules=None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Configurar Tabela de Resumo (Dinâmica)")
        self.setMinimumSize(750, 550) # Ajuste de tamanho

        self.all_headers = all_headers
        self.numeric_headers = numeric_headers
        self.operations = ["Soma", "Média", "Contagem", "Mínimo", "Máximo", "Contagem Única"]

        # Layout Principal
        main_layout = QVBoxLayout(self)

        # --- SEÇÃO DE AGRUPAMENTO (COM A MELHORIA DE UI) ---
        group_by_box = QGroupBox("1. Agrupar por (Linhas)")
        group_by_v_layout = QVBoxLayout(group_by_box)
        group_by_v_layout.addWidget(QLabel("Selecione as colunas para agrupar (esquerda) e veja sua seleção (direita):"))

        # Layout horizontal para as duas listas
        group_by_h_layout = QHBoxLayout()

        # Painel da Esquerda: Lista de todas as colunas para seleção
        self.group_by_list = QListWidget()
        self.group_by_list.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.group_by_list.addItems(self.all_headers)
        # Conecta a mudança na seleção à atualização do painel da direita
        self.group_by_list.itemSelectionChanged.connect(self.update_group_by_display)
        group_by_h_layout.addWidget(self.group_by_list)

        # Painel da Direita: Apenas para exibir as colunas selecionadas
        self.selected_group_by_display = QListWidget()
        self.selected_group_by_display.setMaximumWidth(220) # Largura fixa para o painel de exibição
        # Desabilita a interação direta com este painel
        self.selected_group_by_display.setSelectionMode(QAbstractItemView.NoSelection)
        self.selected_group_by_display.setStyleSheet("QListWidget { background-color: #ECECEC; }") # Cor de fundo para indicar que é um display
        group_by_h_layout.addWidget(self.selected_group_by_display)

        group_by_v_layout.addLayout(group_by_h_layout)
        main_layout.addWidget(group_by_box)

        # --- Seção de Agregações (sem alterações) ---
        agg_box = QGroupBox("2. Cálculos (Valores)")
        agg_layout = QVBoxLayout(agg_box)
        
        agg_actions_layout = QHBoxLayout()
        agg_actions_layout.addWidget(QLabel("Defina as colunas e as operações de cálculo:"))
        agg_actions_layout.addStretch()
        self.add_agg_button = QPushButton("Adicionar Cálculo")
        self.add_agg_button.clicked.connect(self.add_aggregation_row)
        agg_actions_layout.addWidget(self.add_agg_button)
        agg_layout.addLayout(agg_actions_layout)

        self.agg_table = QTableWidget()
        self.agg_table.setColumnCount(3)
        self.agg_table.setHorizontalHeaderLabels(["Coluna para Calcular", "Operação", "Ação"])
        self.agg_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.agg_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.agg_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        agg_layout.addWidget(self.agg_table)
        main_layout.addWidget(agg_box)

        self.only_pivot_checkbox = QCheckBox("Gerar apenas a Tabela de Resumo (ignorar dados consolidados na saída)")
        main_layout.addWidget(self.only_pivot_checkbox)

        # --- Botões Finais (sem alterações) ---
        self.button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel | QDialogButtonBox.Reset)
        self.button_box.button(QDialogButtonBox.Reset).setText("Limpar Regras")
        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)
        self.button_box.button(QDialogButtonBox.Reset).clicked.connect(self.clear_rules)
        main_layout.addWidget(self.button_box)

        if existing_rules:
            self.populate_from_rules(existing_rules)
        else:
            self.add_aggregation_row()
    
    def update_group_by_display(self):
        """NOVO: Atualiza o painel da direita para espelhar a seleção de agrupamento."""
        self.selected_group_by_display.clear()
        selected_items = self.group_by_list.selectedItems()
        if selected_items:
            # Pega o texto de cada item selecionado e adiciona ao painel de exibição
            self.selected_group_by_display.addItems(sorted([item.text() for item in selected_items]))

    def add_aggregation_row(self, rule=None):
        """Adiciona uma linha à tabela de agregação."""
        row_position = self.agg_table.rowCount()
        self.agg_table.insertRow(row_position)

        # Coluna 0: Coluna de Valor
        col_combo = QComboBox()
        col_combo.addItems(self.numeric_headers)
        self.agg_table.setCellWidget(row_position, 0, col_combo)

        # Coluna 1: Operação
        op_combo = QComboBox()
        op_combo.addItems(self.operations)
        self.agg_table.setCellWidget(row_position, 1, op_combo)
        
        # Coluna 2: Botão de Remover
        remove_button = QPushButton("X")
        remove_button.setFixedWidth(40)
        # Usamos uma lambda para passar a linha correta no momento do clique
        remove_button.clicked.connect(lambda: self.agg_table.removeRow(self.agg_table.indexAt(remove_button.pos()).row()))
        self.agg_table.setCellWidget(row_position, 2, remove_button)
        
        # Preenche com dados da regra, se fornecida
        if rule:
            col_combo.setCurrentText(rule.get("column", ""))
            op_combo.setCurrentText(rule.get("operation", ""))

    def populate_from_rules(self, rules):
        """Preenche o diálogo com regras existentes."""
        # Limpa o que já existe
        self.group_by_list.clearSelection()
        while self.agg_table.rowCount() > 0:
            self.agg_table.removeRow(0)

        # Popula agrupamentos
        for item_text in rules.get("group_by", []):
            items = self.group_by_list.findItems(item_text, Qt.MatchExactly)
            if items:
                items[0].setSelected(True)
        
        self.update_group_by_display()
        # Popula agregações
        for rule in rules.get("aggregations", []):
            self.add_aggregation_row(rule)
        self.only_pivot_checkbox.setChecked(rules.get("only_pivot", False))
        

    def clear_rules(self):
        """Limpa todas as seleções e regras no diálogo."""
        self.group_by_list.clearSelection()
        while self.agg_table.rowCount() > 0:
            self.agg_table.removeRow(0)
        self.add_aggregation_row() # Adiciona uma linha em branco
        
    def get_rules(self):
        """Lê os widgets e retorna um dicionário com as regras de pivot."""
        group_by_cols = [item.text() for item in self.group_by_list.selectedItems()]
        
        aggregations = []
        for row in range(self.agg_table.rowCount()):
            col_combo = self.agg_table.cellWidget(row, 0)
            op_combo = self.agg_table.cellWidget(row, 1)
            
            if col_combo and op_combo and col_combo.currentText():
                aggregations.append({
                    "column": col_combo.currentText(),
                    "operation": op_combo.currentText()
                })
        
        # Só retorna regras se o usuário definiu agrupamentos e agregações
        if not group_by_cols or not aggregations:
            return {}

        return {
            "group_by": group_by_cols,
            "aggregations": aggregations,
            "only_pivot": self.only_pivot_checkbox.isChecked() 
        }

class FilterDialog(QDialog):
    def __init__(self, final_headers, existing_filters=None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Definir Filtros de Dados")
        self.setMinimumSize(600, 400)

        self.final_headers = sorted(final_headers)
        self.filter_rows = []  # Para manter referência aos widgets de cada linha

        # --- Layout Principal ---
        main_layout = QVBoxLayout(self)

        # --- Descrição ---
        description_label = QLabel("Adicione regras para filtrar os dados. Regras na mesma coluna (exceto 'Entre') são unidas com OU. Grupos de colunas diferentes são unidos com E.")
        main_layout.addWidget(description_label)

        # --- Layout para as Linhas de Filtro ---
        self.filters_layout = QVBoxLayout()
        self.filters_layout.setAlignment(Qt.AlignTop)

        # Widget e ScrollArea para conter as linhas de filtro
        filters_widget = QWidget()
        filters_widget.setLayout(self.filters_layout)
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setWidget(filters_widget)
        main_layout.addWidget(scroll_area)

        # --- Botões de Ação ---
        add_filter_button = QPushButton("Adicionar Filtro")
        add_filter_button.clicked.connect(self.add_filter_row)
        main_layout.addWidget(add_filter_button)

        # Botões OK e Cancelar
        self.button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)
        main_layout.addWidget(self.button_box)

        # Preencher com filtros existentes ou iniciar com uma linha em branco
        if existing_filters:
            for f in existing_filters:
                self.add_filter_row(filter_data=f)
        else:
            self.add_filter_row()

    def add_filter_row(self, filter_data=None):
        """Adiciona uma nova linha de widget para criar uma regra de filtro."""
        row_widget = QWidget()
        row_layout = QHBoxLayout(row_widget)
        row_layout.setContentsMargins(0, 0, 0, 0)

        column_combo = QComboBox()
        if self.final_headers:
            column_combo.addItems(self.final_headers)
        else:
            column_combo.addItem("Nenhum cabeçalho mapeado"); column_combo.setEnabled(False)

        operator_combo = QComboBox()
        operator_combo.addItems(OPERATOR_OPTIONS)

        value1_edit = QLineEdit()
        value1_edit.setPlaceholderText("Valor")
        and_label = QLabel(" e ")
        value2_edit = QLineEdit()
        value2_edit.setPlaceholderText("Valor Máximo")
        
        remove_button = QPushButton("X"); remove_button.setFixedWidth(30)
        
        row_layout.addWidget(column_combo)
        row_layout.addWidget(operator_combo)
        row_layout.addWidget(value1_edit)
        row_layout.addWidget(and_label)
        row_layout.addWidget(value2_edit)
        row_layout.addWidget(remove_button)

        # Guarda as referências aos widgets da linha para fácil acesso
        row_widgets = {"widget": row_widget, "op_combo": operator_combo, "val1": value1_edit, "and_label": and_label, "val2": value2_edit}
        self.filter_rows.append(row_widgets)

        self.filters_layout.addWidget(row_widget)

        # Conecta o botão de remover
        remove_button.clicked.connect(lambda: self.remove_filter_row(row_widgets))
        # Conecta a mudança do operador à lógica de visibilidade
        operator_combo.currentTextChanged.connect(lambda text: self._on_operator_changed(text, row_widgets))
        
        # Preenche os dados se for um filtro existente
        if filter_data:
            column_combo.setCurrentText(filter_data.get("column", ""))
            operator_combo.setCurrentText(filter_data.get("operator", ""))
            if isinstance(filter_data.get("value"), list): # Se for "Entre"
                value1_edit.setText(filter_data["value"][0])
                value2_edit.setText(filter_data["value"][1])
            else:
                value1_edit.setText(filter_data.get("value", ""))

        # Dispara a lógica de visibilidade para o estado inicial
        self._on_operator_changed(operator_combo.currentText(), row_widgets)

    def _on_operator_changed(self, text, row_widgets):
        """Ajusta a visibilidade dos campos de valor com base no operador."""
        is_between = (text == "Entre")
        is_no_value = (text in OPERATORS_NO_VALUE)
        
        row_widgets["val1"].setVisible(not is_no_value)
        row_widgets["and_label"].setVisible(is_between)
        row_widgets["val2"].setVisible(is_between)

    def remove_filter_row(self, row_widget):
        """Remove uma linha de filtro da interface e da nossa lista de referência."""
        if row_widget in self.filter_rows:
            row_widget['widget'].deleteLater()
            self.filter_rows.remove(row_widget)

    def get_filters(self):
        """Lê todos os widgets de filtro e retorna uma lista de dicionários com as regras."""
        rules = []
        for row_data in self.filter_rows:
            # Encontrar os widgets filhos pelo nome do objeto seria mais robusto, mas isso funciona
            column_combo = row_data["widget"].findChild(QComboBox)
            operator = row_data["op_combo"].currentText()
            
            value = ""
            if operator == "Entre":
                val1 = row_data["val1"].text()
                val2 = row_data["val2"].text()
                value = [val1, val2] # Salva como uma lista
            else:
                value = row_data["val1"].text()

            rules.append({
                "column": column_combo.currentText(),
                "operator": operator,
                "value": value,
            })
        return rules

class SplitGroupDialog(QDialog):
    """Um diálogo para dividir um grupo de cabeçalhos."""
    def __init__(self, group_to_split, parent = None):
        super().__init__(parent)
        self.setWindowTitle("Dividir Grupo de Cabeçalhos")
        self.setMinimumWidth(500)
        self.layout = QVBoxLayout(self)
        self.layout.addWidget(QLabel("Marque os cabeçalhos que você deseja mover para um novo grupo:"))
        self.list_widget = QListWidget()
        for original_name, file_path, sheet_name in group_to_split:
            # Formata o texto do item para ser informativo
            source_text = os.path.basename(file_path)
            if sheet_name:
                source_text += f" ({sheet_name})"
            item_text = f"'{original_name}' (Fonte: {source_text})"
            list_item = QListWidgetItem(item_text)
            list_item.setFlags(list_item.flags() | Qt.ItemIsUserCheckable)
            list_item.setCheckState(Qt.Unchecked)
            # Armazena a tupla original no item para referência posterior
            list_item.setData(Qt.UserRole, (original_name, file_path, sheet_name))
            self.list_widget.addItem(list_item)
        self.layout.addWidget(self.list_widget)
        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        self.layout.addWidget(button_box)
    
    def get_selected_to_split(self):
        """Retorna uma lista das tuplas de origem que foram marcadas
        para serem divididas."""
        to_split = []
        # all_items = self.list_widget.findItems("*", Qt.MatchWildcard)
        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i)
            is_checked = (item.checkState() == Qt.Checked)
            # print(f"Item {i} ('{item.text()}'): Marcado? {is_checked}")
            # if item.checkState() == Qt.Checked:
            if is_checked:
                to_split.append(item.data(Qt.UserRole))
            # print(f"--- DEBUG: Itens que serão retornados para divisão: {to_split} ---\n")
        return to_split

class HeaderMappingDialog(QDialog):
    def __init__(self, suggested_groups, parent=None, existing_mapping=None, existing_duplicate_keys=None):
        super().__init__(parent)
        self.setWindowTitle("Mapeamento e Agrupamento de Cabeçalhos")
        self.setMinimumSize(950, 600)

        self.groups = suggested_groups
        # O existing_mapping pode ser usado no futuro para pré-preencher, por enquanto simplificamos
        
        layout = QVBoxLayout(self)
        description_label = QLabel(
            "O algoritmo sugeriu os grupos abaixo. Confirme, divida ou mescle os grupos e defina o mapeamento final."
        )
        layout.addWidget(description_label)

        # --- Layout de Pesquisa ---
        search_layout = QHBoxLayout()
        search_layout.addWidget(QLabel("Pesquisar Grupo:"))
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Digite para filtrar por nome do grupo ou membros...")
        self.search_input.textChanged.connect(self.filter_table)
        search_layout.addWidget(self.search_input)
        layout.addLayout(search_layout)

        # --- Layout para botões de ação (com os novos botões) ---
        actions_layout = QHBoxLayout()
        self.split_group_button = QPushButton("Dividir Grupo...")
        self.split_group_button.clicked.connect(self.split_selected_group)
        self.merge_groups_button = QPushButton("Mesclar Grupos")
        self.merge_groups_button.clicked.connect(self.merge_selected_groups)
        self.batch_type_change_button = QPushButton("Alterar Tipo para Selecionados")
        self.batch_type_change_button.clicked.connect(self.change_type_for_selected)
        
        self.mark_all_button = QPushButton("Marcar Visíveis")
        self.mark_all_button.clicked.connect(lambda: self.mark_or_unmark_all_visible(check=True))
        self.unmark_all_button = QPushButton("Desmarcar Visíveis")
        self.unmark_all_button.clicked.connect(lambda: self.mark_or_unmark_all_visible(check=False))

        actions_layout.addWidget(self.split_group_button)
        actions_layout.addWidget(self.merge_groups_button)
        actions_layout.addWidget(self.batch_type_change_button)
        actions_layout.addStretch() # Espaçador
        actions_layout.addWidget(self.mark_all_button)
        actions_layout.addWidget(self.unmark_all_button)
        layout.addLayout(actions_layout)

        # --- Tabela Principal ---
        self.table_widget = QTableWidget()
        self.table_widget.setColumnCount(4)
        self.table_widget.setHorizontalHeaderLabels(["Grupo Sugerido / Cabeçalho", "Cabeçalho Final", 'Tipo de Dados', "Incluir?"])
        self.table_widget.setSelectionMode(QAbstractItemView.ExtendedSelection) # Permite selecionar múltiplas linhas
        
        self.populate_table() # Popula a tabela com os grupos iniciais

        self.table_widget.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.table_widget.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.table_widget.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.table_widget.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        layout.addWidget(self.table_widget)

        # --- Seção para Remoção de Duplicatas ---
        duplicates_box = QGroupBox("Remoção de Duplicatas (Opcional)")
        duplicates_layout = QVBoxLayout(duplicates_box)
        duplicates_layout.addWidget(QLabel("Selecione as colunas-chave para remover linhas duplicadas. Uma linha será removida se a combinação de valores nestas colunas já existir."))
        
        self.report_duplicates_checkbox = QCheckBox("Gerar aba com as linhas duplicadas que foram removidas.")
        self.report_duplicates_checkbox.setChecked(True)
        duplicates_layout.addWidget(self.report_duplicates_checkbox)

        self.duplicate_check_list = QListWidget()
        self.duplicate_check_list.setSelectionMode(QAbstractItemView.ExtendedSelection)
        # Popula a lista com os nomes finais sugeridos
        final_header_names = [group[0][0] for group in self.groups]
        self.duplicate_check_list.addItems(sorted(final_header_names))
        
        # Pré-seleciona chaves existentes, se houver
        if existing_duplicate_keys:
            for key_col in existing_duplicate_keys:
                items = self.duplicate_check_list.findItems(key_col, Qt.MatchExactly)
                if items:
                    items[0].setSelected(True)

        duplicates_layout.addWidget(self.duplicate_check_list)
        layout.addWidget(duplicates_box)

        # Botões OK e Cancelar
        self.button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)
        layout.addWidget(self.button_box)

    def get_duplicate_check_columns(self):
        """Retorna a lista de colunas selecionadas para a verificação de duplicatas."""
        return [item.text() for item in self.duplicate_check_list.selectedItems()]
    
    def get_duplicates_config(self):
        """
        Retorna a configuração completa para remoção de duplicatas.
        """
        key_columns = [item.text() for item in self.duplicate_check_list.selectedItems()]
        generate_report = self.report_duplicates_checkbox.isChecked()
        return {
            "key_columns": key_columns,
            "generate_report": generate_report
        }

    def populate_table(self):
        """Limpa e preenche a tabela com base na lista self.groups atual."""
        self.table_widget.setRowCount(0) # Limpa a tabela
        self.table_widget.setRowCount(len(self.groups))

        for row, group_list in enumerate(self.groups):
            # group_list é uma lista de tuplas de origem, ex: [('CNPJ', 'f1', 's1'), ...]
            
            # Coluna 0: Representação do Grupo
            # Usa o primeiro nome original do grupo como nome de exibição
            display_name = group_list[0][0]
            item_group = QTableWidgetItem(display_name)
            item_group.setFlags(item_group.flags() & ~Qt.ItemIsEditable)
            
            # Cria o texto do tooltip para mostrar todos os membros do grupo
            tooltip_text = "Membros do grupo:\n" + "\n".join(
                f"- '{orig_name}' (em {os.path.basename(path)}{f' | {s_name}' if s_name else ''})"
                for orig_name, path, s_name in group_list
            )
            item_group.setToolTip(tooltip_text)
            item_group.setData(Qt.UserRole, group_list) # Armazena os dados do grupo no item
            self.table_widget.setItem(row, 0, item_group)

            # Coluna 1: Cabeçalho Final (QLineEdit)
            final_name_edit = QLineEdit(display_name) # Sugere o nome do grupo como nome final
            self.table_widget.setCellWidget(row, 1, final_name_edit)

            # Coluna 2: Tipo de Dados (QComboBox)
            type_combo = QComboBox()
            type_combo.addItems(DATA_TYPES_OPTIONS)
            self.table_widget.setCellWidget(row, 2, type_combo)

            # Coluna 3: Incluir? (QCheckBox)
            checkbox, checkbox_widget = self._create_checkbox()
            checkbox.setChecked(True) # Incluir por padrão
            self.table_widget.setCellWidget(row, 3, checkbox_widget)

    def _create_checkbox(self):
        """Helper para criar um QCheckBox centralizado dentro de um widget."""
        checkbox_widget = QWidget()
        checkbox_layout = QHBoxLayout(checkbox_widget)
        checkbox = QCheckBox()
        checkbox_layout.addWidget(checkbox)
        checkbox_layout.setAlignment(Qt.AlignCenter)
        checkbox_layout.setContentsMargins(0, 0, 0, 0)
        return checkbox, checkbox_widget

    def filter_table(self, search_text):
        """Filtra as linhas da tabela com base no texto de pesquisa."""
        search_text_lower = search_text.lower().strip()

        for row in range(self.table_widget.rowCount()):
            group_item = self.table_widget.item(row, 0)
            if group_item:
                # Pesquisa no nome do grupo e no tooltip (que contém todos os membros)
                display_name = group_item.text().lower()
                tooltip_text = group_item.toolTip().lower()

                # A linha é visível se o texto de pesquisa estiver no nome ou no tooltip
                is_visible = (search_text_lower in display_name) or \
                             (search_text_lower in tooltip_text)

                self.table_widget.setRowHidden(row, not is_visible)

    def mark_or_unmark_all_visible(self, check=True):
        """Marca ou desmarca a caixa 'Incluir?' para todas as linhas VISÍVEIS na tabela."""
        for row in range(self.table_widget.rowCount()):
            # Aplica a alteração apenas se a linha não estiver oculta pela pesquisa
            if not self.table_widget.isRowHidden(row):
                checkbox_widget = self.table_widget.cellWidget(row, 3)
                if checkbox_widget:
                    checkbox = checkbox_widget.layout().itemAt(0).widget()
                    checkbox.setChecked(check)

    def change_type_for_selected(self):
        """Altera o tipo de dado para todas as linhas selecionadas na tabela."""
        selected_rows = sorted(list(set(index.row() for index in self.table_widget.selectedIndexes())))

        if not selected_rows:
            # Você pode adicionar um QMessageBox aqui para informar o usuário, se desejar
            return

        # Usa QInputDialog para pegar o novo tipo do usuário
        new_type, ok = QInputDialog.getItem(
            self,
            "Alterar Tipo de Dados em Lote",
            "Selecione o novo tipo de dados para os grupos selecionados:",
            DATA_TYPES_OPTIONS,  # A constante global com os tipos
            0,                   # Índice inicial
            False                # Não editável
        )

        if ok and new_type:
            for row in selected_rows:
                type_combo = self.table_widget.cellWidget(row, 2)
                if isinstance(type_combo, QComboBox):
                    type_combo.setCurrentText(new_type)

    def split_selected_group(self):
        """Abre o diálogo para dividir o grupo atualmente selecionado."""
        selected_rows = sorted(list(set(index.row() for index in self.table_widget.selectedIndexes())))
        if len(selected_rows) != 1:
            # Informar o usuário que apenas um grupo pode ser dividido por vez
            return

        row_to_split = selected_rows[0]
        group_item = self.table_widget.item(row_to_split, 0)
        group_data = group_item.data(Qt.UserRole)
        # print(f"DEBUG - Grupo para Dividir: {group_data}")

        if len(group_data) < 2: # Não pode dividir um grupo de 1
            return

        split_dialog = SplitGroupDialog(group_data, self)
        if split_dialog.exec() == QDialog.Accepted:
            items_to_move = split_dialog.get_selected_to_split()
            if not items_to_move: return

            # Atualizar self.groups: remover os itens do grupo antigo e criar um novo
            new_group = items_to_move
            self.groups.append(new_group)
            
            # Lista atualizada do grupo antigo
            updated_old_group = [item for item in group_data if item not in items_to_move]
            
            if not updated_old_group: # Se todos os itens foram movidos
                self.groups.pop(self.groups.index(group_data))
            else:
                self.groups[self.groups.index(group_data)] = updated_old_group
            
            self.populate_table() # Redesenha a tabela com os grupos atualizados

    def merge_selected_groups(self):
        """Mescla duas ou mais linhas (grupos) selecionadas em uma só."""
        selected_rows = sorted(list(set(index.row() for index in self.table_widget.selectedIndexes())), reverse=True)
        if len(selected_rows) < 2:
            # Informar o usuário que precisa selecionar pelo menos 2 grupos para mesclar
            return

        new_merged_group = []
        groups_to_remove = []
        for row in selected_rows:
            group_data = self.table_widget.item(row, 0).data(Qt.UserRole)
            new_merged_group.extend(group_data)
            groups_to_remove.append(group_data)
        
        # Remover os grupos antigos e adicionar o novo grupo mesclado
        for group in groups_to_remove:
            self.groups.remove(group)
        self.groups.insert(0, new_merged_group) # Adiciona no topo para fácil visualização

        self.populate_table() # Redesenha a tabela

    def get_mapping(self):
        """Lê a tabela e retorna o dicionário de mapeamento final."""
        final_mapping = {}
        for row in range(self.table_widget.rowCount()):
            # Obter os dados do grupo armazenados no item da Coluna 0
            group_data = self.table_widget.item(row, 0).data(Qt.UserRole)

            if not group_data:
                continue
            
            # Obter as configurações definidas pelo usuário para este grupo
            final_name = self.table_widget.cellWidget(row, 1).text().strip()
            type_str = self.table_widget.cellWidget(row, 2).currentText()
            include = self.table_widget.cellWidget(row, 3).layout().itemAt(0).widget().isChecked()

            if not final_name: # Fallback se o nome final for deixado em branco
                final_name = self.table_widget.item(row, 0).text()

            # "Desdobra" o grupo, aplicando a mesma regra a todos os seus membros
            # for original_name, _, _ in group_data:
            for source_tuple in group_data:
                final_mapping[source_tuple] = {
                    "final_name": final_name,
                    "type_str": type_str,
                    "include": include
                }
        return final_mapping

class HelpDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Guia do Usuário - DataFlow")
        self.setMinimumSize(800, 600)

        # --- Estrutura do Conteúdo da Ajuda ---
        # Chave: Título do Tópico, Valor: Conteúdo em HTML
        self.help_content = {
            "Visão Geral": """
                <h1>Bem-vindo ao DataFlow!</h1>
                <p>Esta ferramenta foi projetada para automatizar o processo de unificar múltiplos arquivos
                (Excel, CSV, TXT) em um único arquivo de saída, realizando limpeza e transformações inteligentes no processo.</p>
                <p>Navegue pelos tópicos à esquerda para aprender sobre cada funcionalidade.</p>
            """,
            "1. Seleção e Leitura": """
                <h2>1. Seleção de Pasta e Opções de Leitura</h2>
                <p><b>Selecionar Pasta:</b> O primeiro passo é sempre selecionar a pasta onde seus arquivos de dados estão localizados.</p>
                <p><b>Atualizar Pasta:</b> Se você adicionar ou remover arquivos da pasta com o programa aberto, ou também alterar o delimitador, clique no botão 'Atualizar' (com o ícone de recarregar) para que a lista de arquivos e delimitador sejam atualizados.</p>
                <p><b>Delimitador:</b> Para arquivos <b>.CSV</b> e <b>.TXT</b>, é crucial escolher o caractere que separa as colunas (delimitador). As opções mais comuns estão disponíveis, ou você pode especificar um customizado em 'Outro...'.</p>
                <br>
                <h3>Detecção Automática de Cabeçalho</h3>
                <p>A ferramenta detecta automaticamente em qual linha o cabeçalho se encontra, ignorando títulos ou linhas em branco no topo dos arquivos. Isso funciona tanto na pré-visualização quanto na consolidação final.</p>
            """,
            "2. Mapeamento de Cabeçalhos": """
                <h2>2. Mapeamento e Agrupamento Inteligente</h2>
                <p>Esta é a etapa mais poderosa da ferramenta.</p>
                <p><b>Análise Inteligente:</b> Ao clicar em 'Analisar/Mapear Cabeçalhos', o programa lê uma amostra de cada arquivo e cria uma "impressão digital" de cada coluna, analisando não apenas o nome, mas também o tipo de dado do conteúdo.</p>
                <p><b>Grupos Sugeridos:</b> Com base nessa análise, ele agrupa automaticamente colunas que parecem ser a mesma coisa, mesmo que tenham nomes diferentes (ex: 'CNPJ' e 'C.N.P.J.').</p>
                <p><b>Tooltip de Detalhes:</b> Passe o mouse sobre um nome na coluna 'Grupo Sugerido' para ver todas as variações originais que foram agrupadas ali.</p>
                <p><b>Sua Supervisão:</b> Você tem o controle final! Use os botões <b>'Dividir Grupo'</b> para separar colunas agrupadas incorretamente, e <b>'Mesclar Grupos'</b> para unir grupos que você sabe que são a mesma coisa.</p>
            """,
            "3. Filtros de Dados": """
                <h2>3. Filtros de Dados</h2>
                <p>A funcionalidade de filtro permite refinar os dados que serão incluídos na consolidação final.</p>
                <p><b>Lógica E / OU:</b> O sistema aplica uma lógica inteligente:</p>
                <ul>
                    <li>Regras para a <b>mesma coluna</b> são combinadas com <b>OU</b> (ex: `Nome = "João" OU Nome = "Maria"`).</li>
                    <li>Regras para <b>colunas diferentes</b> são combinadas com <b>E</b> (ex: `(Nome = "João") E (Status = "Ativo")`).</li>
                    <li>Regras de exclusão ('Não contém', 'Diferente de') são aplicadas após as de inclusão.</li>
                </ul>
                <p>Isso permite criar filtros complexos, como 'incluir todas as Compras e Vendas, mas não incluir as que forem Vendas de Ativos'.</p>
            """,
            "4. Consolidação e Saída": """
                <h2>4. Consolidação e Arquivo Final</h2>
                <p>Após definir o mapeamento e os filtros, escolha o local e o formato de saída (XLSX ou CSV) e clique em 'Iniciar Consolidação'.</p>
                <h3>Recursos da Saída em Excel</h3>
                <ul>
                    <li><b>Coluna 'Origem':</b> Uma coluna extra é adicionada ao final, indicando de qual arquivo e aba cada linha de dado veio.</li>
                    <li><b>Estilo Profissional:</b> O cabeçalho é formatado com um fundo escuro e texto claro.</li>
                    <li><b>Múltiplas Abas:</b> Se o resultado tiver mais de ~1 milhão de linhas, ele será automaticamente dividido em múltiplas abas ('Dados_Parte_1', 'Dados_Parte_2', etc.).</li>
                    <li><b>Formatação Adicional:</b> A planilha vem com painéis congelados, zoom ajustado e sem linhas de grade para uma melhor visualização.</li>
                </ul>
            """,
        }

        # --- Layout da Janela ---
        layout = QHBoxLayout(self)

        # Painel esquerdo com os tópicos
        self.topics_list = QListWidget()
        self.topics_list.setMaximumWidth(200)
        self.topics_list.addItems(self.help_content.keys())
        
        # Painel direito para exibir o conteúdo
        self.content_display = QTextEdit()
        self.content_display.setReadOnly(True)

        layout.addWidget(self.topics_list)
        layout.addWidget(self.content_display)
        
        # Conectar a seleção de tópico à exibição de conteúdo
        self.topics_list.currentItemChanged.connect(self.display_topic_content)
        # Selecionar o primeiro item por padrão
        self.topics_list.setCurrentRow(0)

    def display_topic_content(self, current_item, previous_item):
        """Exibe o conteúdo HTML do tópico selecionado."""
        if current_item:
            topic = current_item.text()
            self.content_display.setHtml(self.help_content.get(topic, "<p>Tópico não encontrado.</p>"))

class SheetSelectionDialog(QDialog):
    """Diálogo para seleção global de abas."""
    def __init__(self, unique_sheet_names, existing_rules=None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Seleção Global de Abas")
        self.setMinimumSize(500, 450)

        layout = QVBoxLayout(self)
        
        # --- Modo de Seleção (Incluir vs Excluir) ---
        mode_box = QGroupBox("Modo de Operação")
        mode_layout = QVBoxLayout()
        self.include_radio = QRadioButton("Incluir APENAS as abas marcadas abaixo")
        self.exclude_radio = QRadioButton("Excluir TODAS as abas marcadas abaixo")
        mode_layout.addWidget(self.include_radio)
        mode_layout.addWidget(self.exclude_radio)
        mode_box.setLayout(mode_layout)
        layout.addWidget(mode_box)
        
        # --- Lista de Abas ---
        list_box = QGroupBox("Abas Encontradas em Todos os Arquivos")
        list_layout = QVBoxLayout()
        
        self.list_widget = QListWidget()
        for name in sorted(list(unique_sheet_names)):
            item = QListWidgetItem(name)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setCheckState(Qt.Unchecked)
            self.list_widget.addItem(item)
        
        list_layout.addWidget(self.list_widget)

        # Botões de ação para a lista
        list_actions_layout = QHBoxLayout()
        mark_all_button = QPushButton("Marcar Todos")
        unmark_all_button = QPushButton("Desmarcar Todos")
        list_actions_layout.addStretch()
        list_actions_layout.addWidget(mark_all_button)
        list_actions_layout.addWidget(unmark_all_button)
        list_layout.addLayout(list_actions_layout)
        
        list_box.setLayout(list_layout)
        layout.addWidget(list_box)

        # --- Botões OK e Cancelar ---
        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel | QDialogButtonBox.Reset)
        button_box.button(QDialogButtonBox.Reset).setText("Limpar Regras")
        layout.addWidget(button_box)

        # --- Conectar Sinais ---
        mark_all_button.clicked.connect(lambda: self._set_all_check_state(Qt.Checked))
        unmark_all_button.clicked.connect(lambda: self._set_all_check_state(Qt.Unchecked))
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        button_box.button(QDialogButtonBox.Reset).clicked.connect(self.clear_rules)

        # --- Carregar regras existentes ---
        if existing_rules and existing_rules.get("names"):
            if existing_rules.get("mode") == "exclude":
                self.exclude_radio.setChecked(True)
            else:
                self.include_radio.setChecked(True)
            
            for i in range(self.list_widget.count()):
                item = self.list_widget.item(i)
                if item.text() in existing_rules["names"]:
                    item.setCheckState(Qt.Checked)
        else:
            # Estado padrão
            self.include_radio.setChecked(True)

    def _set_all_check_state(self, state):
        for i in range(self.list_widget.count()):
            self.list_widget.item(i).setCheckState(state)

    def clear_rules(self):
        """Limpa a seleção e reseta para o modo padrão."""
        self.include_radio.setChecked(True)
        self._set_all_check_state(Qt.Unchecked)
        # Opcional: pode fechar o diálogo ou apenas limpar a tela.
        # Por enquanto, apenas limpa a seleção.

    def get_rules(self):
        """Retorna as regras definidas pelo usuário."""
        mode = "exclude" if self.exclude_radio.isChecked() else "include"
        selected_names = set()
        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i)
            if item.checkState() == Qt.Checked:
                selected_names.add(item.text())
        
        # Se nenhuma aba for marcada, retorna uma regra vazia para desativar o filtro global
        if not selected_names:
            return {}
            
        return {"mode": mode, "names": selected_names}
