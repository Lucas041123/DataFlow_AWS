import polars as pl
from PySide6.QtCore import QAbstractTableModel, Qt


class PolarsTableModel(QAbstractTableModel):
    def __init__(self, data=None):
        super().__init__()
        self._data = data if data is not None else pl.DataFrame()

    def rowCount(self, parent=None):
        return self._data.height

    def columnCount(self, parent=None):
        return self._data.width

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None
        if role == Qt.DisplayRole:
            try:
                # Polars pode retornar um único valor ou uma Series.
                # Se for Series (ao pegar uma célula), pegue o primeiro valor.
                value = self._data[index.row(), index.column()]
                if isinstance(value, pl.Series): # Acesso a célula pode retornar Series de 1 elemento
                    return str(value[0]) if value.len() > 0 else ""
                return str(value) # Converte para string para exibição
            except Exception:
                return "" # Em caso de erro ao acessar, retorna string vazia
        return None

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal: # Cabeçalhos das colunas
                return str(self._data.columns[section])
            if orientation == Qt.Vertical: # Cabeçalhos das linhas (números)
                return str(section + 1)
        return None

    def load_data(self, new_data: pl.DataFrame):
        self.beginResetModel()
        self._data = new_data if new_data is not None else pl.DataFrame()
        self.endResetModel()

    def clear_data(self):
        self.load_data(pl.DataFrame())
