import polars as pl
import re
from unidecode import unidecode
from collections import Counter
from enum import Enum

# Definir os tipos de dados que o usuário pode escolher
DATA_TYPES_OPTIONS = ["Automático/String", "Inteiro", "Decimal (Float)", "Data", "Booleano"]
# Mapeamento para tipos Polars (pode ser um dict global ou dentro do worker)
TYPE_STRING_TO_POLARS = {
    "Automático/String": pl.String,
    "Inteiro": pl.Int64,
    "Decimal (Float)": pl.Float64,
    "Data": pl.Date, # ou pl.Datetime se precisar de hora
    "Booleano": pl.Boolean
}
# --- Constantes para as Opções de Filtro ---
OPERATOR_OPTIONS = [
    "Igual a",
    "Diferente de",
    "Contém",
    "Não contém",
    "Começa com",
    "Termina com",
    "Maior que",
    "Menor que",
    "Entre",
    "Está em branco",
    "Não está em branco",
]
# Operadores que não precisam de um campo de valor
OPERATORS_NO_VALUE = {"Está em branco", "Não está em branco"}

CONFIG_FILE_NAME = "config_consolidador.json" # Nome do arquivo de configuração

def _normalize_header_name(header_name: str) -> str:
    if not isinstance(header_name, str):
        header_name = str(header_name)
    
    # 1. Remove acentos (ex: "Endereço" -> "Endereco")
    text = unidecode(header_name)
    # 2. Converte para minúsculas
    text = text.lower()
    # 3. Substitui separadores comuns por espaço (para depois lidar com "valorICMS" vs "valor_ICMS")
    text = re.sub(r'[._-]+', ' ', text)
    # 4. Adiciona espaço antes de letras maiúsculas (camelCase -> camel Case)
    text = re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', text)
    # 5. Remove todos os caracteres não alfanuméricos e junta tudo
    text = re.sub(r'[^a-z0-9]', '', text)
    return text

def _find_header_row_index(df_sample: pl.DataFrame, max_rows_to_check: int = 20) -> int:
    """
    Analisa as primeiras N linhas de um DataFrame e retorna o índice da linha
    que tem a maior probabilidade de ser o cabeçalho, usando uma heurística de
    transição de tipos.
    """
    best_score = -999
    header_row_index = 0
    rows_to_check = min(df_sample.height, max_rows_to_check)

    # Nós só podemos checar até a penúltima linha, pois sempre olhamos para a linha seguinte
    for i in range(rows_to_check - 1):
        row_current = df_sample.row(i)
        row_next = df_sample.row(i + 1)
        
        # --- Cálculo da Pontuação da Linha Atual (baseado na lógica anterior) ---
        base_score = 0
        string_count = 0
        null_count = 0
        numeric_string_count = 0
        
        for value in row_current:
            if value is None: null_count += 1
            elif isinstance(value, str) and value.strip():
                string_count += 1
                if value.strip().isnumeric(): numeric_string_count += 1
        
        num_cells = len(row_current)
        non_null_cells = num_cells - null_count

        if non_null_cells == 0:
            base_score = -100
        else:
            uniqueness_ratio = len(set(v for v in row_current if v is not None)) / non_null_cells
            string_ratio = string_count / non_null_cells
            numeric_string_ratio = numeric_string_count / non_null_cells
            base_score += uniqueness_ratio * 3
            base_score += string_ratio * 3
            base_score -= numeric_string_ratio * 5
            base_score -= (null_count / num_cells) * 2

        # --- NOVA HEURÍSTICA: Pontuação por Transição de Tipos ---
        transition_score = 0
        for j in range(num_cells):
            type_current = type(row_current[j])
            type_next = type(row_next[j])
            
            # Recompensa fortemente a transição de Texto para Número
            if type_current is str and type_next in (int, float):
                transition_score += 1
            # Penaliza levemente se os tipos são iguais (exceto None)
            elif type_current is not None and type_current == type_next:
                transition_score -= 0.2

        transition_bonus = (transition_score / num_cells) * 5 # Bônus forte
        
        # Combina as pontuações
        final_score = base_score + transition_bonus
        
        if final_score > best_score:
            best_score = final_score
            header_row_index = i
            
    return header_row_index

def _make_headers_unique(header_names: list) -> list:
    """
    Garante que todos os nomes de cabeçalho em uma lista sejam únicos
    """
    counts = Counter(header_names)
    duplicates = {name for name, count in counts.items() if count > 1}
    if not duplicates:
            return header_names
    new_headers = []
    running_counts = Counter()
    for name in header_names:
        if name in duplicates:
            running_counts[name] += 1
            new_headers.append(f"{name}_{running_counts[name]}")
        else:
            new_headers.append(name)
    return new_headers

class LogLevel(Enum):
    INFO = "[INFO]"
    WARNING = "[AVISO]"
    ERROR = "[ERRO]"
    SUCCESS = "[SUCESSO]"