"""
Este arquivo é destinado às configurações centralizadas do projeto.

Aqui ele carrega todas as configurações do .env
e disponibiliza para o resto da aplicação.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
"""
Aqui é carregado as variáveis do arquivo .env, procura o .env
na raiz do projeto.
"""

# Configurações da API:
API_URL = os.getenv("API_URL")
API_KEY = os.getenv("API_KEY") # "os.getenv("API_KEY")" é para puxar a chave no .env para não expor diretamente ao código

# URL completa do endpoint de orgãos SIAFI (API)
API_ORGAOS_URL = f"{API_URL}/orgaos-siafi"

# Configurações do banco de dados:
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

DB_SCHEMA = os.getenv("DB_SCHEMA", "public")
DB_TABLE = os.getenv("DB_TABLE", "orgaos_siafi")

# String de conexão para o SQLAlchemy:
DB_CONNECTION_STRING = (
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
    f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

# Configurações de paths.
# Diretório raiz do projeto:
PROJECT_ROOT = Path(__file__).parent.parent

# Caminhos dos dados:
DATA_RAW_PATH = PROJECT_ROOT / os.dotenv("DATA_RAW_PATH", data/raw)
DATA_PROCESSED_PATH = PROJECT_ROOT / os.getenv("DATA_PROCESSED_PATH", "data/processed")
LOG_PATH = PROJECT_ROOT / os.getenv("LOG_PATH", "data/logs")

# Para criar diretórios se não existirem:
for path in [DATA_RAW_PATH, DATA_PROCESSED_PATH, LOG_PATH]:
    path.mkdir(parents=True, exist_ok=True)

# Valodação (garante que as configs essenciais existem):
def validate_config():
    """
    "Fail fast": se config está errada, melhor falhar logo
    do que descobrir depois de processar dados.
    """
    errors = []
    if not API_URL:
        errors.append("API_URL não configurada.")

    if not DB_CONFIG['database']:
        errors.append("DB_NAME não configurado.")

    if not DB_CONFIG['password']:
        errors.append("DB_PASSWORD não configurado.")

    if errors:
        raise ValueError(
            "Configurações faltando:\n" + "\n".join(f"- {e}" for e in errors)
        )
    
# Valida ao importar o módulo:
validate_config()