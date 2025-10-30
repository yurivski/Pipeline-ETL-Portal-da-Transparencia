# Executa APENAS a etapa de extração.
import sys
from pathlib import Path

# Adiciona diretório raiz ao path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.extract import extract_data
from loguru import logger

# Configura logging
logger.add(
    "data/logs/extract_{time}.log",
    rotation="1 day",
    retention="7 days",
    level="INFO"
)


def main():
    """Função principal."""
    print("\n" + "="*60)
    print("PIPELINE ETL - ETAPA 1: EXTRAÇÃO")
    print("="*60 + "\n")
    
    try:
        # Extrai dados
        df = extract_data()
        
        print("\n" + "="*60)
        print("EXTRAÇÃO CONCLUÍDA COM SUCESSO!")
        print("="*60)
        print(f"Registros extraídos: {len(df)}")
        print(f"Arquivo salvo em: data/raw/")
        print("\nPróximo passo: python pipelines/run_transform.py")
        print("="*60 + "\n")
        
        return 0  # Código de sucesso
        
    except Exception as e:
        print("\n" + "="*60)
        print("ERRO NA EXTRAÇÃO!")
        print("="*60)
        print(f"Erro: {e}")
        print("\nVerifique os logs em: data/logs/")
        print("="*60 + "\n")
        
        return 1  # Código de erro


if __name__ == "__main__":
    """Ponto de entrada do script."""
    exit_code = main()
    sys.exit(exit_code)
