# Executa APENAS a etapa de carga.
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.load import load_to_postgres
from loguru import logger

logger.add(
    "data/logs/load_{time}.log",
    rotation="1 day",
    retention="7 days",
    level="INFO"
)

def main():
    print("\n" + "="*60)
    print("PIPELINE ETL - ETAPA 3: CARGA")
    print("="*60 + "\n")
    
    try:
        # Carrega no PostgreSQL
        success = load_to_postgres(mode='replace')
        
        if success:
            print("\n" + "="*60)
            print("CARGA CONCLUÍDA COM SUCESSO!")
            print("="*60)
            print("Dados carregados no PostgreSQL")
            print("\nVisualize os dados: streamlit run dashboard/app.py")
            print("="*60 + "\n")
            
            return 0
        else:
            raise Exception("Carga falhou")
        
    except Exception as e:
        print("\n" + "="*60)
        print("ERRO NA CARGA!")
        print("="*60)
        print(f"Erro: {e}")
        print("\nVerifique os logs em: data/logs/")
        print("="*60 + "\n")
        
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)