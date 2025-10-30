# Executa APENAS a etapa de transformação.
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.transform import transform_data
from loguru import logger

logger.add(
    "data/logs/transform_{time}.log",
    rotation="1 day",
    retention="7 days",
    level="INFO"
)

def main():
    print("\n" + "="*60)
    print("PIPELINE ETL - ETAPA 2: TRANSFORMAÇÃO")
    print("="*60 + "\n")
    
    try:
        # Transforma dados
        df = transform_data()
        
        print("\n" + "="*60)
        print("TRANSFORMAÇÃO CONCLUÍDA COM SUCESSO!")
        print("="*60)
        print(f"Registros processados: {len(df)}")
        print(f"Arquivo salvo em: data/processed/")
        print("\nPróximo passo: python pipelines/run_load.py")
        print("="*60 + "\n")
        
        return 0
        
    except Exception as e:
        print("\n" + "="*60)
        print("ERRO NA TRANSFORMAÇÃO!")
        print("="*60)
        print(f"Erro: {e}")
        print("\nVerifique os logs em: data/logs/")
        print("="*60 + "\n")
        
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)