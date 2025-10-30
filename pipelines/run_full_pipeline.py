# Executa pipeline ETL completo: Extract + Transform + Load.
import sys
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.extract import extract_data
from src.transform import transform_data
from src.load import load_to_postgres
from loguru import logger

# Configura logging
logger.add(
    "data/logs/full_pipeline_{time}.log",
    rotation="1 day",
    retention="7 days",
    level="INFO"
)


def main():
    """
    Executa pipeline completo.
    
    Execução:
    1. Extract: API -> CSV
    2. Transform: CSV -> CSV limpo
    3. Load: CSV limpo -> PostgreSQL
    """
    
    start_time = datetime.now()
    
    print("\n" + "="*70)
    print("PIPELINE ETL COMPLETO - PORTAL DA TRANSPARÊNCIA")
    print("="*70)
    print(f"Início: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70 + "\n")
    
    try:
        # EXTRAÇÃO:
        print("ETAPA 1/3: EXTRAÇÃO")
        print("-" * 70)
        
        df_raw = extract_data()
        
        print(f"Extraído: {len(df_raw)} registros\n")
        
        # TRANSFORMAÇÃO
        print("ETAPA 2/3: TRANSFORMAÇÃO")
        print("-" * 70)
        
        df_clean = transform_data(df_raw)
        
        print(f"Transformado: {len(df_clean)} registros\n")
        
        # ETAPA 3: CARGA
        print("ETAPA 3/3: CARGA NO POSTGRESQL")
        print("-" * 70)
        
        success = load_to_postgres(df_clean, mode='replace')
        
        if not success:
            raise Exception("Falha na carga")
        
        print(f"Carregado: {len(df_clean)} registros\n")
        
        # RESUMO FINAL
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("="*70)
        print("PIPELINE CONCLUÍDO COM SUCESSO!")
        print("="*70)
        print(f"Resumo:")
        print(f"   • Registros extraídos:   {len(df_raw)}")
        print(f"   • Registros removidos:   {len(df_raw) - len(df_clean)}")
        print(f"   • Registros carregados:  {len(df_clean)}")
        print(f"   • Duração total:         {duration:.2f}s")
        print(f"   • Fim:                   {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("\nPróximos passos:")
        print("   1. Verifique dados no PostgreSQL")
        print("   2. Visualize no dashboard: streamlit run dashboard/app.py")
        print("="*70 + "\n")
        
        return 0
        
    except Exception as e:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "="*70)
        print("ERRO NO PIPELINE!")
        print("="*70)
        print(f"Erro: {e}")
        print(f"Duração até erro: {duration:.2f}s")
        print("\nVerifique os logs em: data/logs/")
        print("="*70 + "\n")
        
        logger.exception("Erro no pipeline completo")
        
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)