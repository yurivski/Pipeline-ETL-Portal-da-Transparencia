# Limpeza, validação e enriquecimento de dados
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from loguru import logger

#importação das pastas:
from config.settings import DATA_RAW_PATH, DATA_PROCESSED_PATH


class DataTransformer:
    """
    Transformação de dados:
    1. Remover duplicatas;
    2. Tratar valores nulos;
    3. Padronizar tipos de dados;
    4. Validar qualidade;
    5. Adicionar metadados.
    """
    
    def __init__(self):
        """Inicializa transformador."""
        self.metrics = {
            "rows_input": 0,
            "rows_output": 0,
            "rows_removed": 0,
            "null_values_found": 0,
            "duplicates_found": 0
        }
        
        logger.info("Transformador inicializado")
    
    def transform(self, df=None): # Transforma DataFrame.
        logger.info("="*60)
        logger.info("INICIANDO TRANSFORMAÇÃO")
        logger.info("="*60)
        
        try:
            # Carrega dados se não fornecidos
            if df is None:
                df = self._load_latest_raw_data()
            
            self.metrics["rows_input"] = len(df)
            logger.info(f"Input: {len(df)} linhas, {len(df.columns)} colunas")
            
            # Aplica transformações
            df = self._remove_duplicates(df)
            df = self._handle_nulls(df)
            df = self._standardize_types(df)
            df = self._add_metadata(df)
            df = self._validate_quality(df)
            
            # Métricas finais
            self.metrics["rows_output"] = len(df)
            self.metrics["rows_removed"] = (
                self.metrics["rows_input"] - self.metrics["rows_output"]
            )
            
            # Salva dados transformados
            filename = self._save_processed_data(df)
            
            logger.info(f"Transformação concluída: {len(df)} registros")
            logger.info(f"Removidos: {self.metrics['rows_removed']} registros")
            logger.info(f"Arquivo salvo: {filename}")
            
            return df
            
        except Exception as e:
            logger.error(f"Erro na transformação: {e}")
            raise
    
    def _load_latest_raw_data(self):
        # Carrega o arquivo bruto mais recente.

        path = DATA_RAW_PATH
        
        # Busca todos os CSVs
        csv_files = list(path.glob("*.csv"))
        
        if not csv_files:
            raise FileNotFoundError(f"Nenhum arquivo CSV em {path}")
        
        # Pega o mais recente (maior data de modificação):
        latest_file = max(csv_files, key=lambda x: x.stat().st_mtime)
        
        logger.info(f"Carregando: {latest_file.name}")
        
        df = pd.read_csv(latest_file)
        
        logger.info(f"Arquivo carregado: {len(df)} linhas")
        
        return df
    
    def _remove_duplicates(self, df):
        # Remove linhas duplicadas:
        rows_before = len(df)
        
        # Conta duplicatas
        duplicates = df.duplicated().sum()
        self.metrics["duplicates_found"] = duplicates
        
        if duplicates > 0:
            logger.warning(f"Duplicatas encontradas: {duplicates}")
            
            # Remove duplicatas (mantém primeira ocorrência)
            df = df.drop_duplicates(keep='first')
            
            rows_removed = rows_before - len(df)
            logger.info(f"Duplicatas removidas: {rows_removed}")
        else:
            logger.info("Nenhuma duplicata encontrada")
        
        return df
    
    def _handle_nulls(self, df):
        """
        Trata valores nulos;
        Conta nulos totais.
        """
        null_count = df.isnull().sum().sum()
        self.metrics["null_values_found"] = null_count
        
        if null_count > 0:
            logger.warning(f"Valores nulos encontrados: {null_count}")
            
            # Verifica nulos por coluna
            null_by_column = df.isnull().sum()
            
            # Mostra colunas com nulos
            columns_with_nulls = [
                col for col in df.columns 
                if null_by_column[col] > 0
            ]

            for col in columns_with_nulls:
                logger.debug(f"   {col}: {null_by_column[col]} nulos")
            
            # Remove linhas com nulos em colunas
            critical_columns = ['codigo', 'descricao']
            
            df = df.dropna(subset=critical_columns)
            
            logger.info("Nulos em colunas críticas removidos")
        else:
            logger.info("Nenhum valor nulo encontrado")
        
        return df
    
    def _standardize_types(self, df):
        """
        Padroniza tipos de dados e converte o tipo de uma coluna
        """
        logger.info("Padronizando tipos de dados")
        
        # Converte codigo para string (pode ter zeros à esquerda)
        if 'codigo' in df.columns:
            df['codigo'] = df['codigo'].astype(str)
        
        # Converte descricao para string
        if 'descricao' in df.columns:
            df['descricao'] = df['descricao'].astype(str)
        
        # Remove espaços extras
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
        
        logger.info("✅ Tipos padronizados")
        
        return df
    
    def _add_metadata(self, df):

        # Adiciona colunas de metadados.
        logger.info("Adicionando metadados")
        
        df['data_extracao'] = datetime.now()
        df['fonte'] = 'Portal da Transparência'
        df['versao_pipeline'] = '1.0.0'
        
        logger.info("Metadados adicionados")
        
        return df
    
    def _validate_quality(self, df):
        """
        Valida a qualidade dos dados:
        - assert condição, "mensagem"
        - Se condição é False, lança erro com mensagem
        - É uma validação forte (quebra execução)
        """
        logger.info("Validando qualidade dos dados")
        
        # Validações críticas
        assert len(df) > 0, "DataFrame vazio!"
        assert 'codigo' in df.columns, "Coluna 'codigo' não encontrada!"
        assert 'descricao' in df.columns, "Coluna 'descricao' não encontrada!"
        
        # Validações com warning
        if df['codigo'].duplicated().any():
            logger.warning("Códigos duplicados encontrados!")
        
        # Estatísticas
        logger.info(f"Registros válidos: {len(df)}")
        logger.info(f"Colunas: {len(df.columns)}")
        
        logger.info("Validação concluída")
        
        return df
    
    def _save_processed_data(self, df):
        # Salva dados processados:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"orgaos_siafi_processed_{timestamp}.csv"
        filepath = DATA_PROCESSED_PATH / filename
        
        df.to_csv(filepath, index=False, encoding='utf-8')
        
        logger.debug(f"💾 Arquivo salvo: {filepath}")
        
        return filename

def transform_data(df=None):
    # Função para transformar os dados 
    transformer = DataTransformer()
    return transformer.transform(df)