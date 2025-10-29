# carregar os dados limpos no banco de dados (PostgreSQL)
import pandas as pd
from datetime import datetime
from pathlib import Path
import psycopg2
from sqlalchemy import create_engine, text, inspect
from loguru import logger

from config.settings import (
    DB_CONNECTION_STRING,
    DB_CONFIG,
    DB_SCHEMA,
    DB_TABLE,
    DATA_PROCESSED_PATH
)


class PostgresLoader:
    # Carregador de dados para PostgreSQL:
    def __init__(self):
        """Inicializa loader."""
        self.engine = None
        self.connection = None
        
        self.metrics = {
            "rows_loaded": 0,
            "load_time_seconds": 0,
            "table_created": False
        }
        
        logger.info("Loader inicializado")
    
    def load(self, df=None, mode='replace'):
        """
        Carrega dados no PostgreSQL:
        
        - Apaga a tabela e cria nova (TRUNCATE + INSERT)
        - Adicionar no fim (INSERT)
        - Atualizar se existir, insere se não existir (ON CONFLICT UPDATE)
        """
        logger.info("="*60)
        logger.info("INICIANDO CARGA NO POSTGRESQL")
        logger.info("="*60)
        
        start_time = datetime.now()
        
        try:
            # Conecta ao banco
            self._connect()
            
            # Carrega dados se não fornecidos
            if df is None:
                df = self._load_latest_processed_data()
            
            logger.info(f"Carregando {len(df)} registros")
            
            # Remove colunas de metadados internos
            df = self._clean_dataframe(df)
            
            # Cria tabela se não existir
            if not self._table_exists():
                self._create_table(df)
                self.metrics["table_created"] = True
            
            # Carrega dados conforme modo
            if mode == 'replace':
                self._load_replace(df)
            elif mode == 'append':
                self._load_append(df)
            elif mode == 'upsert':
                self._load_upsert(df)
            else:
                raise ValueError(f"Modo inválido: {mode}")
            
            # Métricas
            self.metrics["rows_loaded"] = len(df)
            self.metrics["load_time_seconds"] = (
                datetime.now() - start_time
            ).total_seconds()
            
            logger.info(f"Carga concluída: {len(df)} registros")
            logger.info(f"Tempo: {self.metrics['load_time_seconds']:.2f}s")
            logger.info(f"Tabela: {DB_SCHEMA}.{DB_TABLE}")
            
            return True
            
        except Exception as e:
            logger.error(f"Erro na carga: {e}")
            
            # Rollback se em transação
            if self.connection:
                try:
                    self.connection.rollback()
                    logger.info("Rollback executado")
                except:
                    pass
            
            return False
            
        finally:
            # Sempre fecha conexões
            self._disconnect()
    
    def _connect(self):
        #conecta ao PostgreSQL:
        try:
            logger.info("Conectando ao PostgreSQL...")
            
            # Engine SQLAlchemy (para pandas)
            self.engine = create_engine(
                DB_CONNECTION_STRING,
                pool_size=5,  # Mantém até 5 conexões abertas
                max_overflow=10,  # Permite mais 10 temporárias
                pool_pre_ping=True  # Testa conexão antes de usar
            )
            
            # Conexão psycopg2 (para SQL customizado)
            self.connection = psycopg2.connect(**DB_CONFIG)
            self.connection.autocommit = False  # Controle manual de transações
            
            logger.info("Conexão estabelecida")
            
            # Testa conexão
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT version()"))
                version = result.fetchone()[0]
                logger.debug(f"PostgreSQL: {version[:50]}...")
            
        except Exception as e:
            logger.error(f"Erro ao conectar: {e}")
            raise
    
    def _disconnect(self):
        """Fecha conexões."""
        if self.connection and not self.connection.closed:
            self.connection.close()
            logger.debug("Conexão psycopg2 fechada")
        
        if self.engine:
            self.engine.dispose()
            logger.debug("Engine SQLAlchemy fechada")
    
    def _load_latest_processed_data(self):
        # Para carregar arquivo processado mais recente.
        path = DATA_PROCESSED_PATH
        
        # Busca CSVs processados
        csv_files = list(path.glob("*_processed_*.csv"))
        
        if not csv_files:
            raise FileNotFoundError(f"Nenhum arquivo processado em {path}")
        
        # Pega mais recente
        latest_file = max(csv_files, key=lambda x: x.stat().st_mtime)
        
        logger.info(f"Carregando: {latest_file.name}")
        
        df = pd.read_csv(latest_file)
        
        logger.info(f"Arquivo carregado: {len(df)} linhas")
        
        return df
    
    def _clean_dataframe(self, df):
        # Para remover as colunas internas não necessárias no banco.
        internal_cols = [col for col in df.columns if col.startswith('_')] # Colunas que começam com _ são internas
        
        if internal_cols:
            df = df.drop(columns=internal_cols)
            logger.debug(f"Colunas internas removidas: {internal_cols}")
        
        return df
    
    def _table_exists(self):
        """
        Verifica se tabela existe.
        - inspect(engine) = objeto que inspeciona banco
        - has_table() = verifica existência de tabela
        """
        inspector = inspect(self.engine)
        exists = inspector.has_table(DB_TABLE, schema=DB_SCHEMA)
        
        if exists:
            logger.info(f"Tabela {DB_SCHEMA}.{DB_TABLE} existe")
        else:
            logger.info(f"Tabela {DB_SCHEMA}.{DB_TABLE} não existe")
        
        return exists
    
    def _create_table(self, df):
        # Cria a tabela baseado no DataFrame.
        logger.info(f"Criando tabela {DB_SCHEMA}.{DB_TABLE}")
        
        # Cria schema se não existir
        with self.connection.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
            self.connection.commit()
        
        # Cria tabela (pandas infere tipos)
        df.head(0).to_sql(
            name=DB_TABLE,
            con=self.engine,
            schema=DB_SCHEMA,
            if_exists='replace',
            index=False
        )
        
        logger.info(f"Tabela {DB_SCHEMA}.{DB_TABLE} criada")
    
    def _load_replace(self, df):
        """
        Carga com substituição total.
        
        - TRUNCATE = apaga todos os dados (mais rápido que DELETE)
        - CASCADE = apaga também em tabelas relacionadas
        """
        logger.info("Modo REPLACE: truncando tabela")
        
        # Trunca tabela (apaga dados)
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"TRUNCATE TABLE {DB_SCHEMA}.{DB_TABLE} CASCADE"
            )
            self.connection.commit()
        
        logger.info("Tabela truncada")
        
        # Insere novos dados
        self._load_append(df)
    
    def _load_append(self, df):
        """Carga por append (INSERT)."""
        logger.info("Modo APPEND: inserindo dados")
        
        # Insere em lotes
        batch_size = 1000
        total_batches = (len(df) + batch_size - 1) // batch_size
        
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            
            logger.debug(f"Lote {batch_num}/{total_batches}: {len(batch)} registros")
            
            batch.to_sql(
                name=DB_TABLE,
                con=self.engine,
                schema=DB_SCHEMA,
                if_exists='append',
                index=False,
                method='multi'  # Múltiplas linhas por INSERT
            )
        
        logger.info(f"{len(df)} registros inseridos")
    
    def _load_upsert(self, df):
        """
        Carga com upsert (INSERT ... ON CONFLICT UPDATE).
        
        - Tenta INSERT
        - Se conflito (chave já existe): UPDATE
        - PostgreSQL: INSERT ... ON CONFLICT DO UPDATE
        - Precisa ter constraint UNIQUE ou PRIMARY KEY
        - Aqui usei 'codigo' como chave
        """
        logger.info("Modo UPSERT: inserindo/atualizando")
        
        # Colunas do DataFrame
        columns = list(df.columns)
        
        # Monta SQL manualmente
        cols_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        
        # Colunas para UPDATE (todas menos a chave)
        update_cols = [col for col in columns if col != 'codigo']
        update_str = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])
        
        # SQL completo
        sql = f"""
            INSERT INTO {DB_SCHEMA}.{DB_TABLE} ({cols_str})
            VALUES ({placeholders})
            ON CONFLICT (codigo) 
            DO UPDATE SET {update_str}
        """
        
        # Executa em lotes
        with self.connection.cursor() as cursor:
            batch_size = 1000
            
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i+batch_size]
                
                # Converte para lista de tuplas
                values = [tuple(row) for row in batch.values]
                
                """
                Executa upsert para cada linha e
                executa SQL múltiplas vezes com diferentes valores
                """
                cursor.executemany(sql, values)
            
            # Commit da transação
            self.connection.commit()
        
        logger.info(f"{len(df)} registros processados (upsert)")

def load_to_postgres(df=None, mode='replace'):
    # Função para carregar os dados
    loader = PostgresLoader()
    return loader.load(df, mode)
