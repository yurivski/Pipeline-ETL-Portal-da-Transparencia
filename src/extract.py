"""
O objetivo deste arquivo é buscar dados da API e salvar em formato bruto.
"""
import requests
import pandas as pd
import json
from datetime import datetime
from pathlib import Path
from loguru import logger

# Importa as configurações "settings.py"
from config.settings import (
    API_URL,
    API_KEY,
    DATA_RAW_PATH
)

class APIExtractor:
    """
    Extrator de dados da API do Portal da Trasnparência.
    """
    def __init__(self):
        
        self.url = API_URL
        self.headers = self._build_headers()

        # Métricas da extração
        self.metrics = {
            "timestamp": None,
            "records_extracted": 0,
            "api_status_code": None,
            "execution_time_seconds": 0
        }

        logger.info(f"Extrator inicializado para: {self.url}")
    
    def _build_headers(self):
        """
        Monta headers HTTP para a requisição. Não é uma regra.
        Headers: Cabeçalho HTTP (metadados da requisição).
        User-Agent: Identifica quem está fazendo a requisição.
        chave-api-dados: Autenticação (se a API exigir).
        """
        headers = {
            "User-Agent": "ETL-Pipeline",
            "Accept": "application/json"
        }

        # Adiciona chave da API se existir:
        if API_KEY:
            headers["chave-api-dados"] = API_KEY
            logger.debug("API KEY configurada")

        return headers
    
    def extract(self):
        """
        Agora sim, extrai dados da API.
        1. Faz requisição HTTP para a API;
        2. Valida a resposta;
        3. Converte JSON para DataFrame;
        4. Salva arquivo bruto;
        5. Retorna DataFrame.
        """
        logger.info("="*60)
        logger.info("INICIANDO EXTRAÇÂO")
        logger.info("="*60)

        start_time = datetime.now()

        try:
            df = self._fetch_from_api() #Faz requisição para a API

            filename = self._save_raw_data(df) #Salva dados brutos
            
            #Atualiza métricas
            self.metrics["timestamp"] = start_time
            self.metrics["records_extracted"] = len(df)
            self.metrics["execution_time_seconds"] = (
                datetime.now() - start_time
            ).total_seconds()

            logger.info(f"Extração concluída: {len(df)} registros")
            logger.info(f"Arquivo salvo: {filename}")
            logger.info(f"Tempo: {self.metrics['execution_time_seconds']:.2f}s")

            return df
        
        except Exception as e:
            logger.info(f"Erro na extração: {e}")
            raise

    def _fetch_from_api(self):
        """
        Faz requisição HTTP para a API.

        Códigos:
        200: OK (sucesso)
        400: Bad Request (erro do cliente)
        404: Not Found (não encontrado)
        500: Server Error (erro do servidor)
        """

        logger.info(f"Requisição GET: {self.url}")

        #Aqui faz a requisição de fato
        response = requests.get(
            url=self.url,
            headers=self.headers,
            timeout=30 #Segundos
        )

        #Guarda status code
        self.metrics["api_status_code"] = response.status_code

        #Verifica se deu certo:
        if response.status_code != 200: #Código de sucesso
            logger.error(f"status_code: {response.status_code}")
            response.raise_for_status() #Lança exceção se der erro

        logger.info(f"Status: {response.status_code}")

        #Converte JSON para python (lista de dicionários)
        data = response.json()

        #Converte para DataFrame
        df = pd.DataFrame(data)

        logger.info(f"Dados recebidos: {len(df)} linhas, {len(df.columns)} colunas")
        logger.debug(f"Colunas: {list(df.columns)}")

        return df
    
    def _save_raw_data(self, df):
        """
        Salva dados brutos no arquivo.

        Formatos:
        CSV;
        JSON;
        Parquet.
        """

        # Cria timestamp para nome do arquivo:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Nomes dos arquivos:
        filename_csv = f"orgaos_siafi_{timestamp}.csv"
        filename_json = f"orgaos_siafi_{timestamp}.json"

        # Caminhos completos:
        filepath_csv = DATA_RAW_PATH / filename_csv
        filepath_json = DATA_RAW_PATH / filename_json

        # Salva CSV:
        df.to_csv(filepath_csv, index=False, encoding='utf-8')
        logger.debug(f"CSV salvo: {filepath_csv}")

        # Salva JSON:
        df.to_json(filepath_json, orient='records', indent=2, force_ascii=False)
        logger.debug(f"JSON salvo: {filepath_json}")

        #Salva também metadados da extração:
        metadata = {
            "extraction_date": datetime.now().isoformat(),
            "source_url": self.url,
            "records_count": len(df),
            "columns": list(df.columns),
            "metrics": self.metrics
        }

        metadata_file = DATA_RAW_PATH / f"metadata_{timestamp}.json"

        with open(metadata_file, 'w', enconding='utf-8') as f:
            json.dump(metadata, f, indent=2, default=str)

        return
    
def extract_data():
    """
    Função para extrair os dados
    """
    extractor = APIExtractor()
    return extractor.extract()
