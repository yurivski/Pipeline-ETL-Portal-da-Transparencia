[![Licença: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

---

# Projeto sendo construído...

## <a name="about"></a> Project Structure (Estrutura do Projeto):


```bash
pipeline-etl-portal-da-transparencia/
│
├── .env                          # Credenciais originais
├── .env.example                  # Exemplo de credenciais
├── .gitignore                    # Arquivos sencíveis que o Git deve ignorar
├── requirements.txt              # Bibliotecas para instalação
├── README.md                     # Documentação do projeto (Ainda sendo escrita...)
│
├── config/
│   └── settings.py               # Configurações centralizadas
│
├── data/                         # Dados salvos (ignore esta pasta no Git)
│   ├── raw/                      # Dados brutos da API
│   ├── processed/                # Dados limpos
│   └── logs/                     # Logs de execução
│
├── src/                          # Código-fonte principal
│   ├── __init__.py               # Torna 'src' um pacote Python
│   ├── extract.py                # Etapa 1: Extração
│   ├── transform.py              # Etapa 2: Transformação
│   └── load.py                   # Etapa 3: Carga
│
├── pipelines/
│   ├── run_extract.py            # Executa só extração
│   ├── run_transform.py          # Executa só transformação
│   ├── run_load.py               # Executa só carga
│   └── run_full_pipeline.py      # Executa tudo junto
│
├── dashboard/
    └── app.py                    # Dashboard Streamlit
