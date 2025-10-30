"""
DASHBOARD INTERATIVO COM STREAMLIT

Comando: streamlit run dashboard/app.py
"""

import sys
from pathlib import Path

# Adiciona raiz do projeto ao path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from sqlalchemy import create_engine, text

from config.settings import DB_CONNECTION_STRING, DB_SCHEMA, DB_TABLE

# CONFIGURAÇÃO DA PÁGINA
# - Configura aparência da página
# - Deve ser a PRIMEIRA chamada Streamlit
st.set_page_config(
    page_title="Portal da Transparência - Dashboard",
    layout="wide",  # 'centered' ou 'wide'
    initial_sidebar_state="expanded"  # Sidebar aberta por padrão
)

# FUNÇÕES DE CARREGAMENTO DE DADOS
@st.cache_data(ttl=300)  # Cache por 5 minutos (300 segundos)
def load_data_from_db():
    """
    Carrega dados do PostgreSQL.
    
    - Decorator que cacheia resultado da função
    - Se função for chamada de novo com mesmos parâmetros,
      retorna do cache (não executa de novo)
    - ttl=300 = cache expira após 5 minutos
    - IMPORTANTE: Acelera muito o dashboard!
    """
    try:
        # Conecta ao banco
        engine = create_engine(DB_CONNECTION_STRING)
        
        # Query SQL
        query = f"SELECT * FROM {DB_SCHEMA}.{DB_TABLE}"
        
        # Carrega dados
        # - Executa SQL e retorna DataFrame
        # - Mais simples que psycopg2 + conversão manual
        df = pd.read_sql(query, engine)
        
        # Fecha conexão
        engine.dispose()
        
        return df
        
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        return None


@st.cache_data(ttl=300)
def load_data_from_csv():
    """Carrega dados do CSV mais recente (fallback se banco falhar)."""
    try:
        from config.settings import DATA_PROCESSED_PATH
        
        # Busca CSVs processados
        csv_files = list(DATA_PROCESSED_PATH.glob("*_processed_*.csv"))
        
        if not csv_files:
            return None
        
        # Pega mais recente
        latest_file = max(csv_files, key=lambda x: x.stat().st_mtime)
        
        df = pd.read_csv(latest_file)
        
        return df
        
    except Exception as e:
        st.error(f"Erro ao carregar CSV: {e}")
        return None


def get_data():
    """
    Obtém dados (tenta banco, senão CSV).

    - Tenta primeira opção (banco)
    - Se falhar, tenta segunda (CSV)
    - Padrão: fallback/graceful degradation
    """
    # Tenta banco primeiro
    df = load_data_from_db()
    
    if df is not None and len(df) > 0:
        return df, "PostgreSQL"
    
    # Fallback para CSV
    df = load_data_from_csv()
    
    if df is not None and len(df) > 0:
        return df, "CSV"
    
    return None, None

# FUNÇÕES DE VISUALIZAÇÃO

def create_metrics_cards(df):
    """
    Cria cards com métricas principais.
    
    - Divide tela em colunas
    - col1, col2, col3 = st.columns(3)
    - Cada col é um container
    - Mostra métrica com label e valor
    - delta = variação (verde/vermelho)

    Cria 3 colunas:
    col1, col2, col3 = st.columns(3)
    É como dividir página em 3 partes iguais
    """
    col1, col2, col3 = st.columns(3)
    
    # MÉTRICA 1: Total de órgãos
    with col1:
        # with col1: = tudo dentro vai para col1
        st.metric(
            label="Total de Órgãos",
            value=len(df),
            delta=None  # Sem variação
        )
    
    # MÉTRICA 2: Códigos únicos
    with col2:
        unique_codes = df['codigo'].nunique()
        st.metric(
            label="Códigos Únicos",
            value=unique_codes
        )
    
    # MÉTRICA 3: Última atualização
    with col3:
        if 'data_extracao' in df.columns:
            # Converte para datetime se for string
            # -Converte string para datetime
            # - errors='coerce' = se falhar, vira NaT (Not a Time)
            df['data_extracao'] = pd.to_datetime(df['data_extracao'], errors='coerce')
            
            last_update = df['data_extracao'].max()
            
            if pd.notna(last_update):
                # Formata data
                # - Converte datetime para string formatada
                # - %d/%m/%Y %H:%M = dia/mês/ano hora:minuto
                formatted_date = last_update.strftime('%d/%m/%Y %H:%M')
                st.metric(
                    label="Última Atualização",
                    value=formatted_date
                )
            else:
                st.metric(label="Última Atualização", value="N/A")
        else:
            st.metric(label="Última Atualização", value="N/A")


def create_bar_chart(df):
    """
    Cria gráfico de barras com top N órgãos.

    - Biblioteca de gráficos interativos
    - Hover (passar mouse) mostra detalhes
    - Zoom, pan, download automáticos

    """
    st.subheader("Top 10 Órgãos por Código")
    
    # Conta frequência de cada código
    # - Conta quantas vezes cada valor aparece
    # - Retorna Series ordenada (maior -> menor)
    # - .head(10) pega top 10
    top_codes = df['codigo'].value_counts().head(10)
    
    # Cria DataFrame para o gráfico
    # - Transforma índice em coluna
    # - Series vira DataFrame
    chart_df = top_codes.reset_index()
    chart_df.columns = ['Código', 'Frequência']
    
    # Cria gráfico
    # - API simples para gráficos comuns
    # - px.bar() = gráfico de barras
    fig = px.bar(
        chart_df,
        x='Código',
        y='Frequência',
        title='Top 10 Códigos Mais Frequentes',
        color='Frequência',  # Cor baseada no valor
        color_continuous_scale='Blues'  # Escala de cores
    )
    
    # Customiza layout
    # - Modifica aparência do gráfico
    # - height, margin, font, etc.
    fig.update_layout(
        height=400,
        showlegend=False,
        xaxis_title="Código do Órgão",
        yaxis_title="Número de Ocorrências"
    )
    
    # Mostra gráfico
    # - Renderiza gráfico Plotly no Streamlit
    # - use_container_width=True = ocupa largura total
    st.plotly_chart(fig, use_container_width=True)


def create_data_table(df):
    """
    Cria tabela interativa com dados.
    - Mostra DataFrame interativo
    - Permite ordenação, scroll
    - height = altura em pixels
    """
    st.subheader("Dados Completos")
    
    # Colunas para mostrar
    display_columns = ['codigo', 'descricao']
    
    # Adiciona outras colunas se existirem
    if 'data_extracao' in df.columns:
        display_columns.append('data_extracao')
    
    if 'fonte' in df.columns:
        display_columns.append('fonte')
    
    # Filtra colunas existentes
    # Mantém apenas colunas que existem no DataFrame
    available_columns = [col for col in display_columns if col in df.columns]
    
    # Mostra tabela
    st.dataframe(
        df[available_columns],
        use_container_width=True,
        height=400
    )


def create_search_filter(df):
    """
    Cria filtro de busca por descrição.
    - Campo de texto
    - Retorna o que usuário digitou
    - Atualiza em tempo real
    """
    st.subheader("Buscar Órgão")
    
    # Campo de busca
    # - Cria campo de texto
    # - label = texto acima do campo
    # - value = valor padrão
    # - Retorna string digitada
    search_term = st.text_input(
        label="Digite parte da descrição do órgão:",
        value="",
        placeholder="Ex: Ministério, Secretaria, etc."
    )
    
    # Se usuário digitou algo
    if search_term:
        # Filtra DataFrame
        # - Verifica se string contém termo
        # - case=False = ignora maiúsculas/minúsculas
        # - na=False = considera NaN como False
        filtered_df = df[
            df['descricao'].str.contains(search_term, case=False, na=False)
        ]
        
        st.info(f"Encontrados: {len(filtered_df)} registros")
        
        return filtered_df
    
    return df

def create_download_button(df):
    """
    Cria botão para download dos dados.
    
    - Permite usuário baixar arquivo
    - data = conteúdo do arquivo
    - file_name = nome sugerido
    """
    # Converte DataFrame para CSV
    # - index=False = não inclui índice
    # - Retorna string CSV
    csv = df.to_csv(index=False)
    
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name=f"orgaos_siafi_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv"
    )


def show_statistics(df):
    """Mostra estatísticas descritivas."""
    st.subheader("Estatísticas")
    
    # Divide em 2 colunas
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Informações Gerais:**")
        st.write(f"- Total de registros: {len(df)}")
        st.write(f"- Colunas: {len(df.columns)}")
        st.write(f"- Memória: {df.memory_usage(deep=True).sum() / 1024:.2f} KB")
    
    with col2:
        st.write("**Qualidade dos Dados:**")
        
        # Calcula nulos
        null_count = df.isnull().sum().sum()
        null_pct = (null_count / df.size) * 100
        
        st.write(f"- Valores nulos: {null_count} ({null_pct:.2f}%)")
        
        # Calcula duplicatas
        dup_count = df.duplicated().sum()
        dup_pct = (dup_count / len(df)) * 100
        
        st.write(f"- Duplicatas: {dup_count} ({dup_pct:.2f}%)")

# SIDEBAR (BARRA LATERAL)

def create_sidebar():
    """
    Cria barra lateral com informações e controles.
    
    - Área lateral do dashboard
    - Boa para filtros, configs, info
    """
    with st.sidebar:
        st.title("Informações")
        
        st.markdown("""
        ### Portal da Transparência
        
        Dashboard de visualização dos órgãos SIAFI
        (Sistema Integrado de Administração Financeira).
        
        **Fonte de Dados:**
        - API Portal da Transparência
        - Governo Federal
        
        **Pipeline ETL:**
        1. Extração da API
        2. Transformação e limpeza
        3. Carga no PostgreSQL
        """)
        
        st.markdown("---")
        
        # Botão para atualizar dados
        # - Cria botão clicável
        # - Retorna True quando clicado
        if st.button("Atualizar Dados"):
            # Limpa cache
            # - Limpa todos os caches
            # - Força recarregamento
            st.cache_data.clear()
            
            # Recarrega página
            # - Executa script novamente do início
            # - Atualiza toda a página
            st.rerun()
        
        st.markdown("---")
        
        st.markdown("""
        ### Como usar
        
        1. **Métricas:** Visão geral no topo
        2. **Gráficos:** Análises visuais
        3. **Busca:** Filtro por descrição
        4. **Tabela:** Dados completos
        5. **Download:** Exportar para CSV
        
        ### Tecnologias
        
        - Python 3.11
        - Streamlit
        - Pandas
        - Plotly
        - PostgreSQL
        """)

# FUNÇÃO PRINCIPAL DO DASHBOARD

def main():
    """
    Função principal do dashboard.
    
    FLUXO:
    1. Cria sidebar
    2. Carrega dados
    3. Mostra visualizações
    """
    
    # Sidebar
    create_sidebar()
    
    # Título principal
    st.title("Dashboard - Portal da Transparência")
    st.markdown("### Órgãos SIAFI (Sistema Integrado de Administração Financeira)")
    
    # Carrega dados
    with st.spinner("Carregando dados..."):
        # CONCEITO ST.SPINNER:
        # - Mostra animação de loading
        # - while código executa
        df, source = get_data()
    
    # Verifica se carregou
    if df is None:
        st.error("Nenhum dado disponível!")
        st.info("""
        **Possíveis causas:**
        1. Pipeline ETL ainda não foi executado
        2. Banco de dados não está acessível
        3. Nenhum arquivo CSV encontrado
        
        **Solução:**
        Execute o pipeline: `python pipelines/run_full_pipeline.py`
        """)
        return
    
    # Mostra fonte dos dados
    st.success(f"Dados carregados de: **{source}**")
    
    # Linha separadora
    st.markdown("---")
    
    # Cards de métricas
    create_metrics_cards(df)
    
    st.markdown("---")
    
    # Gráfico de barras
    create_bar_chart(df)
    
    st.markdown("---")
    
    # Filtro de busca
    filtered_df = create_search_filter(df)
    
    st.markdown("---")
    
    # Tabela de dados
    create_data_table(filtered_df)
    
    st.markdown("---")
    
    # Estatísticas
    show_statistics(df)
    
    st.markdown("---")
    
    # Botão de download
    create_download_button(filtered_df)
    
    # Rodapé
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center'>
        <p>Dados: Portal da Transparência | Governo Federal</p>
        <p>Por: Yuri Pontes | Engenheiro de Dados</p>
    </div>
    """, unsafe_allow_html=True)

# EXECUÇÃO

if __name__ == "__main__":
    main()