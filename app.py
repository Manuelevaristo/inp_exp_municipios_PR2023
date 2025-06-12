import streamlit as st
import pandas as pd

# --- Configuração da Página ---
st.set_page_config(
    layout="wide",
    page_title="Análise de Dados Municipais"
)

st.title("📊 Painel de Análise de Dados Municipais")

# --- Funções de Carregamento de Dados ---

@st.cache_data
def carregar_dados_comex():
    """Carrega os dados de importação e exportação de forma independente."""
    try:
        df_imp = pd.read_csv('data/IMP_MUN_PR_2023_limpo.csv', delimiter=';',encoding='latin1')
        df_exp = pd.read_csv('data/EXP_MUN_PR_2023_limpo.csv', delimiter=';',encoding='latin1')
        df_imp['CO_MUN'] = df_imp['CO_MUN'].astype(str)
        df_exp['CO_MUN'] = df_exp['CO_MUN'].astype(str)
        return df_imp, df_exp
    except FileNotFoundError:
        st.error("Erro: Verifique se os arquivos 'importacao.csv' e 'exportacao.csv' estão no diretório.")
        return None, None
    except Exception as e:
        st.error(f"Ocorreu um erro ao carregar os dados de Comércio Exterior: {e}")
        return None, None

@st.cache_data
def carregar_dados_vinculos():
    """Carrega os dados de vínculos de emprego."""
    try:
        df_vinculos = pd.read_csv('data/RAIS_ESTAB_PUB_PR_2023_limpo.csv', encoding='latin1', sep=';') # Assumindo ';' como separador
        # Limpeza e conversão de tipos
        df_vinculos['Qtd Vínculos Ativos'] = pd.to_numeric(df_vinculos['Qtd Vínculos Ativos'], errors='coerce').fillna(0)
        return df_vinculos
    except FileNotFoundError:
        st.error("Erro: Verifique se o arquivo 'vinculos.csv' está no diretório.")
        return None
    except Exception as e:
        st.error(f"Ocorreu um erro ao carregar os dados de Vínculos: {e}")
        return None


# --- Criação das Abas ---
tab_comex, tab_vinculos = st.tabs(["🌎 Comércio Exterior", "👨‍💼 Vínculos de Emprego"])

# --- Lógica da Aba 1: Comércio Exterior ---
with tab_comex:
    st.header("Consulta de Importações e Exportações por Município")
    df_importacao, df_exportacao = carregar_dados_comex()

    if df_importacao is not None and df_exportacao is not None:
        st.sidebar.header("Filtros de Comércio Exterior")
        tipo_filtro_comex = st.sidebar.radio(
            "Filtrar Comex por:",
            ('Nome do Município', 'Código do Município (CO_MUN)'),
            key='filtro_comex'
        )

        municipio_selecionado = None
        codigo_mun_selecionado = None
        
        if tipo_filtro_comex == 'Nome do Município':
            nomes_unicos = sorted(pd.concat([df_importacao['NOME_MUN'], df_exportacao['NOME_MUN']]).unique())
            municipio_selecionado = st.sidebar.selectbox(
                "Selecione o Município:",
                options=nomes_unicos,
                index=None,
                placeholder="Digite ou selecione um município...",
                key='sel_mun_comex'
            )
        else:
            codigo_mun_selecionado = st.sidebar.text_input(
                "Digite o Código do Município (CO_MUN):",
                placeholder="Ex: 3550308",
                key='cod_mun_comex'
            ).strip()

        if municipio_selecionado or codigo_mun_selecionado:
            nome_mun_display, cod_mun_display = "", ""
            if municipio_selecionado:
                nome_mun_display = municipio_selecionado
                if not df_importacao[df_importacao['NOME_MUN'] == municipio_selecionado].empty:
                    cod_mun_display = df_importacao[df_importacao['NOME_MUN'] == municipio_selecionado]['CO_MUN'].iloc[0]
                elif not df_exportacao[df_exportacao['NOME_MUN'] == municipio_selecionado].empty:
                    cod_mun_display = df_exportacao[df_exportacao['NOME_MUN'] == municipio_selecionado]['CO_MUN'].iloc[0]
            else:
                cod_mun_display = codigo_mun_selecionado
                if not df_importacao[df_importacao['CO_MUN'] == codigo_mun_selecionado].empty:
                    nome_mun_display = df_importacao[df_importacao['CO_MUN'] == codigo_mun_selecionado]['NOME_MUN'].iloc[0]
                elif not df_exportacao[df_exportacao['CO_MUN'] == codigo_mun_selecionado].empty:
                    nome_mun_display = df_exportacao[df_exportacao['CO_MUN'] == codigo_mun_selecionado]['NOME_MUN'].iloc[0]
            st.subheader(f"Resultados para: {nome_mun_display} ({cod_mun_display})")
            st.markdown("---")
            st.write("#### Dados de Importação")
            imp_filtrado = df_importacao[df_importacao['NOME_MUN' if municipio_selecionado else 'CO_MUN'] == (municipio_selecionado or codigo_mun_selecionado)]
            if not imp_filtrado.empty:
                st.metric("Total Importado (US$ FOB)", f"{imp_filtrado['VL_FOB'].sum():,.2f}")
                st.dataframe(imp_filtrado)
            else:
                st.info("Nenhum registro de importação encontrado.")
            st.markdown("---")
            st.write("#### Dados de Exportação")
            exp_filtrado = df_exportacao[df_exportacao['NOME_MUN' if municipio_selecionado else 'CO_MUN'] == (municipio_selecionado or codigo_mun_selecionado)]
            if not exp_filtrado.empty:
                st.metric("Total Exportado (US$ FOB)", f"{exp_filtrado['VL_FOB'].sum():,.2f}")
                st.dataframe(exp_filtrado)
            else:
                st.info("Nenhum registro de exportação encontrado.")
        else:
            st.info("⬅️ Utilize os filtros na barra lateral para buscar os dados de Comércio Exterior.")


# --- Lógica da Aba 2: Vínculos de Emprego ---
with tab_vinculos:
    st.header("Análise de Vínculos de Emprego por Atividade Econômica")
    df_vinculos = carregar_dados_vinculos()

    if df_vinculos is not None:
        dados_filtrados = df_vinculos.copy()

        # --- Filtros em cascata (Município e Classe) ---
        st.subheader("Filtros de Vínculos de Emprego")
        
        # MUDANÇA: Layout com 2 colunas para os filtros
        col1, col2 = st.columns(2)

        # Filtro 1: Município
        with col1:
            municipios_vinculos = sorted(df_vinculos['MUNICÍPIO'].dropna().unique())
            municipio_selecionado_vinculos = st.selectbox(
                "1. Filtre por Município:",
                options=municipios_vinculos,
                index=None,
                placeholder="Selecione o município"
            )

        if municipio_selecionado_vinculos:
            dados_filtrados = dados_filtrados[dados_filtrados['MUNICÍPIO'] == municipio_selecionado_vinculos]

        # Filtro 2: Classe CNAE
        with col2:
            if not dados_filtrados.empty and municipio_selecionado_vinculos:
                classes_cnae = sorted(dados_filtrados['CNAE 2.0 Classe'].dropna().unique())
                classe_selecionada = st.selectbox(
                    "2. Filtre por Classe CNAE:",
                    options=classes_cnae,
                    index=None,
                    placeholder="Todas as classes"
                )
                if classe_selecionada:
                    dados_filtrados = dados_filtrados[dados_filtrados['CNAE 2.0 Classe'] == classe_selecionada]
            else:
                st.selectbox("2. Filtre por Classe CNAE:", [], disabled=True)

        # REMOVIDO: Bloco do filtro de Subclasse foi completamente removido.

        st.markdown("---")
        
        # NOVO: Exibir a descrição da atividade selecionada
        if 'classe_selecionada' in locals() and classe_selecionada:
            descricao_atividade = dados_filtrados['DESCRICAO_CNAE'].iloc[0]
            st.info(f"**Descrição da Atividade:** {descricao_atividade}")

        # --- Exibição dos Resultados ---
        st.subheader("Resultados")

        # Variável Totalizada: Vínculos Ativos
        total_vinculos_ativos = dados_filtrados['Qtd Vínculos Ativos'].sum()
        st.metric(
            label=f"Total de Vínculos Ativos para a seleção",
            value=f"{int(total_vinculos_ativos):,}".replace(",", ".")
        )

        # Tabela com dados filtrados
        st.dataframe(dados_filtrados)

    else:
        st.warning("Não foi possível carregar os dados de vínculos para análise.")
