import streamlit as st
import pandas as pd

# --- Configuração da Página ---
st.set_page_config(
    layout="wide",
    page_title="Análise de Comércio Exterior por Município"
)

st.title("🔎 Consulta de Importações e Exportações por Município-PR")

# --- Função para Carregar os Dados ---
# O decorador @st.cache_data garante que os dados sejam carregados apenas uma vez.
@st.cache_data
def carregar_dados():
    """
    Carrega os dados de importação e exportação de forma independente
    a partir de arquivos CSV.
    Retorna dois DataFrames separados.
    """
    try:
        # Tenta carregar os arquivos com codificação 'latin1'.
        df_imp = pd.read_csv('/home/manuel-finda/Documentos/HUB de IA/projetos/filtro/data/IMP_MUN_PR_2023_limpo.csv', delimiter=';',encoding='latin1')
        df_exp = pd.read_csv('/home/manuel-finda/Documentos/HUB de IA/projetos/filtro/data/EXP_MUN_PR_2023_limpo.csv', delimiter=';',encoding='latin1')

        # Converte a coluna de código do município para string para evitar problemas de formatação.
        if 'CO_MUN' in df_imp.columns:
            df_imp['CO_MUN'] = df_imp['CO_MUN'].astype(str)
        if 'CO_MUN' in df_exp.columns:
            df_exp['CO_MUN'] = df_exp['CO_MUN'].astype(str)

                # Converte a coluna de código do município para string para evitar problemas de formatação.
        if 'VL_FOB' in df_imp.columns:
            df_imp['VL_FOB'] = df_imp['VL_FOB'].astype(float)
        if 'VL_FOB' in df_exp.columns:
            df_exp['VL_FOB'] = df_exp['VL_FOB'].astype(float)

        if 'KG_LIQUIDO' in df_imp.columns:
            df_imp['KG_LIQUIDO'] = df_imp['KG_LIQUIDO'].astype(float)
        if 'KG_LIQUIDO' in df_exp.columns:
            df_exp['KG_LIQUIDO'] = df_exp['KG_LIQUIDO'].astype(float)

        return df_imp, df_exp
    except FileNotFoundError:
        st.error("Erro: Verifique se os arquivos 'importacao.csv' e 'exportacao.csv' estão no mesmo diretório.")
        return None, None
    except Exception as e:
        st.error(f"Ocorreu um erro ao carregar os dados: {e}")
        return None, None

# --- Carregamento dos Dados ---
df_importacao, df_exportacao = carregar_dados()

# A aplicação continua apenas se os dados forem carregados com sucesso.
if df_importacao is not None and df_exportacao is not None:

    # --- Barra Lateral de Filtros ---
    st.sidebar.header("Opções de Filtro")

    # Opção para o usuário escolher o tipo de filtro.
    tipo_filtro = st.sidebar.radio(
        "Filtrar por:",
        ('Nome do Município', 'Código do Município (CO_MUN)')
    )

    municipio_selecionado = None
    codigo_mun_selecionado = None
    nome_mun_display = ""
    cod_mun_display = ""


    # --- Lógica de Filtro ---
    if tipo_filtro == 'Nome do Município':
        # Combina os nomes de municípios de ambos os arquivos para ter uma lista completa
        nomes_unicos = sorted(pd.concat([df_importacao['NOME_MUN'], df_exportacao['NOME_MUN']]).unique())
        municipio_selecionado = st.sidebar.selectbox(
            "Selecione o Município:",
            options=nomes_unicos,
            index=None,
            placeholder="Digite ou selecione um município..."
        )
        if municipio_selecionado:
            nome_mun_display = municipio_selecionado
            # Pega o código do primeiro DF que o encontrar (pode ser imp ou exp)
            if not df_importacao[df_importacao['NOME_MUN'] == municipio_selecionado].empty:
                cod_mun_display = df_importacao[df_importacao['NOME_MUN'] == municipio_selecionado]['CO_MUN'].iloc[0]
            elif not df_exportacao[df_exportacao['NOME_MUN'] == municipio_selecionado].empty:
                cod_mun_display = df_exportacao[df_exportacao['NOME_MUN'] == municipio_selecionado]['CO_MUN'].iloc[0]


    else: # Filtro por Código do Município
        codigo_mun_selecionado = st.sidebar.text_input(
            "Digite o Código do Município (CO_MUN):",
            placeholder="Ex: 3550308"
        ).strip()
        if codigo_mun_selecionado:
            cod_mun_display = codigo_mun_selecionado
            # Pega o nome do primeiro DF que o encontrar
            if not df_importacao[df_importacao['CO_MUN'] == codigo_mun_selecionado].empty:
                nome_mun_display = df_importacao[df_importacao['CO_MUN'] == codigo_mun_selecionado]['NOME_MUN'].iloc[0]
            elif not df_exportacao[df_exportacao['CO_MUN'] == codigo_mun_selecionado].empty:
                nome_mun_display = df_exportacao[df_exportacao['CO_MUN'] == codigo_mun_selecionado]['NOME_MUN'].iloc[0]


    # --- Exibição dos Resultados ---
    if municipio_selecionado or codigo_mun_selecionado:
        st.header(f"Resultados para: {nome_mun_display} ({cod_mun_display})")
        st.markdown("---")

        # --- Seção de Importação ---
        st.subheader("Dados de Importação")
        if tipo_filtro == 'Nome do Município':
            imp_filtrado = df_importacao[df_importacao['NOME_MUN'] == municipio_selecionado].copy()
        else:
            imp_filtrado = df_importacao[df_importacao['CO_MUN'] == codigo_mun_selecionado].copy()

        if not imp_filtrado.empty:
            total_kg_imp = imp_filtrado['KG_LIQUIDO'].sum()
            total_fob_imp = imp_filtrado['VL_FOB'].sum()

            col1, col2 = st.columns(2)
            col1.metric("Total Importado (KG)", f"{total_kg_imp:,.2f}")
            col2.metric("Total Importado (US$ FOB)", f"{total_fob_imp:,.2f}")
            st.dataframe(imp_filtrado)
        else:
            st.info("Nenhum registro de importação encontrado para este município.")

        st.markdown("---")

        # --- Seção de Exportação ---
        st.subheader("Dados de Exportação")
        if tipo_filtro == 'Nome do Município':
            exp_filtrado = df_exportacao[df_exportacao['NOME_MUN'] == municipio_selecionado].copy()
        else:
            exp_filtrado = df_exportacao[df_exportacao['CO_MUN'] == codigo_mun_selecionado].copy()

        if not exp_filtrado.empty:
            total_kg_exp = exp_filtrado['KG_LIQUIDO'].sum()
            total_fob_exp = exp_filtrado['VL_FOB'].sum()

            col3, col4 = st.columns(2)
            col3.metric("Total Exportado (KG)", f"{total_kg_exp:,.2f}")
            col4.metric("Total Exportado (US$ FOB)", f"{total_fob_exp:,.2f}")
            st.dataframe(exp_filtrado)
        else:
            st.info("Nenhum registro de exportação encontrado para este município.")

    else:
        # Mensagem inicial se nenhum filtro foi aplicado.
        st.info("⬅️ Utilize os filtros na barra lateral para buscar os dados de um município.")