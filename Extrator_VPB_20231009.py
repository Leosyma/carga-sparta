# -*- coding: utf-8 -*-
"""
Created on Mon Mar  6 15:17:17 2023

@author: 2018459
"""


#%% Bibliotecas
import pandas as pd
import numpy as np
import keyring
import cx_Oracle
import os
import glob
from datetime import datetime


#%% Dados de entrada
#Origem dos dados

# Caminho referencia
pasta = r"C:\Users\2018459\OneDrive - CPFL Energia S A\Documentos\Projetos\2023\BD RTP e RTA\RTP e RTA (concessionaria)\SPARTA\SPARTA 2023\*"
# pasta = r"C:\Users\2018459\OneDrive - CPFL Energia S A\Área de Trabalho\RTP e RTA (concessionaria)\SPARTA TESTE - RTP"

#Arquivos
#Como os dados das empresas estão em arquivos separados, listar todos que serão carregados conforme layout abaixo.
# arquivo ='SPARTA_Cemar_2021.xlsx'
arquivos = glob.glob(pasta)


index = 0 #Flag para inserção dos dados

#Nome da tabela Oracle onde será dada a carga
tabela_oracle_aj_co_calculo_comp_t = 'SPARTA_RTP_AJ_CO_CALCULO_COMP_T'
tabela_oracle_caa = 'SPARTA_RTP_CAA'
tabela_oracle_caom = 'SPARTA_RTP_CAOM'
tabela_oracle_cor = 'SPARTA_RTP_COR'
tabela_oracle_ri = 'SPARTA_RTP_RI'
ano_oracle = "'2023'"



#%% Montar o dataframe da estrutura da aba 'SPARTA_NUC'
#Defino o indice máximo que é igual o número de SPARTA
index_maximo = list(range(0,len(arquivos)))

#Criação da estrutura do tabela NUC
df_aj_co_calculo_comp_t = pd.DataFrame(data = [], columns=['CHAVE','EVENTO_TARIFARIO','ANO','ID','DISTRIBUIDORA','UF','DATA','PERIODO_TARIFARIO','CONTRATO','AJ_PARCELA_B_RS','PARTICIPACAO_CO_ANTES_AJ_PERCENT','AJ_CUSTO_OPERACIONAL_RS'],index=index_maximo)
df_caa = pd.DataFrame(data = [], columns=['CHAVE','EVENTO_TARIFARIO','ANO','ID','DISTRIBUIDORA','UF','DATA','PERIODO_TARIFARIO','CONTRATO','BAR_RS','BARA_RS','BARV_RS','BARI_RS','CAL_RS','CAV_RS','CAI_RS','CAIMI_RS','ATIVO_IMOBILIZADO_RS','INDICE_APROVEITAMENTO_INTEGRAL_RS','OBRIGACOES_ESPECIAIS_BRUTA_RS','BENS_TOTAL_DEPRECIADOS_RS','BASE_REMUN_BRUTA_RS','DEPRECIACAO_ACUMULADA_RS','AIS_LIQUIDO_RS','INDICE_APROVEITAMENTO_DEPRECIADO_RS','VBR_RS','ALMOXARIFADO_OPERACAO_RS','ATIVO_DIFERIDO_RS','OBRIGACOES_ESPECIAIS_LIQUIDA_RS','TERRENOS_SERVIDOES_RS','BASE_REMUN_LIQUIDA_RS','SALDO_RGR_PLPT_RS','SALDO_RGR_DEMAIS_INVEST_RS','TAXA_DEPRECIACAO_PERCENT','QRR_RS','RC_SEM_OBRIGACOES_ESPECIAIS_RS','REMUN_OBRIGACOES_ESPECIAIS_RS','RC_RS','WACC_REAL_ANTES_IMPOSTO_PERCENT','TAXA_RGR_PLPT_REAL_PERCENT','TAXA_RGR_DEMAIS_INVEST_PERCENT','CAA_RCOE_RS','CAOM_DIVIDIDO_POR_CAOM_CAA_RCOE_PERCENT','PARTICIPACAO_CAPITAL_PROPRIO_PERCENT','PRN_PRP_PERCENT','IMPOSTO_RENDA_PERCENT','RC_OBRIGACOES_ESPECIAIS_RS'],index=index_maximo)
df_caom = pd.DataFrame(data = [], columns=['CHAVE','EVENTO_TARIFARIO','ANO','ID','DISTRIBUIDORA','UF','DATA','PERIODO_TARIFARIO','CONTRATO','RECEITA_PARCELA_B_ANO_TESTE_RS','CO_ULTIMA_REVISAO_AJUSTES_RS','VPB_ULTIMA_REVISAO_AJUSTES_RS','COMPONENTE_T_ULTIMA_REVISAO_PERCENT','FATOR_N_MENOS_1_PERCENT','RECEITA_CO_ANO_TESTE_RS','LIMITE_SUPERIOR_CO_EFICIENTE_RS','LIMITE_INFERIOR_CO_EFICIENTE_RS','CO_EFICIENTE_RS','VARIACAO_ANUAL_CO_SEM_LIMITE_PERCENT','VARIACAO_ANUAL_CO_LIMITADA_PERCENT','META_CO_SEM_COMPARTILHAMENTO_RS','OPEX_MEDIO_RS','RAZAO_ENTRE_CO_AJUSTADO_E_OPEX_MEDIO_PERCENT','META_CO_REGULATORIO_RS','CO_REGULATORIO_RS','IPCA_MES_ANTERIOR_DATA_REVISAO_TARIFARIA_RS','IPCA_MES_ANTERIOR_DATA_CALCULO_EFICIENCIA_RS','CUSTO_EFICIENTE_DATA_CALCULO_EFICIENCIA_RS','CUSTO_EFICIENTE_DATA_REVISAO_TARIFARIA_RS','FATOR_ATUALIZACAO_ALPHA','REFERENCIA_EFICIENCIA_MEDIA_PERCENT','EFICIENCIA_APURADA_PERCENT','LIMITE_SUPERIOR_INTERVALO_EFICIENCIA_PERCENT','LIMITE_INFERIOR_INTERVALO_EFICIENCIA_PERCENT','CO_REAL_ESTUDO_EFICIENCIA','IPCA_JUNHO_ANO_MENOS_2','IPCA_DEZEMBRO_ANO_MENOS_2','CONTA_PESSOAL_ANO_MENOS_2_RS','CONTA_MATERIAIS_ANO_MENOS_2_RS','CONTA_TERCEIROS_ANO_MENOS_2_RS','CONTA_SEGUROS_ANO_MENOS_2_RS','CONTA_TRIBUTOS_ANO_MENOS_2_RS','CONTA_OUTROS_ANO_MENOS_2_RS','DEMAIS_CUSTOS_ANO_MENOS_2_RS','CO_ANO_MENOS_2_RS','CO_ATUALIZADO_ANO_MENOS_2_RS','IPCA_JUNHO_ANO_MENOS_1','IPCA_DEZEMBRO_ANO_MENOS_1','CONTA_PESSOAL_ANO_MENOS_1_RS','CONTA_MATERIAIS_ANO_MENOS_1_RS','CONTA_TERCEIROS_ANO_MENOS_1_RS','CONTA_SEGUROS_ANO_MENOS_1_RS','CONTA_TRIBUTOS_ANO_MENOS_1_RS','CONTA_OUTROS_ANO_MENOS_1_RS','DEMAIS_CUSTOS_ANO_MENOS_1_RS','CO_ANO_MENOS_1_RS','CO_ATUALIZADO_ANO_MENOS_1_RS','INDICADOR_MEDIO_PNT_PERCENT','META_PERCENT','DEC_GLOBAL_MEDIO_REALIZADO','LIMITE_V8_GLOBAL','PESO_INSUMO_U','FATOR_ESCALA_PERCENT','REDES_SUBTERRANEAS_KM','REDE_DISTRIBUICAO_AREA_KM','REDE_ALTA_TENSAO_KM','NUMERO_CONSUMIDORES','PNT_AJUSTADA_MWH','CHI_AJUSTADO_HORAS','MERCADO_PONDERADO_MWH','MERCADO_AT_MWH','MERCADO_MT_MWH','MERCADO_BT_MWH','FATOR_ESCALA_PESO','REDES_SUBTERRANEAS_PESO','REDE_DISTRIBUICAO_AREA_PESO','REDE_ALTA_TENSAO_PESO','NUMERO_CONSUMIDORES_PESO','PNT_AJUSTADA_PESO','CHI_AJUSTADO_PESO','MERCADO_PONDERADO_PESO','MERCADO_AT_PESO','MERCADO_MT_PESO','MERCADO_BT_PESO','OUTRAS_RECEITAS_RS','EXCEDENTE_REATIVOS_RS','ULTRAPASSAGEM_DEMANDA_RS','PARCELA_B_DEDUZIDAS_OUTRAS_RECEITAS_RS'],index=index_maximo)
df_ri = pd.DataFrame(data = [], columns=['CHAVE','EVENTO_TARIFARIO','ANO','ID','DISTRIBUIDORA','UF','DATA','PERIODO_TARIFARIO','CONTRATO','RI_ENCARGOS_SETORIAIS_RS','ENCARGOS_DRP_RS','CARGA_TRIBUTARIA_PERCENT','LIMITE_RI_ENCARGOS_PERCENT','DEMAIS_RI_RS','RECEITA_ADICIONAL_BANDEIRA_RS','ENERGIA_COMPRADA_RS','TRANSPORTE_ENERGIA_RS','LIMITE_DEMAIS_RI_PERCENT','RI_TOTAL_RS','RESIDENCIAL_PARTIPACAO_CONSUMO_PERCENT','INDUSTRIAL_PARTIPACAO_CONSUMO_PERCENT','COMERCIAL_PARTIPACAO_CONSUMO_PERCENT','RURAL_PARTIPACAO_CONSUMO_PERCENT','PODER_PUBLICO_PARTIPACAO_CONSUMO_PERCENT','ILUMINACAO_PUBLICA_PARTIPACAO_CONSUMO_PERCENT','SERVICO_PUBLICO_PARTIPACAO_CONSUMO_PERCENT','RESIDENCIAL_LIMITE_DEMAIS_RI_PERCENT','INDUSTRIAL_LIMITE_DEMAIS_RI_PERCENT','COMERCIAL_LIMITE_DEMAIS_RI_PERCENT','RURAL_LIMITE_DEMAIS_RI_PERCENT','PODER_PUBLICO_LIMITE_DEMAIS_RI_PERCENT','ILUMINACAO_PUBLICA_LIMITE_DEMAIS_RI_PERCENT','SERVICO_PUBLICO_LIMITE_DEMAIS_RI_PERCENT','RESIDENCIAL_MEDIANA_INADIMPLENCIAS_PERCENT','INDUSTRIAL_MEDIANA_INADIMPLENCIAS_PERCENT','COMERCIAL_MEDIANA_INADIMPLENCIAS_PERCENT','RURAL_MEDIANA_INADIMPLENCIAS_PERCENT','PODER_PUBLICO_MEDIANA_INADIMPLENCIAS_PERCENT','ILUMINACAO_PUBLICA_MEDIANA_INADIMPLENCIAS_PERCENT','SERVICO_PUBLICO_MEDIANA_INADIMPLENCIAS_PERCENT','RESIDENCIAL_LIMITE_NEUTRALIDADE_PERCENT','INDUSTRIAL_LIMITE_NEUTRALIDADE_PERCENT','COMERCIAL_LIMITE_NEUTRALIDADE_PERCENT','RURAL_LIMITE_NEUTRALIDADE_PERCENT','PODER_PUBLICO_LIMITE_NEUTRALIDADE_PERCENT','ILUMINACAO_PUBLICA_LIMITE_NEUTRALIDADE_PERCENT','SERVICO_PUBLICO_LIMITE_NEUTRALIDADE_PERCENT','RESIDENCIAL_LIMITE_RI_ENCARGOS_PERCENT','INDUSTRIAL_LIMITE_RI_ENCARGOS_PERCENT','COMERCIAL_LIMITE_RI_ENCARGOS_PERCENT','RURAL_LIMITE_RI_ENCARGOS_PERCENT','PODER_PUBLICO_LIMITE_RI_ENCARGOS_PERCENT','ILUMINACAO_PUBLICA_LIMITE_RI_ENCARGOS_PERCENT','SERVICO_PUBLICO_LIMITE_RI_ENCARGOS_PERCENT'],index=index_maximo)
df_cor = pd.DataFrame(data = [], columns=['CHAVE','EVENTO_TARIFARIO','ANO','ID','DISTRIBUIDORA','UF','DATA','PERIODO_TARIFARIO','CONTRATO','THETA_INF','THETA_CENTRO','THETA_SUP','THETA_REF','OPEX_REAL','PMSO_CORRIGIDO','FATOR_ESCALA','U','VS_YRSUB','VS_YRDIST_A','VS_YRALTA','VS_YCONS','VS_YMPONDERADO','VS_YD_PERDAS_DIF2','VS_YD_DEC_V8','P_AT','P_MT','P_BT','PMSO_2013','CONT_ASSOCIATIVA','PESO_AT_PERCENT','PESO_MT_PERCENT','PESO_BT_PERCENT'],index=index_maximo)

#Criação dataframe vazio
df_sparta_resultado = pd.DataFrame(data = [])
df_sparta_capa = pd.DataFrame(data=[])
df_sparta_mercado = pd.DataFrame(data=[])



# df_sparta_resultado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Resultado'
#                               ,header=12
#                               ,nrows=10
#                               ,usecols=[10,11])


# df_sparta_mercado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Mercado'
#                               ,header=7
#                               ,nrows=49
#                               ,usecols=[1,2,3,4,5,6,7])
   
# df_sparta_vpb = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'VPB e Fator X'
#                                 ,header=11
#                                 ,usecols='B:G')

# df_sparta_resultado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Resultado'
#                                 ,header=17
#                                 ,usecols='F:H')

# df_sparta_bd = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'BD'
#                                 ,header=5
#                                 ,nrows = 40
#                                 ,usecols='S:X')

# df_sparta_vpb = df_sparta_vpb.astype('str')
# df_sparta_resultado = df_sparta_resultado.astype('str')
# df_sparta_bd = df_sparta_bd.astype('str')


# linhas_vpb = range(len(df_sparta_vpb.index))
# linhas_resultado = range(len(df_sparta_resultado.index))
# linhas_bd = range(len(df_sparta_bd.index))
# colunas_vpb = range(len(df_sparta_vpb.columns))
# colunas_resultado = range(len(df_sparta_resultado.columns))

#Função para extrair os dados de energia da SPARTA
def determina_contrato_aj_co_calculo_comp_t(df_aj_co_calculo_comp_t,df_sparta_mercado,index):
    #Se essa 'string' estiver presente nessa aba o contrato é NOVO
    if 'Percentual RI' in df_sparta_mercado.iloc[:,:].values:
          df_aj_co_calculo_comp_t.at[index,'CONTRATO'] = 'NOVO' 
    #Caso contrário o contrato é ANTIGO      
    else:
          df_aj_co_calculo_comp_t.at[index,'CONTRATO'] = 'ANTIGO'
          

def determina_contrato_caa(df_caa,df_sparta_mercado,index):
    #Se essa 'string' estiver presente nessa aba o contrato é NOVO
    if 'Percentual RI' in df_sparta_mercado.iloc[:,:].values:
          df_caa.at[index,'CONTRATO'] = 'NOVO' 
    #Caso contrário o contrato é ANTIGO      
    else:
          df_caa.at[index,'CONTRATO'] = 'ANTIGO'
          
          
def determina_contrato_caom(df_caom,df_sparta_mercado,index):
    #Se essa 'string' estiver presente nessa aba o contrato é NOVO
    if 'Percentual RI' in df_sparta_mercado.iloc[:,:].values:
          df_caom.at[index,'CONTRATO'] = 'NOVO' 
    #Caso contrário o contrato é ANTIGO      
    else:
          df_caom.at[index,'CONTRATO'] = 'ANTIGO'
          
          
def determina_contrato_ri(df_ri,df_sparta_mercado,index):
    #Se essa 'string' estiver presente nessa aba o contrato é NOVO
    if 'Percentual RI' in df_sparta_mercado.iloc[:,:].values:
          df_ri.at[index,'CONTRATO'] = 'NOVO' 
    #Caso contrário o contrato é ANTIGO      
    else:
          df_ri.at[index,'CONTRATO'] = 'ANTIGO'
          
          
def determina_contrato_cor(df_cor,df_sparta_mercado,index):
    #Se essa 'string' estiver presente nessa aba o contrato é NOVO
    if 'Percentual RI' in df_sparta_mercado.iloc[:,:].values:
          df_cor.at[index,'CONTRATO'] = 'NOVO' 
    #Caso contrário o contrato é ANTIGO      
    else:
          df_cor.at[index,'CONTRATO'] = 'ANTIGO'


#Função para extrair dados da tabela SPARTA_RTP_AJ_CO_CALCULO_COMP_T
def extrai_aj_co_calculo_comp_t(df_aj_co_calculo_comp_t,df_sparta_vpb,index):
    for linha in linhas_vpb:
        if 'VALOR DE AJUSTE DA PARCELA B' in df_sparta_vpb.iloc[linha,3].upper():
            df_aj_co_calculo_comp_t.at[index,'AJ_PARCELA_B_RS'] = df_sparta_vpb.iloc[linha,5]
        elif 'PARTICIPAÇÃO DOS CO NO VPB' in df_sparta_vpb.iloc[linha,3].upper():
             df_aj_co_calculo_comp_t.at[index,'PARTICIPACAO_CO_ANTES_AJ_PERCENT'] = df_sparta_vpb.iloc[linha,5]  
        elif 'VALOR DE AJUSTE DOS CUSTOS OPERACIONAIS' in df_sparta_vpb.iloc[linha,3].upper():
             df_aj_co_calculo_comp_t.at[index,'AJ_CUSTO_OPERACIONAL_RS'] = df_sparta_vpb.iloc[linha,5]

def extrai_caa(df_caa,df_sparta_vpb,index):
    for linha in linhas_vpb:
        #Custo Anual das Instalações Móveis e Imóveis (CAIMI)
        if '(BAR)' in df_sparta_vpb.iloc[linha,0].upper():
            df_caa.at[index,'BAR_RS'] = df_sparta_vpb.iloc[linha,1]
        elif '(BARA)' in df_sparta_vpb.iloc[linha,0].upper():
            df_caa.at[index,'BARA_RS'] = df_sparta_vpb.iloc[linha,1]
        elif '(BARV)' in df_sparta_vpb.iloc[linha,0].upper():
            df_caa.at[index,'BARV_RS'] = df_sparta_vpb.iloc[linha,1]
        elif '(BARI)' in df_sparta_vpb.iloc[linha,0].upper():
            df_caa.at[index,'BARI_RS'] = df_sparta_vpb.iloc[linha,1]
        elif '(CAL)' in df_sparta_vpb.iloc[linha,0].upper():
            df_caa.at[index,'CAL_RS'] = df_sparta_vpb.iloc[linha,1]
        elif '(CAV)' in df_sparta_vpb.iloc[linha,0].upper():
            df_caa.at[index,'CAV_RS'] = df_sparta_vpb.iloc[linha,1]
        elif '(CAI)' in df_sparta_vpb.iloc[linha,0].upper():
            df_caa.at[index,'CAI_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'CAIMI = ' in df_sparta_vpb.iloc[linha,0].upper():
            df_caa.at[index,'CAIMI_RS'] = df_sparta_vpb.iloc[linha,1]
        
    for linha in linhas_vpb:
        #Base de Remuneração Bruta e Quota de Reintegração Regulatória (QRR)
        if 'ATIVO IMOBILIZADO EM SERVIÇO' in df_sparta_vpb.iloc[linha,0].upper():
            df_caa.at[index,'ATIVO_IMOBILIZADO_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'ÍNDICE DE APROVEITAMENTO INTEGRAL' in df_sparta_vpb.iloc[linha,0].upper():
            df_caa.at[index,'INDICE_APROVEITAMENTO_INTEGRAL_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'OBRIGAÇÕES ESPECIAIS BRUTA' in df_sparta_vpb.iloc[linha,0].upper():
            df_caa.at[index,'OBRIGACOES_ESPECIAIS_BRUTA_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'BENS TOTALMENTE DEPRECIADOS' in df_sparta_vpb.iloc[linha,0].upper():
            df_caa.at[index,'BENS_TOTAL_DEPRECIADOS_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'BASE DE REMUNERAÇÃO BRUTA' in df_sparta_vpb.iloc[linha,0].upper():
            df_caa.at[index,'BASE_REMUN_BRUTA_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'DEPRECIAÇÃO ACUMULADA' in df_sparta_vpb.iloc[linha,0].upper():
            df_caa.at[index,'DEPRECIACAO_ACUMULADA_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'AIS LÍQUIDO' in df_sparta_vpb.iloc[linha,0].upper():
            df_caa.at[index,'AIS_LIQUIDO_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'ÍNDICE DE APROVEITAMENTO DEPRECIADO' in df_sparta_vpb.iloc[linha,0].upper():
            df_caa.at[index,'INDICE_APROVEITAMENTO_DEPRECIADO_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'VALOR DA BASE DE REMUNERAÇÃO' in df_sparta_vpb.iloc[linha,0].upper():
            df_caa.at[index,'VBR_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'ALMOXARIFADO EM OPERAÇÃO' in df_sparta_vpb.iloc[linha,0].upper():
            df_caa.at[index,'ALMOXARIFADO_OPERACAO_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'ATIVO DIFERIDO' in df_sparta_vpb.iloc[linha,0].upper():
            df_caa.at[index,'ATIVO_DIFERIDO_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'OBRIGAÇÕES ESPECIAIS LÍQUIDA' in df_sparta_vpb.iloc[linha,0].upper():
            df_caa.at[index,'OBRIGACOES_ESPECIAIS_LIQUIDA_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'TERRENOS E SERVIDÕES' in df_sparta_vpb.iloc[linha,0].upper():
            df_caa.at[index,'TERRENOS_SERVIDOES_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'BASE DE REMUNERAÇÃO LÍQUIDA' in df_sparta_vpb.iloc[linha,0].upper():
            df_caa.at[index,'BASE_REMUN_LIQUIDA_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'SALDO RGR PLPT' in df_sparta_vpb.iloc[linha,0].upper():
            df_caa.at[index,'SALDO_RGR_PLPT_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'SALDO RGR DEMAIS INVESTIMENTOS' in df_sparta_vpb.iloc[linha,0].upper():
            df_caa.at[index,'SALDO_RGR_DEMAIS_INVEST_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'TAXA DE DEPRECIAÇÃO' in df_sparta_vpb.iloc[linha,0].upper():
            df_caa.at[index,'TAXA_DEPRECIACAO_PERCENT'] = df_sparta_vpb.iloc[linha,1]
        elif 'QUOTA DE REINTEGRAÇÃO REGULATÓRIA' in df_sparta_vpb.iloc[linha,0].upper():
            df_caa.at[index,'QRR_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'RC SEM OBRIGAÇÕES ESPECIAIS' in df_sparta_vpb.iloc[linha,0].upper():
            df_caa.at[index,'RC_SEM_OBRIGACOES_ESPECIAIS_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'REMUNERAÇÃO DE OBRIGAÇÕES ESPECIAIS' in df_sparta_vpb.iloc[linha,0].upper():
            df_caa.at[index,'REMUN_OBRIGACOES_ESPECIAIS_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'REMUNERAÇÃO DO CAPITAL' in df_sparta_vpb.iloc[linha,0].upper():
            df_caa.at[index,'RC_RS'] = df_sparta_vpb.iloc[linha,1]
        
        #Saio do loop, pois existem tabelas duplicadas na SPARTA de 2013
        elif 'ATIVOS DE GERAÇÃO' in df_sparta_vpb.iloc[linha,0].upper():
            break  
            

    for linha in linhas_vpb:
        #Custo Médio do Capital
        if 'WACC REAL ANTES DE IMPOSTOS' in df_sparta_vpb.iloc[linha,3].upper():
            df_caa.at[index,'WACC_REAL_ANTES_IMPOSTO_PERCENT'] = df_sparta_vpb.iloc[linha,4]
        elif 'TAXA RGR/PLPT REAL' in df_sparta_vpb.iloc[linha,3].upper():
            df_caa.at[index,'TAXA_RGR_PLPT_REAL_PERCENT'] = df_sparta_vpb.iloc[linha,4]
        elif 'TAXA RGR DEMAIS INVESTIMENTOS' in df_sparta_vpb.iloc[linha,3].upper():
            df_caa.at[index,'TAXA_RGR_DEMAIS_INVEST_PERCENT'] = df_sparta_vpb.iloc[linha,4]
        
        #Remuneração sobre Obrigações Especiais   
        if '(CAOM+CAA-RCOE)' in df_sparta_vpb.iloc[linha,3].upper():
            df_caa.at[index,'CAOM_DIVIDIDO_POR_CAOM_CAA_RCOE_PERCENT'] = df_sparta_vpb.iloc[linha,5]
        elif 'PARTICIPAÇÃO DO CAPITAL PRÓPRIO' in df_sparta_vpb.iloc[linha,3].upper():
            df_caa.at[index,'PARTICIPACAO_CAPITAL_PROPRIO_PERCENT'] = df_sparta_vpb.iloc[linha,5]
        elif 'PREMIO DE RISCO DO NEGÓCIO E FINANCEIRO' in df_sparta_vpb.iloc[linha,3].upper():
            df_caa.at[index,'PRN_PRP_PERCENT'] = df_sparta_vpb.iloc[linha,5]
        elif 'IMPOSTOS E CONTRIBUIÇÕES SOBRE A RENDA' in df_sparta_vpb.iloc[linha,3].upper():
            df_caa.at[index,'IMPOSTO_RENDA_PERCENT'] = df_sparta_vpb.iloc[linha,5]
        elif 'RC OBRIGAÇÕES ESPECIAIS' in df_sparta_vpb.iloc[linha,3].upper():
            df_caa.at[index,'RC_OBRIGACOES_ESPECIAIS_RS'] = df_sparta_vpb.iloc[linha,5]
    
    df_caa.at[index,'CAA_RCOE_RS'] = float(df_caa.at[index,'CAIMI_RS']) + float(df_caa.at[index,'QRR_RS']) + float(df_caa.at[index,'RC_SEM_OBRIGACOES_ESPECIAIS_RS'])


def extrai_caom(df_caom,df_sparta_vpb,df_sparta_resultado,index):
    for linha in linhas_vpb:
        #Receita de Custos Operacionais no Ano Teste
        if 'RECEITA DE PARCELA B NO ANO TESTE' in df_sparta_vpb.iloc[linha,0].upper():
            df_caom.at[index,'RECEITA_PARCELA_B_ANO_TESTE_RS'] = df_sparta_vpb.iloc[linha,1]
        elif ('CUSTOS OPERACIONAIS DA ÚLTIMA REVISÃO COM AJUSTES' in df_sparta_vpb.iloc[linha,0].upper()) or ('CUSTOS OPERACIONAIS CICLO TARIFÁRIO COM AJUSTES' in df_sparta_vpb.iloc[linha,0].upper()):
            df_caom.at[index,'CO_ULTIMA_REVISAO_AJUSTES_RS'] = df_sparta_vpb.iloc[linha,1]
        elif ('VPB DA ÚLTIMA REVISÃO COM AJUSTES' in df_sparta_vpb.iloc[linha,0].upper()) or ('VPB CICLO TARIFÁRIO COM AJUSTES' in df_sparta_vpb.iloc[linha,0].upper()):
            df_caom.at[index,'VPB_ULTIMA_REVISAO_AJUSTES_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'COMPONENTE T DA ÚLTIMA REVISÃO' in df_sparta_vpb.iloc[linha,0].upper():
            df_caom.at[index,'COMPONENTE_T_ULTIMA_REVISAO_PERCENT'] = df_sparta_vpb.iloc[linha,1] 
        elif 'FATOR (1-TREV)N-1' in df_sparta_vpb.iloc[linha,0].upper():
            df_caom.at[index,'FATOR_N_MENOS_1_PERCENT'] = df_sparta_vpb.iloc[linha,1]
        elif 'RECEITA DE CUSTOS OPERACIONAIS NO ANO TESTE' in df_sparta_vpb.iloc[linha,0].upper():
            df_caom.at[index,'RECEITA_CO_ANO_TESTE_RS'] = df_sparta_vpb.iloc[linha,1]
            
        #Meta e Custos Operacionais Regulatórios
        if 'LIMITE SUPERIOR DOS CUSTOS OPERACIONAIS EFICIENTES' in df_sparta_vpb.iloc[linha,0].upper():
            df_caom.at[index,'LIMITE_SUPERIOR_CO_EFICIENTE_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'LIMITE INFERIOR DOS CUSTOS OPERACIONAIS EFICIENTES' in df_sparta_vpb.iloc[linha,0].upper():
            df_caom.at[index,'LIMITE_INFERIOR_CO_EFICIENTE_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'CUSTO OPERACIONAL EFICIENTE' in df_sparta_vpb.iloc[linha,0].upper():
            df_caom.at[index,'CO_EFICIENTE_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'VARIAÇÃO ANUAL DOS CUSTOS OPERACIONAIS - SEM LIMITE' in df_sparta_vpb.iloc[linha,0].upper():
            df_caom.at[index,'VARIACAO_ANUAL_CO_SEM_LIMITE_PERCENT'] = df_sparta_vpb.iloc[linha,1]
        elif 'VARIAÇÃO ANUAL DOS CUSTOS OPERACIONAIS - LIMITADA' in df_sparta_vpb.iloc[linha,0].upper():
            df_caom.at[index,'VARIACAO_ANUAL_CO_LIMITADA_PERCENT'] = df_sparta_vpb.iloc[linha,1]
        elif 'META CUSTOS OPERACIONAIS SEM COMPARTILHAMENTO' in df_sparta_vpb.iloc[linha,0].upper():
            df_caom.at[index,'META_CO_SEM_COMPARTILHAMENTO_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'MÉDIA DOS CUSTOS OPERACIONAIS REAIS (OPEX MEDIO)' in df_sparta_vpb.iloc[linha,0].upper():
            df_caom.at[index,'OPEX_MEDIO_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'RAZÃO ENTRE CO REGULATÓRIOS AJUSTADO E CUSTOS OPERACIONAIS REAIS MÉDIOS' in df_sparta_vpb.iloc[linha,0].upper():
            df_caom.at[index,'RAZAO_ENTRE_CO_AJUSTADO_E_OPEX_MEDIO_PERCENT'] = df_sparta_vpb.iloc[linha,1]
        elif 'META CUSTOS OPERACIONAIS REGULATÓRIOS' in df_sparta_vpb.iloc[linha,0].upper():
            df_caom.at[index,'META_CO_REGULATORIO_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'CUSTOS OPERACIONAIS REGULATÓRIOS' in df_sparta_vpb.iloc[linha,0].upper():
            df_caom.at[index,'CO_REGULATORIO_RS'] = df_sparta_vpb.iloc[linha,1]

        #Intervalo de Custos Eficientes
        if 'NÚMERO ÍNDICE DO IPCA NO MÊS ANTERIOR À DATA BASE DA REVISÃO TARIFÁRIA' in df_sparta_vpb.iloc[linha,0].upper():
            df_caom.at[index,'IPCA_MES_ANTERIOR_DATA_REVISAO_TARIFARIA_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'NÚMERO ÍNDICE DO IPCA NO MÊS ANTERIOR À DATA BASE DO CÁLCULO DA EFICIÊNCIA' in df_sparta_vpb.iloc[linha,0].upper():
            df_caom.at[index,'IPCA_MES_ANTERIOR_DATA_CALCULO_EFICIENCIA_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'CUSTO EFICIENTE ESTIMADO NA DATA BASE DO CÁLCULO DA EFICIÊNCIA' in df_sparta_vpb.iloc[linha,0].upper():
            df_caom.at[index,'CUSTO_EFICIENTE_DATA_CALCULO_EFICIENCIA_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'CUSTO EFICIENTE ESTIMADO NA DATA DA REVISÃO TARIFÁRIA' in df_sparta_vpb.iloc[linha,0].upper():
            df_caom.at[index,'CUSTO_EFICIENTE_DATA_REVISAO_TARIFARIA_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'FATOR DE ATUALIZAÇÃO' in df_sparta_vpb.iloc[linha,0].upper():
            df_caom.at[index,'FATOR_ATUALIZACAO_ALPHA'] = df_sparta_vpb.iloc[linha,1]
        elif 'REFERÊNCIA DE EFICIÊNCIA MÉDIA' in df_sparta_vpb.iloc[linha,0].upper():
            df_caom.at[index,'REFERENCIA_EFICIENCIA_MEDIA_PERCENT'] = df_sparta_vpb.iloc[linha,1]
        elif 'EFICIÊNCIA APURADA PARA A EMPRESA' in df_sparta_vpb.iloc[linha,0].upper():
            df_caom.at[index,'EFICIENCIA_APURADA_PERCENT'] = df_sparta_vpb.iloc[linha,1]
        elif 'LIMITE SUPEROR DO INTERVALO DE EFICIÊNCIA' in df_sparta_vpb.iloc[linha,0].upper():
            df_caom.at[index,'LIMITE_SUPERIOR_INTERVALO_EFICIENCIA_PERCENT'] = df_sparta_vpb.iloc[linha,1]
        elif 'LIMITE INFERIOR DO INTERVALO DE EFICIÊNCIA' in df_sparta_vpb.iloc[linha,0].upper():
            df_caom.at[index,'LIMITE_INFERIOR_INTERVALO_EFICIENCIA_PERCENT'] = df_sparta_vpb.iloc[linha,1]
        elif 'CUSTO OPERACIONAL REAL USADO NO ESTUDO DE EFICIÊNCIA' in df_sparta_vpb.iloc[linha,0].upper():
            df_caom.at[index,'CO_REAL_ESTUDO_EFICIENCIA'] = df_sparta_vpb.iloc[linha,1]
        
        #Custos Operacionais Reais (Opex) - Valores Atualizados
        if 'IPCA JUNHO' in df_sparta_vpb.iloc[linha,3].upper():
            df_caom.at[index,'IPCA_JUNHO_ANO_MENOS_2'] = df_sparta_vpb.iloc[linha,4]
            df_caom.at[index,'IPCA_JUNHO_ANO_MENOS_1'] = df_sparta_vpb.iloc[linha,5]
        elif 'IPCA DEZEMBRO' in df_sparta_vpb.iloc[linha,3].upper():
            df_caom.at[index,'IPCA_DEZEMBRO_ANO_MENOS_2'] = df_sparta_vpb.iloc[linha,4]
            df_caom.at[index,'IPCA_DEZEMBRO_ANO_MENOS_1'] = df_sparta_vpb.iloc[linha,5]
        elif 'CONTA PESSOAL' in df_sparta_vpb.iloc[linha,3].upper():
            df_caom.at[index,'CONTA_PESSOAL_ANO_MENOS_2_RS'] = df_sparta_vpb.iloc[linha,4]
            df_caom.at[index,'CONTA_PESSOAL_ANO_MENOS_1_RS'] = df_sparta_vpb.iloc[linha,5]
        elif 'CONTA MATERIAIS' in df_sparta_vpb.iloc[linha,3].upper():
            df_caom.at[index,'CONTA_MATERIAIS_ANO_MENOS_2_RS'] = df_sparta_vpb.iloc[linha,4]
            df_caom.at[index,'CONTA_MATERIAIS_ANO_MENOS_1_RS'] = df_sparta_vpb.iloc[linha,5]
        elif 'CONTA TERCEIROS' in df_sparta_vpb.iloc[linha,3].upper():
            df_caom.at[index,'CONTA_TERCEIROS_ANO_MENOS_2_RS'] = df_sparta_vpb.iloc[linha,4]
            df_caom.at[index,'CONTA_TERCEIROS_ANO_MENOS_1_RS'] = df_sparta_vpb.iloc[linha,5]
        elif 'CONTA SEGUROS' in df_sparta_vpb.iloc[linha,3].upper():
            df_caom.at[index,'CONTA_SEGUROS_ANO_MENOS_2_RS'] = df_sparta_vpb.iloc[linha,4]
            df_caom.at[index,'CONTA_SEGUROS_ANO_MENOS_1_RS'] = df_sparta_vpb.iloc[linha,5]
        elif 'CONTA TRIBUTOS' in df_sparta_vpb.iloc[linha,3].upper():
            df_caom.at[index,'CONTA_TRIBUTOS_ANO_MENOS_2_RS'] = df_sparta_vpb.iloc[linha,4]
            df_caom.at[index,'CONTA_TRIBUTOS_ANO_MENOS_1_RS'] = df_sparta_vpb.iloc[linha,5]
        elif 'CONTA OUTROS' in df_sparta_vpb.iloc[linha,3].upper():
            df_caom.at[index,'CONTA_OUTROS_ANO_MENOS_2_RS'] = df_sparta_vpb.iloc[linha,4]
            df_caom.at[index,'CONTA_OUTROS_ANO_MENOS_1_RS'] = df_sparta_vpb.iloc[linha,5]
        elif 'DEMAIS CUSTOS' in df_sparta_vpb.iloc[linha,3].upper():
            df_caom.at[index,'DEMAIS_CUSTOS_ANO_MENOS_2_RS'] = df_sparta_vpb.iloc[linha,4]
            df_caom.at[index,'DEMAIS_CUSTOS_ANO_MENOS_1_RS'] = df_sparta_vpb.iloc[linha,5]
        elif df_sparta_vpb.iloc[linha,3].upper() == 'CUSTO OPERACIONAL ':
            df_caom.at[index,'CO_ANO_MENOS_2_RS'] = df_sparta_vpb.iloc[linha,4]
            df_caom.at[index,'CO_ANO_MENOS_1_RS'] = df_sparta_vpb.iloc[linha,5]
        elif df_sparta_vpb.iloc[linha,3].upper() == 'CUSTO OPERACIONAL ATUALIZADO':
            df_caom.at[index,'CO_ATUALIZADO_ANO_MENOS_2_RS'] = df_sparta_vpb.iloc[linha,4]
            df_caom.at[index,'CO_ATUALIZADO_ANO_MENOS_1_RS'] = df_sparta_vpb.iloc[linha,5]

        #Custo Eficiente Estimado
        #Parametros
        #Não extraimos nenhum dado das SPARTAs de 2013, pois não possuem esses indicadores
    for linha in linhas_vpb:
        #Saio do loop porque não queremos extrair esses indicadores para SPARTA 2013
        if 'OPEX' in df_sparta_vpb.iloc[0,0].upper():
            break
            
        else:
            if 'INDICADOR MÉDIO DE PERDAS NÃO TÉCNICAS (% PNT / BT)' in df_sparta_vpb.iloc[linha,0].upper():
                df_caom.at[index,'INDICADOR_MEDIO_PNT_PERCENT'] = df_sparta_vpb.iloc[linha,2]
            elif 'META (% PNT / BT)' in df_sparta_vpb.iloc[linha,0].upper():
                df_caom.at[index,'META_PERCENT'] = df_sparta_vpb.iloc[linha,2]
            elif 'DEC GLOBAL MÉDIO REALIZADO' in df_sparta_vpb.iloc[linha,0].upper():
                df_caom.at[index,'DEC_GLOBAL_MEDIO_REALIZADO'] = df_sparta_vpb.iloc[linha,2]
            elif 'LIMITE V8 GLOBAL' in df_sparta_vpb.iloc[linha,0].upper():
                df_caom.at[index,'LIMITE_V8_GLOBAL'] = df_sparta_vpb.iloc[linha,2]
            elif 'PESO DO INSUMO (U)' in df_sparta_vpb.iloc[linha,0].upper():
                df_caom.at[index,'PESO_INSUMO_U'] = df_sparta_vpb.iloc[linha,2]
            #Produto
            if '"FATOR DE ESCALA" DA EMPRESA' in df_sparta_vpb.iloc[linha,0].upper():
                df_caom.at[index,'FATOR_ESCALA_PERCENT'] = df_sparta_vpb.iloc[linha,2]
                df_caom.at[index,'FATOR_ESCALA_PESO'] = df_sparta_vpb.iloc[linha,3]
            elif 'EXTENSÃO DE REDES SUBTERRÂNEAS' in df_sparta_vpb.iloc[linha,0].upper():
                df_caom.at[index,'REDES_SUBTERRANEAS_KM'] = df_sparta_vpb.iloc[linha,2]
                df_caom.at[index,'REDES_SUBTERRANEAS_PESO'] = df_sparta_vpb.iloc[linha,3]
            elif 'EXTENSÃO DE REDE DE DISTRIBUIÇÃO AÉREA' in df_sparta_vpb.iloc[linha,0].upper():
                df_caom.at[index,'REDE_DISTRIBUICAO_AREA_KM'] = df_sparta_vpb.iloc[linha,2]
                df_caom.at[index,'REDE_DISTRIBUICAO_AREA_PESO'] = df_sparta_vpb.iloc[linha,3]
            elif 'EXTENSÃO DE REDE DE ALTA TENSÃO' in df_sparta_vpb.iloc[linha,0].upper():
                df_caom.at[index,'REDE_ALTA_TENSAO_KM'] = df_sparta_vpb.iloc[linha,2]
                df_caom.at[index,'REDE_ALTA_TENSAO_PESO'] = df_sparta_vpb.iloc[linha,3]
            elif 'NÚMERO DE CONSUMIDORES' in df_sparta_vpb.iloc[linha,0].upper():
                df_caom.at[index,'NUMERO_CONSUMIDORES'] = df_sparta_vpb.iloc[linha,2]
                df_caom.at[index,'NUMERO_CONSUMIDORES_PESO'] = df_sparta_vpb.iloc[linha,3]
            elif 'PERDAS NÃO TÉCNICAS AJUSTADAS' in df_sparta_vpb.iloc[linha,0].upper():
                df_caom.at[index,'PNT_AJUSTADA_MWH'] = df_sparta_vpb.iloc[linha,2]
                df_caom.at[index,'PNT_AJUSTADA_PESO'] = df_sparta_vpb.iloc[linha,3]
            elif 'CONSUMIDOR HORA INTERROMPIDO AJUSTADO' in df_sparta_vpb.iloc[linha,0].upper():
                df_caom.at[index,'CHI_AJUSTADO_HORAS'] = df_sparta_vpb.iloc[linha,2]
                df_caom.at[index,'CHI_AJUSTADO_PESO'] = df_sparta_vpb.iloc[linha,3]
            elif 'MERCADO PONDERADO' in df_sparta_vpb.iloc[linha,0].upper():
                df_caom.at[index,'MERCADO_PONDERADO_MWH'] = df_sparta_vpb.iloc[linha,2]
                df_caom.at[index,'MERCADO_PONDERADO_PESO'] = df_sparta_vpb.iloc[linha,3]
            elif 'MERCADO AT' in df_sparta_vpb.iloc[linha,0].upper():
                df_caom.at[index,'MERCADO_AT_MWH'] = df_sparta_vpb.iloc[linha,2]
                df_caom.at[index,'MERCADO_AT_PESO'] = df_sparta_vpb.iloc[linha,3]
            elif 'MERCADO MT' in df_sparta_vpb.iloc[linha,0].upper():
                df_caom.at[index,'MERCADO_MT_MWH'] = df_sparta_vpb.iloc[linha,2]
                df_caom.at[index,'MERCADO_MT_PESO'] = df_sparta_vpb.iloc[linha,3]
            elif 'MERCADO BT' in df_sparta_vpb.iloc[linha,0].upper():
                df_caom.at[index,'MERCADO_BT_MWH'] = df_sparta_vpb.iloc[linha,2]
                df_caom.at[index,'MERCADO_BT_PESO'] = df_sparta_vpb.iloc[linha,3]

    #Parcela B com Ajustes
    for linha in linhas_resultado:
        for coluna in colunas_resultado:
            if '(OR)' in df_sparta_resultado.iloc[linha,coluna].upper():
                df_caom.at[index,'OUTRAS_RECEITAS_RS'] = df_sparta_resultado.iloc[linha,(coluna+1)]
            elif ('(ER)' in df_sparta_resultado.iloc[linha,coluna].upper()) or (df_sparta_resultado.iloc[linha,coluna].upper() == 'EXCEDENTE DE REATIVOS'):
                df_caom.at[index,'EXCEDENTE_REATIVOS_RS'] = df_sparta_resultado.iloc[linha,(coluna+1)]
            elif ('(UD)' in df_sparta_resultado.iloc[linha,coluna].upper()) or (df_sparta_resultado.iloc[linha,coluna].upper() == 'ULTRAPASSAGEM DE DEMANDA'):
                df_caom.at[index,'ULTRAPASSAGEM_DEMANDA_RS'] = df_sparta_resultado.iloc[linha,(coluna+1)]
            elif 'PARCELA B DEDUZIDAS AS OUTRAS RECEITAS' in df_sparta_resultado.iloc[linha,coluna].upper():
                df_caom.at[index,'PARCELA_B_DEDUZIDAS_OUTRAS_RECEITAS_RS'] = df_sparta_resultado.iloc[linha,(coluna+1)]


def extrai_ri(df_ri,df_sparta_vpb,index):
    for linha in linhas_vpb:
        #Receitas Irrecuperáveis
        if 'RECEITAS IRRECUPERÁVEIS DE ENCARGOS SETORIAIS' in df_sparta_vpb.iloc[linha,0].upper():
            df_ri.at[index,'RI_ENCARGOS_SETORIAIS_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'ENCARGOS DRP' in df_sparta_vpb.iloc[linha,0].upper():
            df_ri.at[index,'ENCARGOS_DRP_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'CARGA TRIBUTÁRIA (POR DENTRO)' in df_sparta_vpb.iloc[linha,0].upper():
            df_ri.at[index,'CARGA_TRIBUTARIA_PERCENT'] = df_sparta_vpb.iloc[linha,1]
        elif 'LIMITE RI ENCARGOS' in df_sparta_vpb.iloc[linha,0].upper():
            df_ri.at[index,'LIMITE_RI_ENCARGOS_PERCENT'] = df_sparta_vpb.iloc[linha,1]
        elif 'DEMAIS RECEITAS IRRECUPERÁVEIS (VSE)' in df_sparta_vpb.iloc[linha,0].upper():
            df_ri.at[index,'DEMAIS_RI_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'RECEITA ADICIONAL DE BANDEIRA (12 MESES)' in df_sparta_vpb.iloc[linha,0].upper():
            df_ri.at[index,'RECEITA_ADICIONAL_BANDEIRA_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'ENERGIA COMPRADA' in df_sparta_vpb.iloc[linha,0].upper():
            df_ri.at[index,'ENERGIA_COMPRADA_RS'] = df_sparta_vpb.iloc[linha,1]
        elif '(CT)' in df_sparta_vpb.iloc[linha,0].upper():
            df_ri.at[index,'TRANSPORTE_ENERGIA_RS'] = df_sparta_vpb.iloc[linha,1]
        elif 'LIMITE DEMAIS RI' in df_sparta_vpb.iloc[linha,0].upper():
            df_ri.at[index,'LIMITE_DEMAIS_RI_PERCENT'] = df_sparta_vpb.iloc[linha,1]
        elif 'RECEITAS IRRECUPERÁVEIS (TOTAL)' in df_sparta_vpb.iloc[linha,0].upper():
            df_ri.at[index,'RI_TOTAL_RS'] = df_sparta_vpb.iloc[linha,1]

        #Limite para Receitas Irrecuperáveis
        if ('RESIDENCIAL' in df_sparta_vpb.iloc[linha,0].upper()) and ('PARTICIPAÇÃO NO CONSUMO' in df_sparta_vpb.iloc[(linha-1),1].upper()):
            df_ri.at[index,'RESIDENCIAL_PARTIPACAO_CONSUMO_PERCENT'] = df_sparta_vpb.iloc[linha,1]
            df_ri.at[index,'RESIDENCIAL_LIMITE_DEMAIS_RI_PERCENT'] = df_sparta_vpb.iloc[linha,2]
            df_ri.at[index,'RESIDENCIAL_MEDIANA_INADIMPLENCIAS_PERCENT'] = df_sparta_vpb.iloc[linha,3]
            df_ri.at[index,'RESIDENCIAL_LIMITE_NEUTRALIDADE_PERCENT'] = df_sparta_vpb.iloc[linha,4]
            df_ri.at[index,'RESIDENCIAL_LIMITE_RI_ENCARGOS_PERCENT'] = df_sparta_vpb.iloc[linha,5]
        if ('INDUSTRIAL' in df_sparta_vpb.iloc[linha,0].upper()) and ('PARTICIPAÇÃO NO CONSUMO' in df_sparta_vpb.iloc[(linha-2),1].upper()):
            df_ri.at[index,'INDUSTRIAL_PARTIPACAO_CONSUMO_PERCENT'] = df_sparta_vpb.iloc[linha,1]
            df_ri.at[index,'INDUSTRIAL_LIMITE_DEMAIS_RI_PERCENT'] = df_sparta_vpb.iloc[linha,2]
            df_ri.at[index,'INDUSTRIAL_MEDIANA_INADIMPLENCIAS_PERCENT'] = df_sparta_vpb.iloc[linha,3]
            df_ri.at[index,'INDUSTRIAL_LIMITE_NEUTRALIDADE_PERCENT'] = df_sparta_vpb.iloc[linha,4]
            df_ri.at[index,'INDUSTRIAL_LIMITE_RI_ENCARGOS_PERCENT'] = df_sparta_vpb.iloc[linha,5]
        if 'COMERCIAL' in df_sparta_vpb.iloc[linha,0].upper() and ('PARTICIPAÇÃO NO CONSUMO' in df_sparta_vpb.iloc[(linha-3),1].upper()):
            df_ri.at[index,'COMERCIAL_PARTIPACAO_CONSUMO_PERCENT'] = df_sparta_vpb.iloc[linha,1]
            df_ri.at[index,'COMERCIAL_LIMITE_DEMAIS_RI_PERCENT'] = df_sparta_vpb.iloc[linha,2]
            df_ri.at[index,'COMERCIAL_MEDIANA_INADIMPLENCIAS_PERCENT'] = df_sparta_vpb.iloc[linha,3]
            df_ri.at[index,'COMERCIAL_LIMITE_NEUTRALIDADE_PERCENT'] = df_sparta_vpb.iloc[linha,4]
            df_ri.at[index,'COMERCIAL_LIMITE_RI_ENCARGOS_PERCENT'] = df_sparta_vpb.iloc[linha,5]
        if 'RURAL' in df_sparta_vpb.iloc[linha,0].upper() and ('PARTICIPAÇÃO NO CONSUMO' in df_sparta_vpb.iloc[(linha-4),1].upper()):
            df_ri.at[index,'RURAL_PARTIPACAO_CONSUMO_PERCENT'] = df_sparta_vpb.iloc[linha,1]
            df_ri.at[index,'RURAL_LIMITE_DEMAIS_RI_PERCENT'] = df_sparta_vpb.iloc[linha,2]
            df_ri.at[index,'RURAL_MEDIANA_INADIMPLENCIAS_PERCENT'] = df_sparta_vpb.iloc[linha,3]
            df_ri.at[index,'RURAL_LIMITE_NEUTRALIDADE_PERCENT'] = df_sparta_vpb.iloc[linha,4]
            df_ri.at[index,'RURAL_LIMITE_RI_ENCARGOS_PERCENT'] = df_sparta_vpb.iloc[linha,5]
        if 'PODER PÚBLICO' in df_sparta_vpb.iloc[linha,0].upper() and ('PARTICIPAÇÃO NO CONSUMO' in df_sparta_vpb.iloc[(linha-5),1].upper()):
            df_ri.at[index,'PODER_PUBLICO_PARTIPACAO_CONSUMO_PERCENT'] = df_sparta_vpb.iloc[linha,1]
            df_ri.at[index,'PODER_PUBLICO_LIMITE_DEMAIS_RI_PERCENT'] = df_sparta_vpb.iloc[linha,2]
            df_ri.at[index,'PODER_PUBLICO_MEDIANA_INADIMPLENCIAS_PERCENT'] = df_sparta_vpb.iloc[linha,3]
            df_ri.at[index,'PODER_PUBLICO_LIMITE_NEUTRALIDADE_PERCENT'] = df_sparta_vpb.iloc[linha,4]
            df_ri.at[index,'PODER_PUBLICO_LIMITE_RI_ENCARGOS_PERCENT'] = df_sparta_vpb.iloc[linha,5]
        if 'ILUMINAÇÃO PÚBLICA' in df_sparta_vpb.iloc[linha,0].upper() and ('PARTICIPAÇÃO NO CONSUMO' in df_sparta_vpb.iloc[(linha-6),1].upper()):
            df_ri.at[index,'ILUMINACAO_PUBLICA_PARTIPACAO_CONSUMO_PERCENT'] = df_sparta_vpb.iloc[linha,1]
            df_ri.at[index,'ILUMINACAO_PUBLICA_LIMITE_DEMAIS_RI_PERCENT'] = df_sparta_vpb.iloc[linha,2]
            df_ri.at[index,'ILUMINACAO_PUBLICA_MEDIANA_INADIMPLENCIAS_PERCENT'] = df_sparta_vpb.iloc[linha,3]
            df_ri.at[index,'ILUMINACAO_PUBLICA_LIMITE_NEUTRALIDADE_PERCENT'] = df_sparta_vpb.iloc[linha,4]
            df_ri.at[index,'ILUMINACAO_PUBLICA_LIMITE_RI_ENCARGOS_PERCENT'] = df_sparta_vpb.iloc[linha,5]
        if 'SERVIÇO PÚBLICO' in df_sparta_vpb.iloc[linha,0].upper() and ('PARTICIPAÇÃO NO CONSUMO' in df_sparta_vpb.iloc[(linha-7),1].upper()):
            df_ri.at[index,'SERVICO_PUBLICO_PARTIPACAO_CONSUMO_PERCENT'] = df_sparta_vpb.iloc[linha,1]
            df_ri.at[index,'SERVICO_PUBLICO_LIMITE_DEMAIS_RI_PERCENT'] = df_sparta_vpb.iloc[linha,2]
            df_ri.at[index,'SERVICO_PUBLICO_MEDIANA_INADIMPLENCIAS_PERCENT'] = df_sparta_vpb.iloc[linha,3]
            df_ri.at[index,'SERVICO_PUBLICO_LIMITE_NEUTRALIDADE_PERCENT'] = df_sparta_vpb.iloc[linha,4]
            df_ri.at[index,'SERVICO_PUBLICO_LIMITE_RI_ENCARGOS_PERCENT'] = df_sparta_vpb.iloc[linha,5]


def extrai_cor(df_cor,df_sparta_bd,index):
    for linha in linhas_bd:
        #Dados para Revisão
        if 'INF' in df_sparta_bd.iloc[linha,2].upper():
            df_cor.at[index,'THETA_INF'] = df_sparta_bd.iloc[linha,3]
        elif 'CENTRO' in df_sparta_bd.iloc[linha,2].upper():
            df_cor.at[index,'THETA_CENTRO'] = df_sparta_bd.iloc[linha,3]
        elif 'CO3' in df_sparta_bd.iloc[linha,2].upper():
            df_cor.at[index,'THETA_SUP'] = df_sparta_bd.iloc[linha,3]
        elif 'CO4' in df_sparta_bd.iloc[linha,2].upper():
            df_cor.at[index,'THETA_REF'] = df_sparta_bd.iloc[linha,3]
        elif 'CO5' in df_sparta_bd.iloc[linha,2].upper():
            df_cor.at[index,'OPEX_REAL'] = df_sparta_bd.iloc[linha,3]
        elif 'CO6' in df_sparta_bd.iloc[linha,2].upper():
            df_cor.at[index,'PMSO_CORRIGIDO'] = df_sparta_bd.iloc[linha,3]
        elif 'CO7' in df_sparta_bd.iloc[linha,2].upper():
            df_cor.at[index,'FATOR_ESCALA'] = df_sparta_bd.iloc[linha,3]
        elif 'CO8' in df_sparta_bd.iloc[linha,2].upper():
            df_cor.at[index,'U'] = df_sparta_bd.iloc[linha,3]
        elif 'CO9' in df_sparta_bd.iloc[linha,2].upper():
            df_cor.at[index,'VS_YRSUB'] = df_sparta_bd.iloc[linha,3]
        if 'CO10' in df_sparta_bd.iloc[linha,2].upper():
            df_cor.at[index,'VS_YRDIST_A'] = df_sparta_bd.iloc[linha,3]
        if 'CO11' in df_sparta_bd.iloc[linha,2].upper():
            df_cor.at[index,'VS_YRALTA'] = df_sparta_bd.iloc[linha,3]
        if 'CO12' in df_sparta_bd.iloc[linha,2].upper():
            df_cor.at[index,'VS_YCONS'] = df_sparta_bd.iloc[linha,3]
        if 'CO13' in df_sparta_bd.iloc[linha,2].upper():
            df_cor.at[index,'VS_YMPONDERADO'] = df_sparta_bd.iloc[linha,3]
        if 'CO14' in df_sparta_bd.iloc[linha,2].upper():
            df_cor.at[index,'VS_YD_PERDAS_DIF2'] = df_sparta_bd.iloc[linha,3]
        if 'CO15' in df_sparta_bd.iloc[linha,2].upper():
            df_cor.at[index,'VS_YD_DEC_V8'] = df_sparta_bd.iloc[linha,3]
        if 'CO16' in df_sparta_bd.iloc[linha,2].upper():
            df_cor.at[index,'P_AT'] = df_sparta_bd.iloc[linha,3]
        if 'CO17' in df_sparta_bd.iloc[linha,2].upper():
            df_cor.at[index,'P_MT'] = df_sparta_bd.iloc[linha,3]
        if 'CO18' in df_sparta_bd.iloc[linha,2].upper():
            df_cor.at[index,'P_BT'] = df_sparta_bd.iloc[linha,3]
        if 'CO19' in df_sparta_bd.iloc[linha,2].upper():
            df_cor.at[index,'PMSO_2013'] = df_sparta_bd.iloc[linha,3]
        if 'CO20' in df_sparta_bd.iloc[linha,2].upper():
            df_cor.at[index,'CONT_ASSOCIATIVA'] = df_sparta_bd.iloc[linha,3]
        elif 'PESOAT' in df_sparta_bd.iloc[linha,2].upper():
            df_cor.at[index,'PESO_AT_PERCENT'] = df_sparta_bd.iloc[linha,3]
        elif 'PESOMT' in df_sparta_bd.iloc[linha,2].upper():
            df_cor.at[index,'PESO_MT_PERCENT'] = df_sparta_bd.iloc[linha,3]
        elif 'PESOBT' in df_sparta_bd.iloc[linha,2].upper():
            df_cor.at[index,'PESO_BT_PERCENT'] = df_sparta_bd.iloc[linha,3]


def distribuidora_aj_co_calculo_comp_t(df_aj_co_calculo_comp_t,df_sparta_capa,index):
    #Determina o ANO da SPARTA
    df_aj_co_calculo_comp_t.at[index,'ANO'] = df_sparta_capa.iloc[8,1].strftime('%Y')
    
    #Determina o ID da Distribuidora
    df_aj_co_calculo_comp_t.at[index,'ID'] = df_sparta_capa.iloc[1,1]
    
    #Determina o NOME da Distribuidora
    df_aj_co_calculo_comp_t.at[index,'DISTRIBUIDORA'] = df_sparta_capa.iloc[0,0].upper()
    
    #Determina a DATA da SPARTA
    df_aj_co_calculo_comp_t.at[index,'DATA'] = df_sparta_capa.iloc[8,1].strftime('%Y-%m-%d')
    
    #Determina o EVENTO TARIFARIO
    if 'Reajuste' in df_sparta_capa.iloc[0,1]:
        df_aj_co_calculo_comp_t.at[index,'EVENTO_TARIFARIO'] = 'RTA'
    elif 'Revisão' in df_sparta_capa.iloc[0,1]:
        df_aj_co_calculo_comp_t.at[index,'EVENTO_TARIFARIO'] = 'RTP'
    else:
        df_aj_co_calculo_comp_t.at[index,'EVENTO_TARIFARIO'] = 'RTE'
        
    #Determina a CHAVE
    df_aj_co_calculo_comp_t.at[index,'CHAVE'] = df_aj_co_calculo_comp_t.loc[index,'EVENTO_TARIFARIO']+df_aj_co_calculo_comp_t.loc[index,'ANO']+df_aj_co_calculo_comp_t.loc[index,'ID']
    
    #Determina UF e PERIODO TARIFARIO
    #AES SUL
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D01':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'RS'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #AME
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D02':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'AM'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #AMPLA
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D03':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'RJ'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #BANDEIRANTE
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D04':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'SP'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #BOA VISTA
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D05':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'RR'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CAIUA
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D06':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'SP'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEA
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D07':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'AP'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CEAL
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D08':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'AL'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CEB
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D09':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'DF'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '4'
    
    #CEEE
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D10':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'RS'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
      
    #CELESC
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D11':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'SC'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CELG
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D12':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'GO'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CELPA
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D13':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'PA'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CELPE
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D14':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'PE'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CELTINS
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D15':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'TO'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEMAR
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D16':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'MA'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #EMT
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D17':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'MT'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEMIG-D
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D18':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'MG'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEPISA
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D19':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'PI'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CERON
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D20':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'RO'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CERR (CONFIMAR O PERIODO)
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D21':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'RR'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CFLO
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D22':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'PR'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CHESP
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D23':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'GO'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL JAGUARI
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D24':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'SP'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL MOCOCA
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D25':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'SP'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'        
    
    #CPFL Santa Cruz
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D26':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'SP'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CNEE
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D27':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'SP'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COCEL
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D28':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'PR'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COELBA
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D29':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'BA'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COELCE
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D30':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'CE'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #COOPERALIANÇA
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D31':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'SC'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COPEL
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D32':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'PR'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COSERN
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D33':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'RN'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Leste Paulista
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D34':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'SP'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Piratininga
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D35':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'SP'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CPFL Paulista
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D36':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'SP'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Sul Paulista
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D37':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'SP'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #DEMEI
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D38':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'RS'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #DME-PC
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D39':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'MG'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EBO
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D40':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'PB'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #EDEVP
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D41':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'SP'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EEB
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D42':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'SP'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EFLJC
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D43':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'SC'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EFLUL
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D44':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'SC'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ELEKTRO
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D45':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'SP'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELETROACRE
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D46':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'AC'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELETROCAR
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D47':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'RS'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ELETROPAULO
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D48':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'SP'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELFSM
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D49':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'ES'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EMG
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D50':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'MG'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EMS
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D51':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'MS'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ENF
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D52':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'RJ'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EPB
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D53':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'PB'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ESCELSA (CONFIRMAR PERIODO)
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D54':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'ES'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '3'
        
    #ESE
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D55':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'SE'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #FORCEL
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D56':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'PR'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #HIDROPAN
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D57':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'RS'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #IENERGIA
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D58':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'SC'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #JARI (CONFIRMAR PERIODO)
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D59':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'PA'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #LIGHT
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D60':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'RJ'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #MUX-Energia
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D61':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'RS'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
    
    #CPFL RGE
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D62':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'RS'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #SULGIPE
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D63':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'SE'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #UHENPAL
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D64':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'TO'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ESS
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D65':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'SP'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Santa Cruz
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D66':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'SP'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #RGE
    if df_aj_co_calculo_comp_t.loc[index,'ID'] == 'D67':
        df_aj_co_calculo_comp_t.at[index,'UF'] = 'RS'
        df_aj_co_calculo_comp_t.at[index,'PERIODO_TARIFARIO'] = '5'

   
def distribuidora_caa(df_caa,df_sparta_capa,index):
    #Determina o ANO da SPARTA
    df_caa.at[index,'ANO'] = df_sparta_capa.iloc[8,1].strftime('%Y')
    
    #Determina o ID da Distribuidora
    df_caa.at[index,'ID'] = df_sparta_capa.iloc[1,1]
    
    #Determina o NOME da Distribuidora
    df_caa.at[index,'DISTRIBUIDORA'] = df_sparta_capa.iloc[0,0].upper()
    
    #Determina a DATA da SPARTA
    df_caa.at[index,'DATA'] = df_sparta_capa.iloc[8,1].strftime('%Y-%m-%d')
    
    #Determina o EVENTO TARIFARIO
    if 'Reajuste' in df_sparta_capa.iloc[0,1]:
        df_caa.at[index,'EVENTO_TARIFARIO'] = 'RTA'
    elif 'Revisão' in df_sparta_capa.iloc[0,1]:
        df_caa.at[index,'EVENTO_TARIFARIO'] = 'RTP'
    else:
        df_caa.at[index,'EVENTO_TARIFARIO'] = 'RTE'
        
    #Determina a CHAVE
    df_caa.at[index,'CHAVE'] = df_caa.loc[index,'EVENTO_TARIFARIO']+df_caa.loc[index,'ANO']+df_caa.loc[index,'ID']
    
    #Determina UF e PERIODO TARIFARIO
    #AES SUL
    if df_caa.loc[index,'ID'] == 'D01':
        df_caa.at[index,'UF'] = 'RS'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #AME
    if df_caa.loc[index,'ID'] == 'D02':
        df_caa.at[index,'UF'] = 'AM'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #AMPLA
    if df_caa.loc[index,'ID'] == 'D03':
        df_caa.at[index,'UF'] = 'RJ'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #BANDEIRANTE
    if df_caa.loc[index,'ID'] == 'D04':
        df_caa.at[index,'UF'] = 'SP'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #BOA VISTA
    if df_caa.loc[index,'ID'] == 'D05':
        df_caa.at[index,'UF'] = 'RR'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CAIUA
    if df_caa.loc[index,'ID'] == 'D06':
        df_caa.at[index,'UF'] = 'SP'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEA
    if df_caa.loc[index,'ID'] == 'D07':
        df_caa.at[index,'UF'] = 'AP'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CEAL
    if df_caa.loc[index,'ID'] == 'D08':
        df_caa.at[index,'UF'] = 'AL'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CEB
    if df_caa.loc[index,'ID'] == 'D09':
        df_caa.at[index,'UF'] = 'DF'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '4'
    
    #CEEE
    if df_caa.loc[index,'ID'] == 'D10':
        df_caa.at[index,'UF'] = 'RS'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
      
    #CELESC
    if df_caa.loc[index,'ID'] == 'D11':
        df_caa.at[index,'UF'] = 'SC'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CELG
    if df_caa.loc[index,'ID'] == 'D12':
        df_caa.at[index,'UF'] = 'GO'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CELPA
    if df_caa.loc[index,'ID'] == 'D13':
        df_caa.at[index,'UF'] = 'PA'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CELPE
    if df_caa.loc[index,'ID'] == 'D14':
        df_caa.at[index,'UF'] = 'PE'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CELTINS
    if df_caa.loc[index,'ID'] == 'D15':
        df_caa.at[index,'UF'] = 'TO'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEMAR
    if df_caa.loc[index,'ID'] == 'D16':
        df_caa.at[index,'UF'] = 'MA'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #EMT
    if df_caa.loc[index,'ID'] == 'D17':
        df_caa.at[index,'UF'] = 'MT'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEMIG-D
    if df_caa.loc[index,'ID'] == 'D18':
        df_caa.at[index,'UF'] = 'MG'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEPISA
    if df_caa.loc[index,'ID'] == 'D19':
        df_caa.at[index,'UF'] = 'PI'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CERON
    if df_caa.loc[index,'ID'] == 'D20':
        df_caa.at[index,'UF'] = 'RO'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CERR (CONFIMAR O PERIODO)
    if df_caa.loc[index,'ID'] == 'D21':
        df_caa.at[index,'UF'] = 'RR'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CFLO
    if df_caa.loc[index,'ID'] == 'D22':
        df_caa.at[index,'UF'] = 'PR'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CHESP
    if df_caa.loc[index,'ID'] == 'D23':
        df_caa.at[index,'UF'] = 'GO'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL JAGUARI
    if df_caa.loc[index,'ID'] == 'D24':
        df_caa.at[index,'UF'] = 'SP'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL MOCOCA
    if df_caa.loc[index,'ID'] == 'D25':
        df_caa.at[index,'UF'] = 'SP'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'        
    
    #CPFL Santa Cruz
    if df_caa.loc[index,'ID'] == 'D26':
        df_caa.at[index,'UF'] = 'SP'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CNEE
    if df_caa.loc[index,'ID'] == 'D27':
        df_caa.at[index,'UF'] = 'SP'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COCEL
    if df_caa.loc[index,'ID'] == 'D28':
        df_caa.at[index,'UF'] = 'PR'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COELBA
    if df_caa.loc[index,'ID'] == 'D29':
        df_caa.at[index,'UF'] = 'BA'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COELCE
    if df_caa.loc[index,'ID'] == 'D30':
        df_caa.at[index,'UF'] = 'CE'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #COOPERALIANÇA
    if df_caa.loc[index,'ID'] == 'D31':
        df_caa.at[index,'UF'] = 'SC'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COPEL
    if df_caa.loc[index,'ID'] == 'D32':
        df_caa.at[index,'UF'] = 'PR'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COSERN
    if df_caa.loc[index,'ID'] == 'D33':
        df_caa.at[index,'UF'] = 'RN'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Leste Paulista
    if df_caa.loc[index,'ID'] == 'D34':
        df_caa.at[index,'UF'] = 'SP'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Piratininga
    if df_caa.loc[index,'ID'] == 'D35':
        df_caa.at[index,'UF'] = 'SP'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CPFL Paulista
    if df_caa.loc[index,'ID'] == 'D36':
        df_caa.at[index,'UF'] = 'SP'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Sul Paulista
    if df_caa.loc[index,'ID'] == 'D37':
        df_caa.at[index,'UF'] = 'SP'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #DEMEI
    if df_caa.loc[index,'ID'] == 'D38':
        df_caa.at[index,'UF'] = 'RS'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #DME-PC
    if df_caa.loc[index,'ID'] == 'D39':
        df_caa.at[index,'UF'] = 'MG'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EBO
    if df_caa.loc[index,'ID'] == 'D40':
        df_caa.at[index,'UF'] = 'PB'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #EDEVP
    if df_caa.loc[index,'ID'] == 'D41':
        df_caa.at[index,'UF'] = 'SP'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EEB
    if df_caa.loc[index,'ID'] == 'D42':
        df_caa.at[index,'UF'] = 'SP'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EFLJC
    if df_caa.loc[index,'ID'] == 'D43':
        df_caa.at[index,'UF'] = 'SC'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EFLUL
    if df_caa.loc[index,'ID'] == 'D44':
        df_caa.at[index,'UF'] = 'SC'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ELEKTRO
    if df_caa.loc[index,'ID'] == 'D45':
        df_caa.at[index,'UF'] = 'SP'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELETROACRE
    if df_caa.loc[index,'ID'] == 'D46':
        df_caa.at[index,'UF'] = 'AC'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELETROCAR
    if df_caa.loc[index,'ID'] == 'D47':
        df_caa.at[index,'UF'] = 'RS'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ELETROPAULO
    if df_caa.loc[index,'ID'] == 'D48':
        df_caa.at[index,'UF'] = 'SP'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELFSM
    if df_caa.loc[index,'ID'] == 'D49':
        df_caa.at[index,'UF'] = 'ES'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EMG
    if df_caa.loc[index,'ID'] == 'D50':
        df_caa.at[index,'UF'] = 'MG'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EMS
    if df_caa.loc[index,'ID'] == 'D51':
        df_caa.at[index,'UF'] = 'MS'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ENF
    if df_caa.loc[index,'ID'] == 'D52':
        df_caa.at[index,'UF'] = 'RJ'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EPB
    if df_caa.loc[index,'ID'] == 'D53':
        df_caa.at[index,'UF'] = 'PB'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ESCELSA (CONFIRMAR PERIODO)
    if df_caa.loc[index,'ID'] == 'D54':
        df_caa.at[index,'UF'] = 'ES'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '3'
        
    #ESE
    if df_caa.loc[index,'ID'] == 'D55':
        df_caa.at[index,'UF'] = 'SE'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #FORCEL
    if df_caa.loc[index,'ID'] == 'D56':
        df_caa.at[index,'UF'] = 'PR'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #HIDROPAN
    if df_caa.loc[index,'ID'] == 'D57':
        df_caa.at[index,'UF'] = 'RS'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #IENERGIA
    if df_caa.loc[index,'ID'] == 'D58':
        df_caa.at[index,'UF'] = 'SC'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #JARI (CONFIRMAR PERIODO)
    if df_caa.loc[index,'ID'] == 'D59':
        df_caa.at[index,'UF'] = 'PA'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #LIGHT
    if df_caa.loc[index,'ID'] == 'D60':
        df_caa.at[index,'UF'] = 'RJ'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #MUX-Energia
    if df_caa.loc[index,'ID'] == 'D61':
        df_caa.at[index,'UF'] = 'RS'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
    
    #CPFL RGE
    if df_caa.loc[index,'ID'] == 'D62':
        df_caa.at[index,'UF'] = 'RS'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #SULGIPE
    if df_caa.loc[index,'ID'] == 'D63':
        df_caa.at[index,'UF'] = 'SE'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #UHENPAL
    if df_caa.loc[index,'ID'] == 'D64':
        df_caa.at[index,'UF'] = 'TO'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ESS
    if df_caa.loc[index,'ID'] == 'D65':
        df_caa.at[index,'UF'] = 'SP'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Santa Cruz
    if df_caa.loc[index,'ID'] == 'D66':
        df_caa.at[index,'UF'] = 'SP'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #RGE
    if df_caa.loc[index,'ID'] == 'D67':
        df_caa.at[index,'UF'] = 'RS'
        df_caa.at[index,'PERIODO_TARIFARIO'] = '5'


   
def distribuidora_caom(df_caom,df_sparta_capa,index):
    #Determina o ANO da SPARTA
    df_caom.at[index,'ANO'] = df_sparta_capa.iloc[8,1].strftime('%Y')
    
    #Determina o ID da Distribuidora
    df_caom.at[index,'ID'] = df_sparta_capa.iloc[1,1]
    
    #Determina o NOME da Distribuidora
    df_caom.at[index,'DISTRIBUIDORA'] = df_sparta_capa.iloc[0,0].upper()
    
    #Determina a DATA da SPARTA
    df_caom.at[index,'DATA'] = df_sparta_capa.iloc[8,1].strftime('%Y-%m-%d')
    
    #Determina o EVENTO TARIFARIO
    if 'Reajuste' in df_sparta_capa.iloc[0,1]:
        df_caom.at[index,'EVENTO_TARIFARIO'] = 'RTA'
    elif 'Revisão' in df_sparta_capa.iloc[0,1]:
        df_caom.at[index,'EVENTO_TARIFARIO'] = 'RTP'
    else:
        df_caom.at[index,'EVENTO_TARIFARIO'] = 'RTE'
        
    #Determina a CHAVE
    df_caom.at[index,'CHAVE'] = df_caom.loc[index,'EVENTO_TARIFARIO']+df_caom.loc[index,'ANO']+df_caom.loc[index,'ID']
    
    #Determina UF e PERIODO TARIFARIO
    #AES SUL
    if df_caom.loc[index,'ID'] == 'D01':
        df_caom.at[index,'UF'] = 'RS'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #AME
    if df_caom.loc[index,'ID'] == 'D02':
        df_caom.at[index,'UF'] = 'AM'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #AMPLA
    if df_caom.loc[index,'ID'] == 'D03':
        df_caom.at[index,'UF'] = 'RJ'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #BANDEIRANTE
    if df_caom.loc[index,'ID'] == 'D04':
        df_caom.at[index,'UF'] = 'SP'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #BOA VISTA
    if df_caom.loc[index,'ID'] == 'D05':
        df_caom.at[index,'UF'] = 'RR'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CAIUA
    if df_caom.loc[index,'ID'] == 'D06':
        df_caom.at[index,'UF'] = 'SP'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEA
    if df_caom.loc[index,'ID'] == 'D07':
        df_caom.at[index,'UF'] = 'AP'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CEAL
    if df_caom.loc[index,'ID'] == 'D08':
        df_caom.at[index,'UF'] = 'AL'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CEB
    if df_caom.loc[index,'ID'] == 'D09':
        df_caom.at[index,'UF'] = 'DF'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '4'
    
    #CEEE
    if df_caom.loc[index,'ID'] == 'D10':
        df_caom.at[index,'UF'] = 'RS'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
      
    #CELESC
    if df_caom.loc[index,'ID'] == 'D11':
        df_caom.at[index,'UF'] = 'SC'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CELG
    if df_caom.loc[index,'ID'] == 'D12':
        df_caom.at[index,'UF'] = 'GO'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CELPA
    if df_caom.loc[index,'ID'] == 'D13':
        df_caom.at[index,'UF'] = 'PA'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CELPE
    if df_caom.loc[index,'ID'] == 'D14':
        df_caom.at[index,'UF'] = 'PE'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CELTINS
    if df_caom.loc[index,'ID'] == 'D15':
        df_caom.at[index,'UF'] = 'TO'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEMAR
    if df_caom.loc[index,'ID'] == 'D16':
        df_caom.at[index,'UF'] = 'MA'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #EMT
    if df_caom.loc[index,'ID'] == 'D17':
        df_caom.at[index,'UF'] = 'MT'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEMIG-D
    if df_caom.loc[index,'ID'] == 'D18':
        df_caom.at[index,'UF'] = 'MG'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEPISA
    if df_caom.loc[index,'ID'] == 'D19':
        df_caom.at[index,'UF'] = 'PI'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CERON
    if df_caom.loc[index,'ID'] == 'D20':
        df_caom.at[index,'UF'] = 'RO'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CERR (CONFIMAR O PERIODO)
    if df_caom.loc[index,'ID'] == 'D21':
        df_caom.at[index,'UF'] = 'RR'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CFLO
    if df_caom.loc[index,'ID'] == 'D22':
        df_caom.at[index,'UF'] = 'PR'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CHESP
    if df_caom.loc[index,'ID'] == 'D23':
        df_caom.at[index,'UF'] = 'GO'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL JAGUARI
    if df_caom.loc[index,'ID'] == 'D24':
        df_caom.at[index,'UF'] = 'SP'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL MOCOCA
    if df_caom.loc[index,'ID'] == 'D25':
        df_caom.at[index,'UF'] = 'SP'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'        
    
    #CPFL Santa Cruz
    if df_caom.loc[index,'ID'] == 'D26':
        df_caom.at[index,'UF'] = 'SP'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CNEE
    if df_caom.loc[index,'ID'] == 'D27':
        df_caom.at[index,'UF'] = 'SP'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COCEL
    if df_caom.loc[index,'ID'] == 'D28':
        df_caom.at[index,'UF'] = 'PR'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COELBA
    if df_caom.loc[index,'ID'] == 'D29':
        df_caom.at[index,'UF'] = 'BA'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COELCE
    if df_caom.loc[index,'ID'] == 'D30':
        df_caom.at[index,'UF'] = 'CE'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #COOPERALIANÇA
    if df_caom.loc[index,'ID'] == 'D31':
        df_caom.at[index,'UF'] = 'SC'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COPEL
    if df_caom.loc[index,'ID'] == 'D32':
        df_caom.at[index,'UF'] = 'PR'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COSERN
    if df_caom.loc[index,'ID'] == 'D33':
        df_caom.at[index,'UF'] = 'RN'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Leste Paulista
    if df_caom.loc[index,'ID'] == 'D34':
        df_caom.at[index,'UF'] = 'SP'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Piratininga
    if df_caom.loc[index,'ID'] == 'D35':
        df_caom.at[index,'UF'] = 'SP'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CPFL Paulista
    if df_caom.loc[index,'ID'] == 'D36':
        df_caom.at[index,'UF'] = 'SP'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Sul Paulista
    if df_caom.loc[index,'ID'] == 'D37':
        df_caom.at[index,'UF'] = 'SP'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #DEMEI
    if df_caom.loc[index,'ID'] == 'D38':
        df_caom.at[index,'UF'] = 'RS'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #DME-PC
    if df_caom.loc[index,'ID'] == 'D39':
        df_caom.at[index,'UF'] = 'MG'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EBO
    if df_caom.loc[index,'ID'] == 'D40':
        df_caom.at[index,'UF'] = 'PB'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #EDEVP
    if df_caom.loc[index,'ID'] == 'D41':
        df_caom.at[index,'UF'] = 'SP'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EEB
    if df_caom.loc[index,'ID'] == 'D42':
        df_caom.at[index,'UF'] = 'SP'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EFLJC
    if df_caom.loc[index,'ID'] == 'D43':
        df_caom.at[index,'UF'] = 'SC'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EFLUL
    if df_caom.loc[index,'ID'] == 'D44':
        df_caom.at[index,'UF'] = 'SC'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ELEKTRO
    if df_caom.loc[index,'ID'] == 'D45':
        df_caom.at[index,'UF'] = 'SP'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELETROACRE
    if df_caom.loc[index,'ID'] == 'D46':
        df_caom.at[index,'UF'] = 'AC'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELETROCAR
    if df_caom.loc[index,'ID'] == 'D47':
        df_caom.at[index,'UF'] = 'RS'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ELETROPAULO
    if df_caom.loc[index,'ID'] == 'D48':
        df_caom.at[index,'UF'] = 'SP'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELFSM
    if df_caom.loc[index,'ID'] == 'D49':
        df_caom.at[index,'UF'] = 'ES'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EMG
    if df_caom.loc[index,'ID'] == 'D50':
        df_caom.at[index,'UF'] = 'MG'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EMS
    if df_caom.loc[index,'ID'] == 'D51':
        df_caom.at[index,'UF'] = 'MS'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ENF
    if df_caom.loc[index,'ID'] == 'D52':
        df_caom.at[index,'UF'] = 'RJ'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EPB
    if df_caom.loc[index,'ID'] == 'D53':
        df_caom.at[index,'UF'] = 'PB'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ESCELSA (CONFIRMAR PERIODO)
    if df_caom.loc[index,'ID'] == 'D54':
        df_caom.at[index,'UF'] = 'ES'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '3'
        
    #ESE
    if df_caom.loc[index,'ID'] == 'D55':
        df_caom.at[index,'UF'] = 'SE'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #FORCEL
    if df_caom.loc[index,'ID'] == 'D56':
        df_caom.at[index,'UF'] = 'PR'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #HIDROPAN
    if df_caom.loc[index,'ID'] == 'D57':
        df_caom.at[index,'UF'] = 'RS'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #IENERGIA
    if df_caom.loc[index,'ID'] == 'D58':
        df_caom.at[index,'UF'] = 'SC'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #JARI (CONFIRMAR PERIODO)
    if df_caom.loc[index,'ID'] == 'D59':
        df_caom.at[index,'UF'] = 'PA'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #LIGHT
    if df_caom.loc[index,'ID'] == 'D60':
        df_caom.at[index,'UF'] = 'RJ'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #MUX-Energia
    if df_caom.loc[index,'ID'] == 'D61':
        df_caom.at[index,'UF'] = 'RS'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
    
    #CPFL RGE
    if df_caom.loc[index,'ID'] == 'D62':
        df_caom.at[index,'UF'] = 'RS'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #SULGIPE
    if df_caom.loc[index,'ID'] == 'D63':
        df_caom.at[index,'UF'] = 'SE'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #UHENPAL
    if df_caom.loc[index,'ID'] == 'D64':
        df_caom.at[index,'UF'] = 'TO'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ESS
    if df_caom.loc[index,'ID'] == 'D65':
        df_caom.at[index,'UF'] = 'SP'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Santa Cruz
    if df_caom.loc[index,'ID'] == 'D66':
        df_caom.at[index,'UF'] = 'SP'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #RGE
    if df_caom.loc[index,'ID'] == 'D67':
        df_caom.at[index,'UF'] = 'RS'
        df_caom.at[index,'PERIODO_TARIFARIO'] = '5'

   

   
def distribuidora_ri(df_ri,df_sparta_capa,index):
    #Determina o ANO da SPARTA
    df_ri.at[index,'ANO'] = df_sparta_capa.iloc[8,1].strftime('%Y')
    
    #Determina o ID da Distribuidora
    df_ri.at[index,'ID'] = df_sparta_capa.iloc[1,1]
    
    #Determina o NOME da Distribuidora
    df_ri.at[index,'DISTRIBUIDORA'] = df_sparta_capa.iloc[0,0].upper()
    
    #Determina a DATA da SPARTA
    df_ri.at[index,'DATA'] = df_sparta_capa.iloc[8,1].strftime('%Y-%m-%d')
    
    #Determina o EVENTO TARIFARIO
    if 'Reajuste' in df_sparta_capa.iloc[0,1]:
        df_ri.at[index,'EVENTO_TARIFARIO'] = 'RTA'
    elif 'Revisão' in df_sparta_capa.iloc[0,1]:
        df_ri.at[index,'EVENTO_TARIFARIO'] = 'RTP'
    else:
        df_ri.at[index,'EVENTO_TARIFARIO'] = 'RTE'
        
    #Determina a CHAVE
    df_ri.at[index,'CHAVE'] = df_ri.loc[index,'EVENTO_TARIFARIO']+df_ri.loc[index,'ANO']+df_ri.loc[index,'ID']
    
    #Determina UF e PERIODO TARIFARIO
    #AES SUL
    if df_ri.loc[index,'ID'] == 'D01':
        df_ri.at[index,'UF'] = 'RS'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #AME
    if df_ri.loc[index,'ID'] == 'D02':
        df_ri.at[index,'UF'] = 'AM'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #AMPLA
    if df_ri.loc[index,'ID'] == 'D03':
        df_ri.at[index,'UF'] = 'RJ'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #BANDEIRANTE
    if df_ri.loc[index,'ID'] == 'D04':
        df_ri.at[index,'UF'] = 'SP'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #BOA VISTA
    if df_ri.loc[index,'ID'] == 'D05':
        df_ri.at[index,'UF'] = 'RR'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CAIUA
    if df_ri.loc[index,'ID'] == 'D06':
        df_ri.at[index,'UF'] = 'SP'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEA
    if df_ri.loc[index,'ID'] == 'D07':
        df_ri.at[index,'UF'] = 'AP'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CEAL
    if df_ri.loc[index,'ID'] == 'D08':
        df_ri.at[index,'UF'] = 'AL'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CEB
    if df_ri.loc[index,'ID'] == 'D09':
        df_ri.at[index,'UF'] = 'DF'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '4'
    
    #CEEE
    if df_ri.loc[index,'ID'] == 'D10':
        df_ri.at[index,'UF'] = 'RS'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
      
    #CELESC
    if df_ri.loc[index,'ID'] == 'D11':
        df_ri.at[index,'UF'] = 'SC'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CELG
    if df_ri.loc[index,'ID'] == 'D12':
        df_ri.at[index,'UF'] = 'GO'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CELPA
    if df_ri.loc[index,'ID'] == 'D13':
        df_ri.at[index,'UF'] = 'PA'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CELPE
    if df_ri.loc[index,'ID'] == 'D14':
        df_ri.at[index,'UF'] = 'PE'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CELTINS
    if df_ri.loc[index,'ID'] == 'D15':
        df_ri.at[index,'UF'] = 'TO'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEMAR
    if df_ri.loc[index,'ID'] == 'D16':
        df_ri.at[index,'UF'] = 'MA'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #EMT
    if df_ri.loc[index,'ID'] == 'D17':
        df_ri.at[index,'UF'] = 'MT'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEMIG-D
    if df_ri.loc[index,'ID'] == 'D18':
        df_ri.at[index,'UF'] = 'MG'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEPISA
    if df_ri.loc[index,'ID'] == 'D19':
        df_ri.at[index,'UF'] = 'PI'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CERON
    if df_ri.loc[index,'ID'] == 'D20':
        df_ri.at[index,'UF'] = 'RO'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CERR (CONFIMAR O PERIODO)
    if df_ri.loc[index,'ID'] == 'D21':
        df_ri.at[index,'UF'] = 'RR'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CFLO
    if df_ri.loc[index,'ID'] == 'D22':
        df_ri.at[index,'UF'] = 'PR'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CHESP
    if df_ri.loc[index,'ID'] == 'D23':
        df_ri.at[index,'UF'] = 'GO'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL JAGUARI
    if df_ri.loc[index,'ID'] == 'D24':
        df_ri.at[index,'UF'] = 'SP'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL MOCOCA
    if df_ri.loc[index,'ID'] == 'D25':
        df_ri.at[index,'UF'] = 'SP'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'        
    
    #CPFL Santa Cruz
    if df_ri.loc[index,'ID'] == 'D26':
        df_ri.at[index,'UF'] = 'SP'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CNEE
    if df_ri.loc[index,'ID'] == 'D27':
        df_ri.at[index,'UF'] = 'SP'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COCEL
    if df_ri.loc[index,'ID'] == 'D28':
        df_ri.at[index,'UF'] = 'PR'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COELBA
    if df_ri.loc[index,'ID'] == 'D29':
        df_ri.at[index,'UF'] = 'BA'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COELCE
    if df_ri.loc[index,'ID'] == 'D30':
        df_ri.at[index,'UF'] = 'CE'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #COOPERALIANÇA
    if df_ri.loc[index,'ID'] == 'D31':
        df_ri.at[index,'UF'] = 'SC'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COPEL
    if df_ri.loc[index,'ID'] == 'D32':
        df_ri.at[index,'UF'] = 'PR'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COSERN
    if df_ri.loc[index,'ID'] == 'D33':
        df_ri.at[index,'UF'] = 'RN'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Leste Paulista
    if df_ri.loc[index,'ID'] == 'D34':
        df_ri.at[index,'UF'] = 'SP'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Piratininga
    if df_ri.loc[index,'ID'] == 'D35':
        df_ri.at[index,'UF'] = 'SP'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CPFL Paulista
    if df_ri.loc[index,'ID'] == 'D36':
        df_ri.at[index,'UF'] = 'SP'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Sul Paulista
    if df_ri.loc[index,'ID'] == 'D37':
        df_ri.at[index,'UF'] = 'SP'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #DEMEI
    if df_ri.loc[index,'ID'] == 'D38':
        df_ri.at[index,'UF'] = 'RS'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #DME-PC
    if df_ri.loc[index,'ID'] == 'D39':
        df_ri.at[index,'UF'] = 'MG'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EBO
    if df_ri.loc[index,'ID'] == 'D40':
        df_ri.at[index,'UF'] = 'PB'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #EDEVP
    if df_ri.loc[index,'ID'] == 'D41':
        df_ri.at[index,'UF'] = 'SP'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EEB
    if df_ri.loc[index,'ID'] == 'D42':
        df_ri.at[index,'UF'] = 'SP'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EFLJC
    if df_ri.loc[index,'ID'] == 'D43':
        df_ri.at[index,'UF'] = 'SC'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EFLUL
    if df_ri.loc[index,'ID'] == 'D44':
        df_ri.at[index,'UF'] = 'SC'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ELEKTRO
    if df_ri.loc[index,'ID'] == 'D45':
        df_ri.at[index,'UF'] = 'SP'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELETROACRE
    if df_ri.loc[index,'ID'] == 'D46':
        df_ri.at[index,'UF'] = 'AC'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELETROCAR
    if df_ri.loc[index,'ID'] == 'D47':
        df_ri.at[index,'UF'] = 'RS'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ELETROPAULO
    if df_ri.loc[index,'ID'] == 'D48':
        df_ri.at[index,'UF'] = 'SP'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELFSM
    if df_ri.loc[index,'ID'] == 'D49':
        df_ri.at[index,'UF'] = 'ES'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EMG
    if df_ri.loc[index,'ID'] == 'D50':
        df_ri.at[index,'UF'] = 'MG'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EMS
    if df_ri.loc[index,'ID'] == 'D51':
        df_ri.at[index,'UF'] = 'MS'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ENF
    if df_ri.loc[index,'ID'] == 'D52':
        df_ri.at[index,'UF'] = 'RJ'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EPB
    if df_ri.loc[index,'ID'] == 'D53':
        df_ri.at[index,'UF'] = 'PB'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ESCELSA (CONFIRMAR PERIODO)
    if df_ri.loc[index,'ID'] == 'D54':
        df_ri.at[index,'UF'] = 'ES'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '3'
        
    #ESE
    if df_ri.loc[index,'ID'] == 'D55':
        df_ri.at[index,'UF'] = 'SE'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #FORCEL
    if df_ri.loc[index,'ID'] == 'D56':
        df_ri.at[index,'UF'] = 'PR'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #HIDROPAN
    if df_ri.loc[index,'ID'] == 'D57':
        df_ri.at[index,'UF'] = 'RS'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #IENERGIA
    if df_ri.loc[index,'ID'] == 'D58':
        df_ri.at[index,'UF'] = 'SC'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #JARI (CONFIRMAR PERIODO)
    if df_ri.loc[index,'ID'] == 'D59':
        df_ri.at[index,'UF'] = 'PA'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #LIGHT
    if df_ri.loc[index,'ID'] == 'D60':
        df_ri.at[index,'UF'] = 'RJ'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #MUX-Energia
    if df_ri.loc[index,'ID'] == 'D61':
        df_ri.at[index,'UF'] = 'RS'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
    
    #CPFL RGE
    if df_ri.loc[index,'ID'] == 'D62':
        df_ri.at[index,'UF'] = 'RS'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #SULGIPE
    if df_ri.loc[index,'ID'] == 'D63':
        df_ri.at[index,'UF'] = 'SE'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #UHENPAL
    if df_ri.loc[index,'ID'] == 'D64':
        df_ri.at[index,'UF'] = 'TO'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ESS
    if df_ri.loc[index,'ID'] == 'D65':
        df_ri.at[index,'UF'] = 'SP'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Santa Cruz
    if df_ri.loc[index,'ID'] == 'D66':
        df_ri.at[index,'UF'] = 'SP'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #RGE
    if df_ri.loc[index,'ID'] == 'D67':
        df_ri.at[index,'UF'] = 'RS'
        df_ri.at[index,'PERIODO_TARIFARIO'] = '5'

   
def distribuidora_cor(df_cor,df_sparta_capa,index):
    #Determina o ANO da SPARTA
    df_cor.at[index,'ANO'] = df_sparta_capa.iloc[8,1].strftime('%Y')
    
    #Determina o ID da Distribuidora
    df_cor.at[index,'ID'] = df_sparta_capa.iloc[1,1]
    
    #Determina o NOME da Distribuidora
    df_cor.at[index,'DISTRIBUIDORA'] = df_sparta_capa.iloc[0,0].upper()
    
    #Determina a DATA da SPARTA
    df_cor.at[index,'DATA'] = df_sparta_capa.iloc[8,1].strftime('%Y-%m-%d')
    
    #Determina o EVENTO TARIFARIO
    if 'Reajuste' in df_sparta_capa.iloc[0,1]:
        df_cor.at[index,'EVENTO_TARIFARIO'] = 'RTA'
    elif 'Revisão' in df_sparta_capa.iloc[0,1]:
        df_cor.at[index,'EVENTO_TARIFARIO'] = 'RTP'
    else:
        df_cor.at[index,'EVENTO_TARIFARIO'] = 'RTE'
        
    #Determina a CHAVE
    df_cor.at[index,'CHAVE'] = df_cor.loc[index,'EVENTO_TARIFARIO']+df_cor.loc[index,'ANO']+df_cor.loc[index,'ID']
    
    #Determina UF e PERIODO TARIFARIO
    #AES SUL
    if df_cor.loc[index,'ID'] == 'D01':
        df_cor.at[index,'UF'] = 'RS'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #AME
    if df_cor.loc[index,'ID'] == 'D02':
        df_cor.at[index,'UF'] = 'AM'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #AMPLA
    if df_cor.loc[index,'ID'] == 'D03':
        df_cor.at[index,'UF'] = 'RJ'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #BANDEIRANTE
    if df_cor.loc[index,'ID'] == 'D04':
        df_cor.at[index,'UF'] = 'SP'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #BOA VISTA
    if df_cor.loc[index,'ID'] == 'D05':
        df_cor.at[index,'UF'] = 'RR'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CAIUA
    if df_cor.loc[index,'ID'] == 'D06':
        df_cor.at[index,'UF'] = 'SP'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEA
    if df_cor.loc[index,'ID'] == 'D07':
        df_cor.at[index,'UF'] = 'AP'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CEAL
    if df_cor.loc[index,'ID'] == 'D08':
        df_cor.at[index,'UF'] = 'AL'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CEB
    if df_cor.loc[index,'ID'] == 'D09':
        df_cor.at[index,'UF'] = 'DF'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '4'
    
    #CEEE
    if df_cor.loc[index,'ID'] == 'D10':
        df_cor.at[index,'UF'] = 'RS'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
      
    #CELESC
    if df_cor.loc[index,'ID'] == 'D11':
        df_cor.at[index,'UF'] = 'SC'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CELG
    if df_cor.loc[index,'ID'] == 'D12':
        df_cor.at[index,'UF'] = 'GO'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CELPA
    if df_cor.loc[index,'ID'] == 'D13':
        df_cor.at[index,'UF'] = 'PA'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CELPE
    if df_cor.loc[index,'ID'] == 'D14':
        df_cor.at[index,'UF'] = 'PE'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CELTINS
    if df_cor.loc[index,'ID'] == 'D15':
        df_cor.at[index,'UF'] = 'TO'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEMAR
    if df_cor.loc[index,'ID'] == 'D16':
        df_cor.at[index,'UF'] = 'MA'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #EMT
    if df_cor.loc[index,'ID'] == 'D17':
        df_cor.at[index,'UF'] = 'MT'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEMIG-D
    if df_cor.loc[index,'ID'] == 'D18':
        df_cor.at[index,'UF'] = 'MG'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEPISA
    if df_cor.loc[index,'ID'] == 'D19':
        df_cor.at[index,'UF'] = 'PI'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CERON
    if df_cor.loc[index,'ID'] == 'D20':
        df_cor.at[index,'UF'] = 'RO'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CERR (CONFIMAR O PERIODO)
    if df_cor.loc[index,'ID'] == 'D21':
        df_cor.at[index,'UF'] = 'RR'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CFLO
    if df_cor.loc[index,'ID'] == 'D22':
        df_cor.at[index,'UF'] = 'PR'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CHESP
    if df_cor.loc[index,'ID'] == 'D23':
        df_cor.at[index,'UF'] = 'GO'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL JAGUARI
    if df_cor.loc[index,'ID'] == 'D24':
        df_cor.at[index,'UF'] = 'SP'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL MOCOCA
    if df_cor.loc[index,'ID'] == 'D25':
        df_cor.at[index,'UF'] = 'SP'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'        
    
    #CPFL Santa Cruz
    if df_cor.loc[index,'ID'] == 'D26':
        df_cor.at[index,'UF'] = 'SP'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CNEE
    if df_cor.loc[index,'ID'] == 'D27':
        df_cor.at[index,'UF'] = 'SP'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COCEL
    if df_cor.loc[index,'ID'] == 'D28':
        df_cor.at[index,'UF'] = 'PR'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COELBA
    if df_cor.loc[index,'ID'] == 'D29':
        df_cor.at[index,'UF'] = 'BA'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COELCE
    if df_cor.loc[index,'ID'] == 'D30':
        df_cor.at[index,'UF'] = 'CE'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #COOPERALIANÇA
    if df_cor.loc[index,'ID'] == 'D31':
        df_cor.at[index,'UF'] = 'SC'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COPEL
    if df_cor.loc[index,'ID'] == 'D32':
        df_cor.at[index,'UF'] = 'PR'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COSERN
    if df_cor.loc[index,'ID'] == 'D33':
        df_cor.at[index,'UF'] = 'RN'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Leste Paulista
    if df_cor.loc[index,'ID'] == 'D34':
        df_cor.at[index,'UF'] = 'SP'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Piratininga
    if df_cor.loc[index,'ID'] == 'D35':
        df_cor.at[index,'UF'] = 'SP'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CPFL Paulista
    if df_cor.loc[index,'ID'] == 'D36':
        df_cor.at[index,'UF'] = 'SP'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Sul Paulista
    if df_cor.loc[index,'ID'] == 'D37':
        df_cor.at[index,'UF'] = 'SP'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #DEMEI
    if df_cor.loc[index,'ID'] == 'D38':
        df_cor.at[index,'UF'] = 'RS'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #DME-PC
    if df_cor.loc[index,'ID'] == 'D39':
        df_cor.at[index,'UF'] = 'MG'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EBO
    if df_cor.loc[index,'ID'] == 'D40':
        df_cor.at[index,'UF'] = 'PB'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #EDEVP
    if df_cor.loc[index,'ID'] == 'D41':
        df_cor.at[index,'UF'] = 'SP'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EEB
    if df_cor.loc[index,'ID'] == 'D42':
        df_cor.at[index,'UF'] = 'SP'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EFLJC
    if df_cor.loc[index,'ID'] == 'D43':
        df_cor.at[index,'UF'] = 'SC'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EFLUL
    if df_cor.loc[index,'ID'] == 'D44':
        df_cor.at[index,'UF'] = 'SC'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ELEKTRO
    if df_cor.loc[index,'ID'] == 'D45':
        df_cor.at[index,'UF'] = 'SP'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELETROACRE
    if df_cor.loc[index,'ID'] == 'D46':
        df_cor.at[index,'UF'] = 'AC'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELETROCAR
    if df_cor.loc[index,'ID'] == 'D47':
        df_cor.at[index,'UF'] = 'RS'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ELETROPAULO
    if df_cor.loc[index,'ID'] == 'D48':
        df_cor.at[index,'UF'] = 'SP'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELFSM
    if df_cor.loc[index,'ID'] == 'D49':
        df_cor.at[index,'UF'] = 'ES'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EMG
    if df_cor.loc[index,'ID'] == 'D50':
        df_cor.at[index,'UF'] = 'MG'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EMS
    if df_cor.loc[index,'ID'] == 'D51':
        df_cor.at[index,'UF'] = 'MS'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ENF
    if df_cor.loc[index,'ID'] == 'D52':
        df_cor.at[index,'UF'] = 'RJ'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EPB
    if df_cor.loc[index,'ID'] == 'D53':
        df_cor.at[index,'UF'] = 'PB'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ESCELSA (CONFIRMAR PERIODO)
    if df_cor.loc[index,'ID'] == 'D54':
        df_cor.at[index,'UF'] = 'ES'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '3'
        
    #ESE
    if df_cor.loc[index,'ID'] == 'D55':
        df_cor.at[index,'UF'] = 'SE'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #FORCEL
    if df_cor.loc[index,'ID'] == 'D56':
        df_cor.at[index,'UF'] = 'PR'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #HIDROPAN
    if df_cor.loc[index,'ID'] == 'D57':
        df_cor.at[index,'UF'] = 'RS'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #IENERGIA
    if df_cor.loc[index,'ID'] == 'D58':
        df_cor.at[index,'UF'] = 'SC'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #JARI (CONFIRMAR PERIODO)
    if df_cor.loc[index,'ID'] == 'D59':
        df_cor.at[index,'UF'] = 'PA'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #LIGHT
    if df_cor.loc[index,'ID'] == 'D60':
        df_cor.at[index,'UF'] = 'RJ'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #MUX-Energia
    if df_cor.loc[index,'ID'] == 'D61':
        df_cor.at[index,'UF'] = 'RS'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
    
    #CPFL RGE
    if df_cor.loc[index,'ID'] == 'D62':
        df_cor.at[index,'UF'] = 'RS'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #SULGIPE
    if df_cor.loc[index,'ID'] == 'D63':
        df_cor.at[index,'UF'] = 'SE'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #UHENPAL
    if df_cor.loc[index,'ID'] == 'D64':
        df_cor.at[index,'UF'] = 'TO'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ESS
    if df_cor.loc[index,'ID'] == 'D65':
        df_cor.at[index,'UF'] = 'SP'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Santa Cruz
    if df_cor.loc[index,'ID'] == 'D66':
        df_cor.at[index,'UF'] = 'SP'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #RGE
    if df_cor.loc[index,'ID'] == 'D67':
        df_cor.at[index,'UF'] = 'RS'
        df_cor.at[index,'PERIODO_TARIFARIO'] = '5'


   

#%%Inserção dos dados
#Abre a SPARTA de cada arquivo na pasta
for arquivo in arquivos:   
    try:
        df_sparta_capa = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA'
                                       ,header = 5
                                       ,nrows = 14
                                       ,usecols = [1,2])   
    
        df_sparta_mercado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Mercado'
                                  ,header=7
                                  ,nrows=49
                                  ,usecols=[1,2,3,4,5,6,7])
                                    

        df_sparta_resultado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Resultado'
                               ,header=17
                               ,usecols='F:H')

        df_sparta_bd = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'BD'
                               ,header=5
                               ,nrows = 40
                               ,usecols='S:X')
        
        df_sparta_vpb = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'VPB e Fator X'
                               ,header=11
                               ,usecols='B:G')
        
        print('Leu o arquivo: ',arquivo)
    
        #Converte as tabelas para string, pois não é possível comparar string com valor NaN
        df_sparta_vpb = df_sparta_vpb.astype('str')
        df_sparta_resultado = df_sparta_resultado.astype('str')
        df_sparta_bd = df_sparta_bd.astype('str') 

        #Função para extração dos dados da distribuidora e tipo de contrato
        determina_contrato_aj_co_calculo_comp_t(df_aj_co_calculo_comp_t,df_sparta_mercado,index)
        determina_contrato_caa(df_caa,df_sparta_mercado,index)
        determina_contrato_caom(df_caom,df_sparta_mercado,index)
        determina_contrato_ri(df_ri,df_sparta_mercado,index)
        determina_contrato_cor(df_cor,df_sparta_mercado,index)
        distribuidora_aj_co_calculo_comp_t(df_aj_co_calculo_comp_t,df_sparta_capa,index)
        distribuidora_caa(df_caa,df_sparta_capa,index)
        distribuidora_caom(df_caom,df_sparta_capa,index)
        distribuidora_ri(df_ri,df_sparta_capa,index)
        distribuidora_cor(df_cor,df_sparta_capa,index)
   
        #Define o intervalo máximo de linhas e colunas do dataframe 
        linhas_vpb = range(len(df_sparta_vpb.index))
        linhas_resultado = range(len(df_sparta_resultado.index))
        linhas_bd = range(len(df_sparta_bd.index))
        colunas_vpb = range(len(df_sparta_vpb.columns))
        colunas_resultado = range(len(df_sparta_resultado.columns))
    
        #Função para extrair os dados de 'VPB'
        extrai_aj_co_calculo_comp_t(df_aj_co_calculo_comp_t,df_sparta_vpb,index)
        extrai_caa(df_caa,df_sparta_vpb,index)
        extrai_caom(df_caom,df_sparta_vpb,df_sparta_resultado,index)
        extrai_ri(df_ri,df_sparta_vpb,index)
        extrai_cor(df_cor,df_sparta_bd,index)
        print('Extraiu o dado do arquivo: ',arquivo)
        print('Arquivos extraídos: ',round((index/len(arquivos)),2)*100,'%')
        index = index + 1 #Passa para o próximo indice para conseguir dados da proxima SPARTA
             
            
                                                                
    except:
        print('Aba não disponível na SPARTA', arquivo)
    
    

#%%Tratamento de dados 
#Remover dados duplicados e linhas nulas
df_aj_co_calculo_comp_t = df_aj_co_calculo_comp_t.drop_duplicates(subset = 'CHAVE',ignore_index = True)
df_caa = df_caa.drop_duplicates(subset = 'CHAVE',ignore_index = True)
df_caom = df_caom.drop_duplicates(subset = 'CHAVE',ignore_index = True)
df_cor = df_cor.drop_duplicates(subset = 'CHAVE',ignore_index = True)
df_ri = df_ri.drop_duplicates(subset = 'CHAVE',ignore_index = True)
df_aj_co_calculo_comp_t = df_aj_co_calculo_comp_t.dropna(axis=0,how='all')
df_caa = df_caa.dropna(axis=0,how='all')
df_caom = df_caom.dropna(axis=0,how='all')
df_cor = df_cor.dropna(axis=0,how='all')
df_ri = df_ri.dropna(axis=0,how='all')

#Limpeza e Tratamento dos dados
#Tabela 'df_aj_co_calculo_comp_t'
df_aj_co_calculo_comp_t = df_aj_co_calculo_comp_t.astype(str)
df_aj_co_calculo_comp_t['PERIODO_TARIFARIO'] = df_aj_co_calculo_comp_t['PERIODO_TARIFARIO'].replace('nan','0').astype(int)
df_aj_co_calculo_comp_t['AJ_PARCELA_B_RS'] = df_aj_co_calculo_comp_t['AJ_PARCELA_B_RS'].replace('nan','0').astype(float).replace('.',',')
df_aj_co_calculo_comp_t['PARTICIPACAO_CO_ANTES_AJ_PERCENT'] = df_aj_co_calculo_comp_t['PARTICIPACAO_CO_ANTES_AJ_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_aj_co_calculo_comp_t['AJ_CUSTO_OPERACIONAL_RS'] = df_aj_co_calculo_comp_t['AJ_CUSTO_OPERACIONAL_RS'].replace('nan','0').astype(float).replace('.',',')

#Tabela 'df_caa'
df_caa = df_caa.astype(str)
df_caa['PERIODO_TARIFARIO'] = df_caa['PERIODO_TARIFARIO'].replace('nan','0').astype(int)
df_caa['BAR_RS'] = df_caa['BAR_RS'].replace('nan','0').astype(float).replace('.',',')
df_caa['BARA_RS'] = df_caa['BARA_RS'].replace('nan','0').astype(float).replace('.',',')
df_caa['BARV_RS'] = df_caa['BARV_RS'].replace('nan','0').astype(float).replace('.',',')
df_caa['BARI_RS'] = df_caa['BARI_RS'].replace('nan','0').astype(float).replace('.',',')
df_caa['CAL_RS'] = df_caa['CAL_RS'].replace('nan','0').astype(float).replace('.',',')
df_caa['CAV_RS'] = df_caa['CAV_RS'].replace('nan','0').astype(float).replace('.',',')
df_caa['CAI_RS'] = df_caa['CAI_RS'].replace('nan','0').astype(float).replace('.',',')
df_caa['CAIMI_RS'] = df_caa['CAIMI_RS'].replace('nan','0').astype(float).replace('.',',')
df_caa['ATIVO_IMOBILIZADO_RS'] = df_caa['ATIVO_IMOBILIZADO_RS'].replace('nan','0').astype(float).replace('.',',')
df_caa['INDICE_APROVEITAMENTO_INTEGRAL_RS'] = df_caa['INDICE_APROVEITAMENTO_INTEGRAL_RS'].replace('nan','0').astype(float).replace('.',',')
df_caa['OBRIGACOES_ESPECIAIS_BRUTA_RS'] = df_caa['OBRIGACOES_ESPECIAIS_BRUTA_RS'].replace('nan','0').astype(float).replace('.',',')
df_caa['BENS_TOTAL_DEPRECIADOS_RS'] = df_caa['BENS_TOTAL_DEPRECIADOS_RS'].replace('nan','0').astype(float).replace('.',',')
df_caa['BASE_REMUN_BRUTA_RS'] = df_caa['BASE_REMUN_BRUTA_RS'].replace('nan','0').astype(float).replace('.',',')
df_caa['DEPRECIACAO_ACUMULADA_RS'] = df_caa['DEPRECIACAO_ACUMULADA_RS'].replace('nan','0').astype(float).replace('.',',')
df_caa['AIS_LIQUIDO_RS'] = df_caa['AIS_LIQUIDO_RS'].replace('nan','0').astype(float).replace('.',',')
df_caa['INDICE_APROVEITAMENTO_DEPRECIADO_RS'] = df_caa['INDICE_APROVEITAMENTO_DEPRECIADO_RS'].replace('nan','0').astype(float).replace('.',',')
df_caa['VBR_RS'] = df_caa['VBR_RS'].replace('nan','0').astype(float).replace('.',',')
df_caa['ALMOXARIFADO_OPERACAO_RS'] = df_caa['ALMOXARIFADO_OPERACAO_RS'].replace('nan','0').astype(float).replace('.',',')
df_caa['ATIVO_DIFERIDO_RS'] = df_caa['ATIVO_DIFERIDO_RS'].replace('nan','0').astype(float).replace('.',',')
df_caa['OBRIGACOES_ESPECIAIS_LIQUIDA_RS'] = df_caa['OBRIGACOES_ESPECIAIS_LIQUIDA_RS'].replace('nan','0').astype(float).replace('.',',')
df_caa['TERRENOS_SERVIDOES_RS'] = df_caa['TERRENOS_SERVIDOES_RS'].replace('nan','0').astype(float).replace('.',',')
df_caa['BASE_REMUN_LIQUIDA_RS'] = df_caa['BASE_REMUN_LIQUIDA_RS'].replace('nan','0').astype(float).replace('.',',')
df_caa['SALDO_RGR_PLPT_RS'] = df_caa['SALDO_RGR_PLPT_RS'].replace('nan','0').astype(float).replace('.',',')
df_caa['SALDO_RGR_DEMAIS_INVEST_RS'] = df_caa['SALDO_RGR_DEMAIS_INVEST_RS'].replace('nan','0').astype(float).replace('.',',')
df_caa['TAXA_DEPRECIACAO_PERCENT'] = df_caa['TAXA_DEPRECIACAO_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_caa['QRR_RS'] = df_caa['QRR_RS'].replace('nan','0').astype(float).replace('.',',')
df_caa['RC_SEM_OBRIGACOES_ESPECIAIS_RS'] = df_caa['RC_SEM_OBRIGACOES_ESPECIAIS_RS'].replace('nan','0').astype(float).replace('.',',')
df_caa['REMUN_OBRIGACOES_ESPECIAIS_RS'] = df_caa['REMUN_OBRIGACOES_ESPECIAIS_RS'].replace('nan','0').astype(float).replace('.',',')
df_caa['RC_RS'] = df_caa['RC_RS'].replace('nan','0').astype(float).replace('.',',')
df_caa['WACC_REAL_ANTES_IMPOSTO_PERCENT'] = df_caa['WACC_REAL_ANTES_IMPOSTO_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_caa['TAXA_RGR_PLPT_REAL_PERCENT'] = df_caa['TAXA_RGR_PLPT_REAL_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_caa['TAXA_RGR_DEMAIS_INVEST_PERCENT'] = df_caa['TAXA_RGR_DEMAIS_INVEST_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_caa['CAA_RCOE_RS'] = df_caa['CAA_RCOE_RS'].replace('nan','0').astype(float).replace('.',',')
df_caa['CAOM_DIVIDIDO_POR_CAOM_CAA_RCOE_PERCENT'] = df_caa['CAOM_DIVIDIDO_POR_CAOM_CAA_RCOE_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_caa['PARTICIPACAO_CAPITAL_PROPRIO_PERCENT'] = df_caa['PARTICIPACAO_CAPITAL_PROPRIO_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_caa['PRN_PRP_PERCENT'] = df_caa['PRN_PRP_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_caa['IMPOSTO_RENDA_PERCENT'] = df_caa['IMPOSTO_RENDA_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_caa['RC_OBRIGACOES_ESPECIAIS_RS'] = df_caa['RC_OBRIGACOES_ESPECIAIS_RS'].replace('nan','0').astype(float).replace('.',',')

#Tabela 'df_caom'
df_caom = df_caom.astype(str)
df_caom['PERIODO_TARIFARIO'] = df_caom['PERIODO_TARIFARIO'].replace('nan','0').astype(int)
df_caom['RECEITA_PARCELA_B_ANO_TESTE_RS'] = df_caom['RECEITA_PARCELA_B_ANO_TESTE_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['CO_ULTIMA_REVISAO_AJUSTES_RS'] = df_caom['CO_ULTIMA_REVISAO_AJUSTES_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['VPB_ULTIMA_REVISAO_AJUSTES_RS'] = df_caom['VPB_ULTIMA_REVISAO_AJUSTES_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['COMPONENTE_T_ULTIMA_REVISAO_PERCENT'] = df_caom['COMPONENTE_T_ULTIMA_REVISAO_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_caom['FATOR_N_MENOS_1_PERCENT'] = df_caom['FATOR_N_MENOS_1_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_caom['RECEITA_CO_ANO_TESTE_RS'] = df_caom['RECEITA_CO_ANO_TESTE_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['LIMITE_SUPERIOR_CO_EFICIENTE_RS'] = df_caom['LIMITE_SUPERIOR_CO_EFICIENTE_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['LIMITE_INFERIOR_CO_EFICIENTE_RS'] = df_caom['LIMITE_INFERIOR_CO_EFICIENTE_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['CO_EFICIENTE_RS'] = df_caom['CO_EFICIENTE_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['VARIACAO_ANUAL_CO_SEM_LIMITE_PERCENT'] = df_caom['VARIACAO_ANUAL_CO_SEM_LIMITE_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_caom['VARIACAO_ANUAL_CO_LIMITADA_PERCENT'] = df_caom['VARIACAO_ANUAL_CO_LIMITADA_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_caom['META_CO_SEM_COMPARTILHAMENTO_RS'] = df_caom['META_CO_SEM_COMPARTILHAMENTO_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['OPEX_MEDIO_RS'] = df_caom['OPEX_MEDIO_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['RAZAO_ENTRE_CO_AJUSTADO_E_OPEX_MEDIO_PERCENT'] = df_caom['RAZAO_ENTRE_CO_AJUSTADO_E_OPEX_MEDIO_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_caom['META_CO_REGULATORIO_RS'] = df_caom['META_CO_REGULATORIO_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['CO_REGULATORIO_RS'] = df_caom['CO_REGULATORIO_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['IPCA_MES_ANTERIOR_DATA_REVISAO_TARIFARIA_RS'] = df_caom['IPCA_MES_ANTERIOR_DATA_REVISAO_TARIFARIA_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['IPCA_MES_ANTERIOR_DATA_CALCULO_EFICIENCIA_RS'] = df_caom['IPCA_MES_ANTERIOR_DATA_CALCULO_EFICIENCIA_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['CUSTO_EFICIENTE_DATA_CALCULO_EFICIENCIA_RS'] = df_caom['CUSTO_EFICIENTE_DATA_CALCULO_EFICIENCIA_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['CUSTO_EFICIENTE_DATA_REVISAO_TARIFARIA_RS'] = df_caom['CUSTO_EFICIENTE_DATA_REVISAO_TARIFARIA_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['FATOR_ATUALIZACAO_ALPHA'] = df_caom['FATOR_ATUALIZACAO_ALPHA'].replace('nan','0').astype(float).replace('.',',')
df_caom['REFERENCIA_EFICIENCIA_MEDIA_PERCENT'] = df_caom['REFERENCIA_EFICIENCIA_MEDIA_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_caom['EFICIENCIA_APURADA_PERCENT'] = df_caom['EFICIENCIA_APURADA_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_caom['LIMITE_SUPERIOR_INTERVALO_EFICIENCIA_PERCENT'] = df_caom['LIMITE_SUPERIOR_INTERVALO_EFICIENCIA_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_caom['LIMITE_INFERIOR_INTERVALO_EFICIENCIA_PERCENT'] = df_caom['LIMITE_INFERIOR_INTERVALO_EFICIENCIA_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_caom['CO_REAL_ESTUDO_EFICIENCIA'] = df_caom['CO_REAL_ESTUDO_EFICIENCIA'].replace('nan','0').astype(float).replace('.',',')
df_caom['IPCA_JUNHO_ANO_MENOS_2'] = df_caom['IPCA_JUNHO_ANO_MENOS_2'].replace('nan','0').astype(float).replace('.',',')
df_caom['IPCA_DEZEMBRO_ANO_MENOS_2'] = df_caom['IPCA_DEZEMBRO_ANO_MENOS_2'].replace('nan','0').astype(float).replace('.',',')
df_caom['CONTA_PESSOAL_ANO_MENOS_2_RS'] = df_caom['CONTA_PESSOAL_ANO_MENOS_2_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['CONTA_MATERIAIS_ANO_MENOS_2_RS'] = df_caom['CONTA_MATERIAIS_ANO_MENOS_2_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['CONTA_TERCEIROS_ANO_MENOS_2_RS'] = df_caom['CONTA_TERCEIROS_ANO_MENOS_2_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['CONTA_SEGUROS_ANO_MENOS_2_RS'] = df_caom['CONTA_SEGUROS_ANO_MENOS_2_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['CONTA_TRIBUTOS_ANO_MENOS_2_RS'] = df_caom['CONTA_TRIBUTOS_ANO_MENOS_2_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['CONTA_OUTROS_ANO_MENOS_2_RS'] = df_caom['CONTA_OUTROS_ANO_MENOS_2_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['DEMAIS_CUSTOS_ANO_MENOS_2_RS'] = df_caom['DEMAIS_CUSTOS_ANO_MENOS_2_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['CO_ANO_MENOS_2_RS'] = df_caom['CO_ANO_MENOS_2_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['CO_ATUALIZADO_ANO_MENOS_2_RS'] = df_caom['CO_ATUALIZADO_ANO_MENOS_2_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['IPCA_JUNHO_ANO_MENOS_1'] = df_caom['IPCA_JUNHO_ANO_MENOS_1'].replace('nan','0').astype(float).replace('.',',')
df_caom['IPCA_DEZEMBRO_ANO_MENOS_1'] = df_caom['IPCA_DEZEMBRO_ANO_MENOS_1'].replace('nan','0').astype(float).replace('.',',')
df_caom['CONTA_PESSOAL_ANO_MENOS_1_RS'] = df_caom['CONTA_PESSOAL_ANO_MENOS_1_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['CONTA_MATERIAIS_ANO_MENOS_1_RS'] = df_caom['CONTA_MATERIAIS_ANO_MENOS_1_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['CONTA_TERCEIROS_ANO_MENOS_1_RS'] = df_caom['CONTA_TERCEIROS_ANO_MENOS_1_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['CONTA_SEGUROS_ANO_MENOS_1_RS'] = df_caom['CONTA_SEGUROS_ANO_MENOS_1_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['CONTA_TRIBUTOS_ANO_MENOS_1_RS'] = df_caom['CONTA_TRIBUTOS_ANO_MENOS_1_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['CONTA_OUTROS_ANO_MENOS_1_RS'] = df_caom['CONTA_OUTROS_ANO_MENOS_1_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['DEMAIS_CUSTOS_ANO_MENOS_1_RS'] = df_caom['DEMAIS_CUSTOS_ANO_MENOS_1_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['CO_ANO_MENOS_1_RS'] = df_caom['CO_ANO_MENOS_1_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['CO_ATUALIZADO_ANO_MENOS_1_RS'] = df_caom['CO_ATUALIZADO_ANO_MENOS_1_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['INDICADOR_MEDIO_PNT_PERCENT'] = df_caom['INDICADOR_MEDIO_PNT_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_caom['META_PERCENT'] = df_caom['META_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_caom['DEC_GLOBAL_MEDIO_REALIZADO'] = df_caom['DEC_GLOBAL_MEDIO_REALIZADO'].replace('nan','0').astype(float).replace('.',',')
df_caom['LIMITE_V8_GLOBAL'] = df_caom['LIMITE_V8_GLOBAL'].replace('nan','0').astype(float).replace('.',',')
df_caom['PESO_INSUMO_U'] = df_caom['PESO_INSUMO_U'].replace('nan','0').astype(float).replace('.',',')
df_caom['FATOR_ESCALA_PERCENT'] = df_caom['FATOR_ESCALA_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_caom['REDES_SUBTERRANEAS_KM'] = df_caom['REDES_SUBTERRANEAS_KM'].replace('nan','0').astype(float).replace('.',',')
df_caom['REDE_DISTRIBUICAO_AREA_KM'] = df_caom['REDE_DISTRIBUICAO_AREA_KM'].replace('nan','0').astype(float).replace('.',',')
df_caom['REDE_ALTA_TENSAO_KM'] = df_caom['REDE_ALTA_TENSAO_KM'].replace('nan','0').astype(float).replace('.',',')
df_caom['NUMERO_CONSUMIDORES'] = df_caom['NUMERO_CONSUMIDORES'].replace('nan','0').astype(float).replace('.',',')
df_caom['PNT_AJUSTADA_MWH'] = df_caom['PNT_AJUSTADA_MWH'].replace('nan','0').astype(float).replace('.',',')
df_caom['CHI_AJUSTADO_HORAS'] = df_caom['CHI_AJUSTADO_HORAS'].replace('nan','0').astype(float).replace('.',',')
df_caom['MERCADO_PONDERADO_MWH'] = df_caom['MERCADO_PONDERADO_MWH'].replace('nan','0').astype(float).replace('.',',')
df_caom['MERCADO_AT_MWH'] = df_caom['MERCADO_AT_MWH'].replace('nan','0').astype(float).replace('.',',')
df_caom['MERCADO_MT_MWH'] = df_caom['MERCADO_MT_MWH'].replace('nan','0').astype(float).replace('.',',')
df_caom['MERCADO_BT_MWH'] = df_caom['MERCADO_BT_MWH'].replace('nan','0').astype(float).replace('.',',')
df_caom['FATOR_ESCALA_PESO'] = df_caom['FATOR_ESCALA_PESO'].replace('nan','0').astype(float).replace('.',',')
df_caom['REDES_SUBTERRANEAS_PESO'] = df_caom['REDES_SUBTERRANEAS_PESO'].replace('nan','0').astype(float).replace('.',',')
df_caom['REDE_DISTRIBUICAO_AREA_PESO'] = df_caom['REDE_DISTRIBUICAO_AREA_PESO'].replace('nan','0').astype(float).replace('.',',')
df_caom['REDE_ALTA_TENSAO_PESO'] = df_caom['REDE_ALTA_TENSAO_PESO'].replace('nan','0').astype(float).replace('.',',')
df_caom['NUMERO_CONSUMIDORES_PESO'] = df_caom['NUMERO_CONSUMIDORES_PESO'].replace('nan','0').astype(float).replace('.',',')
df_caom['PNT_AJUSTADA_PESO'] = df_caom['PNT_AJUSTADA_PESO'].replace('nan','0').astype(float).replace('.',',')
df_caom['CHI_AJUSTADO_PESO'] = df_caom['CHI_AJUSTADO_PESO'].replace('nan','0').astype(float).replace('.',',')
df_caom['MERCADO_PONDERADO_PESO'] = df_caom['MERCADO_PONDERADO_PESO'].replace('nan','0').astype(float).replace('.',',')
df_caom['MERCADO_AT_PESO'] = df_caom['MERCADO_AT_PESO'].replace('nan','0').astype(float).replace('.',',')
df_caom['MERCADO_MT_PESO'] = df_caom['MERCADO_MT_PESO'].replace('nan','0').astype(float).replace('.',',')
df_caom['MERCADO_BT_PESO'] = df_caom['MERCADO_BT_PESO'].replace('nan','0').astype(float).replace('.',',')
df_caom['OUTRAS_RECEITAS_RS'] = df_caom['OUTRAS_RECEITAS_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['EXCEDENTE_REATIVOS_RS'] = df_caom['EXCEDENTE_REATIVOS_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['ULTRAPASSAGEM_DEMANDA_RS'] = df_caom['ULTRAPASSAGEM_DEMANDA_RS'].replace('nan','0').astype(float).replace('.',',')
df_caom['PARCELA_B_DEDUZIDAS_OUTRAS_RECEITAS_RS'] = df_caom['PARCELA_B_DEDUZIDAS_OUTRAS_RECEITAS_RS'].replace('nan','0').astype(float).replace('.',',')

#Tabela 'df_cor'
df_cor = df_cor.astype(str)
df_cor['PERIODO_TARIFARIO'] = df_cor['PERIODO_TARIFARIO'].replace('nan','0').astype(int)
df_cor['THETA_INF'] = df_cor['THETA_INF'].replace('nan','0').astype(float).replace('.',',')
df_cor['THETA_CENTRO'] = df_cor['THETA_CENTRO'].replace('nan','0').astype(float).replace('.',',')
df_cor['THETA_SUP'] = df_cor['THETA_SUP'].replace('nan','0').astype(float).replace('.',',')
df_cor['THETA_REF'] = df_cor['THETA_REF'].replace('nan','0').astype(float).replace('.',',')
df_cor['OPEX_REAL'] = df_cor['OPEX_REAL'].replace('nan','0').astype(float).replace('.',',')
df_cor['PMSO_CORRIGIDO'] = df_cor['PMSO_CORRIGIDO'].replace('nan','0').astype(float).replace('.',',')
df_cor['FATOR_ESCALA'] = df_cor['FATOR_ESCALA'].replace('nan','0').astype(float).replace('.',',')
df_cor['U'] = df_cor['U'].replace('nan','0').astype(float).replace('.',',')
df_cor['VS_YRSUB'] = df_cor['VS_YRSUB'].replace('nan','0').astype(float).replace('.',',')
df_cor['VS_YRDIST_A'] = df_cor['VS_YRDIST_A'].replace('nan','0').astype(float).replace('.',',')
df_cor['VS_YRALTA'] = df_cor['VS_YRALTA'].replace('nan','0').astype(float).replace('.',',')
df_cor['VS_YCONS'] = df_cor['VS_YCONS'].replace('nan','0').astype(float).replace('.',',')
df_cor['VS_YMPONDERADO'] = df_cor['VS_YMPONDERADO'].replace('nan','0').astype(float).replace('.',',')
df_cor['VS_YD_PERDAS_DIF2'] = df_cor['VS_YD_PERDAS_DIF2'].replace('nan','0').astype(float).replace('.',',')
df_cor['VS_YD_DEC_V8'] = df_cor['VS_YD_DEC_V8'].replace('nan','0').astype(float).replace('.',',')
df_cor['P_AT'] = df_cor['P_AT'].replace('nan','0').astype(float).replace('.',',')
df_cor['P_MT'] = df_cor['P_MT'].replace('nan','0').astype(float).replace('.',',')
df_cor['P_BT'] = df_cor['P_BT'].replace('nan','0').astype(float).replace('.',',')
df_cor['PMSO_2013'] = df_cor['PMSO_2013'].replace('nan','0').astype(float).replace('.',',')
df_cor['CONT_ASSOCIATIVA'] = df_cor['CONT_ASSOCIATIVA'].replace('nan','0').astype(float).replace('.',',')
df_cor['PESO_AT_PERCENT'] = df_cor['PESO_AT_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_cor['PESO_MT_PERCENT'] = df_cor['PESO_MT_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_cor['PESO_BT_PERCENT'] = df_cor['PESO_BT_PERCENT'].replace('nan','0').astype(float).replace('.',',')

#Tabela 'df_ri'
df_ri = df_ri.astype(str)
df_ri['PERIODO_TARIFARIO'] = df_ri['PERIODO_TARIFARIO'].replace('nan','0').astype(int)
df_ri['RI_ENCARGOS_SETORIAIS_RS'] = df_ri['RI_ENCARGOS_SETORIAIS_RS'].replace('nan','0').astype(float).replace('.',',')
df_ri['ENCARGOS_DRP_RS'] = df_ri['ENCARGOS_DRP_RS'].replace('nan','0').astype(float).replace('.',',')
df_ri['CARGA_TRIBUTARIA_PERCENT'] = df_ri['CARGA_TRIBUTARIA_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['LIMITE_RI_ENCARGOS_PERCENT'] = df_ri['LIMITE_RI_ENCARGOS_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['DEMAIS_RI_RS'] = df_ri['DEMAIS_RI_RS'].replace('nan','0').astype(float).replace('.',',')
df_ri['RECEITA_ADICIONAL_BANDEIRA_RS'] = df_ri['RECEITA_ADICIONAL_BANDEIRA_RS'].replace('nan','0').astype(float).replace('.',',')
df_ri['ENERGIA_COMPRADA_RS'] = df_ri['ENERGIA_COMPRADA_RS'].replace('nan','0').astype(float).replace('.',',')
df_ri['TRANSPORTE_ENERGIA_RS'] = df_ri['TRANSPORTE_ENERGIA_RS'].replace('nan','0').astype(float).replace('.',',')
df_ri['LIMITE_DEMAIS_RI_PERCENT'] = df_ri['LIMITE_DEMAIS_RI_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['RI_TOTAL_RS'] = df_ri['RI_TOTAL_RS'].replace('nan','0').astype(float).replace('.',',')
df_ri['RESIDENCIAL_PARTIPACAO_CONSUMO_PERCENT'] = df_ri['RESIDENCIAL_PARTIPACAO_CONSUMO_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['INDUSTRIAL_PARTIPACAO_CONSUMO_PERCENT'] = df_ri['INDUSTRIAL_PARTIPACAO_CONSUMO_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['COMERCIAL_PARTIPACAO_CONSUMO_PERCENT'] = df_ri['COMERCIAL_PARTIPACAO_CONSUMO_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['RURAL_PARTIPACAO_CONSUMO_PERCENT'] = df_ri['RURAL_PARTIPACAO_CONSUMO_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['PODER_PUBLICO_PARTIPACAO_CONSUMO_PERCENT'] = df_ri['PODER_PUBLICO_PARTIPACAO_CONSUMO_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['ILUMINACAO_PUBLICA_PARTIPACAO_CONSUMO_PERCENT'] = df_ri['ILUMINACAO_PUBLICA_PARTIPACAO_CONSUMO_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['SERVICO_PUBLICO_PARTIPACAO_CONSUMO_PERCENT'] = df_ri['SERVICO_PUBLICO_PARTIPACAO_CONSUMO_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['RESIDENCIAL_LIMITE_DEMAIS_RI_PERCENT'] = df_ri['RESIDENCIAL_LIMITE_DEMAIS_RI_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['INDUSTRIAL_LIMITE_DEMAIS_RI_PERCENT'] = df_ri['INDUSTRIAL_LIMITE_DEMAIS_RI_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['COMERCIAL_LIMITE_DEMAIS_RI_PERCENT'] = df_ri['COMERCIAL_LIMITE_DEMAIS_RI_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['RURAL_LIMITE_DEMAIS_RI_PERCENT'] = df_ri['RURAL_LIMITE_DEMAIS_RI_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['PODER_PUBLICO_LIMITE_DEMAIS_RI_PERCENT'] = df_ri['PODER_PUBLICO_LIMITE_DEMAIS_RI_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['ILUMINACAO_PUBLICA_LIMITE_DEMAIS_RI_PERCENT'] = df_ri['ILUMINACAO_PUBLICA_LIMITE_DEMAIS_RI_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['SERVICO_PUBLICO_LIMITE_DEMAIS_RI_PERCENT'] = df_ri['SERVICO_PUBLICO_LIMITE_DEMAIS_RI_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['RESIDENCIAL_MEDIANA_INADIMPLENCIAS_PERCENT'] = df_ri['RESIDENCIAL_MEDIANA_INADIMPLENCIAS_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['INDUSTRIAL_MEDIANA_INADIMPLENCIAS_PERCENT'] = df_ri['INDUSTRIAL_MEDIANA_INADIMPLENCIAS_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['COMERCIAL_MEDIANA_INADIMPLENCIAS_PERCENT'] = df_ri['COMERCIAL_MEDIANA_INADIMPLENCIAS_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['RURAL_MEDIANA_INADIMPLENCIAS_PERCENT'] = df_ri['RURAL_MEDIANA_INADIMPLENCIAS_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['PODER_PUBLICO_MEDIANA_INADIMPLENCIAS_PERCENT'] = df_ri['PODER_PUBLICO_MEDIANA_INADIMPLENCIAS_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['ILUMINACAO_PUBLICA_MEDIANA_INADIMPLENCIAS_PERCENT'] = df_ri['ILUMINACAO_PUBLICA_MEDIANA_INADIMPLENCIAS_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['SERVICO_PUBLICO_MEDIANA_INADIMPLENCIAS_PERCENT'] = df_ri['SERVICO_PUBLICO_MEDIANA_INADIMPLENCIAS_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['RESIDENCIAL_LIMITE_NEUTRALIDADE_PERCENT'] = df_ri['RESIDENCIAL_LIMITE_NEUTRALIDADE_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['INDUSTRIAL_LIMITE_NEUTRALIDADE_PERCENT'] = df_ri['INDUSTRIAL_LIMITE_NEUTRALIDADE_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['COMERCIAL_LIMITE_NEUTRALIDADE_PERCENT'] = df_ri['COMERCIAL_LIMITE_NEUTRALIDADE_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['RURAL_LIMITE_NEUTRALIDADE_PERCENT'] = df_ri['RURAL_LIMITE_NEUTRALIDADE_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['PODER_PUBLICO_LIMITE_NEUTRALIDADE_PERCENT'] = df_ri['PODER_PUBLICO_LIMITE_NEUTRALIDADE_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['ILUMINACAO_PUBLICA_LIMITE_NEUTRALIDADE_PERCENT'] = df_ri['ILUMINACAO_PUBLICA_LIMITE_NEUTRALIDADE_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['SERVICO_PUBLICO_LIMITE_NEUTRALIDADE_PERCENT'] = df_ri['SERVICO_PUBLICO_LIMITE_NEUTRALIDADE_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['RESIDENCIAL_LIMITE_RI_ENCARGOS_PERCENT'] = df_ri['RESIDENCIAL_LIMITE_RI_ENCARGOS_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['INDUSTRIAL_LIMITE_RI_ENCARGOS_PERCENT'] = df_ri['INDUSTRIAL_LIMITE_RI_ENCARGOS_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['COMERCIAL_LIMITE_RI_ENCARGOS_PERCENT'] = df_ri['COMERCIAL_LIMITE_RI_ENCARGOS_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['RURAL_LIMITE_RI_ENCARGOS_PERCENT'] = df_ri['RURAL_LIMITE_RI_ENCARGOS_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['PODER_PUBLICO_LIMITE_RI_ENCARGOS_PERCENT'] = df_ri['PODER_PUBLICO_LIMITE_RI_ENCARGOS_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['ILUMINACAO_PUBLICA_LIMITE_RI_ENCARGOS_PERCENT'] = df_ri['ILUMINACAO_PUBLICA_LIMITE_RI_ENCARGOS_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_ri['SERVICO_PUBLICO_LIMITE_RI_ENCARGOS_PERCENT'] = df_ri['SERVICO_PUBLICO_LIMITE_RI_ENCARGOS_PERCENT'].replace('nan','0').astype(float).replace('.',',')


#Filtrar somente os dados que estão presentes em processo de RTP ou RTE
df_filtro_aj_co_calculo_comp_t = df_aj_co_calculo_comp_t[(df_aj_co_calculo_comp_t['AJ_PARCELA_B_RS'] != 0)]
df_filtro_caa = df_caa[(df_caa['BAR_RS'] != 0)]
df_filtro_caom = df_caom[(df_caom['RECEITA_CO_ANO_TESTE_RS'] != 0)]
df_filtro_cor = df_cor[(df_cor['THETA_INF'] != 0)]
df_filtro_ri = df_ri[(df_ri['RI_TOTAL_RS'] != 0)]

# Define a data de atualização dos dados
data = datetime.today().strftime('%d/%m/%Y')
df_filtro_aj_co_calculo_comp_t['DATA_ATUALIZA'] = data 
df_filtro_caa['DATA_ATUALIZA'] = data  
df_filtro_caom['DATA_ATUALIZA'] = data  
df_filtro_cor['DATA_ATUALIZA'] = data  
df_filtro_ri['DATA_ATUALIZA'] = data   

#%% Inserir dados no banco de dados

#Criar a lista para inserção no banco SQL com os dados da base editada
dados_list_aj_co_calculo_comp_t = df_filtro_aj_co_calculo_comp_t.values.tolist()
dados_list_caa = df_filtro_caa.values.tolist()
dados_list_caom = df_filtro_caom.values.tolist()
dados_list_cor = df_filtro_cor.values.tolist()
dados_list_ri = df_filtro_ri.values.tolist()


#Definir as variáveis para conexão no banco de dados
aplicacao_usuario = "USER_IRA"
aplicacao_senha = "BD_IRA"
aplicacao_dsn = "DSN"
usuario = "IRA"


#Definir conexão com o banco de dados     
try:
    connection = cx_Oracle.connect(user = keyring.get_password(aplicacao_usuario, usuario),
                                   password = keyring.get_password(aplicacao_senha,usuario),
                                   dsn= keyring.get_password(aplicacao_dsn, usuario),
                                   encoding="UTF-8")

#Se der erro na conexão com o banco, irá aparecer a mensagem abaixo
except Exception as err:
    print('Erro na Conexao:', err)    

#Se estiver tudo certo na conexão, irá aparecer a mensagem abaixo
else:
    print('Conexao com o Banco de Dados efetuada com sucesso. Versao da conexao: ' + connection.version)
    
    #O cursor abaixo irá executar o insert de cada uma das linhas da base editada no Banco de Dados Oracle
    try:
        cursor = connection.cursor()
        cursor.execute('''DELETE FROM ''' + tabela_oracle_aj_co_calculo_comp_t + ''' WHERE ANO = ''' + ano_oracle) #Exclui somente os dados de 2023
        cursor.execute('''DELETE FROM ''' + tabela_oracle_caa + ''' WHERE ANO = ''' + ano_oracle) #Exclui somente os dados de 2023
        cursor.execute('''DELETE FROM ''' + tabela_oracle_caom + ''' WHERE ANO = ''' + ano_oracle) #Exclui somente os dados de 2023
        cursor.execute('''DELETE FROM ''' + tabela_oracle_cor + ''' WHERE ANO = ''' + ano_oracle) #Exclui somente os dados de 2023
        cursor.execute('''DELETE FROM ''' + tabela_oracle_ri + ''' WHERE ANO = ''' + ano_oracle) #Exclui somente os dados de 2023
        sql_aj_co_calculo_comp_t = '''INSERT INTO ''' + tabela_oracle_aj_co_calculo_comp_t +''' VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        sql_caa = '''INSERT INTO ''' + tabela_oracle_caa +''' VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26,:27,:28,:29,:30,:31,:32,:33,:34,:35,:36,:37,:38,:39,:40,:41,:42,:43,:44,:45,:46,:47,:48)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        sql_caom = '''INSERT INTO ''' + tabela_oracle_caom +''' VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26,:27,:28,:29,:30,:31,:32,:33,:34,:35,:36,:37,:38,:39,:40,:41,:42,:43,:44,:45,:46,:47,:48,:49,:50,:51,:52,:53,:54,:55,:56,:57,:58,:59,:60,:61,:62,:63,:64,:65,:66,:67,:68,:69,:70,:71,:72,:73,:74,:75,:76,:77,:78,:79,:80,:81,:82,:83,:84,:85,:86,:87,:88,:89)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        sql_cor = '''INSERT INTO ''' + tabela_oracle_cor +''' VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26,:27,:28,:29,:30,:31,:32,:33)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        sql_ri = '''INSERT INTO ''' + tabela_oracle_ri +''' VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26,:27,:28,:29,:30,:31,:32,:33,:34,:35,:36,:37,:38,:39,:40,:41,:42,:43,:44,:45,:46,:47,:48,:49,:50,:51,:52,:53,:54,:55)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        cursor.executemany(sql_aj_co_calculo_comp_t, dados_list_aj_co_calculo_comp_t)
        cursor.executemany(sql_caa, dados_list_caa)
        cursor.executemany(sql_caom, dados_list_caom)
        cursor.executemany(sql_cor, dados_list_cor)
        cursor.executemany(sql_ri, dados_list_ri)
    except Exception as err:
        print('Erro no Insert:', err)
    else:
        print('Carga executada com sucesso!')
        connection.commit() #Caso seja executado com sucesso, esse comando salva a base de dados
    finally:
        cursor.close()
        connection.close()


