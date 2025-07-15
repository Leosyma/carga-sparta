# -*- coding: utf-8 -*-
"""
Created on Wed Jun  7 13:42:16 2023

@author: 2018459
"""


#%% Bibliotecas
import pandas as pd
import numpy as np
import keyring
import cx_Oracle
import os
import glob
import math
from datetime import datetime

#%% Dados de entrada
#Origem dos dados

#Caminho referencia
pasta = r"W:\Inteligência Regulatória Analítica - IRA\2. Projetos\2023\BD PERSAS\02 - Dados\PERSAS\PERSAS 2023\*"
# pasta = r"C:\Users\2018459\OneDrive - CPFL Energia S A\Área de Trabalho\RTP e RTA (permissionarias)\PERSAS"

#Arquivos
#Como os dados das empresas estão em arquivos separados, listar todos que serão carregados conforme layout abaixo.
# arquivo ='PERSAS_tarifas_iniciais _CERVAM_FMF.xlsx'
arquivos = glob.glob(pasta)

colunas_float_caom=['UNIDADES_CONSUMIDORAS','REDES_DISTRIBUICAO_KM','CO_DIVIDIDO_UC_RS_UC','CUSTOS_OPERACIONAIS_RS','RESIDENCIAL_RECEITA_PERCENT','INDUSTRIAL_RECEITA_PERCENT','COMERCIAL_RECEITA_PERCENT','RURAL_RECEITA_PERCENT','ILUMINACAO_RECEITA_PERCENT','PODER_PUBLICO_RECEITA_PERCENT','SERV_PUBLICO_RECEITA_PERCENT','DEMAIS_RECEITA_PERCENT','RESIDENCIAL_RI_PERCENT','INDUSTRIAL_RI_PERCENT','COMERCIAL_RI_PERCENT','RURAL_RI_PERCENT','ILUMINACAO_RI_PERCENT','PODER_PUBLICO_RI_PERCENT','SERV_PUBLICO_RI_PERCENT','DEMAIS_RI_PERCENT','NIVEL_REGULATORIO_RI_PERCENT','BASE_CALCULO_RI_RS','RECEITAS_IRRECUPERAVEIS_RS']
colunas_float_caa=['ATIVO_IMOBILIZADO_RS','OBRIGACOES_ESPECIAIS_BRUTA_RS','BENS_TOTAL_DEPRECIADOS_RS','BASE_REMUN_BRUTA_RS','DEPRECIACAO_ACUMULADA_PERCENT','VBR_RS','OBRIGACOES_ESPECIAIS_LIQUIDA_RS','TERRENOS_RS','ALMOXARIFADO_RS','BASE_REMUNERACAO_LIQUIDA_RS','TAXA_DEPRECIACAO_PERCENT','RWACCPRE_PERCENT','REMUNERACAO_CAPITAL_RS','QRR_RS','BAR_RS','BARA_RS','BARV_RS','BARI_RS','CAL_RS','CAV_RS','CAI_RS','CAIMI_RS']
index = 0 #Flag para inserção dos dados

#Nome da tabela Oracle onde será dada a carga
tabela_oracle_caom = 'PERSAS_TI_CAOM'
tabela_oracle_caa = 'PERSAS_TI_CAA'
ano_oracle = "'2023'"


#%% Montar o dataframe da estrutura
#Defino o indice máximo que é igual o número de persas
index_maximo = list(range(0,len(arquivos)))

#Criação da estrutura do tabela NUC
df_caom = pd.DataFrame(data = [], columns=['CHAVE','EVENTO_TARIFARIO','ANO','SARI','DISTRIBUIDORA','REGIAO','DATA','UNIDADES_CONSUMIDORAS','REDES_DISTRIBUICAO_KM','CO_DIVIDIDO_UC_RS_UC','CUSTOS_OPERACIONAIS_RS','RESIDENCIAL_RECEITA_PERCENT','INDUSTRIAL_RECEITA_PERCENT','COMERCIAL_RECEITA_PERCENT','RURAL_RECEITA_PERCENT','ILUMINACAO_RECEITA_PERCENT','PODER_PUBLICO_RECEITA_PERCENT','SERV_PUBLICO_RECEITA_PERCENT','DEMAIS_RECEITA_PERCENT','RESIDENCIAL_RI_PERCENT','INDUSTRIAL_RI_PERCENT','COMERCIAL_RI_PERCENT','RURAL_RI_PERCENT','ILUMINACAO_RI_PERCENT','PODER_PUBLICO_RI_PERCENT','SERV_PUBLICO_RI_PERCENT','DEMAIS_RI_PERCENT','NIVEL_REGULATORIO_RI_PERCENT','BASE_CALCULO_RI_RS','RECEITAS_IRRECUPERAVEIS_RS'],index=index_maximo)
df_caa = pd.DataFrame(data = [], columns=['CHAVE','EVENTO_TARIFARIO','ANO','SARI','DISTRIBUIDORA','REGIAO','DATA','ATIVO_IMOBILIZADO_RS','OBRIGACOES_ESPECIAIS_BRUTA_RS','BENS_TOTAL_DEPRECIADOS_RS','BASE_REMUN_BRUTA_RS','DEPRECIACAO_ACUMULADA_PERCENT','VBR_RS','OBRIGACOES_ESPECIAIS_LIQUIDA_RS','TERRENOS_RS','ALMOXARIFADO_RS','BASE_REMUNERACAO_LIQUIDA_RS','TAXA_DEPRECIACAO_PERCENT','RWACCPRE_PERCENT','REMUNERACAO_CAPITAL_RS','QRR_RS','BAR_RS','BARA_RS','BARV_RS','BARI_RS','CAL_RS','CAV_RS','CAI_RS','CAIMI_RS'],index=index_maximo)

#Criação dataframe vazio
df_persas_capa = pd.DataFrame(data=[])
df_persas_vpb = pd.DataFrame(data=[])


# df_persas_capa = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA')
    

# df_persas_vpb = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'VPB1'
#                                                               ,nrows=50
#                                                               ,usecols='A:I')

    

# linhas_capa = range(len(df_persas_capa.index))
# colunas_capa = range(len(df_persas_capa.columns))
# linhas_vpb = range(len(df_persas_vpb.index))
# colunas_vpb = range(len(df_persas_vpb.columns))



# df_persas_capa = df_persas_capa.astype('str')
# df_persas_vpb = df_persas_vpb.astype('str')


#%%Funções
# Função para extrair Custos de Administração, Operação e Manutenção (CAOM)
def extrair_caom(df_caom,df_persas_vpb,index):
    # Cálculo dos Custos Operacionais (CO)
    for linha_vpb in linhas_vpb:
        for coluna_vpb in colunas_vpb:
            if 'UNIDADES CONSUMIDORAS' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper():
                df_caom.at[index,'UNIDADES_CONSUMIDORAS'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif 'EXTENSÃO DAS REDES' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper():
                df_caom.at[index,'REDES_DISTRIBUICAO_KM'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif 'CO/UC' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper():
                df_caom.at[index,'CO_DIVIDIDO_UC_RS_UC'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif 'CUSTOS OPERACIONAIS' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper():
                df_caom.at[index,'CUSTOS_OPERACIONAIS_RS'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
    
    # Cálculo das Receitas Irrecuperáveis (RI) - Receita
    for linha_vpb in linhas_vpb:
        for coluna_vpb in colunas_vpb:
            if ('RESIDENCIAL' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper()) and (df_persas_vpb.iloc[(linha_vpb-1),(coluna_vpb+1)].upper() == '% RECEITA'):
                df_caom.at[index,'RESIDENCIAL_RECEITA_PERCENT'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif ('INDUSTRIAL' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper()) and (df_persas_vpb.iloc[(linha_vpb-2),(coluna_vpb+1)].upper() == '% RECEITA'):
                df_caom.at[index,'INDUSTRIAL_RECEITA_PERCENT'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif ('COMERCIAL' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper()) and (df_persas_vpb.iloc[(linha_vpb-3),(coluna_vpb+1)].upper() == '% RECEITA'):
                df_caom.at[index,'COMERCIAL_RECEITA_PERCENT'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif ('RURAL' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper()) and (df_persas_vpb.iloc[(linha_vpb-4),(coluna_vpb+1)].upper() == '% RECEITA'):
                df_caom.at[index,'RURAL_RECEITA_PERCENT'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif ('ILUMINAÇÃO' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper()) and (df_persas_vpb.iloc[(linha_vpb-5),(coluna_vpb+1)].upper() == '% RECEITA'):
                df_caom.at[index,'ILUMINACAO_RECEITA_PERCENT'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif ('PODER PÚBLICO' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper()) and (df_persas_vpb.iloc[(linha_vpb-6),(coluna_vpb+1)].upper() == '% RECEITA'):
                df_caom.at[index,'PODER_PUBLICO_RECEITA_PERCENT'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif ('SERVIÇO PÚBLICO' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper()) and (df_persas_vpb.iloc[(linha_vpb-7),(coluna_vpb+1)].upper() == '% RECEITA'):
                df_caom.at[index,'SERV_PUBLICO_RECEITA_PERCENT'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif ('DEMAIS' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper()) and (df_persas_vpb.iloc[(linha_vpb-8),(coluna_vpb+1)].upper() == '% RECEITA'):
                df_caom.at[index,'DEMAIS_RECEITA_PERCENT'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
      
    # Cálculo das Receitas Irrecuperáveis (RI) - RI
    for linha_vpb in linhas_vpb:
        for coluna_vpb in colunas_vpb:
            if ('RESIDENCIAL' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper()) and (df_persas_vpb.iloc[(linha_vpb-1),(coluna_vpb+2)].upper() == '% RI'):
                df_caom.at[index,'RESIDENCIAL_RI_PERCENT'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+2)]
            elif ('INDUSTRIAL' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper()) and (df_persas_vpb.iloc[(linha_vpb-2),(coluna_vpb+2)].upper() == '% RI'):
                df_caom.at[index,'INDUSTRIAL_RI_PERCENT'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+2)]
            elif ('COMERCIAL' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper()) and (df_persas_vpb.iloc[(linha_vpb-3),(coluna_vpb+2)].upper() == '% RI'):
                df_caom.at[index,'COMERCIAL_RI_PERCENT'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+2)]
            elif ('RURAL' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper()) and (df_persas_vpb.iloc[(linha_vpb-4),(coluna_vpb+2)].upper() == '% RI'):
                df_caom.at[index,'RURAL_RI_PERCENT'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+2)]
            elif ('ILUMINAÇÃO' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper()) and (df_persas_vpb.iloc[(linha_vpb-5),(coluna_vpb+2)].upper() == '% RI'):
                df_caom.at[index,'ILUMINACAO_RI_PERCENT'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+2)]
            elif ('PODER PÚBLICO' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper()) and (df_persas_vpb.iloc[(linha_vpb-6),(coluna_vpb+2)].upper() == '% RI'):
                df_caom.at[index,'PODER_PUBLICO_RI_PERCENT'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+2)]
            elif ('SERVIÇO PÚBLICO' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper()) and (df_persas_vpb.iloc[(linha_vpb-7),(coluna_vpb+2)].upper() == '% RI'):
                df_caom.at[index,'SERV_PUBLICO_RI_PERCENT'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+2)]
            elif ('DEMAIS' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper()) and (df_persas_vpb.iloc[(linha_vpb-8),(coluna_vpb+2)].upper() == '% RI'):
                df_caom.at[index,'DEMAIS_RI_PERCENT'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+2)]
            elif 'NÍVEL REGULATÓRIO DE RECEITAS IRRECUPERÁVEIS' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper():
                df_caom.at[index,'NIVEL_REGULATORIO_RI_PERCENT'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+2)]
            elif 'BASE DE CÁLCULO DAS RECEITAS IRRECUPERÁVEIS' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper():
                df_caom.at[index,'BASE_CALCULO_RI_RS'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif 'RECEITAS IRRECUPERÁVEIS (RI)' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper():
                df_caom.at[index,'RECEITAS_IRRECUPERAVEIS_RS'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
                


# Função para extrair Custo Anual dos Ativos (CAA)
def extrair_caa(df_caa,df_persas_vpb,index):
    # Cálculo da Remuneração de Capital (RC) e Quota de Reintegração Regulatória (QRR)
    for linha_vpb in linhas_vpb:
        for coluna_vpb in colunas_vpb:
            if 'ATIVO IMOBILIZADO' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper():
                df_caa.at[index,'ATIVO_IMOBILIZADO_RS'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif 'OBRIGAÇÕES ESPECIAIS BRUTA' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper():
                df_caa.at[index,'OBRIGACOES_ESPECIAIS_BRUTA_RS'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif 'BENS TOTALMENTE DEPRECIADOS' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper():
                df_caa.at[index,'BENS_TOTAL_DEPRECIADOS_RS'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif 'BASE DE REMUNERAÇÃO BRUTA' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper():
                df_caa.at[index,'BASE_REMUN_BRUTA_RS'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif 'DEPRECIAÇÃO ACUMULADA' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper():
                df_caa.at[index,'DEPRECIACAO_ACUMULADA_PERCENT'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif 'VALOR DA BASE DE REMUNERAÇÃO' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper():
                df_caa.at[index,'VBR_RS'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif 'OBRIGAÇÕES ESPECIAIS LÍQUIDA' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper():
                df_caa.at[index,'OBRIGACOES_ESPECIAIS_LIQUIDA_RS'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif 'TERRENOS E SERVIDÕES' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper():
                df_caa.at[index,'TERRENOS_RS'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif 'ALMOXARIFADO EM OPERAÇÃO' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper():
                df_caa.at[index,'ALMOXARIFADO_RS'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif 'BASE DE REMUNERAÇÃO LÍQUIDA TOTAL' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper():
                df_caa.at[index,'BASE_REMUNERACAO_LIQUIDA_RS'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif 'TAXA DE DEPRECIAÇÃO' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper():
                df_caa.at[index,'TAXA_DEPRECIACAO_PERCENT'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif 'RWACCPRÉ' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper():
                df_caa.at[index,'RWACCPRE_PERCENT'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif 'REMUNERAÇÃO DE CAPITAL' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper():
                df_caa.at[index,'REMUNERACAO_CAPITAL_RS'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif 'QUOTA DE REINTEGRAÇÃO REGULATÓRIA' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper():
                df_caa.at[index,'QRR_RS'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]

   
    # 4.2 Cálculo do Custo Anual das Instalações Móveis e Imóveis (CAIMI)
    for linha_vpb in linhas_vpb:
        for coluna_vpb in colunas_vpb:
            if '(BAR)' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper():
                df_caa.at[index,'BAR_RS'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif '(BARA)' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper():
                df_caa.at[index,'BARA_RS'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif '(BARV)' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper():
                df_caa.at[index,'BARV_RS'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif '(BARI)' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper():
                df_caa.at[index,'BARI_RS'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif '(CAL)' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper():
                df_caa.at[index,'CAL_RS'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif '(CAV)' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper():
                df_caa.at[index,'CAV_RS'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif '(CAI)' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper():
                df_caa.at[index,'CAI_RS'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]
            elif 'CAIMI' in df_persas_vpb.iloc[linha_vpb,coluna_vpb].upper():
                df_caa.at[index,'CAIMI_RS'] = df_persas_vpb.iloc[linha_vpb,(coluna_vpb+1)]


def distribuidora_caa(df_caa,df_persas_capa,index):
    for linha in linhas_capa:
        for coluna in colunas_capa:
            #Determina o ANO
            if 'ANO DO PROCESSO' in df_persas_capa.iloc[linha,coluna].upper():
                df_caa.at[index,'ANO'] = df_persas_capa.iloc[linha,(coluna+1)]
                
            #Determina o SARI da Distribuidora
            elif 'SARI' in df_persas_capa.iloc[linha,coluna].upper():
                df_caa.at[index,'SARI'] = df_persas_capa.iloc[linha,(coluna+1)]
                
            #Determina a DATA
            elif 'REAJUSTE/REVISÃO SUGE' in df_persas_capa.iloc[linha,coluna].upper():
                df_caa.at[index,'DATA'] = df_persas_capa.iloc[linha,(coluna+1)].split(' ')[0]


    #Determina o EVENTO TARIFARIO
    for linha in linhas_capa:
        for coluna in colunas_capa:
            if 'REAJUSTE' in df_persas_capa.iloc[linha,coluna].upper():
                df_caa.at[index,'EVENTO_TARIFARIO'] = 'RTA'
                
            elif 'REVISÃO' in df_persas_capa.iloc[linha,coluna].upper():
                df_caa.at[index,'EVENTO_TARIFARIO'] = 'RTP'
                
            elif 'TARIFAS INICIAIS' in df_persas_capa.iloc[linha,coluna].upper():
                df_caa.at[index,'EVENTO_TARIFARIO'] = 'TI'
                

    #Determina o nome da DISTRIBUIDORA das PERSAS fora do layout padrão
    if (df_persas_capa.iloc[4,1].upper() == 'COOPERMILA') or (df_persas_capa.iloc[4,1].upper() == 'CERTREL'):
        df_caa.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[4,1].upper()
        
    elif (df_persas_capa.iloc[0,1].upper() == 'CERGAL'):
        df_caa.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[0,1].upper()
       
    #PERSAS no layout padrão
    else:
        for linha in linhas_capa:
            for coluna in colunas_capa:
                if df_persas_capa.iloc[linha,coluna].upper() == 'EMPRESA':
                    df_caa.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[linha,(coluna+1)].upper()

    #Determina a REGIAO
    if df_caa.at[index,'DISTRIBUIDORA'] == 'CERCOS':
        df_caa.at[index,'REGIAO'] = 'N/NE'
        
    else:
        df_caa.at[index,'REGIAO'] = 'S/SE/CO'

    #Determina a CHAVE
    df_caa.at[index,'CHAVE'] = df_caa.loc[index,'EVENTO_TARIFARIO']+df_caa.loc[index,'ANO']+df_caa.loc[index,'SARI']


def distribuidora_caom(df_caom,df_persas_capa,index):
    for linha in linhas_capa:
        for coluna in colunas_capa:
            #Determina o ANO
            if 'ANO DO PROCESSO' in df_persas_capa.iloc[linha,coluna].upper():
                df_caom.at[index,'ANO'] = df_persas_capa.iloc[linha,(coluna+1)]
                
            #Determina o SARI da Distribuidora
            elif 'SARI' in df_persas_capa.iloc[linha,coluna].upper():
                df_caom.at[index,'SARI'] = df_persas_capa.iloc[linha,(coluna+1)]
                
            #Determina a DATA
            elif 'REAJUSTE/REVISÃO SUGE' in df_persas_capa.iloc[linha,coluna].upper():
                df_caom.at[index,'DATA'] = df_persas_capa.iloc[linha,(coluna+1)].split(' ')[0]


    #Determina o EVENTO TARIFARIO
    for linha in linhas_capa:
        for coluna in colunas_capa:
            if 'REAJUSTE' in df_persas_capa.iloc[linha,coluna].upper():
                df_caom.at[index,'EVENTO_TARIFARIO'] = 'RTA'
                
            elif 'REVISÃO' in df_persas_capa.iloc[linha,coluna].upper():
                df_caom.at[index,'EVENTO_TARIFARIO'] = 'RTP'
                
            elif 'TARIFAS INICIAIS' in df_persas_capa.iloc[linha,coluna].upper():
                df_caom.at[index,'EVENTO_TARIFARIO'] = 'TI'
                

    #Determina o nome da DISTRIBUIDORA das PERSAS fora do layout padrão
    if (df_persas_capa.iloc[4,1].upper() == 'COOPERMILA') or (df_persas_capa.iloc[4,1].upper() == 'CERTREL'):
        df_caom.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[4,1].upper()
        
    elif (df_persas_capa.iloc[0,1].upper() == 'CERGAL'):
        df_caom.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[0,1].upper()
       
    #PERSAS no layout padrão
    else:
        for linha in linhas_capa:
            for coluna in colunas_capa:
                if df_persas_capa.iloc[linha,coluna].upper() == 'EMPRESA':
                    df_caom.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[linha,(coluna+1)].upper()

    #Determina a REGIAO
    if df_caom.at[index,'DISTRIBUIDORA'] == 'CERCOS':
        df_caom.at[index,'REGIAO'] = 'N/NE'
        
    else:
        df_caom.at[index,'REGIAO'] = 'S/SE/CO'

    #Determina a CHAVE
    df_caom.at[index,'CHAVE'] = df_caom.loc[index,'EVENTO_TARIFARIO']+df_caom.loc[index,'ANO']+df_caom.loc[index,'SARI']




#%%Inserção dos dados
#Abre a PERSAS de cada arquivo na pasta
for arquivo in arquivos:   
    try:
        df_persas_capa = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA')
            
        df_persas_vpb = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'VPB1'
                                                                      ,nrows=50
                                                                      ,usecols='A:I')

                                                                         
    except:
        print('Aba não disponível na PERSAS', arquivo)
 
    #Rodamos as funções
    else:
        try:
            print('Leu o arquivo: ',arquivo)
        
            #Converte as tabelas para string, pois não é possível comparar string com valor NaN
            df_persas_capa = df_persas_capa.astype('str')
            df_persas_vpb = df_persas_vpb.astype('str')

            
            #Define o intervalo máximo de linhas e colunas do dataframe 
            linhas_capa = range(len(df_persas_capa.index))
            colunas_capa = range(len(df_persas_capa.columns))
            linhas_vpb = range(len(df_persas_vpb.index))
            colunas_vpb = range(len(df_persas_vpb.columns))

            
            #Função para extrair os dados
            distribuidora_caom(df_caom,df_persas_capa,index)
            distribuidora_caa(df_caa,df_persas_capa,index)
            extrair_caom(df_caom,df_persas_vpb,index)
            extrair_caa(df_caa,df_persas_vpb,index)
            
            
            print('Extraiu o dado do arquivo: ',arquivo)
            print('Arquivos extraídos: ',round((index/len(arquivos)),2)*100,'%')
    
        except Exception as err:
            print('Não foi possível extrair os dados:',err)
    
        
    index = index + 1 #Passa para o próximo indice para conseguir dados da proxima SPARTA
    
 
#%%Tratamento de dados
# Remover linhas nulas
df_caom = df_caom.dropna(axis=0,how='all')
df_caa = df_caa.dropna(axis=0,how='all')

#Remover dados duplicados
df_caom = df_caom.drop_duplicates(subset = 'CHAVE',ignore_index = True)
df_caa = df_caa.drop_duplicates(subset = 'CHAVE',ignore_index = True)

# Filtra somente os processos de 'Tarifa Iniciais'
df_caom = df_caom[df_caom['EVENTO_TARIFARIO'] == 'TI']
df_caa = df_caa[df_caa['EVENTO_TARIFARIO'] == 'TI']

#Tratamento dos dados
df_caom = df_caom.astype('str')
df_caa = df_caa.astype('str')
for coluna_float_caom in colunas_float_caom:
    df_caom[coluna_float_caom] = df_caom[coluna_float_caom].replace('.',',').replace('nan',0).astype('float')
    
for coluna_float_caa in colunas_float_caa:
    df_caa[coluna_float_caa] = df_caa[coluna_float_caa].replace('.',',').replace('nan',0).astype('float')

# Define a data de atualização dos dados
data = datetime.today().strftime('%d/%m/%Y')
df_caom['DATA_ATUALIZA'] = data 
df_caa['DATA_ATUALIZA'] = data 

#%% Inserir dados no banco de dados

#Criar a lista para inserção no banco SQL com os dados da base editada
dados_list_caom = df_caom.values.tolist()
dados_list_caa = df_caa.values.tolist()


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
        cursor.execute('''DELETE FROM ''' + tabela_oracle_caom + ''' WHERE ANO = ''' + ano_oracle) #Exclui somente os dados de 2023
        sql_caom = '''INSERT INTO ''' + tabela_oracle_caom +''' VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26,:27,:28,:29,:30,:31)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        cursor.execute('''DELETE FROM ''' + tabela_oracle_caa + ''' WHERE ANO = ''' + ano_oracle) #Exclui somente os dados de 2023
        sql_caa = '''INSERT INTO ''' + tabela_oracle_caa +''' VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26,:27,:28,:29,:30)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        cursor.executemany(sql_caom, dados_list_caom)
        cursor.executemany(sql_caa, dados_list_caa)
    except Exception as err:
        print('Erro no Insert:', err)
    else:
        print('Carga executada com sucesso!')
        connection.commit() #Caso seja executado com sucesso, esse comando salva a base de dados
    finally:
        cursor.close()
        connection.close()
    





