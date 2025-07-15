# -*- coding: utf-8 -*-
"""
Created on Wed Dec  7 15:53:49 2022

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

#Caminho referencia
pasta = r"C:\Users\2018459\OneDrive - CPFL Energia S A\Documentos\Projetos\2023\BD RTP e RTA\RTP e RTA (concessionaria)\SPARTA\SPARTA 2023\*"
# pasta = r"C:\Users\2018459\OneDrive - CPFL Energia S A\Documentos\Projetos\2023\BD RTP e RTA\RTP e RTA (concessionaria)\SPARTA TESTE - RTA"

#Arquivos
#Como os dados das empresas estão em arquivos separados, listar todos que serão carregados conforme layout abaixo.
# arquivo ='SPARTA CPFL Paulista RTA 2022.xlsx'
arquivos = glob.glob(pasta)


index = 0 #Flag para inserção dos dados

#Nome da tabela Oracle onde será dada a carga
tabela_oracle = 'SPARTA_DRA'
ano_oracle = "'2023'"


#%% Montar o dataframe da estrutura da aba 'SPARTA_NUC'
#Defino o indice máximo que é igual o número de SPARTA
index_maximo = list(range(0,len(arquivos)))

#Criação da estrutura do tabela NUC
df_dra = pd.DataFrame(data = [], columns=['CHAVE','EVENTO_TARIFARIO','ANO','ID','DISTRIBUIDORA','UF','DATA','PERIODO_TARIFARIO','CONTRATO','ENCARGOS_RS','RGR_RS','CCC_RS','TFSEE_RS','CDE_TOTAL_RS','CFURH_RS','ESS_EER_RS','PROINFA_RS','PD_RS','ONS_RS','TRANSPORTE_RS','REDE_BASICA_RS','REDE_BASICA_FRONTEIRA_RS','REDE_BASICA_ONS_A2_RS','REDE_BASICA_EXPORT_A2_RS','MUST_ITAIPU_RS','TRANSPORTE_ITAIPU_RS','CONEXAO_RS','SISTEMA_DISTRIBUICAO_RS','ENERGIA_COMPRADA_RS','RECEITA_IRRECUPERAVEL_RS','PARCELA_A_RS','PARCELA_B_RS','RA0_RS','ENERGIA_COMPRADA_MWH','FORNECIMENTO_SUPRIMENTO_MWH','FORNECIMENTO_MWH','SUPRIMENTO_TE_MWH','CUSTO_MEDIO_RS_MWH','PERDAS_REGULATORIAS_MWH','PERDA_NAO_TECNICA_MWH','PERDA_TECNICA_MWH','PERDA_RB_DISTRIBUICAO_MWH','PERDA_RB_CATIVO_MWH','PERDA_NAO_TECNICA_PERCENT','PERDA_TECNICA_PERCENT','PERDA_REDE_BASICA_PERCENT'],index=index_maximo) 

#Criação dataframe vazio
df_sparta_resultado = pd.DataFrame(data = [])
df_sparta_energia = pd.DataFrame(data=[])
df_sparta_capa = pd.DataFrame(data=[])
df_sparta_mercado = pd.DataFrame(data=[])

# df_sparta_resultado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Resultado'
#                               ,header=12
#                               ,nrows=35
#                               ,usecols=[1,2,3])

# df_sparta_energia = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Energia'
#                           ,header=3
#                           ,nrows=11
#                           ,usecols=[1,2,3,4,5,6,7,8])

# df_sparta_mercado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Mercado'
#                               ,header=7
#                               ,nrows=49
#                               ,usecols=[1,2,3,4,5,6,7])


# df_sparta_resultado = df_sparta_resultado.astype('str')
# df_sparta_energia = df_sparta_energia.astype('str')
# df_sparta_mercado = df_sparta_mercado.astype('str')

# linhas_resultado = range(len(df_sparta_resultado.index))
# colunas_resultado = range(len(df_sparta_resultado.columns))
# linhas_energia = range(len(df_sparta_energia.index))
# colunas_energia = range(len(df_sparta_energia.columns))
# linhas_mercado = range(len(df_sparta_mercado.index))
# colunas_mercado = range(len(df_sparta_mercado.columns))


#%%Extração dos resultados
def extrai_dra(df_dra,df_sparta_energia,df_sparta_resultado,df_sparta_capa,index):
    #Aba Resultado
    for linha_resultado in linhas_resultado:
        for coluna_resultado in colunas_resultado:
            #ENCARGOS
            if 'RGR' in df_sparta_resultado.iloc[linha_resultado,coluna_resultado].upper():
                df_dra.at[index,'RGR_RS'] = float(df_sparta_resultado.iloc[linha_resultado,(coluna_resultado+1)].replace('nan','0').replace(' R$                          -  ','0').replace(' R$                                             -  ','0').replace('-','0').replace(' R$ 0   ','0').replace('DRA','0'))
            elif 'CCC' in df_sparta_resultado.iloc[linha_resultado,coluna_resultado].upper():
                df_dra.at[index,'CCC_RS'] = float(df_sparta_resultado.iloc[linha_resultado,(coluna_resultado+1)].replace('nan','0').replace(' R$                          -  ','0').replace(' R$                                             -  ','0').replace('-','0').replace(' R$ 0   ','0').replace('DRA','0'))
            elif '– TFSEE' in df_sparta_resultado.iloc[linha_resultado,coluna_resultado].upper():
                df_dra.at[index,'TFSEE_RS'] = float(df_sparta_resultado.iloc[linha_resultado,(coluna_resultado+1)].replace('nan','0').replace(' R$                          -  ','0').replace(' R$                                             -  ','0').replace('-','0').replace(' R$ 0   ','0').replace('DRA','0'))
            elif '– CDE' in df_sparta_resultado.iloc[linha_resultado,coluna_resultado].upper():
                df_dra.at[index,'CDE_TOTAL_RS'] = float(df_sparta_resultado.iloc[linha_resultado,(coluna_resultado+1)].replace('nan','0').replace(' R$                          -  ','0').replace(' R$                                             -  ','0').replace('-','0').replace(' R$ 0   ','0').replace('DRA','0'))
            elif 'CFURH' in df_sparta_resultado.iloc[linha_resultado,coluna_resultado].upper():
                df_dra.at[index,'CFURH_RS'] = float(df_sparta_resultado.iloc[linha_resultado,(coluna_resultado+1)].replace('nan','0').replace(' R$                          -  ','0').replace(' R$                                             -  ','0').replace('-','0').replace(' R$ 0   ','0').replace('DRA','0'))
            elif 'EER' in df_sparta_resultado.iloc[linha_resultado,coluna_resultado].upper():
                df_dra.at[index,'ESS_EER_RS'] = float(df_sparta_resultado.iloc[linha_resultado,(coluna_resultado+1)].replace('nan','0').replace(' R$                          -  ','0').replace(' R$                                             -  ','0').replace('-','0').replace(' R$ 0   ','0').replace('DRA','0'))
            elif 'PROINFA' in df_sparta_resultado.iloc[linha_resultado,coluna_resultado].upper():
                df_dra.at[index,'PROINFA_RS'] = float(df_sparta_resultado.iloc[linha_resultado,(coluna_resultado+1)].replace('nan','0').replace(' R$                          -  ','0').replace(' R$                                             -  ','0').replace('-','0').replace(' R$ 0   ','0').replace('DRA','0'))
            elif 'P&D E EFICIÊNCIA ENERGÉTICA' in df_sparta_resultado.iloc[linha_resultado,coluna_resultado].upper():
                df_dra.at[index,'PD_RS'] = float(df_sparta_resultado.iloc[linha_resultado,(coluna_resultado+1)].replace('nan','0').replace(' R$                          -  ','0').replace(' R$                                             -  ','0').replace('-','0').replace(' R$ 0   ','0').replace('DRA','0'))
            elif ('CONTRIBUIÇÃO ONS' in df_sparta_resultado.iloc[linha_resultado,coluna_resultado].upper()) or (df_sparta_resultado.iloc[linha_resultado,coluna_resultado].upper() == 'ONS'):
                df_dra.at[index,'ONS_RS'] = float(df_sparta_resultado.iloc[linha_resultado,(coluna_resultado+1)].replace('nan','0').replace(' R$                          -  ','0').replace(' R$                                             -  ','0').replace('-','0').replace(' R$ 0   ','0').replace('DRA','0'))
                
            #TRANSPORTE
            elif df_sparta_resultado.iloc[linha_resultado,coluna_resultado].upper() == 'REDE BÁSICA':
                df_dra.at[index,'REDE_BASICA_RS'] = float(df_sparta_resultado.iloc[linha_resultado,(coluna_resultado+1)].replace('nan','0').replace(' R$                          -  ','0').replace(' R$                                             -  ','0').replace('-','0').replace(' R$ 0   ','0').replace('DRA','0'))
            elif 'REDE BÁSICA FRONTEIRA' in df_sparta_resultado.iloc[linha_resultado,coluna_resultado].upper():
                df_dra.at[index,'REDE_BASICA_FRONTEIRA_RS'] = float(df_sparta_resultado.iloc[linha_resultado,(coluna_resultado+1)].replace('nan','0').replace(' R$                          -  ','0').replace(' R$                                             -  ','0').replace('-','0').replace(' R$ 0   ','0').replace('DRA','0'))
            elif 'REDE BÁSICA ONS' in df_sparta_resultado.iloc[linha_resultado,coluna_resultado].upper():
                df_dra.at[index,'REDE_BASICA_ONS_A2_RS'] = float(df_sparta_resultado.iloc[linha_resultado,(coluna_resultado+1)].replace('nan','0').replace(' R$                          -  ','0').replace(' R$                                             -  ','0').replace('-','0').replace(' R$ 0   ','0').replace('DRA','0'))
            elif 'REDE BÁSICA EXPORT.' in df_sparta_resultado.iloc[linha_resultado,coluna_resultado].upper():
                df_dra.at[index,'REDE_BASICA_EXPORT_A2_RS'] = float(df_sparta_resultado.iloc[linha_resultado,(coluna_resultado+1)].replace('nan','0').replace(' R$                          -  ','0').replace(' R$                                             -  ','0').replace('-','0').replace(' R$ 0   ','0').replace('DRA','0'))
            elif 'MUST ITAIPU' in df_sparta_resultado.iloc[linha_resultado,coluna_resultado].upper():
                df_dra.at[index,'MUST_ITAIPU_RS'] = float(df_sparta_resultado.iloc[linha_resultado,(coluna_resultado+1)].replace('nan','0').replace(' R$                          -  ','0').replace(' R$                                             -  ','0').replace('-','0').replace(' R$ 0   ','0').replace('DRA','0'))
            elif 'TRANSPORTE DE ITAIPU' in df_sparta_resultado.iloc[linha_resultado,coluna_resultado].upper():
                df_dra.at[index,'TRANSPORTE_ITAIPU_RS'] = float(df_sparta_resultado.iloc[linha_resultado,(coluna_resultado+1)].replace('nan','0').replace(' R$                          -  ','0').replace(' R$                                             -  ','0').replace('-','0').replace(' R$ 0   ','0').replace('DRA','0'))
            elif 'CONEXÃO' in df_sparta_resultado.iloc[linha_resultado,coluna_resultado].upper():
                df_dra.at[index,'CONEXAO_RS'] = float(df_sparta_resultado.iloc[linha_resultado,(coluna_resultado+1)].replace('nan','0').replace(' R$                          -  ','0').replace(' R$                                             -  ','0').replace('-','0').replace(' R$ 0   ','0').replace('DRA','0'))
            elif 'USO DO SISTEMA DE DISTRIBUIÇÃO' in df_sparta_resultado.iloc[linha_resultado,coluna_resultado].upper():
                df_dra.at[index,'SISTEMA_DISTRIBUICAO_RS'] = float(df_sparta_resultado.iloc[linha_resultado,(coluna_resultado+1)].replace('nan','0').replace(' R$                          -  ','0').replace(' R$                                             -  ','0').replace('-','0').replace(' R$ 0   ','0').replace('DRA','0'))
            
            #ENERGIA
            elif df_sparta_resultado.iloc[linha_resultado,coluna_resultado].upper() == 'ENERGIA':
                df_dra.at[index,'ENERGIA_COMPRADA_RS'] = float(df_sparta_resultado.iloc[linha_resultado,(coluna_resultado+1)].replace('nan','0').replace(' R$                          -  ','0').replace(' R$                                             -  ','0').replace('-','0').replace(' R$ 0   ','0').replace('DRA','0'))
            
            #RECEITA IRRECUPERÁVEL
            elif 'RECEITA IRRECUPERÁVEL' in df_sparta_resultado.iloc[linha_resultado,coluna_resultado].upper():
                df_dra.at[index,'RECEITA_IRRECUPERAVEL_RS'] = float(df_sparta_resultado.iloc[linha_resultado,(coluna_resultado+1)].replace('nan','0').replace(' R$                          -  ','0').replace(' R$                                             -  ','0').replace('-','0').replace(' R$ 0   ','0').replace('DRA','0'))
                
            #PARCELA B
            elif 'PARCELA B' in df_sparta_resultado.iloc[linha_resultado,coluna_resultado].upper():
                df_dra.at[index,'PARCELA_B_RS'] = float(df_sparta_resultado.iloc[linha_resultado,(coluna_resultado+1)].replace('nan','0').replace(' R$                          -  ','0').replace(' R$                                             -  ','0').replace('-','0').replace(' R$ 0   ','0').replace('DRA','0'))
                  
    df_dra.at[index,'ENCARGOS_RS'] = df_dra.at[index,'RGR_RS'] + df_dra.at[index,'CCC_RS'] + df_dra.at[index,'TFSEE_RS'] + df_dra.at[index,'CDE_TOTAL_RS'] + df_dra.at[index,'CFURH_RS'] + df_dra.at[index,'ESS_EER_RS'] + df_dra.at[index,'PROINFA_RS'] + (float(str(df_dra.at[index,'PD_RS']).replace('nan','0'))) + (float(str(df_dra.at[index,'ONS_RS']).replace('nan','0')))
    df_dra.at[index,'TRANSPORTE_RS'] = df_dra.at[index,'REDE_BASICA_RS'] + df_dra.at[index,'REDE_BASICA_FRONTEIRA_RS'] + df_dra.at[index,'REDE_BASICA_ONS_A2_RS'] + df_dra.at[index,'REDE_BASICA_EXPORT_A2_RS'] + df_dra.at[index,'MUST_ITAIPU_RS'] + df_dra.at[index,'TRANSPORTE_ITAIPU_RS'] + df_dra.at[index,'CONEXAO_RS'] + df_dra.at[index,'SISTEMA_DISTRIBUICAO_RS']
    df_dra.at[index,'PARCELA_A_RS'] = df_dra.at[index,'ENCARGOS_RS'] + df_dra.at[index,'TRANSPORTE_RS'] + df_dra.at[index,'ENERGIA_COMPRADA_RS'] + (float(str(df_dra.at[index,'RECEITA_IRRECUPERAVEL_RS']).replace('nan','0')))
    df_dra.at[index,'RA0_RS'] = df_dra.at[index,'PARCELA_A_RS'] + df_dra.at[index,'PARCELA_B_RS']
    
  
    #Aba Energia
    for linha_energia in linhas_energia:
        for coluna_energia in colunas_energia:
            if (df_sparta_energia.iloc[linha_energia,coluna_energia].upper() == 'ENERGIA REQUERIDA') or ('ENERGIA REQUERIDA (FORNECIMENTO' in df_sparta_energia.iloc[linha_energia,coluna_energia].upper()):
                df_dra.at[index,'ENERGIA_COMPRADA_MWH'] = float(df_sparta_energia.iloc[linha_energia,(coluna_energia+1)].replace('nan','0'))
            elif (df_sparta_energia.iloc[linha_energia,coluna_energia].upper() == 'FORNECIMENTO + SUPRIMENTO'):
                df_dra.at[index,'FORNECIMENTO_SUPRIMENTO_MWH'] = float(df_sparta_energia.iloc[linha_energia,(coluna_energia+1)].replace('nan','0'))
            elif (df_sparta_energia.iloc[linha_energia,coluna_energia].upper() == 'FORNECIMENTO'):
                df_dra.at[index,'FORNECIMENTO_MWH'] = float(df_sparta_energia.iloc[linha_energia,(coluna_energia+1)].replace('nan','0'))
            elif (df_sparta_energia.iloc[linha_energia,coluna_energia].upper() == 'SUPRIMENTO (MERCADO TE)'):
                df_dra.at[index,'SUPRIMENTO_TE_MWH'] = float(df_sparta_energia.iloc[linha_energia,(coluna_energia+1)].replace('nan','0'))
            elif (df_sparta_energia.iloc[linha_energia,0].upper() == 'CUSTO MÉDIO'):
                df_dra.at[index,'CUSTO_MEDIO_RS_MWH'] = float(df_sparta_energia.iloc[linha_energia,1].replace('nan','0'))
            elif (df_sparta_energia.iloc[linha_energia,coluna_energia].upper() == 'PERDA NÃO TÉCNICA'):
                df_dra.at[index,'PERDA_NAO_TECNICA_MWH'] = float(df_sparta_energia.iloc[linha_energia,(coluna_energia+1)].replace('nan','0'))
            elif (df_sparta_energia.iloc[linha_energia,coluna_energia].upper() == 'PERDA TÉCNICA'):
                df_dra.at[index,'PERDA_TECNICA_MWH'] = float(df_sparta_energia.iloc[linha_energia,(coluna_energia+1)].replace('nan','0'))
            elif (df_sparta_energia.iloc[linha_energia,coluna_energia].upper() == 'PERDA REDE BÁSICA SOBRE DIST.'):
                df_dra.at[index,'PERDA_RB_DISTRIBUICAO_MWH'] = float(df_sparta_energia.iloc[linha_energia,(coluna_energia+1)].replace('nan','0'))
            elif (df_sparta_energia.iloc[linha_energia,coluna_energia].upper() == 'PERDA REDE BÁSICA SOBRE MERCADO CAT.'):
                df_dra.at[index,'PERDA_RB_CATIVO_MWH'] = float(df_sparta_energia.iloc[linha_energia,(coluna_energia+1)].replace('nan','0'))
            
    if (df_dra.at[index,'ANO'] == '2020' and df_dra.at[index,'ID'] == 'D11') or (df_dra.at[index,'ANO'] == '2020' and df_dra.at[index,'ID'] == 'D31') or (df_dra.at[index,'ANO'] == '2020' and df_dra.at[index,'ID'] == 'D58') or (df_dra.at[index,'ANO'] == '2020' and df_dra.at[index,'ID'] == 'D61') or (df_dra.at[index,'ANO'] == '2018' and df_dra.at[index,'ID'] == 'D10') or (df_dra.at[index,'ANO'] == '2020' and df_dra.at[index,'ID'] == 'D44') or (df_dra.at[index,'ANO'] == '2018' and df_dra.at[index,'ID'] == 'D05') or (df_dra.at[index,'ANO'] == '2018' and df_dra.at[index,'ID'] == 'D08') or (df_dra.at[index,'ANO'] == '2018' and df_dra.at[index,'ID'] == 'D20') or (df_dra.at[index,'ANO'] == '2017' and df_dra.at[index,'ID'] == 'D23'):
        pass
                    
    else:
        for linha_energia in linhas_energia:
            for coluna_energia in colunas_energia:
                if ('% NÃO TÉCNICA (S/ BAIXA TENSÃO)' in df_sparta_energia.iloc[linha_energia,coluna_energia].upper()):
                    df_dra.at[index,'PERDA_NAO_TECNICA_PERCENT'] = float(df_sparta_energia.iloc[linha_energia,(coluna_energia+1)].replace('nan','0'))
                if ('% TÉCNICA' in df_sparta_energia.iloc[linha_energia,coluna_energia].upper()):
                    df_dra.at[index,'PERDA_TECNICA_PERCENT'] = float(df_sparta_energia.iloc[linha_energia,(coluna_energia+1)].replace('nan','0'))
                elif ('% REDE BÁSICA' in df_sparta_energia.iloc[linha_energia,coluna_energia].upper()):
                    df_dra.at[index,'PERDA_REDE_BASICA_PERCENT'] = float(df_sparta_energia.iloc[linha_energia,(coluna_energia+1)].replace('nan','0'))
                    
                    
    df_dra.at[index,'PERDAS_REGULATORIAS_MWH'] = df_dra.at[index,'PERDA_NAO_TECNICA_MWH'] +  df_dra.at[index,'PERDA_TECNICA_MWH'] + df_dra.at[index,'PERDA_RB_DISTRIBUICAO_MWH'] + df_dra.at[index,'PERDA_RB_CATIVO_MWH']
            

def determina_contrato(df_dra,df_sparta_mercado,index):
    if 'Percentual RI' in df_sparta_mercado.iloc[:,:].values: 
        df_dra.at[index,'CONTRATO'] = 'NOVO'
    else:
        df_dra.at[index,'CONTRATO'] = 'ANTIGO'
        

    
def distribuidora(df_dra,df_sparta_capa,index):
    #Determina o ANO da SPARTA
    df_dra.at[index,'ANO'] = df_sparta_capa.iloc[8,1].strftime('%Y')
    
    #Determina o ID da Distribuidora
    df_dra.at[index,'ID'] = df_sparta_capa.iloc[1,1]
    
    #Determina o NOME da Distribuidora
    df_dra.at[index,'DISTRIBUIDORA'] = df_sparta_capa.iloc[0,0].upper()
    
    #Determina a DATA da SPARTA
    df_dra.at[index,'DATA'] = df_sparta_capa.iloc[8,1].strftime('%Y-%m-%d')
    
    #Determina o EVENTO TARIFARIO
    if 'Reajuste' in df_sparta_capa.iloc[0,1]:
        df_dra.at[index,'EVENTO_TARIFARIO'] = 'RTA'
    elif 'Revisão' in df_sparta_capa.iloc[0,1]:
        df_dra.at[index,'EVENTO_TARIFARIO'] = 'RTP'
    else:
        df_dra.at[index,'EVENTO_TARIFARIO'] = 'RTE'
        
    #Determina a CHAVE
    df_dra.at[index,'CHAVE'] = df_dra.loc[index,'EVENTO_TARIFARIO']+df_dra.loc[index,'ANO']+df_dra.loc[index,'ID']
    
    #Determina UF e PERIODO TARIFARIO
    #AES SUL
    if df_dra.loc[index,'ID'] == 'D01':
        df_dra.at[index,'UF'] = 'RS'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #AME
    if df_dra.loc[index,'ID'] == 'D02':
        df_dra.at[index,'UF'] = 'AM'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #AMPLA
    if df_dra.loc[index,'ID'] == 'D03':
        df_dra.at[index,'UF'] = 'RJ'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #BANDEIRANTE
    if df_dra.loc[index,'ID'] == 'D04':
        df_dra.at[index,'UF'] = 'SP'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #BOA VISTA
    if df_dra.loc[index,'ID'] == 'D05':
        df_dra.at[index,'UF'] = 'RR'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CAIUA
    if df_dra.loc[index,'ID'] == 'D06':
        df_dra.at[index,'UF'] = 'SP'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEA
    if df_dra.loc[index,'ID'] == 'D07':
        df_dra.at[index,'UF'] = 'AP'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CEAL
    if df_dra.loc[index,'ID'] == 'D08':
        df_dra.at[index,'UF'] = 'AL'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CEB
    if df_dra.loc[index,'ID'] == 'D09':
        df_dra.at[index,'UF'] = 'DF'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '4'
    
    #CEEE
    if df_dra.loc[index,'ID'] == 'D10':
        df_dra.at[index,'UF'] = 'RS'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
      
    #CELESC
    if df_dra.loc[index,'ID'] == 'D11':
        df_dra.at[index,'UF'] = 'SC'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CELG
    if df_dra.loc[index,'ID'] == 'D12':
        df_dra.at[index,'UF'] = 'GO'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CELPA
    if df_dra.loc[index,'ID'] == 'D13':
        df_dra.at[index,'UF'] = 'PA'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CELPE
    if df_dra.loc[index,'ID'] == 'D14':
        df_dra.at[index,'UF'] = 'PE'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CELTINS
    if df_dra.loc[index,'ID'] == 'D15':
        df_dra.at[index,'UF'] = 'TO'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEMAR
    if df_dra.loc[index,'ID'] == 'D16':
        df_dra.at[index,'UF'] = 'MA'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #EMT
    if df_dra.loc[index,'ID'] == 'D17':
        df_dra.at[index,'UF'] = 'MT'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEMIG-D
    if df_dra.loc[index,'ID'] == 'D18':
        df_dra.at[index,'UF'] = 'MG'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEPISA
    if df_dra.loc[index,'ID'] == 'D19':
        df_dra.at[index,'UF'] = 'PI'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CERON
    if df_dra.loc[index,'ID'] == 'D20':
        df_dra.at[index,'UF'] = 'RO'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CERR (CONFIMAR O PERIODO)
    if df_dra.loc[index,'ID'] == 'D21':
        df_dra.at[index,'UF'] = 'RR'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CFLO
    if df_dra.loc[index,'ID'] == 'D22':
        df_dra.at[index,'UF'] = 'PR'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CHESP
    if df_dra.loc[index,'ID'] == 'D23':
        df_dra.at[index,'UF'] = 'GO'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL JAGUARI
    if df_dra.loc[index,'ID'] == 'D24':
        df_dra.at[index,'UF'] = 'SP'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL MOCOCA
    if df_dra.loc[index,'ID'] == 'D25':
        df_dra.at[index,'UF'] = 'SP'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'        
    
    #CPFL Santa Cruz
    if df_dra.loc[index,'ID'] == 'D26':
        df_dra.at[index,'UF'] = 'SP'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CNEE
    if df_dra.loc[index,'ID'] == 'D27':
        df_dra.at[index,'UF'] = 'SP'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COCEL
    if df_dra.loc[index,'ID'] == 'D28':
        df_dra.at[index,'UF'] = 'PR'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COELBA
    if df_dra.loc[index,'ID'] == 'D29':
        df_dra.at[index,'UF'] = 'BA'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COELCE
    if df_dra.loc[index,'ID'] == 'D30':
        df_dra.at[index,'UF'] = 'CE'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #COOPERALIANÇA
    if df_dra.loc[index,'ID'] == 'D31':
        df_dra.at[index,'UF'] = 'SC'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COPEL
    if df_dra.loc[index,'ID'] == 'D32':
        df_dra.at[index,'UF'] = 'PR'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COSERN
    if df_dra.loc[index,'ID'] == 'D33':
        df_dra.at[index,'UF'] = 'RN'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Leste Paulista
    if df_dra.loc[index,'ID'] == 'D34':
        df_dra.at[index,'UF'] = 'SP'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Piratininga
    if df_dra.loc[index,'ID'] == 'D35':
        df_dra.at[index,'UF'] = 'SP'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CPFL Paulista
    if df_dra.loc[index,'ID'] == 'D36':
        df_dra.at[index,'UF'] = 'SP'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Sul Paulista
    if df_dra.loc[index,'ID'] == 'D37':
        df_dra.at[index,'UF'] = 'SP'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #DEMEI
    if df_dra.loc[index,'ID'] == 'D38':
        df_dra.at[index,'UF'] = 'RS'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #DME-PC
    if df_dra.loc[index,'ID'] == 'D39':
        df_dra.at[index,'UF'] = 'MG'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EBO
    if df_dra.loc[index,'ID'] == 'D40':
        df_dra.at[index,'UF'] = 'PB'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #EDEVP
    if df_dra.loc[index,'ID'] == 'D41':
        df_dra.at[index,'UF'] = 'SP'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EEB
    if df_dra.loc[index,'ID'] == 'D42':
        df_dra.at[index,'UF'] = 'SP'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EFLJC
    if df_dra.loc[index,'ID'] == 'D43':
        df_dra.at[index,'UF'] = 'SC'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EFLUL
    if df_dra.loc[index,'ID'] == 'D44':
        df_dra.at[index,'UF'] = 'SC'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ELEKTRO
    if df_dra.loc[index,'ID'] == 'D45':
        df_dra.at[index,'UF'] = 'SP'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELETROACRE
    if df_dra.loc[index,'ID'] == 'D46':
        df_dra.at[index,'UF'] = 'AC'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELETROCAR
    if df_dra.loc[index,'ID'] == 'D47':
        df_dra.at[index,'UF'] = 'RS'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ELETROPAULO
    if df_dra.loc[index,'ID'] == 'D48':
        df_dra.at[index,'UF'] = 'SP'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELFSM
    if df_dra.loc[index,'ID'] == 'D49':
        df_dra.at[index,'UF'] = 'ES'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EMG
    if df_dra.loc[index,'ID'] == 'D50':
        df_dra.at[index,'UF'] = 'MG'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EMS
    if df_dra.loc[index,'ID'] == 'D51':
        df_dra.at[index,'UF'] = 'MS'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ENF
    if df_dra.loc[index,'ID'] == 'D52':
        df_dra.at[index,'UF'] = 'RJ'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EPB
    if df_dra.loc[index,'ID'] == 'D53':
        df_dra.at[index,'UF'] = 'PB'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ESCELSA (CONFIRMAR PERIODO)
    if df_dra.loc[index,'ID'] == 'D54':
        df_dra.at[index,'UF'] = 'ES'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '3'
        
    #ESE
    if df_dra.loc[index,'ID'] == 'D55':
        df_dra.at[index,'UF'] = 'SE'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #FORCEL
    if df_dra.loc[index,'ID'] == 'D56':
        df_dra.at[index,'UF'] = 'PR'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #HIDROPAN
    if df_dra.loc[index,'ID'] == 'D57':
        df_dra.at[index,'UF'] = 'RS'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #IENERGIA
    if df_dra.loc[index,'ID'] == 'D58':
        df_dra.at[index,'UF'] = 'SC'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #JARI (CONFIRMAR PERIODO)
    if df_dra.loc[index,'ID'] == 'D59':
        df_dra.at[index,'UF'] = 'PA'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #LIGHT
    if df_dra.loc[index,'ID'] == 'D60':
        df_dra.at[index,'UF'] = 'RJ'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #MUX-Energia
    if df_dra.loc[index,'ID'] == 'D61':
        df_dra.at[index,'UF'] = 'RS'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
    
    #CPFL RGE
    if df_dra.loc[index,'ID'] == 'D62':
        df_dra.at[index,'UF'] = 'RS'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #SULGIPE
    if df_dra.loc[index,'ID'] == 'D63':
        df_dra.at[index,'UF'] = 'SE'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #UHENPAL
    if df_dra.loc[index,'ID'] == 'D64':
        df_dra.at[index,'UF'] = 'TO'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ESS
    if df_dra.loc[index,'ID'] == 'D65':
        df_dra.at[index,'UF'] = 'SP'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Santa Cruz
    if df_dra.loc[index,'ID'] == 'D66':
        df_dra.at[index,'UF'] = 'SP'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #RGE
    if df_dra.loc[index,'ID'] == 'D67':
        df_dra.at[index,'UF'] = 'RS'
        df_dra.at[index,'PERIODO_TARIFARIO'] = '5'
    

    return distribuidora


#%%Inserção dos dados
#Abre a SPARTA de cada arquivo
for arquivo in arquivos:   
    try:
        df_sparta_capa = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA'
                                       ,header = 5
                                       ,nrows = 14
                                       ,usecols = [1,2])   
        
        df_sparta_resultado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Resultado'
                                  ,header=12
                                  ,nrows=35
                                  ,usecols=[1,2,3])
    
        df_sparta_energia = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Energia'
                                  ,header=3
                                  ,nrows=11
                                  ,usecols=[1,2,3,4,5,6,7,8])
    
        df_sparta_mercado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Mercado'
                                  ,header=7
                                  ,nrows=49
                                  ,usecols=[1,2,3,4,5,6,7])
    
    
        print('Leu o arquivo: ',arquivo)
        
        #Define dataframe como string
        df_sparta_resultado = df_sparta_resultado.astype('str')
        df_sparta_energia = df_sparta_energia.astype('str')
        df_sparta_mercado = df_sparta_mercado.astype('str')
    
        #Define as linhas e colunas
        linhas_resultado = range(len(df_sparta_resultado.index))
        colunas_resultado = range(len(df_sparta_resultado.columns))
        linhas_energia = range(len(df_sparta_energia.index))
        colunas_energia = range(len(df_sparta_energia.columns))
        linhas_mercado = range(len(df_sparta_mercado.index))
        colunas_mercado = range(len(df_sparta_mercado.columns))
        
        #Função para extração dos dados de cada SPARTA
        distribuidora(df_dra,df_sparta_capa,index)
        extrai_dra(df_dra,df_sparta_energia,df_sparta_resultado,df_sparta_capa,index)
        determina_contrato(df_dra,df_sparta_mercado,index)  
        print('Extraiu o dado do arquivo: ',arquivo)
        print('Arquivos extraídos: ',round((index/len(arquivos)),2)*100,'%')
        index = index + 1 #Passa para o próximo indice para conseguir dados da proxima SPARTA

    except:
        print('Aba não disponível na SPARTA', arquivo) 

#%%Tratamento de dados 
#Remover dados duplicados e linhas nulas
df_dra = df_dra.drop_duplicates(subset = 'CHAVE',ignore_index = True)
df_dra = df_dra.dropna(axis=0,how='all') 

#Limpeza e Tratamento dos dados
df_dra['RGR_RS'] = df_dra['RGR_RS'].replace(np.nan,0).replace('.',',')
df_dra['CCC_RS'] = df_dra['CCC_RS'].replace(np.nan,0).replace('.',',')
df_dra['TFSEE_RS'] = df_dra['TFSEE_RS'].replace(np.nan,0).replace('.',',')
df_dra['CDE_TOTAL_RS'] = df_dra['CDE_TOTAL_RS'].replace(np.nan,0).replace('.',',')
df_dra['CFURH_RS'] = df_dra['CFURH_RS'].replace(np.nan,0).replace('.',',')
df_dra['ESS_EER_RS'] = df_dra['ESS_EER_RS'].replace(np.nan,0).replace('.',',')
df_dra['PROINFA_RS'] = df_dra['PROINFA_RS'].replace(np.nan,0).replace('.',',')
df_dra['PD_RS'] = df_dra['PD_RS'].replace(np.nan,0).replace('.',',')
df_dra['ONS_RS'] = df_dra['ONS_RS'].replace(np.nan,0).replace('.',',')
df_dra['ENCARGOS_RS'] = df_dra['ENCARGOS_RS'].replace(np.nan,0).replace('.',',')
df_dra['TRANSPORTE_RS'] = df_dra['TRANSPORTE_RS'].replace(np.nan,0).replace('.',',')
df_dra['REDE_BASICA_RS'] = df_dra['REDE_BASICA_RS'].replace(np.nan,0).replace('.',',')
df_dra['REDE_BASICA_FRONTEIRA_RS'] = df_dra['REDE_BASICA_FRONTEIRA_RS'].replace(np.nan,0).replace('.',',')
df_dra['REDE_BASICA_ONS_A2_RS'] = df_dra['REDE_BASICA_ONS_A2_RS'].replace(np.nan,0).replace('.',',')
df_dra['REDE_BASICA_EXPORT_A2_RS'] = df_dra['REDE_BASICA_EXPORT_A2_RS'].replace(np.nan,0).replace('.',',')
df_dra['MUST_ITAIPU_RS'] = df_dra['MUST_ITAIPU_RS'].replace(np.nan,0).replace('.',',')
df_dra['TRANSPORTE_ITAIPU_RS'] = df_dra['TRANSPORTE_ITAIPU_RS'].replace(np.nan,0).replace('.',',')
df_dra['CONEXAO_RS'] = df_dra['CONEXAO_RS'].replace(np.nan,0).replace('.',',')
df_dra['SISTEMA_DISTRIBUICAO_RS'] = df_dra['SISTEMA_DISTRIBUICAO_RS'].replace(np.nan,0).replace('.',',')
df_dra['ENERGIA_COMPRADA_RS'] = df_dra['ENERGIA_COMPRADA_RS'].replace(np.nan,0).replace('.',',')
df_dra['RECEITA_IRRECUPERAVEL_RS'] = df_dra['RECEITA_IRRECUPERAVEL_RS'].replace(np.nan,0).replace('.',',')
df_dra['PARCELA_A_RS'] = df_dra['PARCELA_A_RS'].replace(np.nan,0).replace('.',',')
df_dra['PARCELA_B_RS'] = df_dra['PARCELA_B_RS'].replace(np.nan,0).replace('.',',')
df_dra['RA0_RS'] = df_dra['RA0_RS'].replace(np.nan,0).replace('.',',')
df_dra['ENERGIA_COMPRADA_MWH'] = df_dra['ENERGIA_COMPRADA_MWH'].replace(np.nan,0).replace('.',',')
df_dra['FORNECIMENTO_SUPRIMENTO_MWH'] = df_dra['FORNECIMENTO_SUPRIMENTO_MWH'].replace(np.nan,0).replace('.',',')
df_dra['FORNECIMENTO_MWH'] = df_dra['FORNECIMENTO_MWH'].replace(np.nan,0).replace('.',',')
df_dra['SUPRIMENTO_TE_MWH'] = df_dra['SUPRIMENTO_TE_MWH'].replace(np.nan,0).replace('.',',')
df_dra['CUSTO_MEDIO_RS_MWH'] = df_dra['CUSTO_MEDIO_RS_MWH'].replace(np.nan,0).replace('.',',')
df_dra['PERDA_NAO_TECNICA_MWH'] = df_dra['PERDA_NAO_TECNICA_MWH'].replace(np.nan,0).replace('.',',')
df_dra['PERDA_TECNICA_MWH'] = df_dra['PERDA_TECNICA_MWH'].replace(np.nan,0).replace('.',',')
df_dra['PERDA_RB_DISTRIBUICAO_MWH'] = df_dra['PERDA_RB_DISTRIBUICAO_MWH'].replace(np.nan,0).replace('.',',')
df_dra['PERDA_RB_CATIVO_MWH'] = df_dra['PERDA_RB_CATIVO_MWH'].replace(np.nan,0).replace('.',',')
df_dra['PERDAS_REGULATORIAS_MWH'] = df_dra['PERDAS_REGULATORIAS_MWH'].replace(np.nan,0).replace('.',',')
df_dra['PERDA_NAO_TECNICA_PERCENT'] = df_dra['PERDA_NAO_TECNICA_PERCENT'].replace(np.nan,0).replace('.',',')
df_dra['PERDA_TECNICA_PERCENT'] = df_dra['PERDA_TECNICA_PERCENT'].replace(np.nan,0).replace('.',',')
df_dra['PERDA_REDE_BASICA_PERCENT'] = df_dra['PERDA_REDE_BASICA_PERCENT'].replace(np.nan,0).replace('.',',')

# Define a data de atualização dos dados
data = datetime.today().strftime('%d/%m/%Y')
df_dra['DATA_ATUALIZA'] = data

#%% Inserir dados no banco de dados

#Criar a lista para inserção no banco SQL com os dados da base editada
dados_list = df_dra.values.tolist()


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
        cursor.execute('''DELETE FROM ''' + tabela_oracle + ''' WHERE ANO = ''' + ano_oracle) #Exclui somente os dados de 2023
        sql = '''INSERT INTO ''' + tabela_oracle +''' VALUES (:1	,:2	,:3	,:4	,:5	,:6	,:7	,:8	,:9	,:10	,:11	,:12	,:13	,:14	,:15	,:16	,:17	,:18	,:19	,:20	,:21	,:22	,:23	,:24	,:25	,:26	,:27	,:28	,:29	,:30	,:31	,:32	,:33	,:34	,:35	,:36	,:37	,:38	,:39	,:40	,:41	,:42	,:43	,:44	,:45	,:46,:47)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        cursor.executemany(sql, dados_list)
    except Exception as err:
        print('Erro no Insert:', err)
    else:
        print('Carga executada com sucesso!')
        connection.commit() #Caso seja executado com sucesso, esse comando salva a base de dados
    finally:
        cursor.close()
        connection.close()




 
