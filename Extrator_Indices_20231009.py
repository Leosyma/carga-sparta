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
#pasta = r"C:\Users\2018459\OneDrive - CPFL Energia S A\Área de Trabalho\RTP e RTA (concessionaria)\SPARTA TESTE\*"

#Arquivos
#Como os dados das empresas estão em arquivos separados, listar todos que serão carregados conforme layout abaixo.
#arquivo ='SPARTA_BANDEIRANTE_2013.xlsx'
arquivos = glob.glob(pasta)


index = 0 #Flag para inserção dos dados

#Nome da tabela Oracle onde será dada a carga
tabela_oracle = 'SPARTA_INDICES'
ano_oracle = "'2023'"


#%% Montar o dataframe da estrutura da aba 'SPARTA_NUC'
#Defino o indice máximo que é igual o número de SPARTA
index_maximo = list(range(0,len(arquivos)))

#Criação da estrutura do tabela NUC
df_indices = pd.DataFrame(data = [], columns=['CHAVE','EVENTO_TARIFARIO','ANO','ID','DISTRIBUIDORA','UF','DATA','PERIODO_TARIFARIO','CONTRATO','IRT_ECONOMICO_PERCENT','IRT_FINANCEIRO_PERCENT','IRT_FINAN_ECON_PERCENT','EFEITO_AT_PERCENT','EFEITO_BT_PERCENT','EFEITO_TARIFA_AT_BT_PERCENT','TARIFA_RESIDEN_B1_RS','ICMS_PERCENT','PIS_PERCENT','PERDAS_RB_MWH','PERDAS_DITS_MWH'],index=index_maximo) 

#Criação dataframe vazio
df_sparta_indices = pd.DataFrame(data = [])
df_sparta_capa = pd.DataFrame(data=[])
df_sparta_mercado = pd.DataFrame(data=[])
df_sparta_bd = pd.DataFrame(data=[])

# df_sparta_indices = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Resultado'
#                               ,header=4
#                               ,nrows=6
#                               ,usecols=[1,2,3,10,11])

# df_sparta_mercado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Mercado'
#                               ,header=7
#                               ,nrows=49
#                               ,usecols=[1,2,3,4,5,6,7])

# df_sparta_bd = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'BD'
#                               ,header=55
#                               ,nrows=3
#                               ,usecols=[11,12])
 

#%%Extração dos resultados
def extrai_indices(df_sparta_indices,df_indices,df_sparta_mercado,df_sparta_bd,index,df_sparta_capa):
    #Tratamento especifico para a SPARTA de 2020 da ERO D20 - IGPM = 925.887
    if 'D20' in df_sparta_capa.iloc[:,:].values and 925.887 in df_sparta_capa.iloc[:,:].values:
        df_indices.at[index,'IRT_ECONOMICO_PERCENT'] = df_sparta_indices.iloc[0,2] 
        df_indices.at[index,'IRT_FINANCEIRO_PERCENT'] = df_sparta_indices.iloc[1,2]  
        df_indices.at[index,'IRT_FINAN_ECON_PERCENT'] = df_sparta_indices.iloc[2,2]
        df_indices.at[index,'EFEITO_AT_PERCENT'] = df_sparta_indices.iloc[2,3]
        df_indices.at[index,'EFEITO_BT_PERCENT'] = df_sparta_indices.iloc[3,3]
        df_indices.at[index,'EFEITO_TARIFA_AT_BT_PERCENT'] = df_sparta_indices.iloc[4,3]
        df_indices.at[index,'TARIFA_RESIDEN_B1_RS'] = df_sparta_indices.iloc[5,3]
        df_indices.at[index,'ICMS_PERCENT'] = df_sparta_mercado.iloc[0,5]
        df_indices.at[index,'PIS_PERCENT'] = df_sparta_mercado.iloc[1,5]
        df_indices.at[index,'PERDAS_RB_MWH'] = df_sparta_bd.iloc[0,1]
        df_indices.at[index,'PERDAS_DITS_MWH'] = df_sparta_bd.iloc[1,1]
        df_indices.at[index,'CONTRATO'] = 'NOVO'  
    
    #Tratamento especifico para a SPARTA de 2019 da CERON D20 - IGPM = 743.558
    elif 'D20' in df_sparta_capa.iloc[:,:].values and 743.558 in df_sparta_capa.iloc[:,:].values:
        df_indices.at[index,'IRT_ECONOMICO_PERCENT'] = df_sparta_indices.iloc[0,2] 
        df_indices.at[index,'IRT_FINANCEIRO_PERCENT'] = df_sparta_indices.iloc[1,2]  
        df_indices.at[index,'IRT_FINAN_ECON_PERCENT'] = df_sparta_indices.iloc[2,2]
        df_indices.at[index,'EFEITO_AT_PERCENT'] = df_sparta_indices.iloc[2,3]
        df_indices.at[index,'EFEITO_BT_PERCENT'] = df_sparta_indices.iloc[3,3]
        df_indices.at[index,'EFEITO_TARIFA_AT_BT_PERCENT'] = df_sparta_indices.iloc[4,3]
        df_indices.at[index,'TARIFA_RESIDEN_B1_RS'] = df_sparta_indices.iloc[5,3]
        df_indices.at[index,'ICMS_PERCENT'] = df_sparta_mercado.iloc[0,5]
        df_indices.at[index,'PIS_PERCENT'] = df_sparta_mercado.iloc[1,5]
        df_indices.at[index,'PERDAS_RB_MWH'] = df_sparta_bd.iloc[0,1]
        df_indices.at[index,'PERDAS_DITS_MWH'] = df_sparta_bd.iloc[1,1]
        df_indices.at[index,'CONTRATO'] = 'NOVO'    
        
    #Tratamento especifico para a SPARTA de 2013 da EFLJC D43
    elif 'D43' in df_sparta_capa.iloc[:,:].values and 2013 in df_sparta_capa.iloc[:,:].values:
        df_indices.at[index,'IRT_ECONOMICO_PERCENT'] = df_sparta_indices.iloc[0,1] 
        df_indices.at[index,'IRT_FINANCEIRO_PERCENT'] = df_sparta_indices.iloc[1,1]  
        df_indices.at[index,'IRT_FINAN_ECON_PERCENT'] = df_sparta_indices.iloc[2,1]
        df_indices.at[index,'EFEITO_AT_PERCENT'] = '0'
        df_indices.at[index,'EFEITO_BT_PERCENT'] = '0'
        df_indices.at[index,'EFEITO_TARIFA_AT_BT_PERCENT'] = '0'
        df_indices.at[index,'TARIFA_RESIDEN_B1_RS'] = '0'
        df_indices.at[index,'ICMS_PERCENT'] = df_sparta_mercado.iloc[4,5]
        df_indices.at[index,'PIS_PERCENT'] = df_sparta_mercado.iloc[5,5]
        df_indices.at[index,'PERDAS_RB_MWH'] = '0'
        df_indices.at[index,'PERDAS_DITS_MWH'] = '0'
        df_indices.at[index,'CONTRATO'] = 'ANTIGO'
        
    #Tratamento especifico para a SPARTA de 2013 da CELESC D11
    elif 'D11' in df_sparta_capa.iloc[:,:].values and 2013 in df_sparta_capa.iloc[:,:].values:
        df_indices.at[index,'IRT_ECONOMICO_PERCENT'] = df_sparta_indices.iloc[0,1] 
        df_indices.at[index,'IRT_FINANCEIRO_PERCENT'] = df_sparta_indices.iloc[1,1]  
        df_indices.at[index,'IRT_FINAN_ECON_PERCENT'] = df_sparta_indices.iloc[2,1]
        df_indices.at[index,'EFEITO_AT_PERCENT'] = '0'
        df_indices.at[index,'EFEITO_BT_PERCENT'] = '0'
        df_indices.at[index,'EFEITO_TARIFA_AT_BT_PERCENT'] = '0'
        df_indices.at[index,'TARIFA_RESIDEN_B1_RS'] = '0'
        df_indices.at[index,'ICMS_PERCENT'] = df_sparta_mercado.iloc[4,5]
        df_indices.at[index,'PIS_PERCENT'] = df_sparta_mercado.iloc[5,5]
        df_indices.at[index,'PERDAS_RB_MWH'] = '0'
        df_indices.at[index,'PERDAS_DITS_MWH'] = '0'
        df_indices.at[index,'CONTRATO'] = 'ANTIGO' 
        
    #Tratamento especifico para a SPARTA de 2013 da FORCEL D56
    elif 'D56' in df_sparta_capa.iloc[:,:].values and 2013 in df_sparta_capa.iloc[:,:].values:
        df_indices.at[index,'IRT_ECONOMICO_PERCENT'] = df_sparta_indices.iloc[0,2] 
        df_indices.at[index,'IRT_FINANCEIRO_PERCENT'] = df_sparta_indices.iloc[1,2]  
        df_indices.at[index,'IRT_FINAN_ECON_PERCENT'] = df_sparta_indices.iloc[2,2]
        df_indices.at[index,'EFEITO_AT_PERCENT'] = '0'
        df_indices.at[index,'EFEITO_BT_PERCENT'] = '0'
        df_indices.at[index,'EFEITO_TARIFA_AT_BT_PERCENT'] = '0'
        df_indices.at[index,'TARIFA_RESIDEN_B1_RS'] = '0'
        df_indices.at[index,'ICMS_PERCENT'] = df_sparta_mercado.iloc[4,5]
        df_indices.at[index,'PIS_PERCENT'] = df_sparta_mercado.iloc[5,5]
        df_indices.at[index,'PERDAS_RB_MWH'] = '0'
        df_indices.at[index,'PERDAS_DITS_MWH'] = '0'
        df_indices.at[index,'CONTRATO'] = 'ANTIGO'
        
    #Tratamento especifico para a SPARTA de 2013 da ELEKTRO D45
    elif 'D45' in df_sparta_capa.iloc[:,:].values and 2013 in df_sparta_capa.iloc[:,:].values:
        df_indices.at[index,'IRT_ECONOMICO_PERCENT'] = df_sparta_indices.iloc[0,2] 
        df_indices.at[index,'IRT_FINANCEIRO_PERCENT'] = df_sparta_indices.iloc[1,2]  
        df_indices.at[index,'IRT_FINAN_ECON_PERCENT'] = df_sparta_indices.iloc[2,2]
        df_indices.at[index,'EFEITO_AT_PERCENT'] = '0'
        df_indices.at[index,'EFEITO_BT_PERCENT'] = '0'
        df_indices.at[index,'EFEITO_TARIFA_AT_BT_PERCENT'] = '0'
        df_indices.at[index,'TARIFA_RESIDEN_B1_RS'] = '0'
        df_indices.at[index,'ICMS_PERCENT'] = df_sparta_mercado.iloc[4,5]
        df_indices.at[index,'PIS_PERCENT'] = df_sparta_mercado.iloc[5,5]
        df_indices.at[index,'PERDAS_RB_MWH'] = '0'
        df_indices.at[index,'PERDAS_DITS_MWH'] = '0'
        df_indices.at[index,'CONTRATO'] = 'ANTIGO'
        
    #Tratamento especifico para as SPARTA D46, D02, D43, D08 e D05
    elif ('D46' in df_sparta_capa.iloc[:,:].values and 925.887 in df_sparta_capa.iloc[:,:].values) or ('D02' in df_sparta_capa.iloc[:,:].values and 880.7738764 in df_sparta_capa.iloc[:,:].values) or ('D42' in df_sparta_capa.iloc[:,:].values and 648.409 in df_sparta_capa.iloc[:,:].values) or ('D08' in df_sparta_capa.iloc[:,:].values and 775.2272044 in df_sparta_capa.iloc[:,:].values) or ('D05' in df_sparta_capa.iloc[:,:].values and 880.7738764 in df_sparta_capa.iloc[:,:].values):
        df_indices.at[index,'IRT_ECONOMICO_PERCENT'] = df_sparta_indices.iloc[0,2] 
        df_indices.at[index,'IRT_FINANCEIRO_PERCENT'] = df_sparta_indices.iloc[1,2]  
        df_indices.at[index,'IRT_FINAN_ECON_PERCENT'] = df_sparta_indices.iloc[2,2]
        df_indices.at[index,'EFEITO_AT_PERCENT'] = df_sparta_indices.iloc[2,5]
        df_indices.at[index,'EFEITO_BT_PERCENT'] = df_sparta_indices.iloc[3,5]
        df_indices.at[index,'EFEITO_TARIFA_AT_BT_PERCENT'] = df_sparta_indices.iloc[4,5]
        df_indices.at[index,'TARIFA_RESIDEN_B1_RS'] = df_sparta_indices.iloc[5,5]
        df_indices.at[index,'ICMS_PERCENT'] = df_sparta_mercado.iloc[0,5]
        df_indices.at[index,'PIS_PERCENT'] = df_sparta_mercado.iloc[1,5]
        df_indices.at[index,'PERDAS_RB_MWH'] = df_sparta_bd.iloc[0,1]
        df_indices.at[index,'PERDAS_DITS_MWH'] = df_sparta_bd.iloc[1,1]
        df_indices.at[index,'CONTRATO'] = 'NOVO' 
        
    #Tratamento especifico para as SPARTA D29,D36,D52,D42,D06,D55,D14,D27,D28,D33,D41,D62,D17,D18,D64,D01,D32,D50,D51 do ano de 2015
    elif ('D29' in df_sparta_capa.iloc[:,:].values and 2015 in df_sparta_capa.iloc[:,:].values) or ('D36' in df_sparta_capa.iloc[:,:].values and 2015 in df_sparta_capa.iloc[:,:].values) or ('D52' in df_sparta_capa.iloc[:,:].values and 2015 in df_sparta_capa.iloc[:,:].values) or ('D42' in df_sparta_capa.iloc[:,:].values and 2015 in df_sparta_capa.iloc[:,:].values) or ('D06' in df_sparta_capa.iloc[:,:].values and 2015 in df_sparta_capa.iloc[:,:].values) or ('D55' in df_sparta_capa.iloc[:,:].values and 2015 in df_sparta_capa.iloc[:,:].values) or ('D14' in df_sparta_capa.iloc[:,:].values and 2015 in df_sparta_capa.iloc[:,:].values) or ('D27' in df_sparta_capa.iloc[:,:].values and 2015 in df_sparta_capa.iloc[:,:].values) or ('D28' in df_sparta_capa.iloc[:,:].values and 2015 in df_sparta_capa.iloc[:,:].values) or ('D33' in df_sparta_capa.iloc[:,:].values and 2015 in df_sparta_capa.iloc[:,:].values) or ('D41' in df_sparta_capa.iloc[:,:].values and 2015 in df_sparta_capa.iloc[:,:].values) or ('D62' in df_sparta_capa.iloc[:,:].values and 2015 in df_sparta_capa.iloc[:,:].values) or ('D17' in df_sparta_capa.iloc[:,:].values and 2015 in df_sparta_capa.iloc[:,:].values) or ('D18' in df_sparta_capa.iloc[:,:].values and 2015 in df_sparta_capa.iloc[:,:].values) or ('D64' in df_sparta_capa.iloc[:,:].values and 2015 in df_sparta_capa.iloc[:,:].values) or ('D01' in df_sparta_capa.iloc[:,:].values and 2015 in df_sparta_capa.iloc[:,:].values) or ('D32' in df_sparta_capa.iloc[:,:].values and 2015 in df_sparta_capa.iloc[:,:].values) or ('D50' in df_sparta_capa.iloc[:,:].values and 2015 in df_sparta_capa.iloc[:,:].values) or ('D51' in df_sparta_capa.iloc[:,:].values and 2015 in df_sparta_capa.iloc[:,:].values):
        df_indices.at[index,'IRT_ECONOMICO_PERCENT'] = df_sparta_indices.iloc[0,2] 
        df_indices.at[index,'IRT_FINANCEIRO_PERCENT'] = df_sparta_indices.iloc[1,2]  
        df_indices.at[index,'IRT_FINAN_ECON_PERCENT'] = df_sparta_indices.iloc[2,2]
        df_indices.at[index,'EFEITO_AT_PERCENT'] = df_sparta_indices.iloc[2,4]
        df_indices.at[index,'EFEITO_BT_PERCENT'] = df_sparta_indices.iloc[3,4]
        df_indices.at[index,'EFEITO_TARIFA_AT_BT_PERCENT'] = df_sparta_indices.iloc[4,4]
        df_indices.at[index,'TARIFA_RESIDEN_B1_RS'] = df_sparta_indices.iloc[5,4]
        df_indices.at[index,'ICMS_PERCENT'] = df_sparta_mercado.iloc[0,5]
        df_indices.at[index,'PIS_PERCENT'] = df_sparta_mercado.iloc[1,5]
        df_indices.at[index,'PERDAS_RB_MWH'] = '0'
        df_indices.at[index,'PERDAS_DITS_MWH'] = '0'
        df_indices.at[index,'CONTRATO'] = 'ANTIGO'
        
        
    #Se o contrato for novo usa essa posição para inserir os dados
    elif 'Percentual RI' in df_sparta_mercado.iloc[:,:].values: 
        df_indices.at[index,'IRT_ECONOMICO_PERCENT'] = df_sparta_indices.iloc[0,2] 
        df_indices.at[index,'IRT_FINANCEIRO_PERCENT'] = df_sparta_indices.iloc[1,2]  
        df_indices.at[index,'IRT_FINAN_ECON_PERCENT'] = df_sparta_indices.iloc[2,2]
        df_indices.at[index,'EFEITO_AT_PERCENT'] = df_sparta_indices.iloc[2,4]
        df_indices.at[index,'EFEITO_BT_PERCENT'] = df_sparta_indices.iloc[3,4]
        df_indices.at[index,'EFEITO_TARIFA_AT_BT_PERCENT'] = df_sparta_indices.iloc[4,4]
        df_indices.at[index,'TARIFA_RESIDEN_B1_RS'] = df_sparta_indices.iloc[5,4]
        df_indices.at[index,'ICMS_PERCENT'] = df_sparta_mercado.iloc[0,5]
        df_indices.at[index,'PIS_PERCENT'] = df_sparta_mercado.iloc[1,5]
        df_indices.at[index,'PERDAS_RB_MWH'] = df_sparta_bd.iloc[0,1]
        df_indices.at[index,'PERDAS_DITS_MWH'] = df_sparta_bd.iloc[1,1]
        df_indices.at[index,'CONTRATO'] = 'NOVO'   
      
    #Usamos esse formato para SPARTA de 2013,2014 e 2015
    elif 'RI' in df_sparta_mercado.iloc[:,:].values:
        df_indices.at[index,'IRT_ECONOMICO_PERCENT'] = df_sparta_indices.iloc[0,2] 
        df_indices.at[index,'IRT_FINANCEIRO_PERCENT'] = df_sparta_indices.iloc[1,2]  
        df_indices.at[index,'IRT_FINAN_ECON_PERCENT'] = df_sparta_indices.iloc[2,2]
        df_indices.at[index,'EFEITO_AT_PERCENT'] = df_sparta_indices.iloc[2,4]
        df_indices.at[index,'EFEITO_BT_PERCENT'] = df_sparta_indices.iloc[3,4]
        df_indices.at[index,'EFEITO_TARIFA_AT_BT_PERCENT'] = df_sparta_indices.iloc[4,4]
        df_indices.at[index,'TARIFA_RESIDEN_B1_RS'] = df_sparta_indices.iloc[5,4]
        df_indices.at[index,'ICMS_PERCENT'] = df_sparta_mercado.iloc[4,5]
        df_indices.at[index,'PIS_PERCENT'] = df_sparta_mercado.iloc[5,5]
        df_indices.at[index,'PERDAS_RB_MWH'] = '0'
        df_indices.at[index,'PERDAS_DITS_MWH'] = '0'
        df_indices.at[index,'CONTRATO'] = 'ANTIGO'
      
 
    # Se o contrato for antigo usa essa posição para inserir os dados   
    else: 
        df_indices.at[index,'IRT_ECONOMICO_PERCENT'] = df_sparta_indices.iloc[0,2] 
        df_indices.at[index,'IRT_FINANCEIRO_PERCENT'] = df_sparta_indices.iloc[1,2]  
        df_indices.at[index,'IRT_FINAN_ECON_PERCENT'] = df_sparta_indices.iloc[2,2]
        df_indices.at[index,'EFEITO_AT_PERCENT'] = df_sparta_indices.iloc[2,4]
        df_indices.at[index,'EFEITO_BT_PERCENT'] = df_sparta_indices.iloc[3,4]
        df_indices.at[index,'EFEITO_TARIFA_AT_BT_PERCENT'] = df_sparta_indices.iloc[4,4]
        df_indices.at[index,'TARIFA_RESIDEN_B1_RS'] = df_sparta_indices.iloc[5,4]
        df_indices.at[index,'ICMS_PERCENT'] = df_sparta_mercado.iloc[0,5]
        df_indices.at[index,'PIS_PERCENT'] = df_sparta_mercado.iloc[1,5]
        df_indices.at[index,'PERDAS_RB_MWH'] = df_sparta_bd.iloc[0,1]
        df_indices.at[index,'PERDAS_DITS_MWH'] = df_sparta_bd.iloc[1,1]
        df_indices.at[index,'CONTRATO'] = 'ANTIGO'
        

    return extrai_indices



def distribuidora(self,df_indices,index):
    #Determina o ANO da SPARTA
    df_indices.at[index,'ANO'] = df_sparta_capa.iloc[8,1].strftime('%Y')
    
    #Determina o ID da Distribuidora
    df_indices.at[index,'ID'] = df_sparta_capa.iloc[1,1]
    
    #Determina o NOME da Distribuidora
    df_indices.at[index,'DISTRIBUIDORA'] = df_sparta_capa.iloc[0,0].upper()
    
    #Determina a DATA da SPARTA
    df_indices.at[index,'DATA'] = df_sparta_capa.iloc[8,1].strftime('%Y-%m-%d')
    
    #Determina o EVENTO TARIFARIO
    if 'Reajuste' in df_sparta_capa.iloc[0,1]:
        df_indices.at[index,'EVENTO_TARIFARIO'] = 'RTA'
    elif 'Revisão' in df_sparta_capa.iloc[0,1]:
        df_indices.at[index,'EVENTO_TARIFARIO'] = 'RTP'
    else:
        df_indices.at[index,'EVENTO_TARIFARIO'] = 'RTE'
        
    #Determina a CHAVE
    df_indices.at[index,'CHAVE'] = df_indices.loc[index,'EVENTO_TARIFARIO']+df_indices.loc[index,'ANO']+df_indices.loc[index,'ID']
    
    #Determina UF e PERIODO TARIFARIO
    #AES SUL
    if df_indices.loc[index,'ID'] == 'D01':
        df_indices.at[index,'UF'] = 'RS'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #AME
    if df_indices.loc[index,'ID'] == 'D02':
        df_indices.at[index,'UF'] = 'AM'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #AMPLA
    if df_indices.loc[index,'ID'] == 'D03':
        df_indices.at[index,'UF'] = 'RJ'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #BANDEIRANTE
    if df_indices.loc[index,'ID'] == 'D04':
        df_indices.at[index,'UF'] = 'SP'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #BOA VISTA
    if df_indices.loc[index,'ID'] == 'D05':
        df_indices.at[index,'UF'] = 'RR'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CAIUA
    if df_indices.loc[index,'ID'] == 'D06':
        df_indices.at[index,'UF'] = 'SP'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEA
    if df_indices.loc[index,'ID'] == 'D07':
        df_indices.at[index,'UF'] = 'AP'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CEAL
    if df_indices.loc[index,'ID'] == 'D08':
        df_indices.at[index,'UF'] = 'AL'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CEB
    if df_indices.loc[index,'ID'] == 'D09':
        df_indices.at[index,'UF'] = 'DF'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '4'
    
    #CEEE
    if df_indices.loc[index,'ID'] == 'D10':
        df_indices.at[index,'UF'] = 'RS'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
      
    #CELESC
    if df_indices.loc[index,'ID'] == 'D11':
        df_indices.at[index,'UF'] = 'SC'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CELG
    if df_indices.loc[index,'ID'] == 'D12':
        df_indices.at[index,'UF'] = 'GO'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CELPA
    if df_indices.loc[index,'ID'] == 'D13':
        df_indices.at[index,'UF'] = 'PA'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CELPE
    if df_indices.loc[index,'ID'] == 'D14':
        df_indices.at[index,'UF'] = 'PE'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CELTINS
    if df_indices.loc[index,'ID'] == 'D15':
        df_indices.at[index,'UF'] = 'TO'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEMAR
    if df_indices.loc[index,'ID'] == 'D16':
        df_indices.at[index,'UF'] = 'MA'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #EMT
    if df_indices.loc[index,'ID'] == 'D17':
        df_indices.at[index,'UF'] = 'MT'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEMIG-D
    if df_indices.loc[index,'ID'] == 'D18':
        df_indices.at[index,'UF'] = 'MG'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEPISA
    if df_indices.loc[index,'ID'] == 'D19':
        df_indices.at[index,'UF'] = 'PI'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CERON
    if df_indices.loc[index,'ID'] == 'D20':
        df_indices.at[index,'UF'] = 'RO'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CERR (CONFIMAR O PERIODO)
    if df_indices.loc[index,'ID'] == 'D21':
        df_indices.at[index,'UF'] = 'RR'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CFLO
    if df_indices.loc[index,'ID'] == 'D22':
        df_indices.at[index,'UF'] = 'PR'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CHESP
    if df_indices.loc[index,'ID'] == 'D23':
        df_indices.at[index,'UF'] = 'GO'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL JAGUARI
    if df_indices.loc[index,'ID'] == 'D24':
        df_indices.at[index,'UF'] = 'SP'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL MOCOCA
    if df_indices.loc[index,'ID'] == 'D25':
        df_indices.at[index,'UF'] = 'SP'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'        
    
    #CPFL Santa Cruz
    if df_indices.loc[index,'ID'] == 'D26':
        df_indices.at[index,'UF'] = 'SP'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CNEE
    if df_indices.loc[index,'ID'] == 'D27':
        df_indices.at[index,'UF'] = 'SP'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COCEL
    if df_indices.loc[index,'ID'] == 'D28':
        df_indices.at[index,'UF'] = 'PR'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COELBA
    if df_indices.loc[index,'ID'] == 'D29':
        df_indices.at[index,'UF'] = 'BA'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COELCE
    if df_indices.loc[index,'ID'] == 'D30':
        df_indices.at[index,'UF'] = 'CE'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #COOPERALIANÇA
    if df_indices.loc[index,'ID'] == 'D31':
        df_indices.at[index,'UF'] = 'SC'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COPEL
    if df_indices.loc[index,'ID'] == 'D32':
        df_indices.at[index,'UF'] = 'PR'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COSERN
    if df_indices.loc[index,'ID'] == 'D33':
        df_indices.at[index,'UF'] = 'RN'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Leste Paulista
    if df_indices.loc[index,'ID'] == 'D34':
        df_indices.at[index,'UF'] = 'SP'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Piratininga
    if df_indices.loc[index,'ID'] == 'D35':
        df_indices.at[index,'UF'] = 'SP'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CPFL Paulista
    if df_indices.loc[index,'ID'] == 'D36':
        df_indices.at[index,'UF'] = 'SP'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Sul Paulista
    if df_indices.loc[index,'ID'] == 'D37':
        df_indices.at[index,'UF'] = 'SP'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #DEMEI
    if df_indices.loc[index,'ID'] == 'D38':
        df_indices.at[index,'UF'] = 'RS'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #DME-PC
    if df_indices.loc[index,'ID'] == 'D39':
        df_indices.at[index,'UF'] = 'MG'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EBO
    if df_indices.loc[index,'ID'] == 'D40':
        df_indices.at[index,'UF'] = 'PB'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #EDEVP
    if df_indices.loc[index,'ID'] == 'D41':
        df_indices.at[index,'UF'] = 'SP'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EEB
    if df_indices.loc[index,'ID'] == 'D42':
        df_indices.at[index,'UF'] = 'SP'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EFLJC
    if df_indices.loc[index,'ID'] == 'D43':
        df_indices.at[index,'UF'] = 'SC'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EFLUL
    if df_indices.loc[index,'ID'] == 'D44':
        df_indices.at[index,'UF'] = 'SC'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ELEKTRO
    if df_indices.loc[index,'ID'] == 'D45':
        df_indices.at[index,'UF'] = 'SP'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELETROACRE
    if df_indices.loc[index,'ID'] == 'D46':
        df_indices.at[index,'UF'] = 'AC'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELETROCAR
    if df_indices.loc[index,'ID'] == 'D47':
        df_indices.at[index,'UF'] = 'RS'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ELETROPAULO
    if df_indices.loc[index,'ID'] == 'D48':
        df_indices.at[index,'UF'] = 'SP'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELFSM
    if df_indices.loc[index,'ID'] == 'D49':
        df_indices.at[index,'UF'] = 'ES'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EMG
    if df_indices.loc[index,'ID'] == 'D50':
        df_indices.at[index,'UF'] = 'MG'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EMS
    if df_indices.loc[index,'ID'] == 'D51':
        df_indices.at[index,'UF'] = 'MS'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ENF
    if df_indices.loc[index,'ID'] == 'D52':
        df_indices.at[index,'UF'] = 'RJ'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EPB
    if df_indices.loc[index,'ID'] == 'D53':
        df_indices.at[index,'UF'] = 'PB'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ESCELSA (CONFIRMAR PERIODO)
    if df_indices.loc[index,'ID'] == 'D54':
        df_indices.at[index,'UF'] = 'ES'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '3'
        
    #ESE
    if df_indices.loc[index,'ID'] == 'D55':
        df_indices.at[index,'UF'] = 'SE'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #FORCEL
    if df_indices.loc[index,'ID'] == 'D56':
        df_indices.at[index,'UF'] = 'PR'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #HIDROPAN
    if df_indices.loc[index,'ID'] == 'D57':
        df_indices.at[index,'UF'] = 'RS'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #IENERGIA
    if df_indices.loc[index,'ID'] == 'D58':
        df_indices.at[index,'UF'] = 'SC'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #JARI (CONFIRMAR PERIODO)
    if df_indices.loc[index,'ID'] == 'D59':
        df_indices.at[index,'UF'] = 'PA'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #LIGHT
    if df_indices.loc[index,'ID'] == 'D60':
        df_indices.at[index,'UF'] = 'RJ'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #MUX-Energia
    if df_indices.loc[index,'ID'] == 'D61':
        df_indices.at[index,'UF'] = 'RS'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
    
    #CPFL RGE
    if df_indices.loc[index,'ID'] == 'D62':
        df_indices.at[index,'UF'] = 'RS'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #SULGIPE
    if df_indices.loc[index,'ID'] == 'D63':
        df_indices.at[index,'UF'] = 'SE'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #UHENPAL
    if df_indices.loc[index,'ID'] == 'D64':
        df_indices.at[index,'UF'] = 'TO'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ESS
    if df_indices.loc[index,'ID'] == 'D65':
        df_indices.at[index,'UF'] = 'SP'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Santa Cruz
    if df_indices.loc[index,'ID'] == 'D66':
        df_indices.at[index,'UF'] = 'SP'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #RGE
    if df_indices.loc[index,'ID'] == 'D67':
        df_indices.at[index,'UF'] = 'RS'
        df_indices.at[index,'PERIODO_TARIFARIO'] = '5'
    

    return distribuidora

 

#%%Inserção dos dados
#Abre a SPARTA de cada arquivo
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
        
        df_sparta_indices = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Resultado'
                                      ,header=4
                                      ,nrows=6
                                      ,usecols=[1,2,3,10,11,12])
    
        #Tentamos importar a aba 'BD' porque nem todas SPARTA possuem essa aba
        df_sparta_bd = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'BD'
                                      ,header=55
                                      ,nrows=3
                                      ,usecols=[11,12])
        
        print('Leu o arquivo: ',arquivo)
        
        #Função para extração dos dados de cada SPARTA
        extrai_indices(df_sparta_indices,df_indices,df_sparta_mercado,df_sparta_bd,index,df_sparta_capa)
        distribuidora(df_sparta_capa,df_indices,index)
        print('Extraiu o dado do arquivo: ',arquivo)
        print('Arquivos extraídos: ',round((index/len(arquivos)),2)*100,'%')
        index = index + 1 #Passa para o próximo indice para conseguir dados da proxima SPARTA
     
        
    except:
        print('Aba não disponível na SPARTA', arquivo)
          
    
    
#%%Tratamento de dados 
#Remover dados duplicados e linhas nulas
df_indices = df_indices.drop_duplicates(subset = 'CHAVE',ignore_index = True)    
df_indices = df_indices.dropna(axis=0,how='all')    

#Limpeza e Tratamento dos dados
df_indices = df_indices.astype(str)
df_indices['PERIODO_TARIFARIO'] = df_indices['PERIODO_TARIFARIO'].astype(int)
df_indices['IRT_ECONOMICO_PERCENT'] = df_indices['IRT_ECONOMICO_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_indices['IRT_FINANCEIRO_PERCENT'] = df_indices['IRT_FINANCEIRO_PERCENT'].replace('nan','0').astype(float).replace('.',',')   
df_indices['IRT_FINAN_ECON_PERCENT'] = df_indices['IRT_FINAN_ECON_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_indices['EFEITO_AT_PERCENT'] = df_indices['EFEITO_AT_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_indices['EFEITO_BT_PERCENT'] = df_indices['EFEITO_BT_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_indices['EFEITO_TARIFA_AT_BT_PERCENT'] = df_indices['EFEITO_TARIFA_AT_BT_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_indices['TARIFA_RESIDEN_B1_RS'] = df_indices['TARIFA_RESIDEN_B1_RS'].replace('nan','0').astype(float).replace('.',',')
df_indices['ICMS_PERCENT'] = df_indices['ICMS_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_indices['PIS_PERCENT'] = df_indices['PIS_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_indices['PERDAS_RB_MWH'] = df_indices['PERDAS_RB_MWH'].replace('nan','0').astype(float).replace('.',',')
df_indices['PERDAS_DITS_MWH'] = df_indices['PERDAS_DITS_MWH'].replace('2051,35','2051.35').replace('nan','0').astype(float).replace('.',',')

# Define a data de atualização dos dados
data = datetime.today().strftime('%d/%m/%Y')
df_indices['DATA_ATUALIZA'] = data

#%% Inserir dados no banco de dados

#Criar a lista para inserção no banco SQL com os dados da base editada
dados_list = df_indices.values.tolist()


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
        sql = '''INSERT INTO ''' + tabela_oracle +''' VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        cursor.executemany(sql, dados_list)
    except Exception as err:
        print('Erro no Insert:', err)
    else:
        print('Carga executada com sucesso!')
        connection.commit() #Caso seja executado com sucesso, esse comando salva a base de dados
    finally:
        cursor.close()
        connection.close()



