# -*- coding: utf-8 -*-
"""
Created on Wed Mar  1 08:00:47 2023

Os dados da SPARTA da CELESC e EFLJC foram inseridos manualmente e estão estáticos

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
# pasta = r"C:\Users\2018459\OneDrive - CPFL Energia S A\Documentos\Projetos\2023\BD RTP e RTA\RTP e RTA (concessionaria)\SPARTA\SPARTA 2023"

#Arquivos
#Como os dados das empresas estão em arquivos separados, listar todos que serão carregados conforme layout abaixo.
# arquivo ='Cópia de Cotas CCGF Simulação Sparta Montante junho 2023 V2.xlsx'
arquivos = glob.glob(pasta)


index = 0 #Flag para inserção dos dados
abas_inversao = ['Inversões','Inversão','Energia de Inversões'] #Nome possíveis para a aba 'Inversão'

#Nome da tabela Oracle onde será dada a carga
tabela_oracle = 'SPARTA_ENERGIA'
ano_oracle = "'2023'"



#%% Montar o dataframe da estrutura da aba 'SPARTA_NUC'
#Defino o indice máximo que é igual o número de SPARTA
index_maximo = list(range(0,len(arquivos)))

#Criação da estrutura do tabela NUC
df_energia = pd.DataFrame(data = [], columns=['CHAVE','EVENTO_TARIFARIO','ANO','ID','DISTRIBUIDORA','UF','DATA','PERIODO_TARIFARIO','CONTRATO','ENERGIA_BASE_MWH','GERACAO_PROPRIA_MWH','COTA_ANGRA_MWH','COTAS_LEI_12783_MWH','ITAIPU_MWH','PROINFA_MWH','BILATERAL_MWH','CCEAR_MWH','ENERGIA_BASE_RS_MWH','GERACAO_PROPRIA_RS_MWH','COTA_ANGRA_RS_MWH','COTAS_LEI_12783_RS_MWH','ITAIPU_RS_MWH','PROINFA_RS_MWH','BILATERAL_RS_MWH','CCEAR_RS_MWH','ENERGIA_BASE_RS','GERACAO_PROPRIA_RS','COTA_ANGRA_RS','COTAS_LEI_12783_RS','ITAIPU_RS','PROINFA_RS','BILATERAL_RS','CCEAR_RS','ENERGIA_INVERSAO_MWH'],index=index_maximo) 

#Criação dataframe vazio
df_sparta_capa = pd.DataFrame(data=[])
df_sparta_mercado = pd.DataFrame(data=[])
df_sparta_inversao = pd.DataFrame(data=[])



# df_sparta_capa = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA'
#                                 ,header = 5
#                                 ,nrows = 14
#                                 ,usecols = [1,2])   

# df_sparta_mercado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Mercado'
#                           ,header=7
#                           ,nrows=49
#                           ,usecols=[1,2,3,4,5,6,7])
                      
# df_sparta_energia = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Energia'
#                       ,header=18
#                       ,nrows=9
#                       ,usecols=[1,2,3,4])

# for item in abas_inversao:
#     try:
#         df_sparta_inversao = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = item)

#     except Exception as err:
#         print('Aba não existente:',err) 
        
# try:
#     df_sparta_energia = df_sparta_energia.astype('str') 
#     df_sparta_inversao = df_sparta_inversao.astype('str') 
    
# except Exception as err:
#     print('Não foi possível converter a tabela:',err)


# #Define o intervalo máximo de linhas e colunas do dataframe 
# try:
#     linhas = range(len(df_sparta_energia.index)) #Define o intervalo de linhas para a tabela 'Energia'
#     colunas = range(len(df_sparta_inversao.columns))  #Define o intervalo de colunas para a tabela 'Inversão'
#     linhas_inversao = range(len(df_sparta_inversao.index))  #Define o intervalo de linhas para a tabela 'Inversão'
    
# except Exception as err:
#     print('Não foi possível definir o intervalo de linhas ou colunas:',err)


#%%Extração dos resultados
#Funções para extrair dados das SPARTA recentes
def determina_contrato(df_energia,df_sparta_mercado,index):
    #Se essa 'string' estiver presente nessa aba o contrato é NOVO
    if 'Percentual RI' in df_sparta_mercado.iloc[:,:].values:
          df_energia.at[index,'CONTRATO'] = 'NOVO' 
    #Caso contrário o contrato é ANTIGO      
    else:
          df_energia.at[index,'CONTRATO'] = 'ANTIGO'
 
    

    
#Função para extrair os dados de energia da SPARTA
def extrai_energia(df_energia,df_sparta_energia,index):
    for linha in linhas:
        if 'ENERGIA' in df_sparta_energia.iloc[linha,0].upper():
            df_energia.at[index,'ENERGIA_BASE_MWH'] = df_sparta_energia.iloc[linha,1]
            df_energia.at[index,'ENERGIA_BASE_RS_MWH'] = df_sparta_energia.iloc[linha,2]
            df_energia.at[index,'ENERGIA_BASE_RS'] = df_sparta_energia.iloc[linha,3]
        elif 'GERAÇÃO' in df_sparta_energia.iloc[linha,0].upper():
            df_energia.at[index,'GERACAO_PROPRIA_MWH'] = df_sparta_energia.iloc[linha,1]
            df_energia.at[index,'GERACAO_PROPRIA_RS_MWH'] = df_sparta_energia.iloc[linha,2]
            df_energia.at[index,'GERACAO_PROPRIA_RS'] = df_sparta_energia.iloc[linha,3]
        elif 'ANGRA' in df_sparta_energia.iloc[linha,0].upper():
            df_energia.at[index,'COTA_ANGRA_MWH'] = df_sparta_energia.iloc[linha,1]
            df_energia.at[index,'COTA_ANGRA_RS_MWH'] = df_sparta_energia.iloc[linha,2]
            df_energia.at[index,'COTA_ANGRA_RS'] = df_sparta_energia.iloc[linha,3]
        elif 'LEI' in df_sparta_energia.iloc[linha,0].upper():
            df_energia.at[index,'COTAS_LEI_12783_MWH'] = df_sparta_energia.iloc[linha,1]
            df_energia.at[index,'COTAS_LEI_12783_RS_MWH'] = df_sparta_energia.iloc[linha,2]
            df_energia.at[index,'COTAS_LEI_12783_RS'] = df_sparta_energia.iloc[linha,3]
        elif 'ITAIPU' in df_sparta_energia.iloc[linha,0].upper():
            df_energia.at[index,'ITAIPU_MWH'] = df_sparta_energia.iloc[linha,1]
            df_energia.at[index,'ITAIPU_RS_MWH'] = df_sparta_energia.iloc[linha,2]
            df_energia.at[index,'ITAIPU_RS'] = df_sparta_energia.iloc[linha,3]
        elif 'PROINFA' in df_sparta_energia.iloc[linha,0].upper():
            df_energia.at[index,'PROINFA_MWH'] = df_sparta_energia.iloc[linha,1]
            df_energia.at[index,'PROINFA_RS_MWH'] = df_sparta_energia.iloc[linha,2]
            df_energia.at[index,'PROINFA_RS'] = df_sparta_energia.iloc[linha,3]
        elif 'BILATERAL' in df_sparta_energia.iloc[linha,0].upper():
            df_energia.at[index,'BILATERAL_MWH'] = df_sparta_energia.iloc[linha,1]
            df_energia.at[index,'BILATERAL_RS_MWH'] = df_sparta_energia.iloc[linha,2]
            df_energia.at[index,'BILATERAL_RS'] = df_sparta_energia.iloc[linha,3]
        elif 'CCEAR' in df_sparta_energia.iloc[linha,0].upper():
            df_energia.at[index,'CCEAR_MWH'] = df_sparta_energia.iloc[linha,1]
            df_energia.at[index,'CCEAR_RS_MWH'] = df_sparta_energia.iloc[linha,2]
            df_energia.at[index,'CCEAR_RS'] = df_sparta_energia.iloc[linha,3]
        
    #Tratamento especifico para as SPARTA CELESC D11 e EFLJC D43 de 2013
    #Filtra as distribuidoras e o ano
    if ((df_energia.at[index,'ID'] == 'D11') and (df_energia.at[index,'ANO'] == '2013')) or ((df_energia.at[index,'ID'] == 'D43') and (df_energia.at[index,'ANO'] == '2013')):
        index_filtro_celesc = int(df_energia[(df_energia['ID'] == 'D11') & (df_energia['ANO'] == '2013')].index.values)
        index_filtro_efljc = int(df_energia[(df_energia['ID'] == 'D43') & (df_energia['ANO'] == '2013')].index.values)
        
        #Inserimos somente os dados das SPARTAs que tem valores não nulos
        #CELESC
        df_energia.at[index_filtro_celesc,'GERACAO_PROPRIA_MWH'] = 9206
        df_energia.at[index_filtro_celesc,'PROINFA_MWH'] = 393206.27
        df_energia.at[index_filtro_celesc,'ENERGIA_BASE_MWH'] = df_energia.at[index_filtro_celesc,'GERACAO_PROPRIA_MWH'] + df_energia.at[index_filtro_celesc,'PROINFA_MWH']
        df_energia.at[index_filtro_celesc,'GERACAO_PROPRIA_RS_MWH'] = 121.28
        df_energia.at[index_filtro_celesc,'COTA_ANGRA_RS_MWH'] = 135.67
        df_energia.at[index_filtro_celesc,'COTAS_LEI_12783_RS_MWH'] = 32.89
        df_energia.at[index_filtro_celesc,'ENERGIA_BASE_RS_MWH'] = df_energia.at[index_filtro_celesc,'GERACAO_PROPRIA_RS_MWH'] + df_energia.at[index_filtro_celesc,'COTA_ANGRA_RS_MWH'] + df_energia.at[index_filtro_celesc,'COTAS_LEI_12783_RS_MWH']
        df_energia.at[index_filtro_celesc,'GERACAO_PROPRIA_RS'] = 1116497
        df_energia.at[index_filtro_celesc,'ENERGIA_BASE_RS'] = df_energia.at[index_filtro_celesc,'GERACAO_PROPRIA_RS']
        
        #EFLJC
        df_energia.at[index_filtro_efljc,'PROINFA_MWH'] = 346.54
        df_energia.at[index_filtro_efljc,'ENERGIA_BASE_MWH'] = df_energia.at[index_filtro_efljc,'PROINFA_MWH']
        df_energia.at[index_filtro_efljc,'COTA_ANGRA_RS_MWH'] = 135.67
        df_energia.at[index_filtro_efljc,'COTAS_LEI_12783_RS_MWH'] = 32.89
        df_energia.at[index_filtro_efljc,'ENERGIA_BASE_RS_MWH'] = df_energia.at[index_filtro_efljc,'COTA_ANGRA_RS_MWH'] + df_energia.at[index_filtro_efljc,'COTAS_LEI_12783_RS_MWH']


def extrai_inversao(df_energia,df_sparta_inversao,index):
    for linha_inversao in linhas_inversao:
        for coluna in colunas:
            if 'INVERS' in df_sparta_inversao.iloc[linha_inversao,coluna].upper():
                global coluna_inversao
                coluna_inversao = coluna
 
    valor_inversao = pd.to_numeric(df_sparta_inversao.iloc[:,coluna_inversao],errors='coerce')
    valor_inversao_soma = valor_inversao.sum()
    df_energia.at[index,'ENERGIA_INVERSAO_MWH'] = valor_inversao_soma


def distribuidora(df_energia,df_sparta_capa,index):
    #Determina o ANO da SPARTA
    df_energia.at[index,'ANO'] = df_sparta_capa.iloc[8,1].strftime('%Y')
    
    #Determina o ID da Distribuidora
    df_energia.at[index,'ID'] = df_sparta_capa.iloc[1,1]
    
    #Determina o NOME da Distribuidora
    df_energia.at[index,'DISTRIBUIDORA'] = df_sparta_capa.iloc[0,0].upper()
    
    #Determina a DATA da SPARTA
    df_energia.at[index,'DATA'] = df_sparta_capa.iloc[8,1].strftime('%Y-%m-%d')
    
    #Determina o EVENTO TARIFARIO
    if 'Reajuste' in df_sparta_capa.iloc[0,1]:
        df_energia.at[index,'EVENTO_TARIFARIO'] = 'RTA'
    elif 'Revisão' in df_sparta_capa.iloc[0,1]:
        df_energia.at[index,'EVENTO_TARIFARIO'] = 'RTP'
    else:
        df_energia.at[index,'EVENTO_TARIFARIO'] = 'RTE'
        
    #Determina a CHAVE
    df_energia.at[index,'CHAVE'] = df_energia.loc[index,'EVENTO_TARIFARIO']+df_energia.loc[index,'ANO']+df_energia.loc[index,'ID']
    
    #Determina UF e PERIODO TARIFARIO
    #AES SUL
    if df_energia.loc[index,'ID'] == 'D01':
        df_energia.at[index,'UF'] = 'RS'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #AME
    if df_energia.loc[index,'ID'] == 'D02':
        df_energia.at[index,'UF'] = 'AM'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #AMPLA
    if df_energia.loc[index,'ID'] == 'D03':
        df_energia.at[index,'UF'] = 'RJ'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #BANDEIRANTE
    if df_energia.loc[index,'ID'] == 'D04':
        df_energia.at[index,'UF'] = 'SP'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #BOA VISTA
    if df_energia.loc[index,'ID'] == 'D05':
        df_energia.at[index,'UF'] = 'RR'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CAIUA
    if df_energia.loc[index,'ID'] == 'D06':
        df_energia.at[index,'UF'] = 'SP'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEA
    if df_energia.loc[index,'ID'] == 'D07':
        df_energia.at[index,'UF'] = 'AP'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CEAL
    if df_energia.loc[index,'ID'] == 'D08':
        df_energia.at[index,'UF'] = 'AL'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CEB
    if df_energia.loc[index,'ID'] == 'D09':
        df_energia.at[index,'UF'] = 'DF'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '4'
    
    #CEEE
    if df_energia.loc[index,'ID'] == 'D10':
        df_energia.at[index,'UF'] = 'RS'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
      
    #CELESC
    if df_energia.loc[index,'ID'] == 'D11':
        df_energia.at[index,'UF'] = 'SC'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CELG
    if df_energia.loc[index,'ID'] == 'D12':
        df_energia.at[index,'UF'] = 'GO'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CELPA
    if df_energia.loc[index,'ID'] == 'D13':
        df_energia.at[index,'UF'] = 'PA'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CELPE
    if df_energia.loc[index,'ID'] == 'D14':
        df_energia.at[index,'UF'] = 'PE'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CELTINS
    if df_energia.loc[index,'ID'] == 'D15':
        df_energia.at[index,'UF'] = 'TO'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEMAR
    if df_energia.loc[index,'ID'] == 'D16':
        df_energia.at[index,'UF'] = 'MA'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #EMT
    if df_energia.loc[index,'ID'] == 'D17':
        df_energia.at[index,'UF'] = 'MT'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEMIG-D
    if df_energia.loc[index,'ID'] == 'D18':
        df_energia.at[index,'UF'] = 'MG'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEPISA
    if df_energia.loc[index,'ID'] == 'D19':
        df_energia.at[index,'UF'] = 'PI'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CERON
    if df_energia.loc[index,'ID'] == 'D20':
        df_energia.at[index,'UF'] = 'RO'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CERR (CONFIMAR O PERIODO)
    if df_energia.loc[index,'ID'] == 'D21':
        df_energia.at[index,'UF'] = 'RR'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CFLO
    if df_energia.loc[index,'ID'] == 'D22':
        df_energia.at[index,'UF'] = 'PR'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CHESP
    if df_energia.loc[index,'ID'] == 'D23':
        df_energia.at[index,'UF'] = 'GO'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL JAGUARI
    if df_energia.loc[index,'ID'] == 'D24':
        df_energia.at[index,'UF'] = 'SP'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL MOCOCA
    if df_energia.loc[index,'ID'] == 'D25':
        df_energia.at[index,'UF'] = 'SP'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'        
    
    #CPFL Santa Cruz
    if df_energia.loc[index,'ID'] == 'D26':
        df_energia.at[index,'UF'] = 'SP'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CNEE
    if df_energia.loc[index,'ID'] == 'D27':
        df_energia.at[index,'UF'] = 'SP'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COCEL
    if df_energia.loc[index,'ID'] == 'D28':
        df_energia.at[index,'UF'] = 'PR'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COELBA
    if df_energia.loc[index,'ID'] == 'D29':
        df_energia.at[index,'UF'] = 'BA'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COELCE
    if df_energia.loc[index,'ID'] == 'D30':
        df_energia.at[index,'UF'] = 'CE'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #COOPERALIANÇA
    if df_energia.loc[index,'ID'] == 'D31':
        df_energia.at[index,'UF'] = 'SC'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COPEL
    if df_energia.loc[index,'ID'] == 'D32':
        df_energia.at[index,'UF'] = 'PR'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COSERN
    if df_energia.loc[index,'ID'] == 'D33':
        df_energia.at[index,'UF'] = 'RN'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Leste Paulista
    if df_energia.loc[index,'ID'] == 'D34':
        df_energia.at[index,'UF'] = 'SP'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Piratininga
    if df_energia.loc[index,'ID'] == 'D35':
        df_energia.at[index,'UF'] = 'SP'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CPFL Paulista
    if df_energia.loc[index,'ID'] == 'D36':
        df_energia.at[index,'UF'] = 'SP'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Sul Paulista
    if df_energia.loc[index,'ID'] == 'D37':
        df_energia.at[index,'UF'] = 'SP'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #DEMEI
    if df_energia.loc[index,'ID'] == 'D38':
        df_energia.at[index,'UF'] = 'RS'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #DME-PC
    if df_energia.loc[index,'ID'] == 'D39':
        df_energia.at[index,'UF'] = 'MG'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EBO
    if df_energia.loc[index,'ID'] == 'D40':
        df_energia.at[index,'UF'] = 'PB'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #EDEVP
    if df_energia.loc[index,'ID'] == 'D41':
        df_energia.at[index,'UF'] = 'SP'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EEB
    if df_energia.loc[index,'ID'] == 'D42':
        df_energia.at[index,'UF'] = 'SP'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EFLJC
    if df_energia.loc[index,'ID'] == 'D43':
        df_energia.at[index,'UF'] = 'SC'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EFLUL
    if df_energia.loc[index,'ID'] == 'D44':
        df_energia.at[index,'UF'] = 'SC'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ELEKTRO
    if df_energia.loc[index,'ID'] == 'D45':
        df_energia.at[index,'UF'] = 'SP'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELETROACRE
    if df_energia.loc[index,'ID'] == 'D46':
        df_energia.at[index,'UF'] = 'AC'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELETROCAR
    if df_energia.loc[index,'ID'] == 'D47':
        df_energia.at[index,'UF'] = 'RS'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ELETROPAULO
    if df_energia.loc[index,'ID'] == 'D48':
        df_energia.at[index,'UF'] = 'SP'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELFSM
    if df_energia.loc[index,'ID'] == 'D49':
        df_energia.at[index,'UF'] = 'ES'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EMG
    if df_energia.loc[index,'ID'] == 'D50':
        df_energia.at[index,'UF'] = 'MG'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EMS
    if df_energia.loc[index,'ID'] == 'D51':
        df_energia.at[index,'UF'] = 'MS'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ENF
    if df_energia.loc[index,'ID'] == 'D52':
        df_energia.at[index,'UF'] = 'RJ'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EPB
    if df_energia.loc[index,'ID'] == 'D53':
        df_energia.at[index,'UF'] = 'PB'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ESCELSA (CONFIRMAR PERIODO)
    if df_energia.loc[index,'ID'] == 'D54':
        df_energia.at[index,'UF'] = 'ES'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '3'
        
    #ESE
    if df_energia.loc[index,'ID'] == 'D55':
        df_energia.at[index,'UF'] = 'SE'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #FORCEL
    if df_energia.loc[index,'ID'] == 'D56':
        df_energia.at[index,'UF'] = 'PR'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #HIDROPAN
    if df_energia.loc[index,'ID'] == 'D57':
        df_energia.at[index,'UF'] = 'RS'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #IENERGIA
    if df_energia.loc[index,'ID'] == 'D58':
        df_energia.at[index,'UF'] = 'SC'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #JARI (CONFIRMAR PERIODO)
    if df_energia.loc[index,'ID'] == 'D59':
        df_energia.at[index,'UF'] = 'PA'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #LIGHT
    if df_energia.loc[index,'ID'] == 'D60':
        df_energia.at[index,'UF'] = 'RJ'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #MUX-Energia
    if df_energia.loc[index,'ID'] == 'D61':
        df_energia.at[index,'UF'] = 'RS'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
    
    #CPFL RGE
    if df_energia.loc[index,'ID'] == 'D62':
        df_energia.at[index,'UF'] = 'RS'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #SULGIPE
    if df_energia.loc[index,'ID'] == 'D63':
        df_energia.at[index,'UF'] = 'SE'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #UHENPAL
    if df_energia.loc[index,'ID'] == 'D64':
        df_energia.at[index,'UF'] = 'TO'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ESS
    if df_energia.loc[index,'ID'] == 'D65':
        df_energia.at[index,'UF'] = 'SP'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Santa Cruz
    if df_energia.loc[index,'ID'] == 'D66':
        df_energia.at[index,'UF'] = 'SP'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #RGE
    if df_energia.loc[index,'ID'] == 'D67':
        df_energia.at[index,'UF'] = 'RS'
        df_energia.at[index,'PERIODO_TARIFARIO'] = '5'

   

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
                              
        df_sparta_energia = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Energia'
                              ,header=18
                              ,nrows=9
                              ,usecols=[1,2,3,4])
        
        determina_contrato(df_energia,df_sparta_mercado,index)
        
        print('Leu o arquivo: ',arquivo)
        
    except:
        print('Aba não disponível na SPARTA', arquivo)

    for item in abas_inversao:
        try:
            df_sparta_inversao = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = item)
            linha_vazia = pd.DataFrame([[np.nan]*len(df_sparta_inversao.columns)],columns=df_sparta_inversao.columns)   #Cria uma linha vazia
            df_sparta_inversao = pd.concat([linha_vazia,df_sparta_inversao]).reset_index(drop=True) #Insere a linha vazia no dataframe 'Inversão'
            df_sparta_inversao.iloc[0] = df_sparta_inversao.columns #Substitui a linha 0 pelo header
                   
        except Exception as err:
            print('Aba não existente:',err) 

    
    #Converte as tabelas para string, pois não é possível comparar string com valor NaN
    try:
        df_sparta_energia = df_sparta_energia.astype('str') 
        df_sparta_inversao = df_sparta_inversao.astype('str') 
        
    except Exception as err:
        print('Não foi possível converter a tabela:',err)

    
    #Define o intervalo máximo de linhas e colunas do dataframe 
    try:
        linhas = range(len(df_sparta_energia.index)) #Define o intervalo de linhas para a tabela 'Energia'
        colunas = range(len(df_sparta_inversao.columns))  #Define o intervalo de colunas para a tabela 'Inversão'
        linhas_inversao = range(len(df_sparta_inversao.index))  #Define o intervalo de linhas para a tabela 'Inversão'
        
    except Exception as err:
        print('Não foi possível definir o intervalo de linhas ou colunas:',err)
        
    #Função para extrair os dados de 'Energia'
    try:
        #Função para extração dos dados da distribuidora e tipo de contrato
        distribuidora(df_energia,df_sparta_capa,index)
        extrai_energia(df_energia,df_sparta_energia,index)
        extrai_inversao(df_energia,df_sparta_inversao,index)      
        
    except Exception as err:
        print('Não foi possível extrair os dados:',err)
        
    print('Extraiu o dado do arquivo: ',arquivo)
    print('Arquivos extraídos: ',round((index/len(arquivos)),2)*100,'%')
    df_sparta_inversao = pd.DataFrame(data=[])  #Zera o dataframe para que não seja levado para o proximo loop
    index = index + 1 #Passa para o próximo indice para conseguir dados da proxima SPARTA
         
    

#%%Tratamento de dados 
#Remover dados duplicados e linhas nulas
df_energia = df_energia.drop_duplicates(subset = 'CHAVE',ignore_index = True) 
df_energia = df_energia.dropna(axis=0,how='all')   


#Limpeza e Tratamento dos dados
df_energia = df_energia.astype(str)
df_energia['PERIODO_TARIFARIO'] = df_energia['PERIODO_TARIFARIO'].astype(int)
df_energia['ENERGIA_BASE_MWH'] = df_energia['ENERGIA_BASE_MWH'].replace('nan','0').astype(float).replace('.',',')
df_energia['GERACAO_PROPRIA_MWH'] = df_energia['GERACAO_PROPRIA_MWH'].replace('nan','0').astype(float).replace('.',',')
df_energia['COTA_ANGRA_MWH'] = df_energia['COTA_ANGRA_MWH'].replace('nan','0').astype(float).replace('.',',')
df_energia['COTAS_LEI_12783_MWH'] = df_energia['COTAS_LEI_12783_MWH'].replace('nan','0').astype(float).replace('.',',')
df_energia['ITAIPU_MWH'] = df_energia['ITAIPU_MWH'].replace('nan','0').astype(float).replace('.',',')
df_energia['PROINFA_MWH'] = df_energia['PROINFA_MWH'].replace('nan','0').astype(float).replace('.',',')
df_energia['BILATERAL_MWH'] = df_energia['BILATERAL_MWH'].replace('nan','0').astype(float).replace('.',',')
df_energia['CCEAR_MWH'] = df_energia['CCEAR_MWH'].replace('nan','0').astype(float).replace('.',',')
df_energia['ENERGIA_BASE_RS_MWH'] = df_energia['ENERGIA_BASE_RS_MWH'].replace('nan','0').astype(float).replace('.',',')
df_energia['GERACAO_PROPRIA_RS_MWH'] = df_energia['GERACAO_PROPRIA_RS_MWH'].replace('nan','0').astype(float).replace('.',',')
df_energia['COTA_ANGRA_RS_MWH'] = df_energia['COTA_ANGRA_RS_MWH'].replace('nan','0').astype(float).replace('.',',')
df_energia['COTAS_LEI_12783_RS_MWH'] = df_energia['COTAS_LEI_12783_RS_MWH'].replace('nan','0').astype(float).replace('.',',')
df_energia['ITAIPU_RS_MWH'] = df_energia['ITAIPU_RS_MWH'].replace('nan','0').astype(float).replace('.',',')
df_energia['PROINFA_RS_MWH'] = df_energia['PROINFA_RS_MWH'].replace('nan','0').astype(float).replace('.',',')
df_energia['BILATERAL_RS_MWH'] = df_energia['BILATERAL_RS_MWH'].replace('nan','0').astype(float).replace('.',',')
df_energia['CCEAR_RS_MWH'] = df_energia['CCEAR_RS_MWH'].replace('nan','0').astype(float).replace('.',',')
df_energia['ENERGIA_BASE_RS'] = df_energia['ENERGIA_BASE_RS'].replace('nan','0').astype(float).replace('.',',')
df_energia['GERACAO_PROPRIA_RS'] = df_energia['GERACAO_PROPRIA_RS'].replace('nan','0').astype(float).replace('.',',')
df_energia['COTA_ANGRA_RS'] = df_energia['COTA_ANGRA_RS'].replace('nan','0').astype(float).replace('.',',')
df_energia['COTAS_LEI_12783_RS'] = df_energia['COTAS_LEI_12783_RS'].replace('nan','0').astype(float).replace('.',',')
df_energia['ITAIPU_RS'] = df_energia['ITAIPU_RS'].replace('nan','0').astype(float).replace('.',',')
df_energia['PROINFA_RS'] = df_energia['PROINFA_RS'].replace('nan','0').astype(float).replace('.',',')
df_energia['BILATERAL_RS'] = df_energia['BILATERAL_RS'].replace('nan','0').astype(float).replace('.',',')
df_energia['CCEAR_RS'] = df_energia['CCEAR_RS'].replace('nan','0').astype(float).replace('.',',')
df_energia['ENERGIA_INVERSAO_MWH'] = df_energia['ENERGIA_INVERSAO_MWH'].replace('nan','0').astype(float).replace('.',',')

# Define a data de atualização dos dados
data = datetime.today().strftime('%d/%m/%Y')
df_energia['DATA_ATUALIZA'] = data

#%% Inserir dados no banco de dados

#Criar a lista para inserção no banco SQL com os dados da base editada
dados_list = df_energia.values.tolist()


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
        sql = '''INSERT INTO ''' + tabela_oracle +''' VALUES (:1	,:2	,:3	,:4	,:5	,:6	,:7	,:8	,:9	,:10	,:11	,:12	,:13	,:14	,:15	,:16	,:17	,:18	,:19	,:20	,:21	,:22	,:23	,:24	,:25	,:26	,:27	,:28	,:29	,:30	,:31	,:32	,:33	,:34,:35)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        cursor.executemany(sql, dados_list)
    except Exception as err:
        print('Erro no Insert:', err)
    else:
        print('Carga executada com sucesso!')
        connection.commit() #Caso seja executado com sucesso, esse comando salva a base de dados
    finally:
        cursor.close()
        connection.close()


