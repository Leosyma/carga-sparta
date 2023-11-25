# -*- coding: utf-8 -*-
"""
Created on Thu Nov 10 16:03:15 2022

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
# arquivo ='SPARTA_CPFL_Santa_Cruz_2022.xlsx'
#aba_nuc = 'Mercado'
arquivos = glob.glob(pasta)


index = 0 #Flag para inserção dos dados

#Nome da tabela Oracle onde será dada a carga
tabela_oracle = 'SPARTA_NUC'
ano_oracle = "'2023'"


#%% Montar o dataframe da estrutura da aba 'SPARTA_NUC'
#Defino o indice máximo que é igual o número de SPARTA
index_maximo = list(range(0,len(arquivos)))

#Criação da estrutura do tabela NUC
df_nuc = pd.DataFrame(data = [], columns=['CHAVE','EVENTO_TARIFARIO','ANO','ID','DISTRIBUIDORA','UF','DATA','PERIODO_TARIFARIO','CONTRATO','RESIDENCIAL','INDUSTRIAL','COMERCIAL','RURAL','ILUMINACAO','PODER_PUBLICO','SERVICO_PUBLICO','DEMAIS','A1','A2','A3','A3A','A4','AS','BT'],index=index_maximo) 

#Criação dataframe vazio
df_sparta_nuc = pd.DataFrame(data = [])
df_sparta_capa = pd.DataFrame(data=[])


# df_sparta_nuc = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Mercado'
#                               ,header=27
#                               ,nrows=20
#                               ,usecols='B:H')

# df_sparta_capa = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA'
#                                ,header = 5
#                                ,nrows = 14
#                                ,usecols = [1,2])


# df_sparta_nuc = df_sparta_nuc.astype('str')

# linhas_nuc = range(len(df_sparta_nuc.index))
# colunas_nuc = range(len(df_sparta_nuc.columns))



#%%Extração dos resultados
def extrai_nuc(df_nuc,df_sparta_nuc,index):
    #Se o contrato for novo usa essa posição para inserir os dados
    if 'Percentual RI' in df_sparta_nuc.iloc[:,:].values: 
        for linha in linhas_nuc:
            for coluna in colunas_nuc:
                if 'RESIDENCIAL' in df_sparta_nuc.iloc[linha,coluna].upper():
                    df_nuc.at[index,'RESIDENCIAL'] = df_sparta_nuc.iloc[linha,(coluna+3)]
                elif 'INDUSTRIAL' in df_sparta_nuc.iloc[linha,coluna].upper():
                    df_nuc.at[index,'INDUSTRIAL'] = df_sparta_nuc.iloc[linha,(coluna+3)]
                elif 'COMERCIAL' in df_sparta_nuc.iloc[linha,coluna].upper():
                    df_nuc.at[index,'COMERCIAL'] = df_sparta_nuc.iloc[linha,(coluna+3)]
                elif 'RURAL' in df_sparta_nuc.iloc[linha,coluna].upper():
                    df_nuc.at[index,'RURAL'] = df_sparta_nuc.iloc[linha,(coluna+3)]
                elif 'ILUMINAÇÃO' in df_sparta_nuc.iloc[linha,coluna].upper():
                    df_nuc.at[index,'ILUMINACAO'] = df_sparta_nuc.iloc[linha,(coluna+3)]
                elif 'PODER PÚBLICO' in df_sparta_nuc.iloc[linha,coluna].upper():
                    df_nuc.at[index,'PODER_PUBLICO'] = df_sparta_nuc.iloc[linha,(coluna+3)]
                elif 'SERVIÇO PÚBLICO' in df_sparta_nuc.iloc[linha,coluna].upper():
                    df_nuc.at[index,'SERVICO_PUBLICO'] = df_sparta_nuc.iloc[linha,(coluna+3)]
                elif 'DEMAIS' in df_sparta_nuc.iloc[linha,coluna].upper():
                    df_nuc.at[index,'DEMAIS'] = df_sparta_nuc.iloc[linha,(coluna+3)]
                elif df_sparta_nuc.iloc[linha,coluna].upper() == 'A1':
                    df_nuc.at[index,'A1'] = df_sparta_nuc.iloc[linha,(coluna+1)]
                elif df_sparta_nuc.iloc[linha,coluna].upper() == 'A2':
                    df_nuc.at[index,'A2'] = df_sparta_nuc.iloc[linha,(coluna+1)]
                elif df_sparta_nuc.iloc[linha,coluna].upper() == 'A3':
                    df_nuc.at[index,'A3'] = df_sparta_nuc.iloc[linha,(coluna+1)]
                elif df_sparta_nuc.iloc[linha,coluna].upper() == 'A3A':
                    df_nuc.at[index,'A3A'] = df_sparta_nuc.iloc[linha,(coluna+1)]
                elif df_sparta_nuc.iloc[linha,coluna].upper() == 'A4':
                    df_nuc.at[index,'A4'] = df_sparta_nuc.iloc[linha,(coluna+1)]
                elif df_sparta_nuc.iloc[linha,coluna].upper() == 'AS':
                    df_nuc.at[index,'AS'] = df_sparta_nuc.iloc[linha,(coluna+1)]
                elif df_sparta_nuc.iloc[linha,coluna].upper() == 'BT':
                    df_nuc.at[index,'BT'] = df_sparta_nuc.iloc[linha,(coluna+1)]
        df_nuc.at[index,'CONTRATO'] = 'NOVO'
 
    # Se o contrato for antigo usa essa posição para inserir os dados   
    else: 
        for linha in linhas_nuc:
            for coluna in colunas_nuc:
                if 'RESIDENCIAL' in df_sparta_nuc.iloc[linha,coluna].upper():
                    df_nuc.at[index,'RESIDENCIAL'] = df_sparta_nuc.iloc[linha,(coluna+1)]
                elif 'INDUSTRIAL' in df_sparta_nuc.iloc[linha,coluna].upper():
                    df_nuc.at[index,'INDUSTRIAL'] = df_sparta_nuc.iloc[linha,(coluna+1)]
                elif 'COMERCIAL' in df_sparta_nuc.iloc[linha,coluna].upper():
                    df_nuc.at[index,'COMERCIAL'] = df_sparta_nuc.iloc[linha,(coluna+1)]
                elif 'RURAL' in df_sparta_nuc.iloc[linha,coluna].upper():
                    df_nuc.at[index,'RURAL'] = df_sparta_nuc.iloc[linha,(coluna+1)]
                elif 'ILUMINAÇÃO' in df_sparta_nuc.iloc[linha,coluna].upper():
                    df_nuc.at[index,'ILUMINACAO'] = df_sparta_nuc.iloc[linha,(coluna+1)]
                elif 'PODER PÚBLICO' in df_sparta_nuc.iloc[linha,coluna].upper():
                    df_nuc.at[index,'PODER_PUBLICO'] = df_sparta_nuc.iloc[linha,(coluna+1)]
                elif 'SERVIÇO PÚBLICO' in df_sparta_nuc.iloc[linha,coluna].upper():
                    df_nuc.at[index,'SERVICO_PUBLICO'] = df_sparta_nuc.iloc[linha,(coluna+1)]
                elif 'DEMAIS' in df_sparta_nuc.iloc[linha,coluna].upper():
                    df_nuc.at[index,'DEMAIS'] = df_sparta_nuc.iloc[linha,(coluna+1)]
                elif df_sparta_nuc.iloc[linha,coluna].upper() == 'A1':
                    df_nuc.at[index,'A1'] = df_sparta_nuc.iloc[linha,(coluna+1)]
                elif df_sparta_nuc.iloc[linha,coluna].upper() == 'A2':
                    df_nuc.at[index,'A2'] = df_sparta_nuc.iloc[linha,(coluna+1)]
                elif df_sparta_nuc.iloc[linha,coluna].upper() == 'A3':
                    df_nuc.at[index,'A3'] = df_sparta_nuc.iloc[linha,(coluna+1)]
                elif df_sparta_nuc.iloc[linha,coluna].upper() == 'A3A':
                    df_nuc.at[index,'A3A'] = df_sparta_nuc.iloc[linha,(coluna+1)]
                elif df_sparta_nuc.iloc[linha,coluna].upper() == 'A4':
                    df_nuc.at[index,'A4'] = df_sparta_nuc.iloc[linha,(coluna+1)]
                elif df_sparta_nuc.iloc[linha,coluna].upper() == 'AS':
                    df_nuc.at[index,'AS'] = df_sparta_nuc.iloc[linha,(coluna+1)]
                elif df_sparta_nuc.iloc[linha,coluna].upper() == 'BT':
                    df_nuc.at[index,'BT'] = df_sparta_nuc.iloc[linha,(coluna+1)]
        df_nuc.at[index,'CONTRATO'] = 'ANTIGO'



def distribuidora(self,df_nuc,index):
    #Determina o ANO da SPARTA
    df_nuc.at[index,'ANO'] = df_sparta_capa.iloc[8,1].strftime('%Y')
    
    #Determina o ID da Distribuidora
    df_nuc.at[index,'ID'] = df_sparta_capa.iloc[1,1]
    
    #Determina o NOME da Distribuidora
    df_nuc.at[index,'DISTRIBUIDORA'] = df_sparta_capa.iloc[0,0].upper()
    
    #Determina a DATA da SPARTA
    df_nuc.at[index,'DATA'] = df_sparta_capa.iloc[8,1].strftime('%Y-%m-%d')
    
    #Determina o EVENTO TARIFARIO
    if 'Reajuste' in df_sparta_capa.iloc[0,1]:
        df_nuc.at[index,'EVENTO_TARIFARIO'] = 'RTA'
    elif 'Revisão' in df_sparta_capa.iloc[0,1]:
        df_nuc.at[index,'EVENTO_TARIFARIO'] = 'RTP'
    else:
        df_nuc.at[index,'EVENTO_TARIFARIO'] = 'RTE'
        
    #Determina a CHAVE
    df_nuc.at[index,'CHAVE'] = df_nuc.loc[index,'EVENTO_TARIFARIO']+df_nuc.loc[index,'ANO']+df_nuc.loc[index,'ID']
    
    #Determina UF e PERIODO TARIFARIO
    #AES SUL
    if df_nuc.loc[index,'ID'] == 'D01':
        df_nuc.at[index,'UF'] = 'RS'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #AME
    if df_nuc.loc[index,'ID'] == 'D02':
        df_nuc.at[index,'UF'] = 'AM'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #AMPLA
    if df_nuc.loc[index,'ID'] == 'D03':
        df_nuc.at[index,'UF'] = 'RJ'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #BANDEIRANTE
    if df_nuc.loc[index,'ID'] == 'D04':
        df_nuc.at[index,'UF'] = 'SP'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #BOA VISTA
    if df_nuc.loc[index,'ID'] == 'D05':
        df_nuc.at[index,'UF'] = 'RR'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CAIUA
    if df_nuc.loc[index,'ID'] == 'D06':
        df_nuc.at[index,'UF'] = 'SP'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEA
    if df_nuc.loc[index,'ID'] == 'D07':
        df_nuc.at[index,'UF'] = 'AP'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CEAL
    if df_nuc.loc[index,'ID'] == 'D08':
        df_nuc.at[index,'UF'] = 'AL'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CEB
    if df_nuc.loc[index,'ID'] == 'D09':
        df_nuc.at[index,'UF'] = 'DF'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '4'
    
    #CEEE
    if df_nuc.loc[index,'ID'] == 'D10':
        df_nuc.at[index,'UF'] = 'RS'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
      
    #CELESC
    if df_nuc.loc[index,'ID'] == 'D11':
        df_nuc.at[index,'UF'] = 'SC'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CELG
    if df_nuc.loc[index,'ID'] == 'D12':
        df_nuc.at[index,'UF'] = 'GO'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CELPA
    if df_nuc.loc[index,'ID'] == 'D13':
        df_nuc.at[index,'UF'] = 'PA'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CELPE
    if df_nuc.loc[index,'ID'] == 'D14':
        df_nuc.at[index,'UF'] = 'PE'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CELTINS
    if df_nuc.loc[index,'ID'] == 'D15':
        df_nuc.at[index,'UF'] = 'TO'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEMAR
    if df_nuc.loc[index,'ID'] == 'D16':
        df_nuc.at[index,'UF'] = 'MA'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #EMT
    if df_nuc.loc[index,'ID'] == 'D17':
        df_nuc.at[index,'UF'] = 'MT'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEMIG-D
    if df_nuc.loc[index,'ID'] == 'D18':
        df_nuc.at[index,'UF'] = 'MG'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEPISA
    if df_nuc.loc[index,'ID'] == 'D19':
        df_nuc.at[index,'UF'] = 'PI'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CERON
    if df_nuc.loc[index,'ID'] == 'D20':
        df_nuc.at[index,'UF'] = 'RO'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CERR (CONFIMAR O PERIODO)
    if df_nuc.loc[index,'ID'] == 'D21':
        df_nuc.at[index,'UF'] = 'RR'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CFLO
    if df_nuc.loc[index,'ID'] == 'D22':
        df_nuc.at[index,'UF'] = 'PR'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CHESP
    if df_nuc.loc[index,'ID'] == 'D23':
        df_nuc.at[index,'UF'] = 'GO'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL JAGUARI
    if df_nuc.loc[index,'ID'] == 'D24':
        df_nuc.at[index,'UF'] = 'SP'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL MOCOCA
    if df_nuc.loc[index,'ID'] == 'D25':
        df_nuc.at[index,'UF'] = 'SP'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'        
    
    #CPFL Santa Cruz
    if df_nuc.loc[index,'ID'] == 'D26':
        df_nuc.at[index,'UF'] = 'SP'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CNEE
    if df_nuc.loc[index,'ID'] == 'D27':
        df_nuc.at[index,'UF'] = 'SP'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COCEL
    if df_nuc.loc[index,'ID'] == 'D28':
        df_nuc.at[index,'UF'] = 'PR'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COELBA
    if df_nuc.loc[index,'ID'] == 'D29':
        df_nuc.at[index,'UF'] = 'BA'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COELCE
    if df_nuc.loc[index,'ID'] == 'D30':
        df_nuc.at[index,'UF'] = 'CE'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #COOPERALIANÇA
    if df_nuc.loc[index,'ID'] == 'D31':
        df_nuc.at[index,'UF'] = 'SC'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COPEL
    if df_nuc.loc[index,'ID'] == 'D32':
        df_nuc.at[index,'UF'] = 'PR'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COSERN
    if df_nuc.loc[index,'ID'] == 'D33':
        df_nuc.at[index,'UF'] = 'RN'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Leste Paulista
    if df_nuc.loc[index,'ID'] == 'D34':
        df_nuc.at[index,'UF'] = 'SP'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Piratininga
    if df_nuc.loc[index,'ID'] == 'D35':
        df_nuc.at[index,'UF'] = 'SP'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CPFL Paulista
    if df_nuc.loc[index,'ID'] == 'D36':
        df_nuc.at[index,'UF'] = 'SP'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Sul Paulista
    if df_nuc.loc[index,'ID'] == 'D37':
        df_nuc.at[index,'UF'] = 'SP'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #DEMEI
    if df_nuc.loc[index,'ID'] == 'D38':
        df_nuc.at[index,'UF'] = 'RS'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #DME-PC
    if df_nuc.loc[index,'ID'] == 'D39':
        df_nuc.at[index,'UF'] = 'MG'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EBO
    if df_nuc.loc[index,'ID'] == 'D40':
        df_nuc.at[index,'UF'] = 'PB'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #EDEVP
    if df_nuc.loc[index,'ID'] == 'D41':
        df_nuc.at[index,'UF'] = 'SP'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EEB
    if df_nuc.loc[index,'ID'] == 'D42':
        df_nuc.at[index,'UF'] = 'SP'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EFLJC
    if df_nuc.loc[index,'ID'] == 'D43':
        df_nuc.at[index,'UF'] = 'SC'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EFLUL
    if df_nuc.loc[index,'ID'] == 'D44':
        df_nuc.at[index,'UF'] = 'SC'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ELEKTRO
    if df_nuc.loc[index,'ID'] == 'D45':
        df_nuc.at[index,'UF'] = 'SP'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELETROACRE
    if df_nuc.loc[index,'ID'] == 'D46':
        df_nuc.at[index,'UF'] = 'AC'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELETROCAR
    if df_nuc.loc[index,'ID'] == 'D47':
        df_nuc.at[index,'UF'] = 'RS'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ELETROPAULO
    if df_nuc.loc[index,'ID'] == 'D48':
        df_nuc.at[index,'UF'] = 'SP'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELFSM
    if df_nuc.loc[index,'ID'] == 'D49':
        df_nuc.at[index,'UF'] = 'ES'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EMG
    if df_nuc.loc[index,'ID'] == 'D50':
        df_nuc.at[index,'UF'] = 'MG'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EMS
    if df_nuc.loc[index,'ID'] == 'D51':
        df_nuc.at[index,'UF'] = 'MS'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ENF
    if df_nuc.loc[index,'ID'] == 'D52':
        df_nuc.at[index,'UF'] = 'RJ'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EPB
    if df_nuc.loc[index,'ID'] == 'D53':
        df_nuc.at[index,'UF'] = 'PB'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ESCELSA (CONFIRMAR PERIODO)
    if df_nuc.loc[index,'ID'] == 'D54':
        df_nuc.at[index,'UF'] = 'ES'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '3'
        
    #ESE
    if df_nuc.loc[index,'ID'] == 'D55':
        df_nuc.at[index,'UF'] = 'SE'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #FORCEL
    if df_nuc.loc[index,'ID'] == 'D56':
        df_nuc.at[index,'UF'] = 'PR'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #HIDROPAN
    if df_nuc.loc[index,'ID'] == 'D57':
        df_nuc.at[index,'UF'] = 'RS'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #IENERGIA
    if df_nuc.loc[index,'ID'] == 'D58':
        df_nuc.at[index,'UF'] = 'SC'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #JARI (CONFIRMAR PERIODO)
    if df_nuc.loc[index,'ID'] == 'D59':
        df_nuc.at[index,'UF'] = 'PA'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #LIGHT
    if df_nuc.loc[index,'ID'] == 'D60':
        df_nuc.at[index,'UF'] = 'RJ'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #MUX-Energia
    if df_nuc.loc[index,'ID'] == 'D61':
        df_nuc.at[index,'UF'] = 'RS'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
    
    #CPFL RGE
    if df_nuc.loc[index,'ID'] == 'D62':
        df_nuc.at[index,'UF'] = 'RS'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #SULGIPE
    if df_nuc.loc[index,'ID'] == 'D63':
        df_nuc.at[index,'UF'] = 'SE'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #UHENPAL
    if df_nuc.loc[index,'ID'] == 'D64':
        df_nuc.at[index,'UF'] = 'TO'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ESS
    if df_nuc.loc[index,'ID'] == 'D65':
        df_nuc.at[index,'UF'] = 'SP'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Santa Cruz
    if df_nuc.loc[index,'ID'] == 'D66':
        df_nuc.at[index,'UF'] = 'SP'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #RGE
    if df_nuc.loc[index,'ID'] == 'D67':
        df_nuc.at[index,'UF'] = 'RS'
        df_nuc.at[index,'PERIODO_TARIFARIO'] = '5'
    

    return distribuidora



#%%Inserção dos dados
#Abre a SPARTA de cada arquivo
for arquivo in arquivos:
    try:
        df_sparta_nuc = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Mercado'
                                      ,header=27
                                      ,nrows=20
                                      ,usecols='B:H')
        
        df_sparta_capa = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA'
                                       ,header = 5
                                       ,nrows = 14
                                       ,usecols = [1,2])
    
        print('Leu o arquivo: ',arquivo)
        
        df_sparta_nuc = df_sparta_nuc.astype('str')

        linhas_nuc = range(len(df_sparta_nuc.index))
        colunas_nuc = range(len(df_sparta_nuc.columns))
        
        #Função para extração dos dados NUC de cada SPARTA
        extrai_nuc(df_nuc,df_sparta_nuc,index)
        distribuidora(df_sparta_capa,df_nuc,index)
        print('Extraiu o dado do arquivo: ',arquivo)
        print('Arquivos extraídos: ',round((index/len(arquivos)),2)*100,'%')
        index = index + 1 #Passa para o próximo indice para conseguir dados da proxima SPARTA
        
    except:
        print('Aba não disponível na SPARTA', arquivo) 


#%%Tratamento de dados 
#Remover dados duplicados e linhas nulas
df_nuc = df_nuc.drop_duplicates(subset = 'CHAVE',ignore_index = True) 
df_nuc = df_nuc.dropna(axis=0,how='all')


#Limpeza e Tratamento dos dados
df_nuc = df_nuc.astype(str)
df_nuc['PERIODO_TARIFARIO'] = df_nuc['PERIODO_TARIFARIO'].astype(int).replace('nan','-')
df_nuc['RESIDENCIAL'] = df_nuc['RESIDENCIAL'].replace('nan','0').replace('0.0002','0').replace('0.02882694386440767','0').astype(int)
df_nuc['INDUSTRIAL'] = df_nuc['INDUSTRIAL'].replace('nan','0').replace('0.004060566226777952','0').astype(int)
df_nuc['COMERCIAL'] = df_nuc['COMERCIAL'].replace('nan','0').replace('0.006335641576792125','0').astype(int)
df_nuc['RURAL'] = df_nuc['RURAL'].replace('nan','0').replace('0.021830093338455214','0').astype(int)
df_nuc['ILUMINACAO'] = df_nuc['ILUMINACAO'].replace('nan','0').astype(int)
df_nuc['PODER_PUBLICO'] = df_nuc['PODER_PUBLICO'].replace('nan','0').replace('0.0007352622673930835','0').astype(int)
df_nuc['SERVICO_PUBLICO'] = df_nuc['SERVICO_PUBLICO'].replace('nan','0').replace('7.732758089911768e-05','0').astype(int)
df_nuc['DEMAIS'] = df_nuc['DEMAIS'].replace('nan','0').astype(int)
df_nuc['A1'] = df_nuc['A1'].replace('nan','0').astype(int)
df_nuc['A2'] = df_nuc['A2'].replace('nan','0').astype(int)
df_nuc['A3'] = df_nuc['A3'].replace('nan','0').astype(int)
df_nuc['A3A'] = df_nuc['A3A'].replace('nan','0').astype(int)
df_nuc['A4'] = df_nuc['A4'].replace('nan','0').astype(int)
df_nuc['AS'] = df_nuc['AS'].replace('nan','0').astype(int)
df_nuc['BT'] = df_nuc['BT'].replace('nan','0').replace('2058933.3509999996','639030').replace('191975.67600000006','76038').astype(int)

# Define a data de atualização dos dados
data = datetime.today().strftime('%d/%m/%Y')
df_nuc['DATA_ATUALIZA'] = data

#%% Inserir dados no banco de dados

#Criar a lista para inserção no banco SQL com os dados da base editada
dados_list = df_nuc.values.tolist()


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
        sql = '''INSERT INTO ''' + tabela_oracle +''' VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        cursor.executemany(sql, dados_list)
    except Exception as err:
        print('Erro no Insert:', err)
    else:
        print('Carga executada com sucesso!')
        connection.commit() #Caso seja executado com sucesso, esse comando salva a base de dados
    finally:
        cursor.close()
        connection.close()




