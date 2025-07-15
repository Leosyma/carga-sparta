# -*- coding: utf-8 -*-
"""
Created on Tue Dec  6 07:47:34 2022

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
#pasta = r"W:\Inteligência Regulatória Analítica - IRA\2. Projetos\2023\BD SPARTA\Dados\SPARTA"

#Arquivos
#Como os dados das empresas estão em arquivos separados, listar todos que serão carregados conforme layout abaixo.
#arquivo ='SPARTA CPFL_Paulista _2022 RTE.xlsx'
arquivos = glob.glob(pasta)


index = 0 #Flag para inserção dos dados

#Nome da tabela Oracle onde será dada a carga
tabela_oracle = 'SPARTA_RECEITA'
ano_oracle = "'2023'"


#%% Montar o dataframe da estrutura da aba 'SPARTA_NUC'
#Defino o indice máximo que é igual o número de SPARTA
index_maximo = list(range(0,len(arquivos)))

#Criação da estrutura do tabela NUC
df_receita = pd.DataFrame(data = [], columns=['CHAVE','EVENTO_TARIFARIO','ANO','ID','DISTRIBUIDORA','UF','DATA','PERIODO_TARIFARIO','CONTRATO','RESIDENCIAL_RS','INDUSTRIAL_RS','COMERCIAL_RS','RURAL_RS','ILUMINACAO_RS','PODER_PUBLICO_RS','SERVICO_PUBLICO_RS','DEMAIS_RS','FORNECIMENTO_RS','A1_RS','A2_RS','A3_RS','A3A_RS','A4_RS','AS_RS','BT_RS','SUPRIMENTO_RS','LIVRES_A1_RS','DEMAIS_LIVRES_RS','DISTRIBUICAO_RS','GERADOR_RS','TOTAL_RS'],index=index_maximo) 

#Criação dataframe vazio
df_sparta_receita = pd.DataFrame(data = [])
df_sparta_capa = pd.DataFrame(data=[])


#df_sparta_receita = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Mercado'
 #                             ,header=7
  #                            ,nrows=49
   #                           ,usecols=[1,2,3,4,5,6,7])
   
   
#%%Extração dos resultados
def extrai_receita(self,df_receita,index):
    #Se o contrato for novo usa essa posição para inserir os dados
    if 'Percentual RI' in df_sparta_receita.iloc[:,:].values: 
        df_receita.at[index,'RESIDENCIAL_RS'] = df_sparta_receita.iloc[21,1] 
        df_receita.at[index,'INDUSTRIAL_RS'] = df_sparta_receita.iloc[22,1]  
        df_receita.at[index,'COMERCIAL_RS'] = df_sparta_receita.iloc[23,1]
        df_receita.at[index,'RURAL_RS'] = df_sparta_receita.iloc[24,1]
        df_receita.at[index,'ILUMINACAO_RS'] = df_sparta_receita.iloc[25,1]
        df_receita.at[index,'PODER_PUBLICO_RS'] = df_sparta_receita.iloc[26,1]
        df_receita.at[index,'SERVICO_PUBLICO_RS'] = df_sparta_receita.iloc[27,1]
        df_receita.at[index,'DEMAIS_RS'] = df_sparta_receita.iloc[28,1]
        df_receita.at[index,'CONTRATO'] = 'NOVO'
      
    #Usamos esse formato para SPARTA de 2013
    elif 'RI' in df_sparta_receita.iloc[:,:].values:
        df_receita.at[index,'RESIDENCIAL_RS'] = df_sparta_receita.iloc[21,2] 
        df_receita.at[index,'INDUSTRIAL_RS'] = df_sparta_receita.iloc[22,2]  
        df_receita.at[index,'COMERCIAL_RS'] = df_sparta_receita.iloc[23,2]
        df_receita.at[index,'RURAL_RS'] = df_sparta_receita.iloc[24,2]
        df_receita.at[index,'ILUMINACAO_RS'] = df_sparta_receita.iloc[25,2]
        df_receita.at[index,'PODER_PUBLICO_RS'] = df_sparta_receita.iloc[26,2]
        df_receita.at[index,'SERVICO_PUBLICO_RS'] = df_sparta_receita.iloc[27,2]
        df_receita.at[index,'DEMAIS_RS'] = df_sparta_receita.iloc[28,2]
        df_receita.at[index,'CONTRATO'] = 'ANTIGO'
 
    # Se o contrato for antigo usa essa posição para inserir os dados   
    else: 
        df_receita.at[index,'RESIDENCIAL_RS'] = df_sparta_receita.iloc[21,1] 
        df_receita.at[index,'INDUSTRIAL_RS'] = df_sparta_receita.iloc[22,1]  
        df_receita.at[index,'COMERCIAL_RS'] = df_sparta_receita.iloc[23,1]
        df_receita.at[index,'RURAL_RS'] = df_sparta_receita.iloc[24,1]
        df_receita.at[index,'ILUMINACAO_RS'] = df_sparta_receita.iloc[25,1]
        df_receita.at[index,'PODER_PUBLICO_RS'] = df_sparta_receita.iloc[26,1]
        df_receita.at[index,'SERVICO_PUBLICO_RS'] = df_sparta_receita.iloc[27,1]
        df_receita.at[index,'DEMAIS_RS'] = df_sparta_receita.iloc[28,1]
        df_receita.at[index,'CONTRATO'] = 'ANTIGO'
        

    df_receita.at[index,'FORNECIMENTO_RS'] = df_sparta_receita.iloc[0,2]
    df_receita.at[index,'A1_RS'] = df_sparta_receita.iloc[1,2]
    df_receita.at[index,'A2_RS'] = df_sparta_receita.iloc[2,2]
    df_receita.at[index,'A3_RS'] = df_sparta_receita.iloc[3,2]
    df_receita.at[index,'A3A_RS'] = df_sparta_receita.iloc[4,2]
    df_receita.at[index,'A4_RS'] = df_sparta_receita.iloc[5,2]
    df_receita.at[index,'AS_RS'] = df_sparta_receita.iloc[6,2]
    df_receita.at[index,'BT_RS'] = df_sparta_receita.iloc[7,2]
    df_receita.at[index,'SUPRIMENTO_RS'] = df_sparta_receita.iloc[8,2]
    df_receita.at[index,'LIVRES_A1_RS'] = df_sparta_receita.iloc[9,2]
    df_receita.at[index,'DEMAIS_LIVRES_RS'] = df_sparta_receita.iloc[10,2]
    df_receita.at[index,'DISTRIBUICAO_RS'] = df_sparta_receita.iloc[11,2]
    df_receita.at[index,'GERADOR_RS'] = df_sparta_receita.iloc[12,2]
    df_receita.at[index,'TOTAL_RS'] = df_sparta_receita.iloc[15,2]


    
    return extrai_receita


def distribuidora(self,df_receita,index):
    #Determina o ANO da SPARTA
    try:
        df_receita.at[index,'ANO'] = df_sparta_capa.iloc[8,1].strftime('%Y')
        
    except OverflowError as err:
        print('Erro foi na conversão do ANO: ',err)
    
    #Determina o ID da Distribuidora
    df_receita.at[index,'ID'] = df_sparta_capa.iloc[1,1]
    
    #Determina o NOME da Distribuidora
    df_receita.at[index,'DISTRIBUIDORA'] = df_sparta_capa.iloc[0,0].upper()
    
    #Determina a DATA da SPARTA
    try:
        df_receita.at[index,'DATA'] = df_sparta_capa.iloc[8,1].strftime('%Y-%m-%d')
        
    except OverflowError as err:
        print('Erro foi na conversão da DATA :',err)
        
        
    #Determina o EVENTO TARIFARIO
    if 'Reajuste' in df_sparta_capa.iloc[0,1]:
        df_receita.at[index,'EVENTO_TARIFARIO'] = 'RTA'
    elif 'Revisão' in df_sparta_capa.iloc[0,1]:
        df_receita.at[index,'EVENTO_TARIFARIO'] = 'RTP'
    else:
        df_receita.at[index,'EVENTO_TARIFARIO'] = 'RTE'
        
    #Determina a CHAVE
    df_receita.at[index,'CHAVE'] = df_receita.loc[index,'EVENTO_TARIFARIO']+df_receita.loc[index,'ANO']+df_receita.loc[index,'ID']
    
    #Determina UF e PERIODO TARIFARIO
    #AES SUL
    if df_receita.loc[index,'ID'] == 'D01':
        df_receita.at[index,'UF'] = 'RS'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #AME
    if df_receita.loc[index,'ID'] == 'D02':
        df_receita.at[index,'UF'] = 'AM'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #AMPLA
    if df_receita.loc[index,'ID'] == 'D03':
        df_receita.at[index,'UF'] = 'RJ'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #BANDEIRANTE
    if df_receita.loc[index,'ID'] == 'D04':
        df_receita.at[index,'UF'] = 'SP'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #BOA VISTA
    if df_receita.loc[index,'ID'] == 'D05':
        df_receita.at[index,'UF'] = 'RR'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CAIUA
    if df_receita.loc[index,'ID'] == 'D06':
        df_receita.at[index,'UF'] = 'SP'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEA
    if df_receita.loc[index,'ID'] == 'D07':
        df_receita.at[index,'UF'] = 'AP'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CEAL
    if df_receita.loc[index,'ID'] == 'D08':
        df_receita.at[index,'UF'] = 'AL'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CEB
    if df_receita.loc[index,'ID'] == 'D09':
        df_receita.at[index,'UF'] = 'DF'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '4'
    
    #CEEE
    if df_receita.loc[index,'ID'] == 'D10':
        df_receita.at[index,'UF'] = 'RS'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
      
    #CELESC
    if df_receita.loc[index,'ID'] == 'D11':
        df_receita.at[index,'UF'] = 'SC'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CELG
    if df_receita.loc[index,'ID'] == 'D12':
        df_receita.at[index,'UF'] = 'GO'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CELPA
    if df_receita.loc[index,'ID'] == 'D13':
        df_receita.at[index,'UF'] = 'PA'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CELPE
    if df_receita.loc[index,'ID'] == 'D14':
        df_receita.at[index,'UF'] = 'PE'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CELTINS
    if df_receita.loc[index,'ID'] == 'D15':
        df_receita.at[index,'UF'] = 'TO'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEMAR
    if df_receita.loc[index,'ID'] == 'D16':
        df_receita.at[index,'UF'] = 'MA'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #EMT
    if df_receita.loc[index,'ID'] == 'D17':
        df_receita.at[index,'UF'] = 'MT'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEMIG-D
    if df_receita.loc[index,'ID'] == 'D18':
        df_receita.at[index,'UF'] = 'MG'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEPISA
    if df_receita.loc[index,'ID'] == 'D19':
        df_receita.at[index,'UF'] = 'PI'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CERON
    if df_receita.loc[index,'ID'] == 'D20':
        df_receita.at[index,'UF'] = 'RO'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CERR (CONFIMAR O PERIODO)
    if df_receita.loc[index,'ID'] == 'D21':
        df_receita.at[index,'UF'] = 'RR'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CFLO
    if df_receita.loc[index,'ID'] == 'D22':
        df_receita.at[index,'UF'] = 'PR'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CHESP
    if df_receita.loc[index,'ID'] == 'D23':
        df_receita.at[index,'UF'] = 'GO'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL JAGUARI
    if df_receita.loc[index,'ID'] == 'D24':
        df_receita.at[index,'UF'] = 'SP'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL MOCOCA
    if df_receita.loc[index,'ID'] == 'D25':
        df_receita.at[index,'UF'] = 'SP'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'        
    
    #CPFL Santa Cruz
    if df_receita.loc[index,'ID'] == 'D26':
        df_receita.at[index,'UF'] = 'SP'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CNEE
    if df_receita.loc[index,'ID'] == 'D27':
        df_receita.at[index,'UF'] = 'SP'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COCEL
    if df_receita.loc[index,'ID'] == 'D28':
        df_receita.at[index,'UF'] = 'PR'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COELBA
    if df_receita.loc[index,'ID'] == 'D29':
        df_receita.at[index,'UF'] = 'BA'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COELCE
    if df_receita.loc[index,'ID'] == 'D30':
        df_receita.at[index,'UF'] = 'CE'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #COOPERALIANÇA
    if df_receita.loc[index,'ID'] == 'D31':
        df_receita.at[index,'UF'] = 'SC'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COPEL
    if df_receita.loc[index,'ID'] == 'D32':
        df_receita.at[index,'UF'] = 'PR'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COSERN
    if df_receita.loc[index,'ID'] == 'D33':
        df_receita.at[index,'UF'] = 'RN'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Leste Paulista
    if df_receita.loc[index,'ID'] == 'D34':
        df_receita.at[index,'UF'] = 'SP'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Piratininga
    if df_receita.loc[index,'ID'] == 'D35':
        df_receita.at[index,'UF'] = 'SP'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CPFL Paulista
    if df_receita.loc[index,'ID'] == 'D36':
        df_receita.at[index,'UF'] = 'SP'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Sul Paulista
    if df_receita.loc[index,'ID'] == 'D37':
        df_receita.at[index,'UF'] = 'SP'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #DEMEI
    if df_receita.loc[index,'ID'] == 'D38':
        df_receita.at[index,'UF'] = 'RS'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #DME-PC
    if df_receita.loc[index,'ID'] == 'D39':
        df_receita.at[index,'UF'] = 'MG'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EBO
    if df_receita.loc[index,'ID'] == 'D40':
        df_receita.at[index,'UF'] = 'PB'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #EDEVP
    if df_receita.loc[index,'ID'] == 'D41':
        df_receita.at[index,'UF'] = 'SP'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EEB
    if df_receita.loc[index,'ID'] == 'D42':
        df_receita.at[index,'UF'] = 'SP'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EFLJC
    if df_receita.loc[index,'ID'] == 'D43':
        df_receita.at[index,'UF'] = 'SC'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EFLUL
    if df_receita.loc[index,'ID'] == 'D44':
        df_receita.at[index,'UF'] = 'SC'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ELEKTRO
    if df_receita.loc[index,'ID'] == 'D45':
        df_receita.at[index,'UF'] = 'SP'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELETROACRE
    if df_receita.loc[index,'ID'] == 'D46':
        df_receita.at[index,'UF'] = 'AC'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELETROCAR
    if df_receita.loc[index,'ID'] == 'D47':
        df_receita.at[index,'UF'] = 'RS'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ELETROPAULO
    if df_receita.loc[index,'ID'] == 'D48':
        df_receita.at[index,'UF'] = 'SP'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELFSM
    if df_receita.loc[index,'ID'] == 'D49':
        df_receita.at[index,'UF'] = 'ES'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EMG
    if df_receita.loc[index,'ID'] == 'D50':
        df_receita.at[index,'UF'] = 'MG'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EMS
    if df_receita.loc[index,'ID'] == 'D51':
        df_receita.at[index,'UF'] = 'MS'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ENF
    if df_receita.loc[index,'ID'] == 'D52':
        df_receita.at[index,'UF'] = 'RJ'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EPB
    if df_receita.loc[index,'ID'] == 'D53':
        df_receita.at[index,'UF'] = 'PB'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ESCELSA (CONFIRMAR PERIODO)
    if df_receita.loc[index,'ID'] == 'D54':
        df_receita.at[index,'UF'] = 'ES'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '3'
        
    #ESE
    if df_receita.loc[index,'ID'] == 'D55':
        df_receita.at[index,'UF'] = 'SE'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #FORCEL
    if df_receita.loc[index,'ID'] == 'D56':
        df_receita.at[index,'UF'] = 'PR'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #HIDROPAN
    if df_receita.loc[index,'ID'] == 'D57':
        df_receita.at[index,'UF'] = 'RS'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #IENERGIA
    if df_receita.loc[index,'ID'] == 'D58':
        df_receita.at[index,'UF'] = 'SC'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #JARI (CONFIRMAR PERIODO)
    if df_receita.loc[index,'ID'] == 'D59':
        df_receita.at[index,'UF'] = 'PA'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #LIGHT
    if df_receita.loc[index,'ID'] == 'D60':
        df_receita.at[index,'UF'] = 'RJ'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #MUX-Energia
    if df_receita.loc[index,'ID'] == 'D61':
        df_receita.at[index,'UF'] = 'RS'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
    
    #CPFL RGE
    if df_receita.loc[index,'ID'] == 'D62':
        df_receita.at[index,'UF'] = 'RS'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #SULGIPE
    if df_receita.loc[index,'ID'] == 'D63':
        df_receita.at[index,'UF'] = 'SE'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #UHENPAL
    if df_receita.loc[index,'ID'] == 'D64':
        df_receita.at[index,'UF'] = 'TO'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ESS
    if df_receita.loc[index,'ID'] == 'D65':
        df_receita.at[index,'UF'] = 'SP'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Santa Cruz
    if df_receita.loc[index,'ID'] == 'D66':
        df_receita.at[index,'UF'] = 'SP'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #RGE
    if df_receita.loc[index,'ID'] == 'D67':
        df_receita.at[index,'UF'] = 'RS'
        df_receita.at[index,'PERIODO_TARIFARIO'] = '5'
    

    return distribuidora


#%%Inserção dos dados
#Abre a SPARTA de cada arquivo
for arquivo in arquivos:
    try:
        df_sparta_receita = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Mercado'
                                      ,header=7
                                      ,nrows=49
                                      ,usecols=[1,2,3,4,5,6,7])
        df_sparta_capa = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA'
                                       ,header = 5
                                       ,nrows = 14
                                       ,usecols = [1,2])
        print('Leu o arquivo: ',arquivo)
        
        #Função para extração dos dados NUC de cada SPARTA
        extrai_receita(df_sparta_receita, df_receita, index)
        distribuidora(df_sparta_capa,df_receita,index)
        print('Extraiu o dado do arquivo: ',arquivo)
        index = index + 1 #Passa para o próximo indice para conseguir dados da proxima SPARTA
        
    except:
        print('Aba não disponível na SPARTA', arquivo)


#%%Tratamento de dados 
#Remover dados duplicados e linhas nulas
df_receita = df_receita.drop_duplicates(subset = 'CHAVE',ignore_index = True) 
df_receita = df_receita.dropna(axis=0,how='all')    


#Limpeza e Tratamento dos dados
df_receita = df_receita.astype(str)
df_receita['PERIODO_TARIFARIO'] = df_receita['PERIODO_TARIFARIO'].astype(int)
df_receita['RESIDENCIAL_RS'] = df_receita['RESIDENCIAL_RS'].replace('nan','0').astype(float).replace('.',',')
df_receita['INDUSTRIAL_RS'] = df_receita['INDUSTRIAL_RS'].replace('nan','0').astype(float).replace('.',',')
df_receita['COMERCIAL_RS'] = df_receita['COMERCIAL_RS'].replace('nan','0').astype(float).replace('.',',')
df_receita['RURAL_RS'] = df_receita['RURAL_RS'].replace('nan','0').astype(float).replace('.',',')
df_receita['ILUMINACAO_RS'] = df_receita['ILUMINACAO_RS'].replace('nan','0').astype(float).replace('.',',')
df_receita['PODER_PUBLICO_RS'] = df_receita['PODER_PUBLICO_RS'].replace('nan','0').astype(float).replace('.',',')
df_receita['SERVICO_PUBLICO_RS'] = df_receita['SERVICO_PUBLICO_RS'].replace('nan','0').astype(float).replace('.',',')
df_receita['DEMAIS_RS'] = df_receita['DEMAIS_RS'].replace('nan','0').astype(float).replace('.',',')
df_receita['FORNECIMENTO_RS'] = df_receita['FORNECIMENTO_RS'].replace('nan','0').astype(float).replace('.',',')
df_receita['A1_RS'] = df_receita['A1_RS'].replace('nan','0').astype(float).replace('.',',')
df_receita['A2_RS'] = df_receita['A2_RS'].replace('nan','0').astype(float).replace('.',',')
df_receita['A3_RS'] = df_receita['A3_RS'].replace('nan','0').astype(float).replace('.',',')
df_receita['A3A_RS'] = df_receita['A3A_RS'].replace('nan','0').astype(float).replace('.',',')
df_receita['A4_RS'] = df_receita['A4_RS'].replace('nan','0').astype(float).replace('.',',')
df_receita['AS_RS'] = df_receita['AS_RS'].replace('nan','0').astype(float).replace('.',',')
df_receita['BT_RS'] = df_receita['BT_RS'].replace('nan','0').astype(float).replace('.',',')
df_receita['SUPRIMENTO_RS'] = df_receita['SUPRIMENTO_RS'].replace('nan','0').astype(float).replace('.',',')
df_receita['LIVRES_A1_RS'] = df_receita['LIVRES_A1_RS'].replace('nan','0').astype(float).replace('.',',')
df_receita['DEMAIS_LIVRES_RS'] = df_receita['DEMAIS_LIVRES_RS'].replace('nan','0').astype(float).replace('.',',')
df_receita['DISTRIBUICAO_RS'] = df_receita['DISTRIBUICAO_RS'].replace('nan','0').astype(float).replace('.',',')
df_receita['GERADOR_RS'] = df_receita['GERADOR_RS'].replace('nan','0').astype(float).replace('.',',')
df_receita['TOTAL_RS'] = df_receita['TOTAL_RS'].replace('nan','0').astype(float).replace('.',',')

# Define a data de atualização dos dados
data = datetime.today().strftime('%d/%m/%Y')
df_receita['DATA_ATUALIZA'] = data    

#%% Inserir dados no banco de dados

#Criar a lista para inserção no banco SQL com os dados da base editada
dados_list = df_receita.values.tolist()


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
        sql = '''INSERT INTO ''' + tabela_oracle +''' VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26,:27,:28,:29,:30,:31,:32)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        cursor.executemany(sql, dados_list)
    except Exception as err:
        print('Erro no Insert:', err)
    else:
        print('Carga executada com sucesso!')
        connection.commit() #Caso seja executado com sucesso, esse comando salva a base de dados
    finally:
        cursor.close()
        connection.close()


