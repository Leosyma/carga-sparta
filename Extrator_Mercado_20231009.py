# -*- coding: utf-8 -*-
"""
Created on Mon Dec  5 11:18:22 2022

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
# arquivo ='SPARTA  RTA 2022 - CPFL Piratininga.xlsx'
arquivos = glob.glob(pasta)


index = 0 #Flag para inserção dos dados

#Nome da tabela Oracle onde será dada a carga
tabela_oracle = 'SPARTA_MERCADO'
ano_oracle = "'2023'"



#%% Montar o dataframe da estrutura da aba 'SPARTA_NUC'
#Defino o indice máximo que é igual o número de SPARTA
index_maximo = list(range(0,len(arquivos)))

#Criação da estrutura do tabela NUC
df_mercado = pd.DataFrame(data = [], columns=['CHAVE','EVENTO_TARIFARIO','ANO','ID','DISTRIBUIDORA','UF','DATA','PERIODO_TARIFARIO','CONTRATO','RESIDENCIAL_MWH','INDUSTRIAL_MWH','COMERCIAL_MWH','RURAL_MWH','ILUMINACAO_MWH','PODER_PUBLICO_MWH','SERVICO_PUBLICO_MWH','DEMAIS_MWH','FORNECIMENTO_MWH','A1_MWH','A2_MWH','A3_MWH','A3A_MWH','A4_MWH','AS_MWH','BT_MWH','SUPRIMENTO_MWH','LIVRES_A1_MWH','DEMAIS_LIVRES_MWH','DISTRIBUICAO_MWH','GERADOR_MWH','MERCADO_BAIXA_RENDA_MWH','TOTAL_MWH','ANO_ANTERIOR_MWH','VARIACAO_PERCENT'],index=index_maximo) 

#Criação dataframe vazio
df_sparta_mercado = pd.DataFrame(data = [])
df_sparta_capa = pd.DataFrame(data=[])


# df_sparta_mercado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Mercado'
#                              ,header=7
#                               ,nrows=49
#                               ,usecols='B:G')


# df_sparta_mercado = df_sparta_mercado.astype('str')
# linhas_mercado = range(len(df_sparta_mercado.index))
# colunas_mercado = range(len(df_sparta_mercado.columns))




#%%Extração dos resultados
def determina_contrato(df_mercado,df_sparta_mercado,index):
    #Se essa 'string' estiver presente nessa aba o contrato é NOVO
    if 'Percentual RI' in df_sparta_mercado.iloc[:,:].values:
          df_mercado.at[index,'CONTRATO'] = 'NOVO' 
    #Caso contrário o contrato é ANTIGO      
    else:
          df_mercado.at[index,'CONTRATO'] = 'ANTIGO'


def extrai_mercado(df_mercado,df_sparta_mercado,index):
    #Se o contrato for novo usa essa posição para inserir os dados
    if df_mercado.at[index,'CONTRATO'] == 'NOVO': 
        for linha in linhas_mercado:
            for coluna in colunas_mercado:
                if 'RESIDENCIAL' in df_sparta_mercado.iloc[linha,coluna].upper():
                    df_mercado.at[index,'RESIDENCIAL_MWH'] = df_sparta_mercado.iloc[linha,(coluna+4)]
                elif 'INDUSTRIAL' in df_sparta_mercado.iloc[linha,coluna].upper():
                    df_mercado.at[index,'INDUSTRIAL_MWH'] = df_sparta_mercado.iloc[linha,(coluna+4)]
                elif 'COMERCIAL' in df_sparta_mercado.iloc[linha,coluna].upper():
                    df_mercado.at[index,'COMERCIAL_MWH'] = df_sparta_mercado.iloc[linha,(coluna+4)]
                elif 'RURAL' in df_sparta_mercado.iloc[linha,coluna].upper():
                    df_mercado.at[index,'RURAL_MWH'] = df_sparta_mercado.iloc[linha,(coluna+4)]
                elif 'ILUMINAÇÃO' in df_sparta_mercado.iloc[linha,coluna].upper():
                    df_mercado.at[index,'ILUMINACAO_MWH'] = df_sparta_mercado.iloc[linha,(coluna+4)]
                elif 'PODER PÚBLICO' in df_sparta_mercado.iloc[linha,coluna].upper():
                    df_mercado.at[index,'PODER_PUBLICO_MWH'] = df_sparta_mercado.iloc[linha,(coluna+4)]
                elif 'SERVIÇO PÚBLICO' in df_sparta_mercado.iloc[linha,coluna].upper():
                    df_mercado.at[index,'SERVICO_PUBLICO_MWH'] = df_sparta_mercado.iloc[linha,(coluna+4)]
                elif 'DEMAIS' in df_sparta_mercado.iloc[linha,coluna].upper():
                    df_mercado.at[index,'DEMAIS_MWH'] = df_sparta_mercado.iloc[linha,(coluna+4)]
                elif 'MWH ANO ANTERIOR' in df_sparta_mercado.iloc[linha,coluna].upper():
                    df_mercado.at[index,'ANO_ANTERIOR_MWH'] = df_sparta_mercado.iloc[linha,(coluna+1)]
                    df_mercado.at[index,'VARIACAO_PERCENT'] = df_sparta_mercado.iloc[(linha+1),(coluna+1)]
                
    else:
        for linha in linhas_mercado:
            for coluna in colunas_mercado:
                if 'RESIDENCIAL' in df_sparta_mercado.iloc[linha,3].upper():
                    df_mercado.at[index,'RESIDENCIAL_MWH'] = df_sparta_mercado.iloc[linha,(3+2)]
                elif 'INDUSTRIAL' in df_sparta_mercado.iloc[linha,3].upper():
                    df_mercado.at[index,'INDUSTRIAL_MWH'] = df_sparta_mercado.iloc[linha,(3+2)]
                elif 'COMERCIAL' in df_sparta_mercado.iloc[linha,3].upper():
                    df_mercado.at[index,'COMERCIAL_MWH'] = df_sparta_mercado.iloc[linha,(3+2)]
                elif 'RURAL' in df_sparta_mercado.iloc[linha,3].upper():
                    df_mercado.at[index,'RURAL_MWH'] = df_sparta_mercado.iloc[linha,(3+2)]
                elif 'ILUMINAÇÃO' in df_sparta_mercado.iloc[linha,3].upper():
                    df_mercado.at[index,'ILUMINACAO_MWH'] = df_sparta_mercado.iloc[linha,(3+2)]
                elif 'PODER PÚBLICO' in df_sparta_mercado.iloc[linha,3].upper():
                    df_mercado.at[index,'PODER_PUBLICO_MWH'] = df_sparta_mercado.iloc[linha,(3+2)]
                elif 'SERVIÇO PÚBLICO' in df_sparta_mercado.iloc[linha,3].upper():
                    df_mercado.at[index,'SERVICO_PUBLICO_MWH'] = df_sparta_mercado.iloc[linha,(3+2)]
                elif 'DEMAIS' in df_sparta_mercado.iloc[linha,3].upper():
                    df_mercado.at[index,'DEMAIS_MWH'] = df_sparta_mercado.iloc[linha,(3+2)]
                elif 'MWH ANO ANTERIOR' in df_sparta_mercado.iloc[linha,coluna].upper():
                    df_mercado.at[index,'ANO_ANTERIOR_MWH'] = df_sparta_mercado.iloc[linha,(coluna+1)]
                    df_mercado.at[index,'VARIACAO_PERCENT'] = df_sparta_mercado.iloc[(linha+1),(coluna+1)]
        
    
    df_mercado.at[index,'FORNECIMENTO_MWH'] = df_sparta_mercado.iloc[0,1]
    df_mercado.at[index,'A1_MWH'] = df_sparta_mercado.iloc[1,1]
    df_mercado.at[index,'A2_MWH'] = df_sparta_mercado.iloc[2,1]
    df_mercado.at[index,'A3_MWH'] = df_sparta_mercado.iloc[3,1]
    df_mercado.at[index,'A3A_MWH'] = df_sparta_mercado.iloc[4,1]
    df_mercado.at[index,'A4_MWH'] = df_sparta_mercado.iloc[5,1]
    df_mercado.at[index,'AS_MWH'] = df_sparta_mercado.iloc[6,1]
    df_mercado.at[index,'BT_MWH'] = df_sparta_mercado.iloc[7,1]
    df_mercado.at[index,'SUPRIMENTO_MWH'] = df_sparta_mercado.iloc[8,1]
    df_mercado.at[index,'LIVRES_A1_MWH'] = df_sparta_mercado.iloc[9,1]
    df_mercado.at[index,'DEMAIS_LIVRES_MWH'] = df_sparta_mercado.iloc[10,1]
    df_mercado.at[index,'DISTRIBUICAO_MWH'] = df_sparta_mercado.iloc[11,1]
    df_mercado.at[index,'GERADOR_MWH'] = df_sparta_mercado.iloc[12,1]
    df_mercado.at[index,'MERCADO_BAIXA_RENDA_MWH'] = df_sparta_mercado.iloc[17,1]
    df_mercado.at[index,'TOTAL_MWH'] = df_sparta_mercado.iloc[15,1]




def distribuidora(self,df_mercado,index):
    #Determina o ANO da SPARTA
    df_mercado.at[index,'ANO'] = df_sparta_capa.iloc[8,1].strftime('%Y')
    
    #Determina o ID da Distribuidora
    df_mercado.at[index,'ID'] = df_sparta_capa.iloc[1,1]
    
    #Determina o NOME da Distribuidora
    df_mercado.at[index,'DISTRIBUIDORA'] = df_sparta_capa.iloc[0,0].upper()
    
    #Determina a DATA da SPARTA
    df_mercado.at[index,'DATA'] = df_sparta_capa.iloc[8,1].strftime('%Y-%m-%d')
    
    #Determina o EVENTO TARIFARIO
    if 'Reajuste' in df_sparta_capa.iloc[0,1]:
        df_mercado.at[index,'EVENTO_TARIFARIO'] = 'RTA'
    elif 'Revisão' in df_sparta_capa.iloc[0,1]:
        df_mercado.at[index,'EVENTO_TARIFARIO'] = 'RTP'
    else:
        df_mercado.at[index,'EVENTO_TARIFARIO'] = 'RTE'
        
    #Determina a CHAVE
    df_mercado.at[index,'CHAVE'] = df_mercado.loc[index,'EVENTO_TARIFARIO']+df_mercado.loc[index,'ANO']+df_mercado.loc[index,'ID']
    
    #Determina UF e PERIODO TARIFARIO
    #AES SUL
    if df_mercado.loc[index,'ID'] == 'D01':
        df_mercado.at[index,'UF'] = 'RS'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #AME
    if df_mercado.loc[index,'ID'] == 'D02':
        df_mercado.at[index,'UF'] = 'AM'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #AMPLA
    if df_mercado.loc[index,'ID'] == 'D03':
        df_mercado.at[index,'UF'] = 'RJ'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #BANDEIRANTE
    if df_mercado.loc[index,'ID'] == 'D04':
        df_mercado.at[index,'UF'] = 'SP'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #BOA VISTA
    if df_mercado.loc[index,'ID'] == 'D05':
        df_mercado.at[index,'UF'] = 'RR'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CAIUA
    if df_mercado.loc[index,'ID'] == 'D06':
        df_mercado.at[index,'UF'] = 'SP'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEA
    if df_mercado.loc[index,'ID'] == 'D07':
        df_mercado.at[index,'UF'] = 'AP'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CEAL
    if df_mercado.loc[index,'ID'] == 'D08':
        df_mercado.at[index,'UF'] = 'AL'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CEB
    if df_mercado.loc[index,'ID'] == 'D09':
        df_mercado.at[index,'UF'] = 'DF'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '4'
    
    #CEEE
    if df_mercado.loc[index,'ID'] == 'D10':
        df_mercado.at[index,'UF'] = 'RS'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
      
    #CELESC
    if df_mercado.loc[index,'ID'] == 'D11':
        df_mercado.at[index,'UF'] = 'SC'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CELG
    if df_mercado.loc[index,'ID'] == 'D12':
        df_mercado.at[index,'UF'] = 'GO'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CELPA
    if df_mercado.loc[index,'ID'] == 'D13':
        df_mercado.at[index,'UF'] = 'PA'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CELPE
    if df_mercado.loc[index,'ID'] == 'D14':
        df_mercado.at[index,'UF'] = 'PE'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CELTINS
    if df_mercado.loc[index,'ID'] == 'D15':
        df_mercado.at[index,'UF'] = 'TO'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEMAR
    if df_mercado.loc[index,'ID'] == 'D16':
        df_mercado.at[index,'UF'] = 'MA'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #EMT
    if df_mercado.loc[index,'ID'] == 'D17':
        df_mercado.at[index,'UF'] = 'MT'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEMIG-D
    if df_mercado.loc[index,'ID'] == 'D18':
        df_mercado.at[index,'UF'] = 'MG'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEPISA
    if df_mercado.loc[index,'ID'] == 'D19':
        df_mercado.at[index,'UF'] = 'PI'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CERON
    if df_mercado.loc[index,'ID'] == 'D20':
        df_mercado.at[index,'UF'] = 'RO'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CERR (CONFIMAR O PERIODO)
    if df_mercado.loc[index,'ID'] == 'D21':
        df_mercado.at[index,'UF'] = 'RR'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CFLO
    if df_mercado.loc[index,'ID'] == 'D22':
        df_mercado.at[index,'UF'] = 'PR'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CHESP
    if df_mercado.loc[index,'ID'] == 'D23':
        df_mercado.at[index,'UF'] = 'GO'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL JAGUARI
    if df_mercado.loc[index,'ID'] == 'D24':
        df_mercado.at[index,'UF'] = 'SP'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL MOCOCA
    if df_mercado.loc[index,'ID'] == 'D25':
        df_mercado.at[index,'UF'] = 'SP'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'        
    
    #CPFL Santa Cruz
    if df_mercado.loc[index,'ID'] == 'D26':
        df_mercado.at[index,'UF'] = 'SP'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CNEE
    if df_mercado.loc[index,'ID'] == 'D27':
        df_mercado.at[index,'UF'] = 'SP'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COCEL
    if df_mercado.loc[index,'ID'] == 'D28':
        df_mercado.at[index,'UF'] = 'PR'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COELBA
    if df_mercado.loc[index,'ID'] == 'D29':
        df_mercado.at[index,'UF'] = 'BA'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COELCE
    if df_mercado.loc[index,'ID'] == 'D30':
        df_mercado.at[index,'UF'] = 'CE'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #COOPERALIANÇA
    if df_mercado.loc[index,'ID'] == 'D31':
        df_mercado.at[index,'UF'] = 'SC'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COPEL
    if df_mercado.loc[index,'ID'] == 'D32':
        df_mercado.at[index,'UF'] = 'PR'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COSERN
    if df_mercado.loc[index,'ID'] == 'D33':
        df_mercado.at[index,'UF'] = 'RN'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Leste Paulista
    if df_mercado.loc[index,'ID'] == 'D34':
        df_mercado.at[index,'UF'] = 'SP'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Piratininga
    if df_mercado.loc[index,'ID'] == 'D35':
        df_mercado.at[index,'UF'] = 'SP'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CPFL Paulista
    if df_mercado.loc[index,'ID'] == 'D36':
        df_mercado.at[index,'UF'] = 'SP'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Sul Paulista
    if df_mercado.loc[index,'ID'] == 'D37':
        df_mercado.at[index,'UF'] = 'SP'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #DEMEI
    if df_mercado.loc[index,'ID'] == 'D38':
        df_mercado.at[index,'UF'] = 'RS'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #DME-PC
    if df_mercado.loc[index,'ID'] == 'D39':
        df_mercado.at[index,'UF'] = 'MG'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EBO
    if df_mercado.loc[index,'ID'] == 'D40':
        df_mercado.at[index,'UF'] = 'PB'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #EDEVP
    if df_mercado.loc[index,'ID'] == 'D41':
        df_mercado.at[index,'UF'] = 'SP'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EEB
    if df_mercado.loc[index,'ID'] == 'D42':
        df_mercado.at[index,'UF'] = 'SP'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EFLJC
    if df_mercado.loc[index,'ID'] == 'D43':
        df_mercado.at[index,'UF'] = 'SC'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EFLUL
    if df_mercado.loc[index,'ID'] == 'D44':
        df_mercado.at[index,'UF'] = 'SC'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ELEKTRO
    if df_mercado.loc[index,'ID'] == 'D45':
        df_mercado.at[index,'UF'] = 'SP'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELETROACRE
    if df_mercado.loc[index,'ID'] == 'D46':
        df_mercado.at[index,'UF'] = 'AC'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELETROCAR
    if df_mercado.loc[index,'ID'] == 'D47':
        df_mercado.at[index,'UF'] = 'RS'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ELETROPAULO
    if df_mercado.loc[index,'ID'] == 'D48':
        df_mercado.at[index,'UF'] = 'SP'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELFSM
    if df_mercado.loc[index,'ID'] == 'D49':
        df_mercado.at[index,'UF'] = 'ES'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EMG
    if df_mercado.loc[index,'ID'] == 'D50':
        df_mercado.at[index,'UF'] = 'MG'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EMS
    if df_mercado.loc[index,'ID'] == 'D51':
        df_mercado.at[index,'UF'] = 'MS'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ENF
    if df_mercado.loc[index,'ID'] == 'D52':
        df_mercado.at[index,'UF'] = 'RJ'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EPB
    if df_mercado.loc[index,'ID'] == 'D53':
        df_mercado.at[index,'UF'] = 'PB'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ESCELSA (CONFIRMAR PERIODO)
    if df_mercado.loc[index,'ID'] == 'D54':
        df_mercado.at[index,'UF'] = 'ES'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '3'
        
    #ESE
    if df_mercado.loc[index,'ID'] == 'D55':
        df_mercado.at[index,'UF'] = 'SE'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #FORCEL
    if df_mercado.loc[index,'ID'] == 'D56':
        df_mercado.at[index,'UF'] = 'PR'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #HIDROPAN
    if df_mercado.loc[index,'ID'] == 'D57':
        df_mercado.at[index,'UF'] = 'RS'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #IENERGIA
    if df_mercado.loc[index,'ID'] == 'D58':
        df_mercado.at[index,'UF'] = 'SC'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #JARI (CONFIRMAR PERIODO)
    if df_mercado.loc[index,'ID'] == 'D59':
        df_mercado.at[index,'UF'] = 'PA'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #LIGHT
    if df_mercado.loc[index,'ID'] == 'D60':
        df_mercado.at[index,'UF'] = 'RJ'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #MUX-Energia
    if df_mercado.loc[index,'ID'] == 'D61':
        df_mercado.at[index,'UF'] = 'RS'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
    
    #CPFL RGE
    if df_mercado.loc[index,'ID'] == 'D62':
        df_mercado.at[index,'UF'] = 'RS'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #SULGIPE
    if df_mercado.loc[index,'ID'] == 'D63':
        df_mercado.at[index,'UF'] = 'SE'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #UHENPAL
    if df_mercado.loc[index,'ID'] == 'D64':
        df_mercado.at[index,'UF'] = 'TO'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ESS
    if df_mercado.loc[index,'ID'] == 'D65':
        df_mercado.at[index,'UF'] = 'SP'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Santa Cruz
    if df_mercado.loc[index,'ID'] == 'D66':
        df_mercado.at[index,'UF'] = 'SP'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #RGE
    if df_mercado.loc[index,'ID'] == 'D67':
        df_mercado.at[index,'UF'] = 'RS'
        df_mercado.at[index,'PERIODO_TARIFARIO'] = '5'
    

    return distribuidora


#%%Inserção dos dados
#Abre a SPARTA de cada arquivo
for arquivo in arquivos:
    try:
        df_sparta_mercado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Mercado'
                                     ,header=7
                                      ,nrows=49
                                      ,usecols='B:G')
    
        
        
        df_sparta_capa = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA'
                                       ,header = 5
                                       ,nrows = 14
                                       ,usecols = [1,2])
        
        
        df_sparta_mercado = df_sparta_mercado.astype('str')
        linhas_mercado = range(len(df_sparta_mercado.index))
        colunas_mercado = range(len(df_sparta_mercado.columns))
        
        
        print('Leu o arquivo: ',arquivo)
        
        #Função para extração dos dados NUC de cada SPARTA
        determina_contrato(df_mercado,df_sparta_mercado,index)
        extrai_mercado(df_mercado,df_sparta_mercado,index)
        distribuidora(df_sparta_capa,df_mercado,index)
        print('Extraiu o dado do arquivo: ',arquivo)
        print('Arquivos extraídos: ',round((index/len(arquivos)),2)*100,'%')
        index = index + 1 #Passa para o próximo indice para conseguir dados da proxima SPARTA
        
    except:
        print('Aba não disponível na SPARTA', arquivo)



#%%Tratamento dos dados
#Remover dados duplicados e linhas nulas
df_mercado = df_mercado.drop_duplicates(subset = 'CHAVE', ignore_index = True)
df_mercado = df_mercado.dropna(axis=0,how='all')

#Limpeza e Tratamento dos dados
df_mercado = df_mercado.astype(str)
df_mercado['PERIODO_TARIFARIO'] = df_mercado['PERIODO_TARIFARIO'].astype(int).replace('nan','-')
df_mercado['RESIDENCIAL_MWH'] = df_mercado['RESIDENCIAL_MWH'].replace('nan','0').astype(float).replace('.',',')
df_mercado['INDUSTRIAL_MWH'] = df_mercado['INDUSTRIAL_MWH'].replace('nan','0').astype(float).replace('.',',')
df_mercado['COMERCIAL_MWH'] = df_mercado['COMERCIAL_MWH'].replace('nan','0').astype(float).replace('.',',')
df_mercado['RURAL_MWH'] = df_mercado['RURAL_MWH'].replace('nan','0').astype(float).replace('.',',')
df_mercado['ILUMINACAO_MWH'] = df_mercado['ILUMINACAO_MWH'].replace('nan','0').astype(float).replace('.',',')
df_mercado['PODER_PUBLICO_MWH'] = df_mercado['PODER_PUBLICO_MWH'].replace('nan','0').astype(float).replace('.',',')
df_mercado['SERVICO_PUBLICO_MWH'] = df_mercado['SERVICO_PUBLICO_MWH'].replace('nan','0').astype(float).replace('.',',')
df_mercado['DEMAIS_MWH'] = df_mercado['DEMAIS_MWH'].replace('nan','0').astype(float).replace('.',',')
df_mercado['FORNECIMENTO_MWH'] = df_mercado['FORNECIMENTO_MWH'].replace('nan','0').astype(float).replace('.',',')
df_mercado['A1_MWH'] = df_mercado['A1_MWH'].replace('nan','0').astype(float).replace('.',',')
df_mercado['A2_MWH'] = df_mercado['A2_MWH'].replace('nan','0').astype(float).replace('.',',')
df_mercado['A3_MWH'] = df_mercado['A3_MWH'].replace('nan','0').astype(float).replace('.',',')
df_mercado['A3A_MWH'] = df_mercado['A3A_MWH'].replace('nan','0').astype(float).replace('.',',')
df_mercado['A4_MWH'] = df_mercado['A4_MWH'].replace('nan','0').astype(float).replace('.',',')
df_mercado['AS_MWH'] = df_mercado['AS_MWH'].replace('nan','0').astype(float).replace('.',',')
df_mercado['BT_MWH'] = df_mercado['BT_MWH'].replace('nan','0').astype(float).replace('.',',')
df_mercado['SUPRIMENTO_MWH'] = df_mercado['SUPRIMENTO_MWH'].replace('nan','0').astype(float).replace('.',',')
df_mercado['LIVRES_A1_MWH'] = df_mercado['LIVRES_A1_MWH'].replace('nan','0').astype(float).replace('.',',')
df_mercado['DEMAIS_LIVRES_MWH'] = df_mercado['DEMAIS_LIVRES_MWH'].replace('nan','0').astype(float).replace('.',',')
df_mercado['DISTRIBUICAO_MWH'] = df_mercado['DISTRIBUICAO_MWH'].replace('nan','0').astype(float).replace('.',',')
df_mercado['GERADOR_MWH'] = df_mercado['GERADOR_MWH'].replace('nan','0').astype(float).replace('.',',')
df_mercado['MERCADO_BAIXA_RENDA_MWH'] = df_mercado['MERCADO_BAIXA_RENDA_MWH'].replace('nan','0').astype(float).replace('.',',')
df_mercado['TOTAL_MWH'] = df_mercado['TOTAL_MWH'].replace('nan','0').astype(float).replace('.',',')
df_mercado['ANO_ANTERIOR_MWH'] = df_mercado['ANO_ANTERIOR_MWH'].replace('nan','0').astype(float).replace('.',',')
df_mercado['VARIACAO_PERCENT'] = df_mercado['VARIACAO_PERCENT'].replace('nan','0').astype(float).replace('.',',')

# Define a data de atualização dos dados
data = datetime.today().strftime('%d/%m/%Y')
df_mercado['DATA_ATUALIZA'] = data

#%% Inserir dados no banco de dados

#Criar a lista para inserção no banco SQL com os dados da base editada
dados_list = df_mercado.values.tolist()


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
        sql = '''INSERT INTO ''' + tabela_oracle +''' VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26,:27,:28,:29,:30,:31,:32,:33,:34,:35)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        cursor.executemany(sql, dados_list)
    except Exception as err:
        print('Erro no Insert:', err)
    else:
        print('Carga executada com sucesso!')
        connection.commit() #Caso seja executado com sucesso, esse comando salva a base de dados
    finally:
        cursor.close()
        connection.close()




