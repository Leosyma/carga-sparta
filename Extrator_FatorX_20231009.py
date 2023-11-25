# -*- coding: utf-8 -*-
"""
Created on Tue Dec  6 08:52:07 2022

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
#pasta = r"C:\Users\2018459\OneDrive - CPFL Energia S A\Área de Trabalho\RTP e RTA (concessionaria)\SPARTA TESTE"

#Arquivos
#Como os dados das empresas estão em arquivos separados, listar todos que serão carregados conforme layout abaixo.
#arquivo ='SPARTA _CELPA_ 2015.xlsx'
arquivos = glob.glob(pasta)


index = 0 #Flag para inserção dos dados

#Nome da tabela Oracle onde será dada a carga
tabela_oracle = 'SPARTA_FATOR_X'
ano_oracle = "'2023'"


#%% Montar o dataframe da estrutura da aba 'SPARTA_NUC'
#Defino o indice máximo que é igual o número de SPARTA
index_maximo = list(range(0,len(arquivos)))

#Criação da estrutura do tabela NUC
df_fatorx = pd.DataFrame(data = [], columns=['CHAVE','EVENTO_TARIFARIO','ANO','ID','DISTRIBUIDORA','UF','DATA','PERIODO_TARIFARIO','CONTRATO','IGPM_PERCENT','IPCA_PERCENT','COMPONENTE_PD_PERCENT','COMPONENTE_T_PERCENT','COMPONENTE_Q_PERCENT','FATOR_X_PERCENT','IVI_X_PERCENT'],index=index_maximo) 

#Criação dataframe vazio
df_sparta_fatorx = pd.DataFrame(data = [])
df_sparta_capa = pd.DataFrame(data=[])
df_sparta_mercado = pd.DataFrame(data=[])
df_sparta_vpb = pd.DataFrame(data=[])

#df_sparta_fatorx = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'VPB1'
 #                             ,header=36
  #                            ,nrows=7
   #                           ,usecols=[4,5])

#df_sparta_vpb = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'VPB e Fator X'
 #                             ,header=31
  #                            ,nrows=12
   #                           ,usecols=[1,2])
 
#df_sparta_vpb1 = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'VPB e Fator X'
 #                             ,header=123
  #                            ,nrows=8
   #                           ,usecols=[1,2])
 
    
#%%Extração dos resultados
def extrai_fatorx(df_sparta_fatorx,df_sparta_vpb,df_sparta_vpb1,df_sparta_mercado,df_fatorx,index):
    data_ano = df_sparta_capa.iloc[8,1].strftime('%Y')
    #Tratamento especifico para as SPARTA de 2015 da CELPA D13 - IGPM = 584.8470842
    #Tratamento especifico para as SPARTA de 2015 da ELEKTRO D45 - IGPM = 586.426
    #Tratamento especifico para as SPARTA de 2015 da ELEKTRO D48 - IGPM = 581.235
    if ('D13' in df_sparta_capa.iloc[:,:].values and (data_ano == '2015')) or ('D45' in df_sparta_capa.iloc[:,:].values and 586.426 in df_sparta_capa.iloc[:,:].values) or ('D48' in df_sparta_capa.iloc[:,:].values and 2015 in df_sparta_capa.iloc[:,:].values):
        df_fatorx.at[index,'IGPM_PERCENT'] = df_sparta_fatorx.iloc[0,1]
        df_fatorx.at[index,'IPCA_PERCENT'] = df_sparta_fatorx.iloc[1,1]
        df_fatorx.at[index,'COMPONENTE_PD_PERCENT'] = df_sparta_vpb1.iloc[1,1]
        df_fatorx.at[index,'COMPONENTE_T_PERCENT'] = df_sparta_vpb1.iloc[0,1]
        df_fatorx.at[index,'COMPONENTE_Q_PERCENT'] = df_sparta_fatorx.iloc[4,1]
        df_fatorx.at[index,'FATOR_X_PERCENT'] = df_sparta_vpb1.iloc[3,1]
        if 'Percentual RI' in df_sparta_mercado.iloc[:,:].values:
            df_fatorx.at[index,'IVI_X_PERCENT'] = df_fatorx.at[index,'IPCA_PERCENT'] - df_fatorx.at[index,'FATOR_X_PERCENT']
            df_fatorx.at[index,'CONTRATO'] = 'NOVO'
        else:
            df_fatorx.at[index,'IVI_X_PERCENT'] = df_fatorx.at[index,'IGPM_PERCENT'] - df_fatorx.at[index,'FATOR_X_PERCENT']
            df_fatorx.at[index,'CONTRATO'] = 'ANTIGO'
            
    #Tratamento especifico para as SPARTA de 2019 da CELPA D13 - IGPM = 741.346
    elif ('D13' in df_sparta_capa.iloc[:,:].values and 741.346 in df_sparta_capa.iloc[:,:].values):
        df_fatorx.at[index,'IGPM_PERCENT'] = df_sparta_fatorx.iloc[0,1]
        df_fatorx.at[index,'IPCA_PERCENT'] = df_sparta_fatorx.iloc[1,1]
        df_fatorx.at[index,'COMPONENTE_PD_PERCENT'] = df_sparta_vpb.iloc[2,1]
        df_fatorx.at[index,'COMPONENTE_T_PERCENT'] = df_sparta_vpb.iloc[5,1]
        df_fatorx.at[index,'COMPONENTE_Q_PERCENT'] = df_sparta_vpb.iloc[9,1]
        df_fatorx.at[index,'FATOR_X_PERCENT'] = df_sparta_vpb.iloc[10,1]
        if 'Percentual RI' in df_sparta_mercado.iloc[:,:].values:
            df_fatorx.at[index,'IVI_X_PERCENT'] = df_fatorx.at[index,'IPCA_PERCENT'] - df_fatorx.at[index,'FATOR_X_PERCENT']
            df_fatorx.at[index,'CONTRATO'] = 'NOVO'
        else:
            df_fatorx.at[index,'IVI_X_PERCENT'] = df_fatorx.at[index,'IGPM_PERCENT'] - df_fatorx.at[index,'FATOR_X_PERCENT']
            df_fatorx.at[index,'CONTRATO'] = 'ANTIGO'
    
    #Se o processo for RTP usamos o layout abaixo
    elif 'Revisão' in df_sparta_capa.iloc[0,1]:
        df_fatorx.at[index,'IGPM_PERCENT'] = df_sparta_fatorx.iloc[0,1]
        df_fatorx.at[index,'IPCA_PERCENT'] = df_sparta_fatorx.iloc[1,1]
        df_fatorx.at[index,'COMPONENTE_PD_PERCENT'] = df_sparta_vpb.iloc[0,1]
        df_fatorx.at[index,'COMPONENTE_T_PERCENT'] = df_sparta_vpb.iloc[3,1]
        df_fatorx.at[index,'COMPONENTE_Q_PERCENT'] = df_sparta_vpb.iloc[7,1]
        df_fatorx.at[index,'FATOR_X_PERCENT'] = df_sparta_vpb.iloc[8,1]
        if 'Percentual RI' in df_sparta_mercado.iloc[:,:].values:
            df_fatorx.at[index,'IVI_X_PERCENT'] = df_fatorx.at[index,'IPCA_PERCENT'] - df_fatorx.at[index,'FATOR_X_PERCENT']
            df_fatorx.at[index,'CONTRATO'] = 'NOVO'
        else:
            df_fatorx.at[index,'IVI_X_PERCENT'] = df_fatorx.at[index,'IGPM_PERCENT'] - df_fatorx.at[index,'FATOR_X_PERCENT']
            df_fatorx.at[index,'CONTRATO'] = 'ANTIGO'
            
    
    #Se o contrato for novo usa essa posição para inserir os dados
    elif 'Percentual RI' in df_sparta_mercado.iloc[:,:].values: 
        df_fatorx.at[index,'IGPM_PERCENT'] = df_sparta_fatorx.iloc[0,1]
        df_fatorx.at[index,'IPCA_PERCENT'] = df_sparta_fatorx.iloc[1,1]
        df_fatorx.at[index,'COMPONENTE_PD_PERCENT'] = df_sparta_fatorx.iloc[2,1]
        df_fatorx.at[index,'COMPONENTE_T_PERCENT'] = df_sparta_fatorx.iloc[3,1]
        df_fatorx.at[index,'COMPONENTE_Q_PERCENT'] = df_sparta_fatorx.iloc[4,1]
        df_fatorx.at[index,'FATOR_X_PERCENT'] = df_sparta_fatorx.iloc[5,1]
        df_fatorx.at[index,'IVI_X_PERCENT'] = df_fatorx.at[index,'IPCA_PERCENT'] - df_fatorx.at[index,'FATOR_X_PERCENT']
        df_fatorx.at[index,'CONTRATO'] = 'NOVO'
            
 
    # Se o contrato for antigo usa essa posição para inserir os dados   
    else: 
        df_fatorx.at[index,'IGPM_PERCENT'] = df_sparta_fatorx.iloc[0,1]
        df_fatorx.at[index,'IPCA_PERCENT'] = df_sparta_fatorx.iloc[1,1]
        df_fatorx.at[index,'COMPONENTE_PD_PERCENT'] = df_sparta_fatorx.iloc[2,1]
        df_fatorx.at[index,'COMPONENTE_T_PERCENT'] = df_sparta_fatorx.iloc[3,1]
        df_fatorx.at[index,'COMPONENTE_Q_PERCENT'] = df_sparta_fatorx.iloc[4,1]
        df_fatorx.at[index,'FATOR_X_PERCENT'] = df_sparta_fatorx.iloc[5,1]
        df_fatorx.at[index,'IVI_X_PERCENT'] = df_fatorx.at[index,'IGPM_PERCENT'] - df_fatorx.at[index,'FATOR_X_PERCENT']
        df_fatorx.at[index,'CONTRATO'] = 'ANTIGO'
        

    return extrai_fatorx



def distribuidora(self,df_fatorx,index):
    #Determina o ANO da SPARTA
    df_fatorx.at[index,'ANO'] = df_sparta_capa.iloc[8,1].strftime('%Y')
    
    #Determina o ID da Distribuidora
    df_fatorx.at[index,'ID'] = df_sparta_capa.iloc[1,1]
    
    #Determina o NOME da Distribuidora
    df_fatorx.at[index,'DISTRIBUIDORA'] = df_sparta_capa.iloc[0,0].upper()
    
    #Determina a DATA da SPARTA
    df_fatorx.at[index,'DATA'] = df_sparta_capa.iloc[8,1].strftime('%Y-%m-%d') 
      
    #Determina o EVENTO TARIFARIO
    if 'Reajuste' in df_sparta_capa.iloc[0,1]:
        df_fatorx.at[index,'EVENTO_TARIFARIO'] = 'RTA'
    elif 'Revisão' in df_sparta_capa.iloc[0,1]:
        df_fatorx.at[index,'EVENTO_TARIFARIO'] = 'RTP'
    else:
        df_fatorx.at[index,'EVENTO_TARIFARIO'] = 'RTE'
        
    #Determina a CHAVE
    df_fatorx.at[index,'CHAVE'] = df_fatorx.loc[index,'EVENTO_TARIFARIO']+df_fatorx.loc[index,'ANO']+df_fatorx.loc[index,'ID']
    
    #Determina UF e PERIODO TARIFARIO
    #AES SUL
    if df_fatorx.loc[index,'ID'] == 'D01':
        df_fatorx.at[index,'UF'] = 'RS'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #AME
    if df_fatorx.loc[index,'ID'] == 'D02':
        df_fatorx.at[index,'UF'] = 'AM'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #AMPLA
    if df_fatorx.loc[index,'ID'] == 'D03':
        df_fatorx.at[index,'UF'] = 'RJ'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #BANDEIRANTE
    if df_fatorx.loc[index,'ID'] == 'D04':
        df_fatorx.at[index,'UF'] = 'SP'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #BOA VISTA
    if df_fatorx.loc[index,'ID'] == 'D05':
        df_fatorx.at[index,'UF'] = 'RR'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CAIUA
    if df_fatorx.loc[index,'ID'] == 'D06':
        df_fatorx.at[index,'UF'] = 'SP'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEA
    if df_fatorx.loc[index,'ID'] == 'D07':
        df_fatorx.at[index,'UF'] = 'AP'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CEAL
    if df_fatorx.loc[index,'ID'] == 'D08':
        df_fatorx.at[index,'UF'] = 'AL'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CEB
    if df_fatorx.loc[index,'ID'] == 'D09':
        df_fatorx.at[index,'UF'] = 'DF'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '4'
    
    #CEEE
    if df_fatorx.loc[index,'ID'] == 'D10':
        df_fatorx.at[index,'UF'] = 'RS'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
      
    #CELESC
    if df_fatorx.loc[index,'ID'] == 'D11':
        df_fatorx.at[index,'UF'] = 'SC'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CELG
    if df_fatorx.loc[index,'ID'] == 'D12':
        df_fatorx.at[index,'UF'] = 'GO'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CELPA
    if df_fatorx.loc[index,'ID'] == 'D13':
        df_fatorx.at[index,'UF'] = 'PA'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CELPE
    if df_fatorx.loc[index,'ID'] == 'D14':
        df_fatorx.at[index,'UF'] = 'PE'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CELTINS
    if df_fatorx.loc[index,'ID'] == 'D15':
        df_fatorx.at[index,'UF'] = 'TO'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEMAR
    if df_fatorx.loc[index,'ID'] == 'D16':
        df_fatorx.at[index,'UF'] = 'MA'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #EMT
    if df_fatorx.loc[index,'ID'] == 'D17':
        df_fatorx.at[index,'UF'] = 'MT'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEMIG-D
    if df_fatorx.loc[index,'ID'] == 'D18':
        df_fatorx.at[index,'UF'] = 'MG'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEPISA
    if df_fatorx.loc[index,'ID'] == 'D19':
        df_fatorx.at[index,'UF'] = 'PI'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CERON
    if df_fatorx.loc[index,'ID'] == 'D20':
        df_fatorx.at[index,'UF'] = 'RO'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CERR (CONFIMAR O PERIODO)
    if df_fatorx.loc[index,'ID'] == 'D21':
        df_fatorx.at[index,'UF'] = 'RR'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CFLO
    if df_fatorx.loc[index,'ID'] == 'D22':
        df_fatorx.at[index,'UF'] = 'PR'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CHESP
    if df_fatorx.loc[index,'ID'] == 'D23':
        df_fatorx.at[index,'UF'] = 'GO'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL JAGUARI
    if df_fatorx.loc[index,'ID'] == 'D24':
        df_fatorx.at[index,'UF'] = 'SP'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL MOCOCA
    if df_fatorx.loc[index,'ID'] == 'D25':
        df_fatorx.at[index,'UF'] = 'SP'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'        
    
    #CPFL Santa Cruz
    if df_fatorx.loc[index,'ID'] == 'D26':
        df_fatorx.at[index,'UF'] = 'SP'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CNEE
    if df_fatorx.loc[index,'ID'] == 'D27':
        df_fatorx.at[index,'UF'] = 'SP'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COCEL
    if df_fatorx.loc[index,'ID'] == 'D28':
        df_fatorx.at[index,'UF'] = 'PR'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COELBA
    if df_fatorx.loc[index,'ID'] == 'D29':
        df_fatorx.at[index,'UF'] = 'BA'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COELCE
    if df_fatorx.loc[index,'ID'] == 'D30':
        df_fatorx.at[index,'UF'] = 'CE'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #COOPERALIANÇA
    if df_fatorx.loc[index,'ID'] == 'D31':
        df_fatorx.at[index,'UF'] = 'SC'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COPEL
    if df_fatorx.loc[index,'ID'] == 'D32':
        df_fatorx.at[index,'UF'] = 'PR'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COSERN
    if df_fatorx.loc[index,'ID'] == 'D33':
        df_fatorx.at[index,'UF'] = 'RN'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Leste Paulista
    if df_fatorx.loc[index,'ID'] == 'D34':
        df_fatorx.at[index,'UF'] = 'SP'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Piratininga
    if df_fatorx.loc[index,'ID'] == 'D35':
        df_fatorx.at[index,'UF'] = 'SP'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CPFL Paulista
    if df_fatorx.loc[index,'ID'] == 'D36':
        df_fatorx.at[index,'UF'] = 'SP'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Sul Paulista
    if df_fatorx.loc[index,'ID'] == 'D37':
        df_fatorx.at[index,'UF'] = 'SP'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #DEMEI
    if df_fatorx.loc[index,'ID'] == 'D38':
        df_fatorx.at[index,'UF'] = 'RS'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #DME-PC
    if df_fatorx.loc[index,'ID'] == 'D39':
        df_fatorx.at[index,'UF'] = 'MG'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EBO
    if df_fatorx.loc[index,'ID'] == 'D40':
        df_fatorx.at[index,'UF'] = 'PB'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #EDEVP
    if df_fatorx.loc[index,'ID'] == 'D41':
        df_fatorx.at[index,'UF'] = 'SP'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EEB
    if df_fatorx.loc[index,'ID'] == 'D42':
        df_fatorx.at[index,'UF'] = 'SP'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EFLJC
    if df_fatorx.loc[index,'ID'] == 'D43':
        df_fatorx.at[index,'UF'] = 'SC'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EFLUL
    if df_fatorx.loc[index,'ID'] == 'D44':
        df_fatorx.at[index,'UF'] = 'SC'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ELEKTRO
    if df_fatorx.loc[index,'ID'] == 'D45':
        df_fatorx.at[index,'UF'] = 'SP'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELETROACRE
    if df_fatorx.loc[index,'ID'] == 'D46':
        df_fatorx.at[index,'UF'] = 'AC'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELETROCAR
    if df_fatorx.loc[index,'ID'] == 'D47':
        df_fatorx.at[index,'UF'] = 'RS'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ELETROPAULO
    if df_fatorx.loc[index,'ID'] == 'D48':
        df_fatorx.at[index,'UF'] = 'SP'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELFSM
    if df_fatorx.loc[index,'ID'] == 'D49':
        df_fatorx.at[index,'UF'] = 'ES'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EMG
    if df_fatorx.loc[index,'ID'] == 'D50':
        df_fatorx.at[index,'UF'] = 'MG'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EMS
    if df_fatorx.loc[index,'ID'] == 'D51':
        df_fatorx.at[index,'UF'] = 'MS'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ENF
    if df_fatorx.loc[index,'ID'] == 'D52':
        df_fatorx.at[index,'UF'] = 'RJ'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EPB
    if df_fatorx.loc[index,'ID'] == 'D53':
        df_fatorx.at[index,'UF'] = 'PB'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ESCELSA (CONFIRMAR PERIODO)
    if df_fatorx.loc[index,'ID'] == 'D54':
        df_fatorx.at[index,'UF'] = 'ES'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '3'
        
    #ESE
    if df_fatorx.loc[index,'ID'] == 'D55':
        df_fatorx.at[index,'UF'] = 'SE'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #FORCEL
    if df_fatorx.loc[index,'ID'] == 'D56':
        df_fatorx.at[index,'UF'] = 'PR'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #HIDROPAN
    if df_fatorx.loc[index,'ID'] == 'D57':
        df_fatorx.at[index,'UF'] = 'RS'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #IENERGIA
    if df_fatorx.loc[index,'ID'] == 'D58':
        df_fatorx.at[index,'UF'] = 'SC'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #JARI (CONFIRMAR PERIODO)
    if df_fatorx.loc[index,'ID'] == 'D59':
        df_fatorx.at[index,'UF'] = 'PA'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #LIGHT
    if df_fatorx.loc[index,'ID'] == 'D60':
        df_fatorx.at[index,'UF'] = 'RJ'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #MUX-Energia
    if df_fatorx.loc[index,'ID'] == 'D61':
        df_fatorx.at[index,'UF'] = 'RS'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
    
    #CPFL RGE
    if df_fatorx.loc[index,'ID'] == 'D62':
        df_fatorx.at[index,'UF'] = 'RS'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #SULGIPE
    if df_fatorx.loc[index,'ID'] == 'D63':
        df_fatorx.at[index,'UF'] = 'SE'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #UHENPAL
    if df_fatorx.loc[index,'ID'] == 'D64':
        df_fatorx.at[index,'UF'] = 'TO'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ESS
    if df_fatorx.loc[index,'ID'] == 'D65':
        df_fatorx.at[index,'UF'] = 'SP'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Santa Cruz
    if df_fatorx.loc[index,'ID'] == 'D66':
        df_fatorx.at[index,'UF'] = 'SP'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #RGE
    if df_fatorx.loc[index,'ID'] == 'D67':
        df_fatorx.at[index,'UF'] = 'RS'
        df_fatorx.at[index,'PERIODO_TARIFARIO'] = '5'
    

    return distribuidora



#%%Inserção dos dados
#Abre a SPARTA de cada arquivo
for arquivo in arquivos:   
    try:
        df_sparta_mercado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Mercado'
                                      ,header=7
                                      ,nrows=49
                                      ,usecols=[1,2,3,4,5,6,7])
        
        df_sparta_fatorx = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'VPB1'
                                      ,header=36
                                      ,nrows=7
                                      ,usecols=[4,5])
        
        df_sparta_capa = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA'
                                       ,header = 5
                                       ,nrows = 14
                                       ,usecols = [1,2])
    
        #Tentamos importar a aba 'VPB e Fator X' porque nem todas SPARTA possuem essa aba
    
        df_sparta_vpb = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'VPB e Fator X'
                                      ,header=31
                                      ,nrows=12
                                      ,usecols=[1,2])
         
        df_sparta_vpb1 = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'VPB e Fator X'
                                      ,header=123
                                      ,nrows=8
                                      ,usecols=[1,2])
        df_sparta_vpb = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'VPB e Fator X'
                                      ,header=31
                                      ,nrows=12
                                      ,usecols=[1,2])
        
        print('Leu o arquivo: ',arquivo)
        
        #Função para extração dos dados de cada SPARTA
        extrai_fatorx(df_sparta_fatorx,df_sparta_vpb,df_sparta_vpb1,df_sparta_mercado,df_fatorx,index)
        distribuidora(df_sparta_capa,df_fatorx,index)
        print('Extraiu o dado do arquivo: ',arquivo)
        print('Arquivos extraídos: ',round((index/len(arquivos)),2)*100,'%')
        index = index + 1 #Passa para o próximo indice para conseguir dados da proxima SPARTA
        
        
    except:
        print('Aba não disponível na SPARTA', arquivo) 
    
    
    
#%%Tratamento de dados 
#Remover dados duplicados e linhas nulas
df_fatorx = df_fatorx.drop_duplicates(subset = 'CHAVE',ignore_index = True)   
df_fatorx = df_fatorx.dropna(axis=0,how='all') 
    

#Limpeza e Tratamento dos dados
df_fatorx = df_fatorx.astype(str)
df_fatorx['PERIODO_TARIFARIO'] = df_fatorx['PERIODO_TARIFARIO'].astype(int)
df_fatorx['IGPM_PERCENT'] = df_fatorx['IGPM_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_fatorx['IPCA_PERCENT'] = df_fatorx['IPCA_PERCENT'].replace('nan','0').astype(float).replace('.',',')   
df_fatorx['COMPONENTE_PD_PERCENT'] = df_fatorx['COMPONENTE_PD_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_fatorx['COMPONENTE_T_PERCENT'] = df_fatorx['COMPONENTE_T_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_fatorx['COMPONENTE_Q_PERCENT'] = df_fatorx['COMPONENTE_Q_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_fatorx['FATOR_X_PERCENT'] = df_fatorx['FATOR_X_PERCENT'].replace('nan','0').astype(float).replace('.',',')
df_fatorx['IVI_X_PERCENT'] = df_fatorx['IVI_X_PERCENT'].replace('nan','0').astype(float).replace('.',',')

# Define a data de atualização dos dados
data = datetime.today().strftime('%d/%m/%Y')
df_fatorx['DATA_ATUALIZA'] = data

#%% Inserir dados no banco de dados

#Criar a lista para inserção no banco SQL com os dados da base editada
dados_list = df_fatorx.values.tolist()


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
        sql = '''INSERT INTO ''' + tabela_oracle +''' VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        cursor.executemany(sql, dados_list)
    except Exception as err:
        print('Erro no Insert:', err)
    else:
        print('Carga executada com sucesso!')
        connection.commit() #Caso seja executado com sucesso, esse comando salva a base de dados
    finally:
        cursor.close()
        connection.close()



