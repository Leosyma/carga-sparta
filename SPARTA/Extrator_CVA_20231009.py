# -*- coding: utf-8 -*-
"""
Created on Thu Feb  2 14:58:20 2023

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
# pasta = r"C:\Users\2018459\OneDrive - CPFL Energia S A\Área de Trabalho\RTP e RTA (concessionaria)\SPARTA"

#Arquivos
#Como os dados das empresas estão em arquivos separados, listar todos que serão carregados conforme layout abaixo.
# arquivo ='SPARTA_ ENEL-GO_2018 .xlsx'
arquivos = glob.glob(pasta)


index = 0 #Flag para inserção dos dados

#Nome da tabela Oracle onde será dada a carga
tabela_oracle = 'SPARTA_CVA'
ano_oracle = "'2023'"


#%% Montar o dataframe da estrutura da aba 'SPARTA_NUC'
#Defino o indice máximo que é igual o número de SPARTA
index_maximo = list(range(0,len(arquivos)))

#Criação da estrutura do tabela NUC
df_cva = pd.DataFrame(data = [], columns=['CHAVE','EVENTO_TARIFARIO','ANO','ID','DISTRIBUIDORA','UF','DATA','PERIODO_TARIFARIO','CONTRATO','5_DIA_UTIL_ANTERIOR_RS','CCC_5_DIA_UTIL_ANTERIOR_RS','CDE_5_DIA_UTIL_ANTERIOR_RS','CDE_COVID_5_DIA_UTIL_ANTERIOR_RS','REDE_BASICA_5_DIA_UTIL_ANTERIOR_RS','COMPRA_ENERGIA_5_DIA_UTIL_ANTERIOR_RS','CFURH_5_DIA_UTIL_ANTERIOR_RS','TRANSPORTE_ITAIPU_5_DIA_UTIL_ANTERIOR_RS','PROINFA_12_5_DIA_UTIL_ANTERIOR_RS','ESS_ERR_5_DIA_UTIL_ANTERIOR_RS','12_MESES_SUBSEQUENTES_RS','CCC_12_MESES_SUB_RS','CDE_12_MESES_SUB_RS','CDE_COVID_12_MESES_SUB_RS','REDE_BASICA_12_MESES_SUB_RS','COMPRA_ENERGIA_12_MESES_SUB_RS','CFURH_12_MESES_SUB_RS','TRANSPORTE_ITAIPU_12_MESES_SUB_RS','PROINFA_12_MESES_SUB_RS','ESS_ERR_12_MESES_SUB_RS','SALDO_COMPENSAR_RS','CCC_SALDO_COMPENSAR_RS','CDE_SALDO_COMPENSAR_RS','REDE_BASICA_SALDO_COMPENSAR_RS','COMPRA_ENERGIA_SALDO_COMPENSAR_RS','CFURH_12_SALDO_COMPENSAR_RS','TRANSPORTE_ITAIPU_SALDO_COMPENSAR_RS','PROINFA_12_SALDO_COMPENSAR_RS','ESS_ERR_SALDO_COMPENSAR_RS'],index=index_maximo) 

#Criação dataframe vazio
df_sparta_resultado = pd.DataFrame(data = [])
df_sparta_energia = pd.DataFrame(data=[])
df_sparta_capa = pd.DataFrame(data=[])
df_sparta_mercado = pd.DataFrame(data=[])
df_sparta_cva_layout1 = pd.DataFrame(data=[])
df_sparta_cva_layout2 = pd.DataFrame(data=[])
df_sparta_cva_2013a = pd.DataFrame(data=[])
df_sparta_cva_2013b = pd.DataFrame(data=[])


#df_sparta_resultado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Resultado'
 #                             ,header=12
  #                            ,nrows=35
   #                           ,usecols=[1,2,3])

#df_sparta_energia = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Energia'
 #                             ,header=3
  #                            ,nrows=35
   #                           ,usecols=[1,2,3,4,5,6,7,8])

#df_sparta_mercado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Mercado'
 #                             ,header=7
  #                            ,nrows=49
   #                           ,usecols=[1,2,3,4,5,6,7])


# # Layout para SPARTA recentes (2014 -> atualmente)  
# df_sparta_cva_layout1 = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CVA'
#                               ,header=6
#                               ,nrows=12
#                               ,usecols=[1,2,3,4])

# df_sparta_cva_layout2 = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CVA'
#                               ,header=21
#                               ,nrows=12
#                               ,usecols=[1,2,3,4,5,6])

# #Layout para SPARTA de 2013
# df_sparta_cva_2013a = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CVA'
#                               ,header=6
#                               ,nrows=23
#                               ,usecols=[1,2,3,4,5])

# df_sparta_cva_2013b = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CVA'
#                               ,header=32
#                               ,nrows=12
#                               ,usecols=[1,2,3,4])



#%%Extração dos resultados
#Funções para extrair dados das SPARTA recentes
def determina_contrato(df_cva,df_sparta_mercado,index):
    #Se essa 'string' estiver presente nessa aba o contrato é NOVO
    if 'Percentual RI' in df_sparta_mercado.iloc[:,:].values:
          df_cva.at[index,'CONTRATO'] = 'NOVO' 
    #Caso contrário o contrato é ANTIGO      
    else:
          df_cva.at[index,'CONTRATO'] = 'ANTIGO'


#SPARTA 2014 -> atualmente
#Função para determinar a coluna do dado '5º Dia Útil Anterior' e '12 Meses Subsequentes'
def determina_coluna_5dia_12meses(df_sparta_cva_layout1,colunas_layout1):
    #Loop que passa por todas as colunas da linha 0 
    for coluna in colunas_layout1:
        # Determina a posição da coluna onde é o '5º Dia Útil Anterior'
        if '5' in df_sparta_cva_layout1.iloc[0,coluna]:
            global coluna_5_dia  #Definimos a variavel como global para que seja usada fora da função
            coluna_5_dia = coluna  #Salva a posição da coluna desejada em outra variavel    
        
        # Determina a posição da coluna onde é o '12 Meses Subsequentes'
        if '12' in df_sparta_cva_layout1.iloc[0,coluna]:
            global coluna_12_meses  #Definimos a variavel como global para que seja usada fora da função
            coluna_12_meses = coluna  #Salva a posição da coluna desejada em outra variavel
            

#Função para determinar a coluna do dado 'Saldo a Compensar'
def determina_coluna_saldo(df_sparta_cva_layout2,colunas_layout2):
    #Tratamento específico para a SPARTA 2018 da CELG-D - D12, pois está deslocada 1 linha para baixo
    #Definimos o ano da SPARTA
    ano = df_sparta_capa.iloc[8,1].strftime('%Y')
    global coluna_saldo  #Definimos a variavel como global para que seja usada fora da função
    if ('D12' in df_sparta_capa.iloc[:,:].values and ano == '2018'):
        #Loop que passa por todas as colunas da linha 1
        for coluna in colunas_layout2:
            # Determina a posição da coluna onde é o 'Saldo a Compensar'
            if 'Saldo' in df_sparta_cva_layout2.iloc[1,coluna]: 
                coluna_saldo = coluna  #Salva a posição da coluna desejada em outra variavel
    
    #Restante das SPARTAS
    else:
        #Loop que passa por todas as colunas da linha 0
        for coluna in colunas_layout2:
            # Determina a posição da coluna onde é o 'Saldo a Compensar'
            if 'Saldo' in df_sparta_cva_layout2.iloc[0,coluna]:
                coluna_saldo = coluna  #Salva a posição da coluna desejada em outra variavel
                

#SPARTA 2013
#Função para determinar a coluna do dado '5º Dia Útil Anterior' e '12 Meses Subsequentes'
def determina_coluna_5dia_12meses_2013(df_sparta_cva_2013a,colunas_2013a):
    #Loop que passa por todas as colunas da linha 0 
    for coluna in colunas_2013a:
        # Determina a posição da coluna onde é o '5º Dia Útil Anterior'
        if '5' in df_sparta_cva_2013a.iloc[0,coluna]:
            global coluna_5_dia_2013  #Definimos a variavel como global para que seja usada fora da função
            coluna_5_dia_2013 = coluna  #Salva a posição da coluna desejada em outra variavel
            
        # Determina a posição da coluna onde é o '12 Meses Subsequentes'
        if '12' in df_sparta_cva_2013a.iloc[0,coluna]:
            global coluna_12_meses_2013  #Definimos a variavel como global para que seja usada fora da função
            coluna_12_meses_2013 = coluna  #Salva a posição da coluna desejada em outra variavel
            

#Função para determinar a coluna do dado 'Saldo a Compensar'
def determina_coluna_saldo_2013(df_sparta_cva_2013b,colunas_2013b):
    #Loop que passa por todas as colunas da linha 0
    for coluna in colunas_2013b:
        if 'Saldo' in df_sparta_cva_2013b.iloc[0,coluna]:
            global coluna_saldo_2013  #Definimos a variavel como global para que seja usada fora da função
            coluna_saldo_2013 = coluna  #Salva a posição da coluna desejada em outra variavel
            

# FUNÇÕES PARA A SPARTA 2014 -> ATUALMENTE
  
def extrai_cva_5_dia_util(df_sparta_cva_layout1,df_cva,index,coluna_5_dia):
    #Loop para passar por todas as linhas da coluna 0 do dataframe df_sparta_cva_layout1
    for linha in linhas_layout1:
        #Comparo cada linha com o valor desejado na coluna 0 e busco o valor 2 colunas para frente (5º Dia Útil Anterior)
        if 'Total' in df_sparta_cva_layout1.iloc[linha,0]:
            df_cva.at[index,'5_DIA_UTIL_ANTERIOR_RS'] = df_sparta_cva_layout1.iloc[linha,coluna_5_dia]
        elif 'CCC' in df_sparta_cva_layout1.iloc[linha,0]:
            df_cva.at[index,'CCC_5_DIA_UTIL_ANTERIOR_RS'] = df_sparta_cva_layout1.iloc[linha,coluna_5_dia]
        elif 'CDE' == df_sparta_cva_layout1.iloc[linha,0]:
            df_cva.at[index,'CDE_5_DIA_UTIL_ANTERIOR_RS'] = df_sparta_cva_layout1.iloc[linha,coluna_5_dia]
        elif 'CDE Covid' == df_sparta_cva_layout1.iloc[linha,0]:
            df_cva.at[index,'CDE_COVID_5_DIA_UTIL_ANTERIOR_RS'] = df_sparta_cva_layout1.iloc[linha,coluna_5_dia]
        elif 'Rede' in df_sparta_cva_layout1.iloc[linha,0]:
            df_cva.at[index,'REDE_BASICA_5_DIA_UTIL_ANTERIOR_RS'] = df_sparta_cva_layout1.iloc[linha,coluna_5_dia]
        elif 'Compra' in df_sparta_cva_layout1.iloc[linha,0]:
            df_cva.at[index,'COMPRA_ENERGIA_5_DIA_UTIL_ANTERIOR_RS'] = df_sparta_cva_layout1.iloc[linha,coluna_5_dia]
        elif 'CFURH' in df_sparta_cva_layout1.iloc[linha,0]:
            df_cva.at[index,'CFURH_5_DIA_UTIL_ANTERIOR_RS'] = df_sparta_cva_layout1.iloc[linha,coluna_5_dia]
        elif 'Transporte' in df_sparta_cva_layout1.iloc[linha,0]:
            df_cva.at[index,'TRANSPORTE_ITAIPU_5_DIA_UTIL_ANTERIOR_RS'] = df_sparta_cva_layout1.iloc[linha,coluna_5_dia]
        elif 'Proinfa' in df_sparta_cva_layout1.iloc[linha,0]:
            df_cva.at[index,'PROINFA_12_5_DIA_UTIL_ANTERIOR_RS'] = df_sparta_cva_layout1.iloc[linha,coluna_5_dia]
        elif 'ESS' in df_sparta_cva_layout1.iloc[linha,0]:
            df_cva.at[index,'ESS_ERR_5_DIA_UTIL_ANTERIOR_RS'] = df_sparta_cva_layout1.iloc[linha,coluna_5_dia]

         
def extrai_cva_12_meses(df_sparta_cva_layout1,df_cva,index,coluna_12_meses):
    #Loop para passar por todas as linhas da coluna 0 do dataframe df_sparta_cva_layout1
    for linha in linhas_layout1:
        #Comparo cada linha com o valor desejado e busco o valor 3 colunas para frente (12 Meses Subsequentes)
        if 'Total' in df_sparta_cva_layout1.iloc[linha,0]:
            df_cva.at[index,'12_MESES_SUBSEQUENTES_RS'] = df_sparta_cva_layout1.iloc[linha,coluna_12_meses]
        elif 'CCC' in df_sparta_cva_layout1.iloc[linha,0]:
            df_cva.at[index,'CCC_12_MESES_SUB_RS'] = df_sparta_cva_layout1.iloc[linha,coluna_12_meses]
        elif 'CDE' == df_sparta_cva_layout1.iloc[linha,0]:
            df_cva.at[index,'CDE_12_MESES_SUB_RS'] = df_sparta_cva_layout1.iloc[linha,coluna_12_meses]
        elif 'CDE Covid' == df_sparta_cva_layout1.iloc[linha,0]:
            df_cva.at[index,'CDE_COVID_12_MESES_SUB_RS'] = df_sparta_cva_layout1.iloc[linha,coluna_12_meses]
        elif 'Rede' in df_sparta_cva_layout1.iloc[linha,0]:
            df_cva.at[index,'REDE_BASICA_12_MESES_SUB_RS'] = df_sparta_cva_layout1.iloc[linha,coluna_12_meses]
        elif 'Compra' in df_sparta_cva_layout1.iloc[linha,0]:
            df_cva.at[index,'COMPRA_ENERGIA_12_MESES_SUB_RS'] = df_sparta_cva_layout1.iloc[linha,coluna_12_meses]
        elif 'CFURH' in df_sparta_cva_layout1.iloc[linha,0]:
            df_cva.at[index,'CFURH_12_MESES_SUB_RS'] = df_sparta_cva_layout1.iloc[linha,coluna_12_meses]
        elif 'Transporte' in df_sparta_cva_layout1.iloc[linha,0]:
            df_cva.at[index,'TRANSPORTE_ITAIPU_12_MESES_SUB_RS'] = df_sparta_cva_layout1.iloc[linha,coluna_12_meses]
        elif 'Proinfa' in df_sparta_cva_layout1.iloc[linha,0]:
            df_cva.at[index,'PROINFA_12_MESES_SUB_RS'] = df_sparta_cva_layout1.iloc[linha,coluna_12_meses]
        elif 'ESS' in df_sparta_cva_layout1.iloc[linha,0]:
            df_cva.at[index,'ESS_ERR_12_MESES_SUB_RS'] = df_sparta_cva_layout1.iloc[linha,coluna_12_meses]


def extrai_cva_saldo_compensar(df_sparta_cva_layout2,df_cva,index,coluna_saldo):  
    #Loop para passar por todas as linhas da coluna 0 do dataframe df_sparta_cva_layout2
    for linha in linhas_layout2:
        #Comparo cada linha com o valor desejado e busco o valor 5 colunas para frente (CVA Saldo a Compensar)
        if 'Total' in df_sparta_cva_layout2.iloc[linha,0]:
            df_cva.at[index,'SALDO_COMPENSAR_RS'] = df_sparta_cva_layout2.iloc[linha,coluna_saldo]
        elif 'CCC' in df_sparta_cva_layout2.iloc[linha,0]:
            df_cva.at[index,'CCC_SALDO_COMPENSAR_RS'] = df_sparta_cva_layout2.iloc[linha,coluna_saldo]
        elif 'CDE' == df_sparta_cva_layout2.iloc[linha,0]:
            df_cva.at[index,'CDE_SALDO_COMPENSAR_RS'] = df_sparta_cva_layout2.iloc[linha,coluna_saldo]
        elif 'Rede' in df_sparta_cva_layout2.iloc[linha,0]:
            df_cva.at[index,'REDE_BASICA_SALDO_COMPENSAR_RS'] = df_sparta_cva_layout2.iloc[linha,coluna_saldo]
        elif 'Compra' in df_sparta_cva_layout2.iloc[linha,0]:
            df_cva.at[index,'COMPRA_ENERGIA_SALDO_COMPENSAR_RS'] = df_sparta_cva_layout2.iloc[linha,coluna_saldo]
        elif 'CFURH' in df_sparta_cva_layout2.iloc[linha,0]:
            df_cva.at[index,'CFURH_12_SALDO_COMPENSAR_RS'] = df_sparta_cva_layout2.iloc[linha,coluna_saldo]
        elif 'Transporte' in df_sparta_cva_layout2.iloc[linha,0]:
            df_cva.at[index,'TRANSPORTE_ITAIPU_SALDO_COMPENSAR_RS'] = df_sparta_cva_layout2.iloc[linha,coluna_saldo]
        elif 'Proinfa' in df_sparta_cva_layout2.iloc[linha,0]:
            df_cva.at[index,'PROINFA_12_SALDO_COMPENSAR_RS'] = df_sparta_cva_layout2.iloc[linha,coluna_saldo]
        elif 'ESS' in df_sparta_cva_layout2.iloc[linha,0]:
            df_cva.at[index,'ESS_ERR_SALDO_COMPENSAR_RS'] = df_sparta_cva_layout2.iloc[linha,coluna_saldo]


# FUNÇÕES PARA A SPARTA DE 2013 (LAYOUT e NOME DO DADO DIFERENTE)

#Funções para extrair SPARTA de 2013
def extrai_cva_5_dia_util_2013(df_sparta_cva_2013a,df_cva,index,coluna_5_dia_2013):
    #Loop para passar por todas as linhas da coluna 0 do dataframe df_sparta_cva_2013a
    for linha in linhas_2013a:
        #Comparo cada linha com o valor desejado na coluna 0 e busco o valor 3 colunas para frente (5º Dia Útil Anterior)
        if 'TOTAL' in df_sparta_cva_2013a.iloc[linha,0]:
            df_cva.at[index,'5_DIA_UTIL_ANTERIOR_RS'] = df_sparta_cva_2013a.iloc[linha,coluna_5_dia_2013]
        elif 'CCC' in df_sparta_cva_2013a.iloc[linha,0]:
            df_cva.at[index,'CCC_5_DIA_UTIL_ANTERIOR_RS'] = df_sparta_cva_2013a.iloc[linha,coluna_5_dia_2013]
        elif 'CDE' in df_sparta_cva_2013a.iloc[linha,0]:
            df_cva.at[index,'CDE_5_DIA_UTIL_ANTERIOR_RS'] = df_sparta_cva_2013a.iloc[linha,coluna_5_dia_2013]
        elif 'REDE' in df_sparta_cva_2013a.iloc[linha,0]:
            df_cva.at[index,'REDE_BASICA_5_DIA_UTIL_ANTERIOR_RS'] = df_sparta_cva_2013a.iloc[linha,coluna_5_dia_2013]
        elif 'COMPRA' in df_sparta_cva_2013a.iloc[linha,0]:
            df_cva.at[index,'COMPRA_ENERGIA_5_DIA_UTIL_ANTERIOR_RS'] = df_sparta_cva_2013a.iloc[linha,coluna_5_dia_2013]
        elif 'FINANCEIRA' in df_sparta_cva_2013a.iloc[linha,0]:
            df_cva.at[index,'CFURH_5_DIA_UTIL_ANTERIOR_RS'] = df_sparta_cva_2013a.iloc[linha,coluna_5_dia_2013]
        elif 'TRANSPORTE' in df_sparta_cva_2013a.iloc[linha,0]:
            df_cva.at[index,'TRANSPORTE_ITAIPU_5_DIA_UTIL_ANTERIOR_RS'] = df_sparta_cva_2013a.iloc[linha,coluna_5_dia_2013]
        elif 'PROINFA' in df_sparta_cva_2013a.iloc[linha,0]:
            df_cva.at[index,'PROINFA_12_5_DIA_UTIL_ANTERIOR_RS'] = df_sparta_cva_2013a.iloc[linha,coluna_5_dia_2013]
        elif 'ENCARGOS' in df_sparta_cva_2013a.iloc[linha,0]:
            df_cva.at[index,'ESS_ERR_5_DIA_UTIL_ANTERIOR_RS'] = df_sparta_cva_2013a.iloc[linha,coluna_5_dia_2013]
 
         
def extrai_cva_12_meses_2013(df_sparta_cva_2013a,df_cva,index,coluna_12_meses_2013):
    #Loop para passar por todas as linhas da coluna 0 do dataframe df_sparta_cva_2013a
    for linha in linhas_2013a:
        #Comparo cada linha com o valor desejado e busco o valor 4 colunas para frente (12 Meses Subsequentes)
        if 'TOTAL' in df_sparta_cva_2013a.iloc[linha,0]:
            df_cva.at[index,'12_MESES_SUBSEQUENTES_RS'] = df_sparta_cva_2013a.iloc[linha,coluna_12_meses_2013]
        elif 'CCC' in df_sparta_cva_2013a.iloc[linha,0]:
            df_cva.at[index,'CCC_12_MESES_SUB_RS'] = df_sparta_cva_2013a.iloc[linha,coluna_12_meses_2013]
        elif 'CDE' in df_sparta_cva_2013a.iloc[linha,0]:
            df_cva.at[index,'CDE_12_MESES_SUB_RS'] = df_sparta_cva_2013a.iloc[linha,coluna_12_meses_2013]
        elif 'REDE' in df_sparta_cva_2013a.iloc[linha,0]:
            df_cva.at[index,'REDE_BASICA_12_MESES_SUB_RS'] = df_sparta_cva_2013a.iloc[linha,coluna_12_meses_2013]
        elif 'COMPRA' in df_sparta_cva_2013a.iloc[linha,0]:
            df_cva.at[index,'COMPRA_ENERGIA_12_MESES_SUB_RS'] = df_sparta_cva_2013a.iloc[linha,coluna_12_meses_2013]
        elif 'FINANCEIRA' in df_sparta_cva_2013a.iloc[linha,0]:
            df_cva.at[index,'CFURH_12_MESES_SUB_RS'] = df_sparta_cva_2013a.iloc[linha,coluna_12_meses_2013]
        elif 'TRANSPORTE' in df_sparta_cva_2013a.iloc[linha,0]:
            df_cva.at[index,'TRANSPORTE_ITAIPU_12_MESES_SUB_RS'] = df_sparta_cva_2013a.iloc[linha,coluna_12_meses_2013]
        elif 'PROINFA' in df_sparta_cva_2013a.iloc[linha,0]:
            df_cva.at[index,'PROINFA_12_MESES_SUB_RS'] = df_sparta_cva_2013a.iloc[linha,coluna_12_meses_2013]
        elif 'ENCARGOS' in df_sparta_cva_2013a.iloc[linha,0]:
            df_cva.at[index,'ESS_ERR_12_MESES_SUB_RS'] = df_sparta_cva_2013a.iloc[linha,coluna_12_meses_2013]


def extrai_cva_saldo_compensar_2013(df_sparta_cva_2013b,df_cva,index,coluna_saldo_2013):
    #Loop para passar por todas as linhas da coluna 0 do dataframe df_sparta_cva_2013b
    for linha in linhas_2013b:
        #Comparo cada linha com o valor desejado e busco o valor 3 colunas para frente (CVA Saldo a Compensar)
        if 'TOTAL' in df_sparta_cva_2013b.iloc[linha,0]:
            df_cva.at[index,'SALDO_COMPENSAR_RS'] = df_sparta_cva_2013b.iloc[linha,coluna_saldo_2013]
        elif 'CCC' in df_sparta_cva_2013b.iloc[linha,0]:
            df_cva.at[index,'CCC_SALDO_COMPENSAR_RS'] = df_sparta_cva_2013b.iloc[linha,coluna_saldo_2013]
        elif 'CDE' in df_sparta_cva_2013b.iloc[linha,0]:
            df_cva.at[index,'CDE_SALDO_COMPENSAR_RS'] = df_sparta_cva_2013b.iloc[linha,coluna_saldo_2013]
        elif ('REDE' in df_sparta_cva_2013b.iloc[linha,0]) or ('RB' in df_sparta_cva_2013b.iloc[linha,0]):
            df_cva.at[index,'REDE_BASICA_SALDO_COMPENSAR_RS'] = df_sparta_cva_2013b.iloc[linha,coluna_saldo_2013]
        elif ('ENERGIA' in df_sparta_cva_2013b.iloc[linha,0]):
            df_cva.at[index,'COMPRA_ENERGIA_SALDO_COMPENSAR_RS'] = df_sparta_cva_2013b.iloc[linha,coluna_saldo_2013]
        elif ('FINANCEIRA' in df_sparta_cva_2013b.iloc[linha,0]) or ('CFURH' in df_sparta_cva_2013b.iloc[linha,0]):
            df_cva.at[index,'CFURH_12_SALDO_COMPENSAR_RS'] = df_sparta_cva_2013b.iloc[linha,coluna_saldo_2013]
        elif 'TRANS' in df_sparta_cva_2013b.iloc[linha,0]:
            df_cva.at[index,'TRANSPORTE_ITAIPU_SALDO_COMPENSAR_RS'] = df_sparta_cva_2013b.iloc[linha,coluna_saldo_2013]
        elif 'PROINFA' in df_sparta_cva_2013b.iloc[linha,0]:
            df_cva.at[index,'PROINFA_12_SALDO_COMPENSAR_RS'] = df_sparta_cva_2013b.iloc[linha,coluna_saldo_2013]
        elif ('ENCARGOS' in df_sparta_cva_2013b.iloc[linha,0]) or ('ESS' in df_sparta_cva_2013b.iloc[linha,0]):
            df_cva.at[index,'ESS_ERR_SALDO_COMPENSAR_RS'] = df_sparta_cva_2013b.iloc[linha,coluna_saldo_2013]   
 
    
def distribuidora(df_cva,df_sparta_capa,index):
    #Determina o ANO da SPARTA
    df_cva.at[index,'ANO'] = df_sparta_capa.iloc[8,1].strftime('%Y')
    
    #Determina o ID da Distribuidora
    df_cva.at[index,'ID'] = df_sparta_capa.iloc[1,1]
    
    #Determina o NOME da Distribuidora
    df_cva.at[index,'DISTRIBUIDORA'] = df_sparta_capa.iloc[0,0].upper()
    
    #Determina a DATA da SPARTA
    df_cva.at[index,'DATA'] = df_sparta_capa.iloc[8,1].strftime('%Y-%m-%d')
    
    #Determina o EVENTO TARIFARIO
    if 'Reajuste' in df_sparta_capa.iloc[0,1]:
        df_cva.at[index,'EVENTO_TARIFARIO'] = 'RTA'
    elif 'Revisão' in df_sparta_capa.iloc[0,1]:
        df_cva.at[index,'EVENTO_TARIFARIO'] = 'RTP'
    else:
        df_cva.at[index,'EVENTO_TARIFARIO'] = 'RTE'
        
    #Determina a CHAVE
    df_cva.at[index,'CHAVE'] = df_cva.loc[index,'EVENTO_TARIFARIO']+df_cva.loc[index,'ANO']+df_cva.loc[index,'ID']
    
    #Determina UF e PERIODO TARIFARIO
    #AES SUL
    if df_cva.loc[index,'ID'] == 'D01':
        df_cva.at[index,'UF'] = 'RS'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #AME
    if df_cva.loc[index,'ID'] == 'D02':
        df_cva.at[index,'UF'] = 'AM'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #AMPLA
    if df_cva.loc[index,'ID'] == 'D03':
        df_cva.at[index,'UF'] = 'RJ'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #BANDEIRANTE
    if df_cva.loc[index,'ID'] == 'D04':
        df_cva.at[index,'UF'] = 'SP'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #BOA VISTA
    if df_cva.loc[index,'ID'] == 'D05':
        df_cva.at[index,'UF'] = 'RR'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CAIUA
    if df_cva.loc[index,'ID'] == 'D06':
        df_cva.at[index,'UF'] = 'SP'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEA
    if df_cva.loc[index,'ID'] == 'D07':
        df_cva.at[index,'UF'] = 'AP'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CEAL
    if df_cva.loc[index,'ID'] == 'D08':
        df_cva.at[index,'UF'] = 'AL'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CEB
    if df_cva.loc[index,'ID'] == 'D09':
        df_cva.at[index,'UF'] = 'DF'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '4'
    
    #CEEE
    if df_cva.loc[index,'ID'] == 'D10':
        df_cva.at[index,'UF'] = 'RS'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
      
    #CELESC
    if df_cva.loc[index,'ID'] == 'D11':
        df_cva.at[index,'UF'] = 'SC'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CELG
    if df_cva.loc[index,'ID'] == 'D12':
        df_cva.at[index,'UF'] = 'GO'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CELPA
    if df_cva.loc[index,'ID'] == 'D13':
        df_cva.at[index,'UF'] = 'PA'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CELPE
    if df_cva.loc[index,'ID'] == 'D14':
        df_cva.at[index,'UF'] = 'PE'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CELTINS
    if df_cva.loc[index,'ID'] == 'D15':
        df_cva.at[index,'UF'] = 'TO'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEMAR
    if df_cva.loc[index,'ID'] == 'D16':
        df_cva.at[index,'UF'] = 'MA'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #EMT
    if df_cva.loc[index,'ID'] == 'D17':
        df_cva.at[index,'UF'] = 'MT'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEMIG-D
    if df_cva.loc[index,'ID'] == 'D18':
        df_cva.at[index,'UF'] = 'MG'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEPISA
    if df_cva.loc[index,'ID'] == 'D19':
        df_cva.at[index,'UF'] = 'PI'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CERON
    if df_cva.loc[index,'ID'] == 'D20':
        df_cva.at[index,'UF'] = 'RO'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CERR (CONFIMAR O PERIODO)
    if df_cva.loc[index,'ID'] == 'D21':
        df_cva.at[index,'UF'] = 'RR'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CFLO
    if df_cva.loc[index,'ID'] == 'D22':
        df_cva.at[index,'UF'] = 'PR'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CHESP
    if df_cva.loc[index,'ID'] == 'D23':
        df_cva.at[index,'UF'] = 'GO'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL JAGUARI
    if df_cva.loc[index,'ID'] == 'D24':
        df_cva.at[index,'UF'] = 'SP'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL MOCOCA
    if df_cva.loc[index,'ID'] == 'D25':
        df_cva.at[index,'UF'] = 'SP'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'        
    
    #CPFL Santa Cruz
    if df_cva.loc[index,'ID'] == 'D26':
        df_cva.at[index,'UF'] = 'SP'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CNEE
    if df_cva.loc[index,'ID'] == 'D27':
        df_cva.at[index,'UF'] = 'SP'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COCEL
    if df_cva.loc[index,'ID'] == 'D28':
        df_cva.at[index,'UF'] = 'PR'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COELBA
    if df_cva.loc[index,'ID'] == 'D29':
        df_cva.at[index,'UF'] = 'BA'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COELCE
    if df_cva.loc[index,'ID'] == 'D30':
        df_cva.at[index,'UF'] = 'CE'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #COOPERALIANÇA
    if df_cva.loc[index,'ID'] == 'D31':
        df_cva.at[index,'UF'] = 'SC'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COPEL
    if df_cva.loc[index,'ID'] == 'D32':
        df_cva.at[index,'UF'] = 'PR'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COSERN
    if df_cva.loc[index,'ID'] == 'D33':
        df_cva.at[index,'UF'] = 'RN'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Leste Paulista
    if df_cva.loc[index,'ID'] == 'D34':
        df_cva.at[index,'UF'] = 'SP'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Piratininga
    if df_cva.loc[index,'ID'] == 'D35':
        df_cva.at[index,'UF'] = 'SP'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CPFL Paulista
    if df_cva.loc[index,'ID'] == 'D36':
        df_cva.at[index,'UF'] = 'SP'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Sul Paulista
    if df_cva.loc[index,'ID'] == 'D37':
        df_cva.at[index,'UF'] = 'SP'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #DEMEI
    if df_cva.loc[index,'ID'] == 'D38':
        df_cva.at[index,'UF'] = 'RS'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #DME-PC
    if df_cva.loc[index,'ID'] == 'D39':
        df_cva.at[index,'UF'] = 'MG'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EBO
    if df_cva.loc[index,'ID'] == 'D40':
        df_cva.at[index,'UF'] = 'PB'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #EDEVP
    if df_cva.loc[index,'ID'] == 'D41':
        df_cva.at[index,'UF'] = 'SP'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EEB
    if df_cva.loc[index,'ID'] == 'D42':
        df_cva.at[index,'UF'] = 'SP'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EFLJC
    if df_cva.loc[index,'ID'] == 'D43':
        df_cva.at[index,'UF'] = 'SC'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EFLUL
    if df_cva.loc[index,'ID'] == 'D44':
        df_cva.at[index,'UF'] = 'SC'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ELEKTRO
    if df_cva.loc[index,'ID'] == 'D45':
        df_cva.at[index,'UF'] = 'SP'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELETROACRE
    if df_cva.loc[index,'ID'] == 'D46':
        df_cva.at[index,'UF'] = 'AC'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELETROCAR
    if df_cva.loc[index,'ID'] == 'D47':
        df_cva.at[index,'UF'] = 'RS'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ELETROPAULO
    if df_cva.loc[index,'ID'] == 'D48':
        df_cva.at[index,'UF'] = 'SP'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELFSM
    if df_cva.loc[index,'ID'] == 'D49':
        df_cva.at[index,'UF'] = 'ES'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EMG
    if df_cva.loc[index,'ID'] == 'D50':
        df_cva.at[index,'UF'] = 'MG'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EMS
    if df_cva.loc[index,'ID'] == 'D51':
        df_cva.at[index,'UF'] = 'MS'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ENF
    if df_cva.loc[index,'ID'] == 'D52':
        df_cva.at[index,'UF'] = 'RJ'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EPB
    if df_cva.loc[index,'ID'] == 'D53':
        df_cva.at[index,'UF'] = 'PB'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ESCELSA (CONFIRMAR PERIODO)
    if df_cva.loc[index,'ID'] == 'D54':
        df_cva.at[index,'UF'] = 'ES'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '3'
        
    #ESE
    if df_cva.loc[index,'ID'] == 'D55':
        df_cva.at[index,'UF'] = 'SE'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #FORCEL
    if df_cva.loc[index,'ID'] == 'D56':
        df_cva.at[index,'UF'] = 'PR'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #HIDROPAN
    if df_cva.loc[index,'ID'] == 'D57':
        df_cva.at[index,'UF'] = 'RS'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #IENERGIA
    if df_cva.loc[index,'ID'] == 'D58':
        df_cva.at[index,'UF'] = 'SC'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #JARI (CONFIRMAR PERIODO)
    if df_cva.loc[index,'ID'] == 'D59':
        df_cva.at[index,'UF'] = 'PA'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #LIGHT
    if df_cva.loc[index,'ID'] == 'D60':
        df_cva.at[index,'UF'] = 'RJ'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #MUX-Energia
    if df_cva.loc[index,'ID'] == 'D61':
        df_cva.at[index,'UF'] = 'RS'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
    
    #CPFL RGE
    if df_cva.loc[index,'ID'] == 'D62':
        df_cva.at[index,'UF'] = 'RS'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #SULGIPE
    if df_cva.loc[index,'ID'] == 'D63':
        df_cva.at[index,'UF'] = 'SE'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #UHENPAL
    if df_cva.loc[index,'ID'] == 'D64':
        df_cva.at[index,'UF'] = 'TO'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ESS
    if df_cva.loc[index,'ID'] == 'D65':
        df_cva.at[index,'UF'] = 'SP'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Santa Cruz
    if df_cva.loc[index,'ID'] == 'D66':
        df_cva.at[index,'UF'] = 'SP'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #RGE
    if df_cva.loc[index,'ID'] == 'D67':
        df_cva.at[index,'UF'] = 'RS'
        df_cva.at[index,'PERIODO_TARIFARIO'] = '5'


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
                              
        #Layout para SPARTA recentes (2014 -> atualmente) 
        #Dataframe para extrair dados do '5 Dia Util Anterior' e '12 Meses Subsequentes'
        df_sparta_cva_layout1 = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CVA'
                                      ,header=6
                                      ,nrows=12
                                      ,usecols=[1,2,3,4])
        
        #Dataframe para extrair dados do 'Saldo a Compensar'
        df_sparta_cva_layout2 = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CVA'
                                      ,header=21
                                      ,nrows=12
                                      ,usecols=[1,2,3,4,5,6])
    
        #Layout para SPARTA de 2013
        #Dataframe para extrair dados do '5 Dia Util Anterior' e '12 Meses Subsequentes'
        df_sparta_cva_2013a = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CVA'
                                      ,header=6
                                      ,nrows=23
                                      ,usecols=[1,2,3,4,5])
        
        #Dataframe para extrair dados do 'Saldo a Compensar'
        df_sparta_cva_2013b = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CVA'
                                      ,header=31
                                      ,nrows=12
                                      ,usecols=[1,2,3,4])  
        
        print('Leu o arquivo: ',arquivo)

        
        #Mudamos o formato do dado para 'string', pois não conseguimos comparar strings com valores vazios
        df_sparta_cva_layout1 = df_sparta_cva_layout1.astype('str')
        df_sparta_cva_layout2 = df_sparta_cva_layout2.astype('str')
        df_sparta_cva_2013a = df_sparta_cva_2013a.astype('str')
        df_sparta_cva_2013b = df_sparta_cva_2013b.astype('str')

        
        #Função para extração dos dados de cada SPARTA
        determina_contrato(df_cva,df_sparta_mercado,index)
        distribuidora(df_cva,df_sparta_capa,index)
        
        
        #Definimos o range de linhas dos dataframes para que seja possível fazer o loop
        #Definimos as flags das linhas como 0, pois podemos ter dataframes sem linhas
        indice_linhas_2013a = 0
        indice_linhas_2013b = 0
        indice_linhas_layout1 = 0
        
        #SPARTA 2013
        if df_sparta_cva_2013a.iloc[0,0] == 'DESCRIÇÃO CVA':
            #Se a SPARTA for 2013 usamos esse range de linhas para rodar o loop
            indice_linhas_2013a = int(df_sparta_cva_2013a.index.max()+1)
            indice_linhas_2013b = int(df_sparta_cva_2013b.index.max()+1)
            linhas_2013a = range(indice_linhas_2013a)
            linhas_2013b = range(indice_linhas_2013b)
            colunas_2013a = range(len(df_sparta_cva_2013a.columns))
            colunas_2013b = range(len(df_sparta_cva_2013b.columns))
            
            #Rodamos a função para extrair os dados das SPARTA usando o range adequado
            determina_coluna_5dia_12meses_2013(df_sparta_cva_2013a,colunas_2013a)
            determina_coluna_saldo_2013(df_sparta_cva_2013b,colunas_2013b)
            extrai_cva_5_dia_util_2013(df_sparta_cva_2013a,df_cva,index,coluna_5_dia_2013)
            extrai_cva_12_meses_2013(df_sparta_cva_2013a,df_cva,index,coluna_12_meses_2013)
            extrai_cva_saldo_compensar_2013(df_sparta_cva_2013b,df_cva,index,coluna_saldo_2013)
            
            
        #SPARTA recentes (2014 -> atualmente)
        else:
            #Se a SPARTA for depois de 2013 usamos esse range de linhas para rodar o loop
            indice_linhas_layout1 = int(df_sparta_cva_layout1.index.max()+1)
            indice_linhas_layout2 = int(df_sparta_cva_layout2.index.max()+1)
            linhas_layout1 = range(indice_linhas_layout1)
            linhas_layout2 = range(indice_linhas_layout2)
            colunas_layout1 = range(len(df_sparta_cva_layout1.columns))
            colunas_layout2 = range(len(df_sparta_cva_layout2.columns))
            
            #Rodamos a função para extrair os dados das SPARTA usando o range adequado
            determina_coluna_5dia_12meses(df_sparta_cva_layout1,colunas_layout1)
            determina_coluna_saldo(df_sparta_cva_layout2,colunas_layout2)
            extrai_cva_5_dia_util(df_sparta_cva_layout1,df_cva,index,coluna_5_dia)
            extrai_cva_12_meses(df_sparta_cva_layout1,df_cva,index,coluna_12_meses)
            extrai_cva_saldo_compensar(df_sparta_cva_layout2,df_cva,index,coluna_saldo)
        
        print('Extraiu o dado do arquivo: ',arquivo)
        print('Arquivos extraídos: ',round((index/len(arquivos)),2)*100,'%')
        index = index + 1 #Passa para o próximo indice para conseguir dados da proxima SPARTA

                          
    except:
        print('Aba não disponível na SPARTA', arquivo)
      
    

#%%Tratamento de dados 
#Remover dados duplicados e linhas nulas
df_cva = df_cva.drop_duplicates(subset = 'CHAVE',ignore_index = True)
df_cva = df_cva.dropna(axis=0,how='all')    


#Limpeza e Tratamento dos dados
df_cva = df_cva.astype(str)
df_cva['PERIODO_TARIFARIO'] = df_cva['PERIODO_TARIFARIO'].astype(int)
df_cva['5_DIA_UTIL_ANTERIOR_RS'] = df_cva['5_DIA_UTIL_ANTERIOR_RS'].replace('nan','0').astype(float).replace('.',',')
df_cva['CCC_5_DIA_UTIL_ANTERIOR_RS'] = df_cva['CCC_5_DIA_UTIL_ANTERIOR_RS'].replace('nan','0').astype(float).replace('.',',')
df_cva['CDE_5_DIA_UTIL_ANTERIOR_RS'] = df_cva['CDE_5_DIA_UTIL_ANTERIOR_RS'].replace('nan','0').astype(float).replace('.',',')
df_cva['CDE_COVID_5_DIA_UTIL_ANTERIOR_RS'] = df_cva['CDE_COVID_5_DIA_UTIL_ANTERIOR_RS'].replace('nan','0').astype(float).replace('.',',')
df_cva['REDE_BASICA_5_DIA_UTIL_ANTERIOR_RS'] = df_cva['REDE_BASICA_5_DIA_UTIL_ANTERIOR_RS'].replace('nan','0').astype(float).replace('.',',')
df_cva['COMPRA_ENERGIA_5_DIA_UTIL_ANTERIOR_RS'] = df_cva['COMPRA_ENERGIA_5_DIA_UTIL_ANTERIOR_RS'].replace('nan','0').astype(float).replace('.',',')
df_cva['CFURH_5_DIA_UTIL_ANTERIOR_RS'] = df_cva['CFURH_5_DIA_UTIL_ANTERIOR_RS'].replace(' ','0').replace('nan','0').astype(float).replace('.',',')
df_cva['TRANSPORTE_ITAIPU_5_DIA_UTIL_ANTERIOR_RS'] = df_cva['TRANSPORTE_ITAIPU_5_DIA_UTIL_ANTERIOR_RS'].replace('nan','0').astype(float).replace('.',',')
df_cva['PROINFA_12_5_DIA_UTIL_ANTERIOR_RS'] = df_cva['PROINFA_12_5_DIA_UTIL_ANTERIOR_RS'].replace('nan','0').astype(float).replace('.',',')
df_cva['ESS_ERR_5_DIA_UTIL_ANTERIOR_RS'] = df_cva['ESS_ERR_5_DIA_UTIL_ANTERIOR_RS'].replace('nan','0').astype(float).replace('.',',')
df_cva['12_MESES_SUBSEQUENTES_RS'] = df_cva['12_MESES_SUBSEQUENTES_RS'].replace('nan','0').astype(float).replace('.',',')
df_cva['CCC_12_MESES_SUB_RS'] = df_cva['CCC_12_MESES_SUB_RS'].replace('nan','0').astype(float).replace('.',',')
df_cva['CDE_12_MESES_SUB_RS'] = df_cva['CDE_12_MESES_SUB_RS'].replace('nan','0').astype(float).replace('.',',')
df_cva['CDE_COVID_12_MESES_SUB_RS'] = df_cva['CDE_COVID_12_MESES_SUB_RS'].replace('nan','0').astype(float).replace('.',',')
df_cva['REDE_BASICA_12_MESES_SUB_RS'] = df_cva['REDE_BASICA_12_MESES_SUB_RS'].replace('nan','0').astype(float).replace('.',',')
df_cva['COMPRA_ENERGIA_12_MESES_SUB_RS'] = df_cva['COMPRA_ENERGIA_12_MESES_SUB_RS'].replace('nan','0').astype(float).replace('.',',')
df_cva['CFURH_12_MESES_SUB_RS'] = df_cva['CFURH_12_MESES_SUB_RS'].replace(' ','0').replace('nan','0').astype(float).replace('.',',')
df_cva['TRANSPORTE_ITAIPU_12_MESES_SUB_RS'] = df_cva['TRANSPORTE_ITAIPU_12_MESES_SUB_RS'].replace('nan','0').astype(float).replace('.',',')
df_cva['PROINFA_12_MESES_SUB_RS'] = df_cva['PROINFA_12_MESES_SUB_RS'].replace('nan','0').astype(float).replace('.',',')
df_cva['ESS_ERR_12_MESES_SUB_RS'] = df_cva['ESS_ERR_12_MESES_SUB_RS'].replace('nan','0').astype(float).replace('.',',')
df_cva['SALDO_COMPENSAR_RS'] = df_cva['SALDO_COMPENSAR_RS'].replace('nan','0').astype(float).replace('.',',')
df_cva['CCC_SALDO_COMPENSAR_RS'] = df_cva['CCC_SALDO_COMPENSAR_RS'].replace('nan','0').astype(float).replace('.',',')
df_cva['CDE_SALDO_COMPENSAR_RS'] = df_cva['CDE_SALDO_COMPENSAR_RS'].replace('nan','0').astype(float).replace('.',',')
df_cva['REDE_BASICA_SALDO_COMPENSAR_RS'] = df_cva['REDE_BASICA_SALDO_COMPENSAR_RS'].replace('nan','0').astype(float).replace('.',',')
df_cva['COMPRA_ENERGIA_SALDO_COMPENSAR_RS'] = df_cva['COMPRA_ENERGIA_SALDO_COMPENSAR_RS'].replace('nan','0').astype(float).replace('.',',')
df_cva['CFURH_12_SALDO_COMPENSAR_RS'] = df_cva['CFURH_12_SALDO_COMPENSAR_RS'].replace('nan','0').astype(float).replace('.',',')
df_cva['TRANSPORTE_ITAIPU_SALDO_COMPENSAR_RS'] = df_cva['TRANSPORTE_ITAIPU_SALDO_COMPENSAR_RS'].replace('nan','0').astype(float).replace('.',',')
df_cva['PROINFA_12_SALDO_COMPENSAR_RS'] = df_cva['PROINFA_12_SALDO_COMPENSAR_RS'].replace('nan','0').astype(float).replace('.',',')
df_cva['ESS_ERR_SALDO_COMPENSAR_RS'] = df_cva['ESS_ERR_SALDO_COMPENSAR_RS'].replace('nan','0').astype(float).replace('.',',')

# Define a data de atualização dos dados
data = datetime.today().strftime('%d/%m/%Y')
df_cva['DATA_ATUALIZA'] = data

#%% Inserir dados no banco de dados

#Criar a lista para inserção no banco SQL com os dados da base editada
dados_list = df_cva.values.tolist()


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
        sql = '''INSERT INTO ''' + tabela_oracle +''' VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26,:27,:28,:29,:30,:31,:32,:33,:34,:35,:36,:37,:38,:39)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        cursor.executemany(sql, dados_list)
    except Exception as err:
        print('Erro no Insert:', err)
    else:
        print('Carga executada com sucesso!')
        connection.commit() #Caso seja executado com sucesso, esse comando salva a base de dados
    finally:
        cursor.close()
        connection.close()



    
    



