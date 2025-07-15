# -*- coding: utf-8 -*-
"""
Created on Thu May 11 08:31:27 2023

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

# Caminho referencia
pasta = r"W:\Inteligência Regulatória Analítica - IRA\2. Projetos\2023\BD PERSAS\02 - Dados\PERSAS\PERSAS 2023\*"
# pasta = r"C:\Users\2018459\OneDrive - CPFL Energia S A\Área de Trabalho\RTP e RTA (permissionarias)\PERSAS - TESTE"

#Arquivos
#Como os dados das empresas estão em arquivos separados, listar todos que serão carregados conforme layout abaixo.
# arquivo ='PERSAS_coopermila_2018.xlsx'
arquivos = glob.glob(pasta)

colunas_float=['NEUTRALIDADE_TOTAL_RS','TOTAL_FINANCEIRO_RS']
index = 0 #Flag para inserção dos dados

#Nome da tabela Oracle onde será dada a carga
tabela_oracle = 'PERSAS_FINANCEIROS'
ano_oracle = "'2023'"


#%% Montar o dataframe da estrutura
#Defino o indice máximo que é igual o número de persas
index_maximo = list(range(0,len(arquivos)))

#Criação da estrutura do tabela NUC
df_financeiros = pd.DataFrame(data = [], columns=['CHAVE','EVENTO_TARIFARIO','ANO','SARI','DISTRIBUIDORA','REGIAO','DATA','NEUTRALIDADE_TOTAL_RS','TOTAL_FINANCEIRO_RS'],index=index_maximo)

#Criação dataframe vazio
df_persas_capa = pd.DataFrame(data=[])
df_persas_financeiros = pd.DataFrame(data=[])


# df_persas_capa = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA')
    

# df_persas_financeiros = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Financeiros'
#                                                                  ,usecols = 'A:K')
    

# linhas_capa = range(len(df_persas_capa.index))
# colunas_capa = range(len(df_persas_capa.columns))
# linhas_financeiros = range(len(df_persas_financeiros.index))
# colunas_financeiros = range(len(df_persas_financeiros.columns))


# df_persas_capa = df_persas_capa.astype('str')
# df_persas_financeiros = df_persas_financeiros.astype('str')


#%%Funções
#Função para extrair dados de 'Financeiros'
#Aba 'Financeiros'
def extrair_financeiros(df_financeiros,df_persas_financeiros,index):
    neutralidade_soma = 0 #Variavel para guardar os valores de Neutralidade
    financeiros_soma = 0 #Variavel para guardar o total dos Financeiros
    for linha_financeiros in linhas_financeiros:
        if 'NEUTRALIDADE' in df_persas_financeiros.iloc[linha_financeiros,2].upper():
            neutralidade_soma += float(df_persas_financeiros.iloc[linha_financeiros,(2+1)].replace('nan','0'))
        elif 'TOTAL DOS FINANCEIROS' in df_persas_financeiros.iloc[linha_financeiros,2].upper():
            financeiros_soma += float(df_persas_financeiros.iloc[linha_financeiros,(2+1)].replace('nan','0'))
            
    df_financeiros.at[index,'NEUTRALIDADE_TOTAL_RS'] = neutralidade_soma
    df_financeiros.at[index,'TOTAL_FINANCEIRO_RS'] = financeiros_soma


def distribuidora(df_financeiros,df_persas_capa,index):
    for linha in linhas_capa:
        for coluna in colunas_capa:
            #Determina o ANO
            if 'ANO DO PROCESSO' in df_persas_capa.iloc[linha,coluna].upper():
                df_financeiros.at[index,'ANO'] = df_persas_capa.iloc[linha,(coluna+1)]
                
            #Determina o SARI da Distribuidora
            elif 'SARI' in df_persas_capa.iloc[linha,coluna].upper():
                df_financeiros.at[index,'SARI'] = df_persas_capa.iloc[linha,(coluna+1)]
                
            #Determina a DATA
            elif 'REAJUSTE/REVISÃO SUGE' in df_persas_capa.iloc[linha,coluna].upper():
                df_financeiros.at[index,'DATA'] = df_persas_capa.iloc[linha,(coluna+1)].split(' ')[0]


    #Determina o EVENTO TARIFARIO
    for linha in linhas_capa:
        for coluna in colunas_capa:
            if 'REAJUSTE' in df_persas_capa.iloc[linha,coluna].upper():
                df_financeiros.at[index,'EVENTO_TARIFARIO'] = 'RTA'
                
            elif 'REVISÃO' in df_persas_capa.iloc[linha,coluna].upper():
                df_financeiros.at[index,'EVENTO_TARIFARIO'] = 'RTP'
                
            elif 'TARIFAS INICIAIS' in df_persas_capa.iloc[linha,coluna].upper():
                df_financeiros.at[index,'EVENTO_TARIFARIO'] = 'TI'
                

    #Determina o nome da DISTRIBUIDORA das PERSAS fora do layout padrão
    if (df_persas_capa.iloc[4,1].upper() == 'COOPERMILA') or (df_persas_capa.iloc[4,1].upper() == 'CERTREL'):
        df_financeiros.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[4,1].upper()
        
    elif (df_persas_capa.iloc[0,1].upper() == 'CERGAL'):
        df_financeiros.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[0,1].upper()
       
    #PERSAS no layout padrão
    else:
        for linha in linhas_capa:
            for coluna in colunas_capa:
                if df_persas_capa.iloc[linha,coluna].upper() == 'EMPRESA':
                    df_financeiros.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[linha,(coluna+1)].upper()

    #Determina a REGIAO
    if df_financeiros.at[index,'DISTRIBUIDORA'] == 'CERCOS':
        df_financeiros.at[index,'REGIAO'] = 'N/NE'
        
    else:
        df_financeiros.at[index,'REGIAO'] = 'S/SE/CO'

    #Determina a CHAVE
    df_financeiros.at[index,'CHAVE'] = df_financeiros.loc[index,'EVENTO_TARIFARIO']+df_financeiros.loc[index,'ANO']+df_financeiros.loc[index,'SARI']



#%%Inserção dos dados
#Abre a PERSAS de cada arquivo na pasta
for arquivo in arquivos:   
    try:
        df_persas_capa = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA')


        df_persas_financeiros = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Financeiros'
                                                                         ,usecols = 'A:K')
            
                                                                
    except:
        print('Aba não disponível na PERSAS', arquivo)
 
    #Rodamos as funções
    else:
        try:
            print('Leu o arquivo: ',arquivo)
        
            #Converte as tabelas para string, pois não é possível comparar string com valor NaN
            df_persas_capa = df_persas_capa.astype('str')
            df_persas_financeiros = df_persas_financeiros.astype('str')

            
            #Define o intervalo máximo de linhas e colunas do dataframe 
            linhas_capa = range(len(df_persas_capa.index))
            colunas_capa = range(len(df_persas_capa.columns))
            linhas_financeiros = range(len(df_persas_financeiros.index))
            colunas_financeiros = range(len(df_persas_financeiros.columns))

            
            #Função para extrair os dados
            distribuidora(df_financeiros,df_persas_capa,index)
            extrair_financeiros(df_financeiros,df_persas_financeiros,index)
            
            print('Extraiu o dado do arquivo: ',arquivo)
            print('Arquivos extraídos: ',round((index/len(arquivos)),2)*100,'%')
    
        except Exception as err:
            print('Não foi possível extrair os dados:',err)
    
        
    index = index + 1 #Passa para o próximo indice para conseguir dados da proxima SPARTA
    
    
#%%Tratamento de dados
#Remover dados duplicados
df_financeiros = df_financeiros.drop_duplicates(subset = 'CHAVE',ignore_index = True)
df_financeiros = df_financeiros.dropna(axis=0,how='all')

#Tratamento dos dados
df_financeiros = df_financeiros.astype('str')
for coluna_float in colunas_float:
    df_financeiros[coluna_float] = df_financeiros[coluna_float].replace('.',',').replace('nan',0).astype('float')
  
# Define a data de atualização dos dados
data = datetime.today().strftime('%d/%m/%Y')
df_financeiros['DATA_ATUALIZA'] = data   

#%% Inserir dados no banco de dados

#Criar a lista para inserção no banco SQL com os dados da base editada
dados_list = df_financeiros.values.tolist()


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
        sql = '''INSERT INTO ''' + tabela_oracle +''' VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        cursor.executemany(sql, dados_list)
    except Exception as err:
        print('Erro no Insert:', err)
    else:
        print('Carga executada com sucesso!')
        connection.commit() #Caso seja executado com sucesso, esse comando salva a base de dados
    finally:
        cursor.close()
        connection.close()

   



