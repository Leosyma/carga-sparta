# -*- coding: utf-8 -*-
"""
Created on Wed May 24 11:09:34 2023

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
# arquivo ='PERSAS_Cermissões_2018.xlsx'
arquivos = glob.glob(pasta)

colunas_float=['MERCADO_LIVRE_MWH','MERCADO_A1_MWH','MERCADO_BT_MWH','GERACAO_PROPRIA_MWH','PROINFA_MWH','SUPRIMENTO_MWH','ANGRA_MWH','CCGF_MWH','CONTRATOS_BILATERAIS_MWH','GERACAO_PROPRIA_RS','PROINFA_RS','SUPRIMENTO_RS','ANGRA_RS','CCGF_RS','CONTRATOS_BILATERAIS_RS','GERACAO_PROPRIA_RS_MWH','PROINFA_RS_MWH','SUPRIMENTO_RS_MWH','ANGRA_RS_MWH','CCGF_RS_MWH','CONTRATOS_BILATERAIS_RS_MWH']
index = 0 #Flag para inserção dos dados

#Nome da tabela Oracle onde será dada a carga
tabela_oracle = 'PERSAS_ENERGIA'
ano_oracle = "'2023'"


#%% Montar o dataframe da estrutura
#Defino o indice máximo que é igual o número de persas
index_maximo = list(range(0,len(arquivos)))

#Criação da estrutura do tabela NUC
df_energia = pd.DataFrame(data = [], columns=['CHAVE','EVENTO_TARIFARIO','ANO','SARI','DISTRIBUIDORA','REGIAO','DATA','MERCADO_LIVRE_MWH','MERCADO_A1_MWH','MERCADO_BT_MWH','GERACAO_PROPRIA_MWH','PROINFA_MWH','SUPRIMENTO_MWH','ANGRA_MWH','CCGF_MWH','CONTRATOS_BILATERAIS_MWH','GERACAO_PROPRIA_RS','PROINFA_RS','SUPRIMENTO_RS','ANGRA_RS','CCGF_RS','CONTRATOS_BILATERAIS_RS','GERACAO_PROPRIA_RS_MWH','PROINFA_RS_MWH','SUPRIMENTO_RS_MWH','ANGRA_RS_MWH','CCGF_RS_MWH','CONTRATOS_BILATERAIS_RS_MWH'],index=index_maximo)

#Criação dataframe vazio
df_persas_capa = pd.DataFrame(data=[])
df_persas_energia = pd.DataFrame(data=[])


# df_persas_capa = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA')
    

# df_persas_energia = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Energia'
#                                                               ,nrows=35
#                                                               ,usecols='A:G')

# df_persas_mercado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Energia'
#                                                               ,nrows=15
#                                                               ,usecols='I:M')
    

# linhas_capa = range(len(df_persas_capa.index))
# colunas_capa = range(len(df_persas_capa.columns))
# linhas_energia = range(len(df_persas_energia.index))
# colunas_energia = range(len(df_persas_energia.columns))
# linhas_mercado = range(len(df_persas_mercado.index))
# colunas_mercado = range(len(df_persas_mercado.columns))


# df_persas_capa = df_persas_capa.astype('str')
# df_persas_energia = df_persas_energia.astype('str')
# df_persas_mercado = df_persas_mercado.astype('str')



#%%Funções
#Função para definir as colunas
def define_coluna(df_persas_energia):
    global coluna_montante
    global coluna_custo_medio
    global coluna_despesa
    for coluna_energia in colunas_energia:
        for linha_energia in linhas_energia:
            if (df_persas_energia.iloc[linha_energia,coluna_energia].upper() == 'MONTANTE (MWH)') or (df_persas_energia.iloc[linha_energia,coluna_energia].upper() == 'MONTANTE'):
                coluna_montante = coluna_energia
            elif (df_persas_energia.iloc[linha_energia,coluna_energia].upper() == 'CUSTO MÉDIO (R$/MWH)') or ((df_persas_energia.iloc[linha_energia,coluna_energia].upper() == 'CUSTO MÉDIO')):
                coluna_custo_medio = coluna_energia
            elif (df_persas_energia.iloc[linha_energia,coluna_energia].upper() == 'DESPESA'):
                coluna_despesa = coluna_energia

                
#Função para definir as colunas
def define_linha(df_persas_energia):
    global linha_montante
    global linha_custo_medio
    global linha_despesa
    for linha_energia in linhas_energia:
        for coluna_energia in colunas_energia:
            if (df_persas_energia.iloc[linha_energia,coluna_energia].upper() == 'MONTANTE (MWH)') or (df_persas_energia.iloc[linha_energia,coluna_energia].upper() == 'MONTANTE'):
                linha_montante = linha_energia
            elif df_persas_energia.iloc[linha_energia,coluna_energia].upper() == 'CUSTO MÉDIO (R$/MWH)' or ((df_persas_energia.iloc[linha_energia,coluna_energia].upper() == 'CUSTO MÉDIO')):
                linha_custo_medio = linha_energia
            elif df_persas_energia.iloc[linha_energia,coluna_energia].upper() == 'DESPESA':
                linha_despesa = linha_energia


#Função para extrair dados de 'Energia'
#Aba 'Energia'
def extrair_energia(df_energia,df_persas_energia,df_persas_mercado,index):
    #Extrai dados de 'Mercado MWH'
    for linha_mercado in linhas_mercado:
        for coluna_mercado in colunas_mercado:
            if df_persas_mercado.iloc[linha_mercado,coluna_mercado].upper() == 'CONSUMIDOR LIVRE':
                df_energia.at[index,'MERCADO_LIVRE_MWH'] = df_persas_mercado.iloc[linha_mercado,(coluna_mercado+1)]
            elif df_persas_mercado.iloc[linha_mercado,coluna_mercado].upper() == 'A1':
                df_energia.at[index,'MERCADO_A1_MWH'] = df_persas_mercado.iloc[linha_mercado,(coluna_mercado+1)]
            elif df_persas_mercado.iloc[linha_mercado,coluna_mercado].upper() == 'BT':
                df_energia.at[index,'MERCADO_BT_MWH'] = df_persas_mercado.iloc[linha_mercado,(coluna_mercado+1)]
            
    
    #Extrai dados de 'Montante'
    for linha_energia in linhas_energia:
        for coluna_energia in colunas_energia:
            if (df_persas_energia.iloc[linha_energia,1].upper() == 'GERAÇÃO PRÓPRIA') and (df_persas_energia.iloc[linha_montante,coluna_montante].upper() == 'MONTANTE (MWH)' or df_persas_energia.iloc[linha_montante,coluna_montante].upper() == 'MONTANTE'):
                df_energia.at[index,'GERACAO_PROPRIA_MWH'] = df_persas_energia.iloc[linha_energia,coluna_montante]
            elif (df_persas_energia.iloc[linha_energia,1].upper() == 'PROINFA') and (df_persas_energia.iloc[linha_montante,coluna_montante].upper() == 'MONTANTE (MWH)' or df_persas_energia.iloc[linha_montante,coluna_montante].upper() == 'MONTANTE'):
                df_energia.at[index,'PROINFA_MWH'] = df_persas_energia.iloc[linha_energia,coluna_montante]
            elif (df_persas_energia.iloc[linha_energia,1].upper() == 'SUPRIMENTO') and (df_persas_energia.iloc[linha_montante,coluna_montante].upper() == 'MONTANTE (MWH)' or df_persas_energia.iloc[linha_montante,coluna_montante].upper() == 'MONTANTE'):
                df_energia.at[index,'SUPRIMENTO_MWH'] = df_persas_energia.iloc[linha_energia,coluna_montante]
            elif (df_persas_energia.iloc[linha_energia,1].upper() == 'ANGRA') and (df_persas_energia.iloc[linha_montante,coluna_montante].upper() == 'MONTANTE (MWH)' or df_persas_energia.iloc[linha_montante,coluna_montante].upper() == 'MONTANTE'):
                df_energia.at[index,'ANGRA_MWH'] = df_persas_energia.iloc[linha_energia,coluna_montante]
            elif (df_persas_energia.iloc[linha_energia,1].upper() == 'CCGF') and (df_persas_energia.iloc[linha_montante,coluna_montante].upper() == 'MONTANTE (MWH)' or df_persas_energia.iloc[linha_montante,coluna_montante].upper() == 'MONTANTE'):
                df_energia.at[index,'CCGF_MWH'] = df_persas_energia.iloc[linha_energia,coluna_montante]
            elif (df_persas_energia.iloc[linha_energia,1].upper() == 'CONTRATOS BILATERAIS') and (df_persas_energia.iloc[linha_montante,coluna_montante].upper() == 'MONTANTE (MWH)' or df_persas_energia.iloc[linha_montante,coluna_montante].upper() == 'MONTANTE'):
                df_energia.at[index,'CONTRATOS_BILATERAIS_MWH'] = df_persas_energia.iloc[linha_energia,coluna_montante]
                   

    #Extrai dados de 'Custo Médio'
    for linha_energia in linhas_energia:
        if (df_persas_energia.iloc[linha_energia,1].upper() == 'GERAÇÃO PRÓPRIA') and (df_persas_energia.iloc[linha_custo_medio,coluna_custo_medio].upper() == 'CUSTO MÉDIO (R$/MWH)' or df_persas_energia.iloc[linha_custo_medio,coluna_custo_medio].upper() == 'CUSTO MÉDIO'):
            df_energia.at[index,'GERACAO_PROPRIA_RS_MWH'] = df_persas_energia.iloc[linha_energia,coluna_custo_medio]
        elif (df_persas_energia.iloc[linha_energia,1].upper() == 'PROINFA') and (df_persas_energia.iloc[linha_custo_medio,coluna_custo_medio].upper() == 'CUSTO MÉDIO (R$/MWH)' or df_persas_energia.iloc[linha_custo_medio,coluna_custo_medio].upper() == 'CUSTO MÉDIO'):
            df_energia.at[index,'PROINFA_RS_MWH'] = df_persas_energia.iloc[linha_energia,coluna_custo_medio]
        elif (df_persas_energia.iloc[linha_energia,1].upper()) == 'SUPRIMENTO' and (df_persas_energia.iloc[linha_custo_medio,coluna_custo_medio].upper() == 'CUSTO MÉDIO (R$/MWH)' or df_persas_energia.iloc[linha_custo_medio,coluna_custo_medio].upper() == 'CUSTO MÉDIO'):
            df_energia.at[index,'SUPRIMENTO_RS_MWH'] = df_persas_energia.iloc[linha_energia,coluna_custo_medio]
        elif (df_persas_energia.iloc[linha_energia,1].upper() == 'ANGRA') and (df_persas_energia.iloc[linha_custo_medio,coluna_custo_medio].upper() == 'CUSTO MÉDIO (R$/MWH)' or df_persas_energia.iloc[linha_custo_medio,coluna_custo_medio].upper() == 'CUSTO MÉDIO'):
            df_energia.at[index,'ANGRA_RS_MWH'] = df_persas_energia.iloc[linha_energia,coluna_custo_medio]
        elif (df_persas_energia.iloc[linha_energia,1].upper() == 'CCGF') and (df_persas_energia.iloc[linha_custo_medio,coluna_custo_medio].upper() == 'CUSTO MÉDIO (R$/MWH)' or df_persas_energia.iloc[linha_custo_medio,coluna_custo_medio].upper() == 'CUSTO MÉDIO'):
            df_energia.at[index,'CCGF_RS_MWH'] = df_persas_energia.iloc[linha_energia,coluna_custo_medio]
        elif (df_persas_energia.iloc[linha_energia,1].upper() == 'CONTRATOS BILATERAIS') and (df_persas_energia.iloc[linha_custo_medio,coluna_custo_medio].upper() == 'CUSTO MÉDIO (R$/MWH)' or df_persas_energia.iloc[linha_custo_medio,coluna_custo_medio].upper() == 'CUSTO MÉDIO'):
            df_energia.at[index,'CONTRATOS_BILATERAIS_RS_MWH'] = df_persas_energia.iloc[linha_energia,coluna_custo_medio]

    #Extrai dados de 'Despesa'
    for linha_energia in linhas_energia:
        if (df_persas_energia.iloc[linha_energia,1].upper() == 'GERAÇÃO PRÓPRIA') and (df_persas_energia.iloc[linha_despesa,coluna_despesa].upper() == 'DESPESA'):
            df_energia.at[index,'GERACAO_PROPRIA_RS'] = df_persas_energia.iloc[linha_energia,coluna_despesa]
        elif (df_persas_energia.iloc[linha_energia,1].upper()) == 'PROINFA' and (df_persas_energia.iloc[linha_despesa,coluna_despesa].upper() == 'DESPESA'):
            df_energia.at[index,'PROINFA_RS'] = df_persas_energia.iloc[linha_energia,coluna_despesa]
        elif (df_persas_energia.iloc[linha_energia,1].upper()) == 'SUPRIMENTO' and (df_persas_energia.iloc[linha_despesa,coluna_despesa].upper() == 'DESPESA'):
            df_energia.at[index,'SUPRIMENTO_RS'] = df_persas_energia.iloc[linha_energia,coluna_despesa]
        elif (df_persas_energia.iloc[linha_energia,1].upper()) == 'ANGRA' and (df_persas_energia.iloc[linha_despesa,coluna_despesa].upper() == 'DESPESA'):
            df_energia.at[index,'ANGRA_RS'] = df_persas_energia.iloc[linha_energia,coluna_despesa]
        elif (df_persas_energia.iloc[linha_energia,1].upper()) == 'CCGF' and (df_persas_energia.iloc[linha_despesa,coluna_despesa].upper() == 'DESPESA'):
            df_energia.at[index,'CCGF_RS'] = df_persas_energia.iloc[linha_energia,coluna_despesa]
        elif (df_persas_energia.iloc[linha_energia,1].upper()) == 'CONTRATOS BILATERAIS' and (df_persas_energia.iloc[linha_despesa,coluna_despesa].upper() == 'DESPESA'):
            df_energia.at[index,'CONTRATOS_BILATERAIS_RS'] = df_persas_energia.iloc[linha_energia,coluna_despesa]



def distribuidora(df_energia,df_persas_capa,index):
    for linha in linhas_capa:
        for coluna in colunas_capa:
            #Determina o ANO
            if 'ANO DO PROCESSO' in df_persas_capa.iloc[linha,coluna].upper():
                df_energia.at[index,'ANO'] = df_persas_capa.iloc[linha,(coluna+1)]
                
            #Determina o SARI da Distribuidora
            elif 'SARI' in df_persas_capa.iloc[linha,coluna].upper():
                df_energia.at[index,'SARI'] = df_persas_capa.iloc[linha,(coluna+1)]
                
            #Determina a DATA
            elif 'REAJUSTE/REVISÃO SUGE' in df_persas_capa.iloc[linha,coluna].upper():
                df_energia.at[index,'DATA'] = df_persas_capa.iloc[linha,(coluna+1)].split(' ')[0]


    #Determina o EVENTO TARIFARIO
    for linha in linhas_capa:
        for coluna in colunas_capa:
            if 'REAJUSTE' in df_persas_capa.iloc[linha,coluna].upper():
                df_energia.at[index,'EVENTO_TARIFARIO'] = 'RTA'
                
            elif 'REVISÃO' in df_persas_capa.iloc[linha,coluna].upper():
                df_energia.at[index,'EVENTO_TARIFARIO'] = 'RTP'
                
            elif 'TARIFAS INICIAIS' in df_persas_capa.iloc[linha,coluna].upper():
                df_energia.at[index,'EVENTO_TARIFARIO'] = 'TI'
                

    #Determina o nome da DISTRIBUIDORA das PERSAS fora do layout padrão
    if (df_persas_capa.iloc[4,1].upper() == 'COOPERMILA') or (df_persas_capa.iloc[4,1].upper() == 'CERTREL'):
        df_energia.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[4,1].upper()
        
    elif (df_persas_capa.iloc[0,1].upper() == 'CERGAL'):
        df_energia.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[0,1].upper()
       
    #PERSAS no layout padrão
    else:
        for linha in linhas_capa:
            for coluna in colunas_capa:
                if df_persas_capa.iloc[linha,coluna].upper() == 'EMPRESA':
                    df_energia.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[linha,(coluna+1)].upper()

    #Determina a REGIAO
    if df_energia.at[index,'DISTRIBUIDORA'] == 'CERCOS':
        df_energia.at[index,'REGIAO'] = 'N/NE'
        
    else:
        df_energia.at[index,'REGIAO'] = 'S/SE/CO'

    #Determina a CHAVE
    df_energia.at[index,'CHAVE'] = df_energia.loc[index,'EVENTO_TARIFARIO']+df_energia.loc[index,'ANO']+df_energia.loc[index,'SARI']


#%%Inserção dos dados
#Abre a PERSAS de cada arquivo na pasta
for arquivo in arquivos:   
    try:
        df_persas_capa = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA')
            

        df_persas_energia = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Energia'
                                                                      ,nrows=35
                                                                      ,usecols='A:G')

        df_persas_mercado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Energia'
                                                                      ,nrows=15
                                                                      ,usecols='I:M')
            
                                                                
    except:
        print('Aba não disponível na PERSAS', arquivo)
 
    #Rodamos as funções
    else:
        try:
            print('Leu o arquivo: ',arquivo)
        
            #Converte as tabelas para string, pois não é possível comparar string com valor NaN
            df_persas_capa = df_persas_capa.astype('str')
            df_persas_energia = df_persas_energia.astype('str')
            df_persas_mercado = df_persas_mercado.astype('str')

            
            #Define o intervalo máximo de linhas e colunas do dataframe 
            linhas_capa = range(len(df_persas_capa.index))
            colunas_capa = range(len(df_persas_capa.columns))
            linhas_energia = range(len(df_persas_energia.index))
            colunas_energia = range(len(df_persas_energia.columns))
            linhas_mercado = range(len(df_persas_mercado.index))
            colunas_mercado = range(len(df_persas_mercado.columns))

            
            #Função para extrair os dados
            distribuidora(df_energia,df_persas_capa,index)
            define_coluna(df_persas_energia)
            define_linha(df_persas_energia)
            extrair_energia(df_energia,df_persas_energia,df_persas_mercado,index)
            
            print('Extraiu o dado do arquivo: ',arquivo)
            print('Arquivos extraídos: ',round((index/len(arquivos)),2)*100,'%')
    
        except Exception as err:
            print('Não foi possível extrair os dados:',err)
    
        
    index = index + 1 #Passa para o próximo indice para conseguir dados da proxima SPARTA
    
  
#%%Tratamento de dados
#Remover dados duplicados
df_energia = df_energia.drop_duplicates(subset = 'CHAVE',ignore_index = True)
df_energia = df_energia.dropna(axis=0,how='all')

#Tratamento dos dados
df_energia = df_energia.astype('str')
for coluna_float in colunas_float:
    df_energia[coluna_float] = df_energia[coluna_float].replace('.',',').replace('nan',0).astype('float')

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
        sql = '''INSERT INTO ''' + tabela_oracle +''' VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26,:27,:28,:29)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        cursor.executemany(sql, dados_list)
    except Exception as err:
        print('Erro no Insert:', err)
    else:
        print('Carga executada com sucesso!')
        connection.commit() #Caso seja executado com sucesso, esse comando salva a base de dados
    finally:
        cursor.close()
        connection.close()
    



