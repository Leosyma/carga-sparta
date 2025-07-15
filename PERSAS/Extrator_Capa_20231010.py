# -*- coding: utf-8 -*-
"""
Created on Wed Mar 29 14:52:55 2023

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

# Caminho referencia
pasta = r"W:\Inteligência Regulatória Analítica - IRA\2. Projetos\2023\BD PERSAS\02 - Dados\PERSAS\PERSAS 2023\*"
# pasta = r"C:\Users\2018459\OneDrive - CPFL Energia S A\Área de Trabalho\RTP e RTA (permissionarias)\PERSAS - TESTE"

#Arquivos
#Como os dados das empresas estão em arquivos separados, listar todos que serão carregados conforme layout abaixo.
# arquivo ='CERGAL_2012.xlsx'
arquivos = glob.glob(pasta)


index = 0 #Flag para inserção dos dados

#Nome da tabela Oracle onde será dada a carga
tabela_oracle_capa = 'PERSAS_CAPA'
ano_oracle = "'2023'"



#%% Montar o dataframe da estrutura
#Defino o indice máximo que é igual o número de persas
index_maximo = list(range(0,len(arquivos)))

#Criação da estrutura do tabela NUC
df_capa = pd.DataFrame(data = [], columns=['CHAVE','EVENTO_TARIFARIO','ANO','SARI','DISTRIBUIDORA','REGIAO','DATA','SUPRIDORA1','SUPRIDORA2'],index=index_maximo)

#Criação dataframe vazio
df_persas_capa = pd.DataFrame(data=[])



# df_persas_capa = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA')


# df_persas_capa = df_persas_capa.astype('str')


# linhas_capa = range(len(df_persas_capa.index))
# colunas_capa = range(len(df_persas_capa.columns))



#%%Funções
#Função para extrair dados da 'Capa'
def extrair_capa(df_capa,df_persas_capa,index):
    for linha in linhas_capa:
        for coluna in colunas_capa:
            if 'SUPRIDORA 1' in df_persas_capa.iloc[linha,coluna].upper():
                df_capa.at[index,'SUPRIDORA1'] = df_persas_capa.iloc[linha,(coluna+1)].upper()
            elif 'SUPRIDORA 2' in df_persas_capa.iloc[linha,coluna].upper():
                df_capa.at[index,'SUPRIDORA2'] = df_persas_capa.iloc[linha,(coluna+1)].upper()
        

def distribuidora(df_capa,df_persas_capa,index):
    for linha in linhas_capa:
        for coluna in colunas_capa:
            #Determina o ANO
            if 'ANO DO PROCESSO' in df_persas_capa.iloc[linha,coluna].upper():
                df_capa.at[index,'ANO'] = df_persas_capa.iloc[linha,(coluna+1)]
                
            #Determina o SARI da Distribuidora
            elif 'SARI' in df_persas_capa.iloc[linha,coluna].upper():
                df_capa.at[index,'SARI'] = df_persas_capa.iloc[linha,(coluna+1)]
                
            #Determina a DATA
            elif 'REAJUSTE/REVISÃO SUGE' in df_persas_capa.iloc[linha,coluna].upper():
                df_capa.at[index,'DATA'] = df_persas_capa.iloc[linha,(coluna+1)].split(' ')[0]


    #Determina o EVENTO TARIFARIO
    for linha in linhas_capa:
        for coluna in colunas_capa:
            if 'REAJUSTE' in df_persas_capa.iloc[linha,coluna].upper():
                df_capa.at[index,'EVENTO_TARIFARIO'] = 'RTA'
                
            elif 'REVISÃO' in df_persas_capa.iloc[linha,coluna].upper():
                df_capa.at[index,'EVENTO_TARIFARIO'] = 'RTP'
                
            elif 'TARIFAS INICIAIS' in df_persas_capa.iloc[linha,coluna].upper():
                df_capa.at[index,'EVENTO_TARIFARIO'] = 'TI'
                

    #Determina o nome da DISTRIBUIDORA das PERSAS fora do layout padrão
    if (df_persas_capa.iloc[4,1].upper() == 'COOPERMILA') or (df_persas_capa.iloc[5,1].upper() == 'CERTREL'):
        df_capa.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[4,1].upper()
        
    elif (df_persas_capa.iloc[0,1].upper() == 'CERGAL'):
        df_capa.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[0,1].upper()
       
    #PERSAS no layout padrão
    else:
        for linha in linhas_capa:
            for coluna in colunas_capa:
                if df_persas_capa.iloc[linha,coluna].upper() == 'EMPRESA':
                    df_capa.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[linha,(coluna+1)].upper()

    #Determina a REGIAO
    if df_capa.at[index,'DISTRIBUIDORA'] == 'CERCOS':
        df_capa.at[index,'REGIAO'] = 'N/NE'
        
    else:
        df_capa.at[index,'REGIAO'] = 'S/SE/CO'

    #Determina a CHAVE
    df_capa.at[index,'CHAVE'] = df_capa.loc[index,'EVENTO_TARIFARIO']+df_capa.loc[index,'ANO']+df_capa.loc[index,'SARI']


#%%Inserção dos dados
#Abre a PERSAS de cada arquivo na pasta
for arquivo in arquivos:   
    try:
        df_persas_capa = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA')   
                
                                                                
    except:
        print('Aba não disponível na PERSAS', arquivo)
 
    #Rodamos as funções
    else:
        try:
            print('Leu o arquivo: ',arquivo)
        
            #Converte as tabelas para string, pois não é possível comparar string com valor NaN
            df_persas_capa = df_persas_capa.astype('str')
    
            
            #Define o intervalo máximo de linhas e colunas do dataframe 
            linhas_capa = range(len(df_persas_capa.index))
            colunas_capa = range(len(df_persas_capa.columns))
            
            
            #Função para extrair os dados
            extrair_capa(df_capa,df_persas_capa,index)
            distribuidora(df_capa,df_persas_capa,index)
            
            print('Extraiu o dado do arquivo: ',arquivo)
            print('Arquivos extraídos: ',round((index/len(arquivos)),2)*100,'%')
    
        except Exception as err:
            print('Não foi possível extrair os dados:',err)
    
        
    index = index + 1 #Passa para o próximo indice para conseguir dados da proxima SPARTA



#%%Tratamento de dados
#Remover dados duplicados e linhas nulas
df_capa = df_capa.drop_duplicates(subset = 'CHAVE',ignore_index = True)
df_capa = df_capa.dropna(axis=0,how='all')

#Define todo o dataframe como string
df_capa = df_capa.astype('str')

#Tratamento dos dados
df_capa['SUPRIDORA2'] = df_capa['SUPRIDORA2'].replace(' E ','').replace('NAN','-').replace('nan','-').replace(' E',' ').replace('!VAZIO!','-').replace(' E ESS','ESS').replace(' E CEEE','CEEE').replace(' E CEEE','CEEE').replace(' E NOVA PALMA','NOVA PALMA').replace(' E RGE','RGE').replace(' E EFLUL','EFLUL').replace(' E RGE SUL','RGE SUL').replace(' E CERSUL','CERSUL').replace(' E CPFL PAULISTA','CPFL PAULISTA').replace(' E ELEKTRO','ELEKTRO')
df_capa['SUPRIDORA1'] = df_capa['SUPRIDORA1'].replace('nan','-')     

# Define a data de atualização dos dados
data = datetime.today().strftime('%d/%m/%Y')
df_capa['DATA_ATUALIZA'] = data


#%% Inserir dados no banco de dados

#Criar a lista para inserção no banco SQL com os dados da base editada
dados_list_capa = df_capa.values.tolist()


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
        cursor.execute('''DELETE FROM ''' + tabela_oracle_capa + ''' WHERE ANO = ''' + ano_oracle) #Exclui somente os dados de 2023
        sql = '''INSERT INTO ''' + tabela_oracle_capa +''' VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        cursor.executemany(sql, dados_list_capa)
    except Exception as err:
        print('Erro no Insert:', err)
    else:
        print('Carga executada com sucesso!')
        connection.commit() #Caso seja executado com sucesso, esse comando salva a base de dados
    finally:
        cursor.close()
        connection.close()






