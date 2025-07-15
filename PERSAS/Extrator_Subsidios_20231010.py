# -*- coding: utf-8 -*-
"""
Created on Mon May  8 15:26:50 2023

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
from unidecode import unidecode
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

colunas_float=['SUBSIDIO_CARGA_RS','SUBSIDIO_GERACAO_RS','SUBSIDIO_DISTRIBUICAO_RS','SUBSIDIO_AGUA_RS','SUBSIDIO_RURAL_RS','SUBSIDIO_IRRIGANTE_RS']
index = 0 #Flag para inserção dos dados

#Nome da tabela Oracle onde será dada a carga
tabela_oracle = 'PERSAS_SUBSIDIOS'
ano_oracle = "'2023'"


#%% Montar o dataframe da estrutura
#Defino o indice máximo que é igual o número de persas
index_maximo = list(range(0,len(arquivos)))

#Criação da estrutura do tabela NUC
df_subsidios = pd.DataFrame(data = [], columns=['CHAVE','EVENTO_TARIFARIO','ANO','SARI','DISTRIBUIDORA','REGIAO','DATA','SUBSIDIO_CARGA_RS','SUBSIDIO_GERACAO_RS','SUBSIDIO_DISTRIBUICAO_RS','SUBSIDIO_AGUA_RS','SUBSIDIO_RURAL_RS','SUBSIDIO_IRRIGANTE_RS'],index=index_maximo)

#Criação dataframe vazio
df_persas_capa = pd.DataFrame(data=[])
df_persas_resultado = pd.DataFrame(data=[])


# df_persas_capa = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA')
    

# df_persas_resultado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Resultado')
    

# linhas_capa = range(len(df_persas_capa.index))
# colunas_capa = range(len(df_persas_capa.columns))
# linhas_resultado = range(len(df_persas_resultado.index))
# colunas_resultado = range(len(df_persas_resultado.columns))


# df_persas_capa = df_persas_capa.astype('str')
# df_persas_resultado = df_persas_resultado.astype('str')


#%%Funções
#Função para extrair dados de 'Subsidios'
#Aba 'Resultado'
def extrair_resultado(df_subsidios,df_persas_resultado,df_persas_capa,index):
    #Tratamento especifico para as PERSAS 2018 da CETRIL e CERIM
    if (df_subsidios.at[index,'ANO'] == '2018' and df_subsidios.at[index,'SARI'] == '5379') or (df_subsidios.at[index,'ANO'] == '2018' and df_subsidios.at[index,'SARI'] == '5386'):
        for linha_resultado in linhas_resultado:
            for coluna_resultado in colunas_resultado:
                if 'SUBSÍDIO CARGA' in df_persas_resultado.iloc[linha_resultado,coluna_resultado].upper():
                    df_subsidios.at[index,'SUBSIDIO_CARGA_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+2)]
                elif 'SUBSÍDIO GERAÇÃO' in df_persas_resultado.iloc[linha_resultado,coluna_resultado].upper():
                    df_subsidios.at[index,'SUBSIDIO_GERACAO_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+2)]
                elif 'SUBSÍDIO DISTRIBUIÇÃO' in df_persas_resultado.iloc[linha_resultado,coluna_resultado].upper():
                    df_subsidios.at[index,'SUBSIDIO_DISTRIBUICAO_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+2)]
                elif 'SUBSÍDIO ÁGUA' in df_persas_resultado.iloc[linha_resultado,coluna_resultado].upper():
                    df_subsidios.at[index,'SUBSIDIO_AGUA_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+2)]
                elif 'SUBSÍDIO RURAL' in df_persas_resultado.iloc[linha_resultado,coluna_resultado].upper():
                    df_subsidios.at[index,'SUBSIDIO_RURAL_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+2)]
                elif 'SUBSÍDIO IRRIGANTE' in df_persas_resultado.iloc[linha_resultado,coluna_resultado].upper():
                    df_subsidios.at[index,'SUBSIDIO_IRRIGANTE_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+2)]
                
    
    #PERSAS 2018
    elif (df_subsidios.at[index,'ANO'] == '2018'):
        for linha_capa in linhas_capa:
            for coluna_capa in colunas_capa:
                if ('SUBSIDIO CARGA' in df_persas_capa.iloc[linha_capa,coluna_capa].upper()) and (df_persas_capa.iloc[(linha_capa-1),(coluna_capa+1)].upper() == 'PREVISÃO 2018/2019'):
                    df_subsidios.at[index,'SUBSIDIO_CARGA_RS'] = df_persas_capa.iloc[linha_capa,(coluna_capa+1)]
                elif ('SUBSIDIO GERAÇÃO' in df_persas_capa.iloc[linha_capa,coluna_capa].upper()) and (df_persas_capa.iloc[(linha_capa-2),(coluna_capa+1)].upper() == 'PREVISÃO 2018/2019'):
                    df_subsidios.at[index,'SUBSIDIO_GERACAO_RS'] = df_persas_capa.iloc[linha_capa,(coluna_capa+1)]
                elif ('SUBSIDIO DISTRIBUIÇÃO' in df_persas_capa.iloc[linha_capa,coluna_capa].upper()) and (df_persas_capa.iloc[(linha_capa-3),(coluna_capa+1)].upper() == 'PREVISÃO 2018/2019'):
                    df_subsidios.at[index,'SUBSIDIO_DISTRIBUICAO_RS'] = df_persas_capa.iloc[linha_capa,(coluna_capa+1)]
                elif ('SUBSIDIO ÁGUA' in df_persas_capa.iloc[linha_capa,coluna_capa].upper()) and (df_persas_capa.iloc[(linha_capa-4),(coluna_capa+1)].upper() == 'PREVISÃO 2018/2019'):
                    df_subsidios.at[index,'SUBSIDIO_AGUA_RS'] = df_persas_capa.iloc[linha_capa,(coluna_capa+1)]
                elif ('SUBSIDIO RURAL' in df_persas_capa.iloc[linha_capa,coluna_capa].upper()) and (df_persas_capa.iloc[(linha_capa-5),(coluna_capa+1)].upper() == 'PREVISÃO 2018/2019'):
                    df_subsidios.at[index,'SUBSIDIO_RURAL_RS'] = df_persas_capa.iloc[linha_capa,(coluna_capa+1)]
                elif ('SUBSIDIO - IRRIGANTE' in df_persas_capa.iloc[linha_capa,coluna_capa].upper()) and (df_persas_capa.iloc[(linha_capa-6),(coluna_capa+1)].upper() == 'PREVISÃO 2018/2019'):
                    df_subsidios.at[index,'SUBSIDIO_IRRIGANTE_RS'] = df_persas_capa.iloc[linha_capa,(coluna_capa+1)]
 

    #Demais PERSAS
    else:
        for linha_resultado in linhas_resultado:
            for coluna_resultado in colunas_resultado:
                if 'SUBSIDIO CARGA' in unidecode(df_persas_resultado.iloc[linha_resultado,coluna_resultado]).upper():
                    df_subsidios.at[index,'SUBSIDIO_CARGA_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+2)]
                elif 'SUBSIDIO GERACAO' in unidecode(df_persas_resultado.iloc[linha_resultado,coluna_resultado]).upper():
                    df_subsidios.at[index,'SUBSIDIO_GERACAO_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+2)]
                elif 'SUBSIDIO DISTRIBUICAO' in unidecode(df_persas_resultado.iloc[linha_resultado,coluna_resultado]).upper():
                    df_subsidios.at[index,'SUBSIDIO_DISTRIBUICAO_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+2)]
                elif 'SUBSIDIO AGUA' in unidecode(df_persas_resultado.iloc[linha_resultado,coluna_resultado]).upper():
                    df_subsidios.at[index,'SUBSIDIO_AGUA_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+2)]
                elif 'SUBSIDIO RURAL' in unidecode(df_persas_resultado.iloc[linha_resultado,coluna_resultado]).upper():
                    df_subsidios.at[index,'SUBSIDIO_RURAL_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+2)]
                elif 'SUBSIDIO IRRIGANTE' in unidecode(df_persas_resultado.iloc[linha_resultado,coluna_resultado]).upper():
                    df_subsidios.at[index,'SUBSIDIO_IRRIGANTE_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+2)]
                

def distribuidora(df_subsidios,df_persas_capa,index):
    for linha in linhas_capa:
        for coluna in colunas_capa:
            #Determina o ANO
            if 'ANO DO PROCESSO' in df_persas_capa.iloc[linha,coluna].upper():
                df_subsidios.at[index,'ANO'] = df_persas_capa.iloc[linha,(coluna+1)]
                
            #Determina o SARI da Distribuidora
            elif 'SARI' in df_persas_capa.iloc[linha,coluna].upper():
                df_subsidios.at[index,'SARI'] = df_persas_capa.iloc[linha,(coluna+1)]
                
            #Determina a DATA
            elif 'REAJUSTE/REVISÃO SUGE' in df_persas_capa.iloc[linha,coluna].upper():
                df_subsidios.at[index,'DATA'] = df_persas_capa.iloc[linha,(coluna+1)].split(' ')[0]


    #Determina o EVENTO TARIFARIO
    for linha in linhas_capa:
        for coluna in colunas_capa:
            if 'REAJUSTE' in df_persas_capa.iloc[linha,coluna].upper():
                df_subsidios.at[index,'EVENTO_TARIFARIO'] = 'RTA'
                
            elif 'REVISÃO' in df_persas_capa.iloc[linha,coluna].upper():
                df_subsidios.at[index,'EVENTO_TARIFARIO'] = 'RTP'
                
            elif 'TARIFAS INICIAIS' in df_persas_capa.iloc[linha,coluna].upper():
                df_subsidios.at[index,'EVENTO_TARIFARIO'] = 'TI'
                

    #Determina o nome da DISTRIBUIDORA das PERSAS fora do layout padrão
    if (df_persas_capa.iloc[4,1].upper() == 'COOPERMILA') or (df_persas_capa.iloc[4,1].upper() == 'CERTREL'):
        df_subsidios.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[4,1].upper()
        
    elif (df_persas_capa.iloc[0,1].upper() == 'CERGAL'):
        df_subsidios.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[0,1].upper()
       
    #PERSAS no layout padrão
    else:
        for linha in linhas_capa:
            for coluna in colunas_capa:
                if df_persas_capa.iloc[linha,coluna].upper() == 'EMPRESA':
                    df_subsidios.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[linha,(coluna+1)].upper()

    #Determina a REGIAO
    if df_subsidios.at[index,'DISTRIBUIDORA'] == 'CERCOS':
        df_subsidios.at[index,'REGIAO'] = 'N/NE'
        
    else:
        df_subsidios.at[index,'REGIAO'] = 'S/SE/CO'

    #Determina a CHAVE
    df_subsidios.at[index,'CHAVE'] = df_subsidios.loc[index,'EVENTO_TARIFARIO']+df_subsidios.loc[index,'ANO']+df_subsidios.loc[index,'SARI']



#%%Inserção dos dados
#Abre a PERSAS de cada arquivo na pasta
for arquivo in arquivos:   
    try:
        df_persas_capa = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA')


        df_persas_resultado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Resultado')
            
                                                                
    except:
        print('Aba não disponível na PERSAS', arquivo)
 
    #Rodamos as funções
    else:
        try:
            print('Leu o arquivo: ',arquivo)
        
            #Converte as tabelas para string, pois não é possível comparar string com valor NaN
            df_persas_capa = df_persas_capa.astype('str')
            df_persas_resultado = df_persas_resultado.astype('str')

            
            #Define o intervalo máximo de linhas e colunas do dataframe 
            linhas_capa = range(len(df_persas_capa.index))
            colunas_capa = range(len(df_persas_capa.columns))
            linhas_resultado = range(len(df_persas_resultado.index))
            colunas_resultado = range(len(df_persas_resultado.columns))

            
            #Função para extrair os dados
            distribuidora(df_subsidios,df_persas_capa,index)
            extrair_resultado(df_subsidios,df_persas_resultado,df_persas_capa,index)
            
            print('Extraiu o dado do arquivo: ',arquivo)
            print('Arquivos extraídos: ',round((index/len(arquivos)),2)*100,'%')
    
        except Exception as err:
            print('Não foi possível extrair os dados:',err)
    
        
    index = index + 1 #Passa para o próximo indice para conseguir dados da proxima SPARTA
    
    
  
#%%Tratamento de dados
#Remover dados duplicados
df_subsidios = df_subsidios.drop_duplicates(subset = 'CHAVE',ignore_index = True)
df_subsidios = df_subsidios.dropna(axis=0,how='all')

#Tratamento dos dados
df_subsidios = df_subsidios.astype('str')
for coluna_float in colunas_float:
    df_subsidios[coluna_float] = df_subsidios[coluna_float].replace('.',',').replace('nan',0).astype('float')
  
# Define a data de atualização dos dados
data = datetime.today().strftime('%d/%m/%Y')
df_subsidios['DATA_ATUALIZA'] = data   

#%% Inserir dados no banco de dados

#Criar a lista para inserção no banco SQL com os dados da base editada
dados_list = df_subsidios.values.tolist()


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
        sql = '''INSERT INTO ''' + tabela_oracle +''' VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        cursor.executemany(sql, dados_list)
    except Exception as err:
        print('Erro no Insert:', err)
    else:
        print('Carga executada com sucesso!')
        connection.commit() #Caso seja executado com sucesso, esse comando salva a base de dados
    finally:
        cursor.close()
        connection.close()

   

  




