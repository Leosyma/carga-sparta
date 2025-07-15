# -*- coding: utf-8 -*-
"""
Created on Mon Apr 10 14:21:17 2023

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
# pasta = r"C:\Users\2018459\OneDrive - CPFL Energia S A\Área de Trabalho\RTP e RTA (permissionarias)\PERSAS"

#Arquivos
#Como os dados das empresas estão em arquivos separados, listar todos que serão carregados conforme layout abaixo.
# arquivo ='PERSAS_CERTREL2013.xlsx'
arquivos = glob.glob(pasta)


index = 0 #Flag para inserção dos dados

#Nome da tabela Oracle onde será dada a carga
tabela_oracle = 'PERSAS_FATOR_X'
ano_oracle = "'2023'"


#%% Montar o dataframe da estrutura
#Defino o indice máximo que é igual o número de persas
index_maximo = list(range(0,len(arquivos)))

#Criação da estrutura do tabela NUC
df_fatorx = pd.DataFrame(data = [], columns=['CHAVE','EVENTO_TARIFARIO','ANO','SARI','DISTRIBUIDORA','REGIAO','DATA','COMPONENTE_PD_PERCENT','COMPONENTE_T_PERCENT','FATOR_X_PERCENT','IGPM_PERCENT','IPCA_PERCENT'],index=index_maximo)

#Criação dataframe vazio
df_persas_capa = pd.DataFrame(data=[])
df_persas_fatorx = pd.DataFrame(data=[])
df_persas_fatorx2 = pd.DataFrame(data=[])
df_persas_fatorx3 = pd.DataFrame(data=[])



# df_persas_capa = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA'
#                                                           ,usecols = 'A:M')



# try:
#     df_persas_fatorx = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'VPB1'
#                                                                   ,header = 3
#                                                                   ,nrows = 4
#                                                                   ,usecols = 'F:G')
    
# except Exception as err:
#     print('Aba não disponível: ',err)
    


# try:
#     df_persas_fatorx2 = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Persada'
#                                                                   ,header = 69
#                                                                   ,nrows = 3
#                                                                   ,usecols = 'B:C')
    
# except Exception as err:
#     print('Aba não disponível: ',err)


# try:
#     df_persas_fatorx3 = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA'
#                                                           ,nrows = 60
#                                                           ,usecols = 'A:H')

# except Exception as err:
#     print('Aba não disponível: ',err)


# df_persas_capa = df_persas_capa.astype('str')
# df_persas_fatorx = df_persas_fatorx.astype('str')
# df_persas_fatorx2 = df_persas_fatorx2.astype('str')
# df_persas_fatorx3 = df_persas_fatorx3.astype('str')


# linhas_capa = range(len(df_persas_capa.index))
# colunas_capa = range(len(df_persas_capa.columns))
# linhas_fatorx = range(len(df_persas_fatorx.index))
# colunas_fatorx = range(len(df_persas_fatorx.columns))
# linhas_fatorx3 = range(len(df_persas_fatorx3.index))
# colunas_fatorx3 = range(len(df_persas_fatorx3.columns))


#%%Funções
#Função para extrair dados de 'Fator X'
def extrair_fatorx(df_fatorx,df_persas_fatorx,df_persas_fatorx2,df_persas_capa,index):
    # Função para extrair das PERSAS de 2012 e a PERSA CERTREL de 2013
    if (df_fatorx.at[index,'ANO'] == '2012') or (df_fatorx.at[index,'ANO'] == '2013' and df_fatorx.at[index,'SARI'] == '5369'):
        df_fatorx.at[index,'FATOR_X_PERCENT'] = df_persas_fatorx.iloc[0,1]
        df_fatorx.at[index,'COMPONENTE_PD_PERCENT'] = df_persas_fatorx.iloc[1,1]
        df_fatorx.at[index,'COMPONENTE_T_PERCENT'] = df_persas_fatorx.iloc[2,1]


    # Função para extrair da PERSA COOPERMILA 2013
    elif (df_fatorx.at[index,'ANO'] == '2013' and df_fatorx.at[index,'SARI'] == '5373'):
        df_fatorx.at[index,'FATOR_X_PERCENT'] = df_persas_fatorx2.iloc[0,1]
        df_fatorx.at[index,'COMPONENTE_PD_PERCENT'] = df_persas_fatorx2.iloc[1,1]
        df_fatorx.at[index,'COMPONENTE_T_PERCENT'] = df_persas_fatorx2.iloc[2,1]
       
        
    # Função para o restante das PERSAS
    else:
        for linha in linhas_capa:
            for coluna in colunas_capa:
                if df_persas_capa.iloc[linha,coluna].upper() == 'FATOR X':
                    #Se a célula estiver vazia na coluna+2 pegamos o valor na coluna+1
                    # if math.isnan(float(df_persas_capa.iloc[linha,(coluna+2)])):
                    if df_persas_capa.iloc[linha,(coluna+2)] == 'nan':
                        df_fatorx.at[index,'FATOR_X_PERCENT'] = df_persas_capa.iloc[linha,(coluna+1)]
                    else:
                        df_fatorx.at[index,'FATOR_X_PERCENT'] = df_persas_capa.iloc[linha,(coluna+2)]
                elif df_persas_capa.iloc[linha,coluna].upper() == 'PD':
                    df_fatorx.at[index,'COMPONENTE_PD_PERCENT'] = df_persas_capa.iloc[linha,(coluna+2)]
                elif df_persas_capa.iloc[linha,coluna].upper() == 'T':
                    df_fatorx.at[index,'COMPONENTE_T_PERCENT'] = df_persas_capa.iloc[linha,(coluna+2)]

                    
    # Função para extrair dados de IGPM e IPCA
    for linha in linhas_capa:
        for coluna in colunas_capa:
            if (df_persas_capa.iloc[linha,coluna].upper() == 'IGPM PERÍODO DE REFERÊNCIA') or (df_persas_capa.iloc[linha,coluna].upper() == 'IGPM PARA O PERÍODO DE REFERÊNCIA'):
                df_fatorx.at[index,'IGPM_PERCENT'] = df_persas_capa.iloc[linha,(coluna+1)]
            elif (df_persas_capa.iloc[linha,coluna].upper() == 'IPCA PERÍODO DE REFERÊNCIA') or (df_persas_capa.iloc[linha,coluna].upper() == 'IPCA PARA O PERÍODO DE REFERÊNCIA'):
                df_fatorx.at[index,'IPCA_PERCENT'] = df_persas_capa.iloc[linha,(coluna+1)]
            

def distribuidora(df_fatorx,df_persas_capa,index):
    for linha in linhas_capa:
        for coluna in colunas_capa:
            #Determina o ANO
            if 'ANO DO PROCESSO' in df_persas_capa.iloc[linha,coluna].upper():
                df_fatorx.at[index,'ANO'] = df_persas_capa.iloc[linha,(coluna+1)]
                
            #Determina o SARI da Distribuidora
            elif 'SARI' in df_persas_capa.iloc[linha,coluna].upper():
                df_fatorx.at[index,'SARI'] = df_persas_capa.iloc[linha,(coluna+1)]
                
            #Determina a DATA
            elif 'REAJUSTE/REVISÃO SUGE' in df_persas_capa.iloc[linha,coluna].upper():
                df_fatorx.at[index,'DATA'] = df_persas_capa.iloc[linha,(coluna+1)].split(' ')[0]


    #Determina o EVENTO TARIFARIO
    for linha in linhas_capa:
        for coluna in colunas_capa:
            if 'REAJUSTE' in df_persas_capa.iloc[linha,coluna].upper():
                df_fatorx.at[index,'EVENTO_TARIFARIO'] = 'RTA'
                
            elif 'REVISÃO' in df_persas_capa.iloc[linha,coluna].upper():
                df_fatorx.at[index,'EVENTO_TARIFARIO'] = 'RTP'
                
            elif 'TARIFAS INICIAIS' in df_persas_capa.iloc[linha,coluna].upper():
                df_fatorx.at[index,'EVENTO_TARIFARIO'] = 'TI'
                

    #Determina o nome da DISTRIBUIDORA das PERSAS fora do layout padrão
    if (df_persas_capa.iloc[4,1].upper() == 'COOPERMILA') or (df_persas_capa.iloc[4,1].upper() == 'CERTREL'):
        df_fatorx.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[4,1].upper()
        
    elif (df_persas_capa.iloc[0,1].upper() == 'CERGAL'):
        df_fatorx.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[0,1].upper()
       
    #PERSAS no layout padrão
    else:
        for linha in linhas_capa:
            for coluna in colunas_capa:
                if df_persas_capa.iloc[linha,coluna].upper() == 'EMPRESA':
                    df_fatorx.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[linha,(coluna+1)].upper()

    #Determina a REGIAO
    if df_fatorx.at[index,'DISTRIBUIDORA'] == 'CERCOS':
        df_fatorx.at[index,'REGIAO'] = 'N/NE'
        
    else:
        df_fatorx.at[index,'REGIAO'] = 'S/SE/CO'

    #Determina a CHAVE
    df_fatorx.at[index,'CHAVE'] = df_fatorx.loc[index,'EVENTO_TARIFARIO']+df_fatorx.loc[index,'ANO']+df_fatorx.loc[index,'SARI']



#%%Inserção dos dados
#Abre a PERSAS de cada arquivo na pasta
for arquivo in arquivos:   
    try:
        df_persas_capa = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA'
                                                                  ,usecols = 'A:M')  

        try:
            df_persas_fatorx = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'VPB1'
                                                                  ,header = 3
                                                                  ,nrows = 4
                                                                  ,usecols = 'F:G')
    
        except Exception as err:
            print('Aba não disponível: ',err)
    


        try:
            df_persas_fatorx2 = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Persada'
                                                                  ,header = 69
                                                                  ,nrows = 3
                                                                  ,usecols = 'B:C')
    
        except Exception as err:
            print('Aba não disponível: ',err)
                
                                                                
    except:
        print('Aba não disponível na PERSAS', arquivo)
 
    #Rodamos as funções
    else:
        try:
            print('Leu o arquivo: ',arquivo)
        
            #Converte as tabelas para string, pois não é possível comparar string com valor NaN
            df_persas_capa = df_persas_capa.astype('str')
            df_persas_fatorx = df_persas_fatorx.astype('str')
            df_persas_fatorx2 = df_persas_fatorx2.astype('str')
    
            
            #Define o intervalo máximo de linhas e colunas do dataframe 
            linhas_capa = range(len(df_persas_capa.index))
            colunas_capa = range(len(df_persas_capa.columns))
        
            
            #Função para extrair os dados
            distribuidora(df_fatorx,df_persas_capa,index)
            extrair_fatorx(df_fatorx,df_persas_fatorx,df_persas_fatorx2,df_persas_capa,index)
            
            print('Extraiu o dado do arquivo: ',arquivo)
            print('Arquivos extraídos: ',round((index/len(arquivos)),2)*100,'%')
    
        except Exception as err:
            print('Não foi possível extrair os dados:',err)
    
        
    index = index + 1 #Passa para o próximo indice para conseguir dados da proxima SPARTA



# df_fatorx.to_excel(r'C:\Users\2018459\OneDrive - CPFL Energia S A\Área de Trabalho\RTP e RTA (permissionarias)\Tabelas\PERSAS_FATOR_X.xlsx')



#%%Tratamento de dados
#Remover dados duplicados
df_fatorx = df_fatorx.drop_duplicates(subset = 'CHAVE',ignore_index = True)
df_fatorx = df_fatorx.dropna(axis=0,how='all')

#Tratamento dos dados
df_fatorx = df_fatorx.astype('str')
df_fatorx['COMPONENTE_PD_PERCENT'] = df_fatorx['COMPONENTE_PD_PERCENT'].replace('.',',').replace('nan',0).astype('float')
df_fatorx['COMPONENTE_T_PERCENT'] = df_fatorx['COMPONENTE_T_PERCENT'].replace('.',',').replace('nan',0).astype('float')
df_fatorx['FATOR_X_PERCENT'] = df_fatorx['FATOR_X_PERCENT'].replace('.',',').replace('nan',0).astype('float')
df_fatorx['IGPM_PERCENT'] = df_fatorx['IGPM_PERCENT'].replace('.',',').replace('nan',0).astype('float')
df_fatorx['IPCA_PERCENT'] = df_fatorx['IPCA_PERCENT'].replace('.',',').replace('nan',0).astype('float')

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
        sql = '''INSERT INTO ''' + tabela_oracle +''' VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        cursor.executemany(sql, dados_list)
    except Exception as err:
        print('Erro no Insert:', err)
    else:
        print('Carga executada com sucesso!')
        connection.commit() #Caso seja executado com sucesso, esse comando salva a base de dados
    finally:
        cursor.close()
        connection.close()










