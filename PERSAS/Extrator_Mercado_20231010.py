# -*- coding: utf-8 -*-
"""
Created on Tue Apr  4 08:11:56 2023

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
# pasta = r"C:\Users\2018459\OneDrive - CPFL Energia S A\Área de Trabalho\RTP e RTA (permissionarias)\PERSAS"

#Arquivos
#Como os dados das empresas estão em arquivos separados, listar todos que serão carregados conforme layout abaixo.
# arquivo ='CERRP_PERSAS_2019.xlsx'
arquivos = glob.glob(pasta)


index = 0 #Flag para inserção dos dados
abas_mercado = ['RA0','Calc_Mercado']

#Nome da tabela Oracle onde será dada a carga
tabela_oracle = 'PERSAS_MERCADO'
ano_oracle = "'2023'"


#%% Montar o dataframe da estrutura
#Defino o indice máximo que é igual o número de persas
index_maximo = list(range(0,len(arquivos)))

#Criação da estrutura do tabela NUC
df_mercado = pd.DataFrame(data = [], columns=['CHAVE','EVENTO_TARIFARIO','ANO','SARI','DISTRIBUIDORA','REGIAO','DATA','FORNECIMENTO_MWH','A1_MWH','A2_MWH','A3_MWH','A3A_MWH','A4_MWH','AS_MWH','BT_MWH','MERCADO_BASE_TOTAL_MWH','SUPRIMENTO_MWH','LIVRES_A1_MWH','DEMAIS_LIVRES_MWH','DISTRIBUICAO_MWH','GERADOR_MWH','TOTAL_MWH','NUC','VARIACAO_PERCENT'],index=index_maximo)

#Criação dataframe vazio
df_persas_capa = pd.DataFrame(data=[])
df_persas_mercado = pd.DataFrame(data=[])



# df_persas_capa = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA')


# for aba in abas_mercado:
#     try:
#         df_persas_mercado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = aba
#                                                                       ,nrows = 35
#                                                                       ,usecols = 'A:O')
        
#     except Exception as err:
#         print('Aba não disponível: ',err)


# df_persas_capa = df_persas_capa.astype('str')
# df_persas_mercado = df_persas_mercado.astype('str')


# linhas_capa = range(len(df_persas_capa.index))
# colunas_capa = range(len(df_persas_capa.columns))
# linhas_mercado = range(len(df_persas_mercado.index))
# colunas_mercado = range(len(df_persas_mercado.columns))



#%%Funções
#Função para extrair dados de 'Mercado'
def extrair_mercado(df_mercado,df_persas_mercado,index):
    for linha in linhas_mercado:
        for coluna in colunas_mercado:
            if 'FORNECIMENTO' in df_persas_mercado.iloc[linha,coluna].upper():
                df_mercado.at[index,'FORNECIMENTO_MWH'] = df_persas_mercado.iloc[linha,(coluna+1)]
            elif 'A1' in df_persas_mercado.iloc[linha,coluna].upper():
                df_mercado.at[index,'A1_MWH'] = df_persas_mercado.iloc[linha,(coluna+1)]
            elif 'A2' in df_persas_mercado.iloc[linha,coluna].upper():
                df_mercado.at[index,'A2_MWH'] = df_persas_mercado.iloc[linha,(coluna+1)]
            elif 'A3' in df_persas_mercado.iloc[linha,coluna].upper():
                df_mercado.at[index,'A3_MWH'] = df_persas_mercado.iloc[linha,(coluna+1)]
            if df_persas_mercado.iloc[linha,coluna].upper() == 'A3A':
                df_mercado.at[index,'A3A_MWH'] = df_persas_mercado.iloc[linha,(coluna+1)]
            elif df_persas_mercado.iloc[linha,coluna].upper() == 'A4':
                df_mercado.at[index,'A4_MWH'] = df_persas_mercado.iloc[linha,(coluna+1)]
            elif df_persas_mercado.iloc[linha,coluna].upper() == 'AS':
                df_mercado.at[index,'AS_MWH'] = df_persas_mercado.iloc[linha,(coluna+1)]
            elif ('BT' in df_persas_mercado.iloc[linha,coluna].upper()) or (df_persas_mercado.iloc[linha,coluna].upper() == 'B'):
                df_mercado.at[index,'BT_MWH'] = df_persas_mercado.iloc[linha,(coluna+1)]
            if 'MERCADOBASE' in df_persas_mercado.iloc[linha,coluna].upper():
                df_mercado.at[index,'MERCADO_BASE_TOTAL_MWH'] = df_persas_mercado.iloc[linha,(coluna+1)]
            elif 'SUPRIMENTO' in df_persas_mercado.iloc[linha,coluna].upper():
                df_mercado.at[index,'SUPRIMENTO_MWH'] = df_persas_mercado.iloc[linha,(coluna+1)]
            elif ('LIVRES A1' in df_persas_mercado.iloc[linha,coluna].upper()) or (df_persas_mercado.iloc[linha,coluna].upper() == 'LIVREA1'):
                df_mercado.at[index,'LIVRES_A1_MWH'] = df_persas_mercado.iloc[linha,(coluna+1)]
            elif ('LIVRES (DEMAIS)' in df_persas_mercado.iloc[linha,coluna].upper()) or (df_persas_mercado.iloc[linha,coluna].upper() == 'DEMAIS LIVRES'):
                df_mercado.at[index,'DEMAIS_LIVRES_MWH'] = df_persas_mercado.iloc[linha,(coluna+1)]
            elif 'DISTRIBUIÇÃO' in df_persas_mercado.iloc[linha,coluna].upper():
                df_mercado.at[index,'DISTRIBUICAO_MWH'] = df_persas_mercado.iloc[linha,(coluna+1)]
            elif ('GERADOR' in df_persas_mercado.iloc[linha,coluna].upper()) or ('GERAÇÃO' in df_persas_mercado.iloc[linha,coluna].upper()):
                df_mercado.at[index,'GERADOR_MWH'] = df_persas_mercado.iloc[linha,(coluna+1)]
            if df_persas_mercado.iloc[linha,coluna].upper() == 'TOTAL':
                df_mercado.at[index,'TOTAL_MWH'] = df_persas_mercado.iloc[linha,(coluna+1)]
            elif 'VARIAÇÃO' in df_persas_mercado.iloc[linha,coluna].upper():
                df_mercado.at[index,'VARIACAO_PERCENT'] = df_persas_mercado.iloc[linha,(coluna+1)]
       
            
    if (df_mercado.at[index,'ANO'] == '2012') or (df_mercado.at[index,'ANO'] == '2013'):
        for linha in linhas_mercado:
            for coluna in colunas_mercado:
                if 'CONSUMIDORES' in df_persas_mercado.iloc[linha,coluna].upper():
                    df_mercado.at[index,'NUC'] = df_persas_mercado.iloc[(linha+1),(coluna+1)]
    else:
        for linha in linhas_mercado:
            for coluna in colunas_mercado:
                if 'CONSUMIDORAS' in df_persas_mercado.iloc[linha,coluna].upper():
                    df_mercado.at[index,'NUC'] = df_persas_mercado.iloc[linha,(coluna+1)]
   
                    
        

def distribuidora(df_mercado,df_persas_capa,index):
    for linha in linhas_capa:
        for coluna in colunas_capa:
            #Determina o ANO
            if 'ANO DO PROCESSO' in df_persas_capa.iloc[linha,coluna].upper():
                df_mercado.at[index,'ANO'] = df_persas_capa.iloc[linha,(coluna+1)]
                
            #Determina o SARI da Distribuidora
            elif 'SARI' in df_persas_capa.iloc[linha,coluna].upper():
                df_mercado.at[index,'SARI'] = df_persas_capa.iloc[linha,(coluna+1)]
                
            #Determina a DATA
            elif 'REAJUSTE/REVISÃO SUGE' in df_persas_capa.iloc[linha,coluna].upper():
                df_mercado.at[index,'DATA'] = df_persas_capa.iloc[linha,(coluna+1)].split(' ')[0]


    #Determina o EVENTO TARIFARIO
    for linha in linhas_capa:
        for coluna in colunas_capa:
            if 'REAJUSTE' in df_persas_capa.iloc[linha,coluna].upper():
                df_mercado.at[index,'EVENTO_TARIFARIO'] = 'RTA'
                
            elif 'REVISÃO' in df_persas_capa.iloc[linha,coluna].upper():
                df_mercado.at[index,'EVENTO_TARIFARIO'] = 'RTP'
                
            elif 'TARIFAS INICIAIS' in df_persas_capa.iloc[linha,coluna].upper():
                df_mercado.at[index,'EVENTO_TARIFARIO'] = 'TI'
                

    #Determina o nome da DISTRIBUIDORA das PERSAS fora do layout padrão
    if (df_persas_capa.iloc[4,1].upper() == 'COOPERMILA') or (df_persas_capa.iloc[5,1].upper() == 'CERTREL'):
        df_mercado.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[4,1].upper()
        
    elif (df_persas_capa.iloc[0,1].upper() == 'CERGAL'):
        df_mercado.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[0,1].upper()
       
    #PERSAS no layout padrão
    else:
        for linha in linhas_capa:
            for coluna in colunas_capa:
                if df_persas_capa.iloc[linha,coluna].upper() == 'EMPRESA':
                    df_mercado.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[linha,(coluna+1)].upper()

    #Determina a REGIAO
    if df_mercado.at[index,'DISTRIBUIDORA'] == 'CERCOS':
        df_mercado.at[index,'REGIAO'] = 'N/NE'
        
    else:
        df_mercado.at[index,'REGIAO'] = 'S/SE/CO'

    #Determina a CHAVE
    df_mercado.at[index,'CHAVE'] = df_mercado.loc[index,'EVENTO_TARIFARIO']+df_mercado.loc[index,'ANO']+df_mercado.loc[index,'SARI']



#%%Inserção dos dados
#Abre a PERSAS de cada arquivo na pasta
for arquivo in arquivos:   
    try:
        df_persas_capa = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA')  

        for aba in abas_mercado:
            try:
                df_persas_mercado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = aba
                                                                     ,nrows = 35
                                                                     ,usecols = 'A:O')
        
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
            df_persas_mercado = df_persas_mercado.astype('str')
    
            
            #Define o intervalo máximo de linhas e colunas do dataframe 
            linhas_capa = range(len(df_persas_capa.index))
            colunas_capa = range(len(df_persas_capa.columns))
            linhas_mercado = range(len(df_persas_mercado.index))
            colunas_mercado = range(len(df_persas_mercado.columns))

            
            
            #Função para extrair os dados
            distribuidora(df_mercado,df_persas_capa,index)
            extrair_mercado(df_mercado,df_persas_mercado,index)
            
            print('Extraiu o dado do arquivo: ',arquivo)
            print('Arquivos extraídos: ',round((index/len(arquivos)),2)*100,'%')
    
        except Exception as err:
            print('Não foi possível extrair os dados:',err)
    
        
    index = index + 1 #Passa para o próximo indice para conseguir dados da proxima SPARTA




#%%Tratamento de dados
#Remover dados duplicados
df_mercado = df_mercado.drop_duplicates(subset = 'CHAVE',ignore_index = True)
df_mercado = df_mercado.dropna(axis=0,how='all')

#Tratamento dos dados
df_mercado = df_mercado.astype('str')
df_mercado['FORNECIMENTO_MWH'] = df_mercado['FORNECIMENTO_MWH'].replace('.',',').replace('nan',0).astype('float')
df_mercado['A1_MWH'] = df_mercado['A1_MWH'].replace('.',',').replace('nan',0).astype('float')
df_mercado['A2_MWH'] = df_mercado['A2_MWH'].replace('.',',').replace('nan',0).astype('float')
df_mercado['A3_MWH'] = df_mercado['A3_MWH'].replace('.',',').replace('nan',0).astype('float')
df_mercado['A3A_MWH'] = df_mercado['A3A_MWH'].replace('.',',').replace('nan',0).astype('float')
df_mercado['A4_MWH'] = df_mercado['A4_MWH'].replace('.',',').replace('nan',0).astype('float')
df_mercado['AS_MWH'] = df_mercado['AS_MWH'].replace('.',',').replace('nan',0).astype('float')
df_mercado['BT_MWH'] = df_mercado['BT_MWH'].replace('.',',').replace('nan',0).astype('float')
df_mercado['MERCADO_BASE_TOTAL_MWH'] = df_mercado['MERCADO_BASE_TOTAL_MWH'].replace('.',',').replace('nan',0).astype('float')
df_mercado['SUPRIMENTO_MWH'] = df_mercado['SUPRIMENTO_MWH'].replace('.',',').replace('nan',0).astype('float')
df_mercado['LIVRES_A1_MWH'] = df_mercado['LIVRES_A1_MWH'].replace('.',',').replace('nan',0).astype('float')
df_mercado['DEMAIS_LIVRES_MWH'] = df_mercado['DEMAIS_LIVRES_MWH'].replace('.',',').replace('nan',0).astype('float')
df_mercado['DISTRIBUICAO_MWH'] = df_mercado['DISTRIBUICAO_MWH'].replace('.',',').replace('nan',0).astype('float')
df_mercado['GERADOR_MWH'] = df_mercado['GERADOR_MWH'].replace('.',',').replace('nan',0).astype('float')
df_mercado['TOTAL_MWH'] = df_mercado['TOTAL_MWH'].replace('.',',').replace('nan',0).astype('float')
df_mercado['NUC'] = df_mercado['NUC'].replace('.',',').replace('nan',0).astype('int')
df_mercado['VARIACAO_PERCENT'] = df_mercado['VARIACAO_PERCENT'].replace('.',',').replace('nan',0).astype('float')
 
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






