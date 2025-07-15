# -*- coding: utf-8 -*-
"""
Created on Thu Apr  6 08:28:42 2023

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
abas_receita = ['RA0','Calc_Mercado']

#Nome da tabela Oracle onde será dada a carga
tabela_oracle = 'PERSAS_RECEITA'
ano_oracle = "'2023'"


#%% Montar o dataframe da estrutura
#Defino o indice máximo que é igual o número de persas
index_maximo = list(range(0,len(arquivos)))

#Criação da estrutura do tabela NUC
df_receita = pd.DataFrame(data = [], columns=['CHAVE','EVENTO_TARIFARIO','ANO','SARI','DISTRIBUIDORA','REGIAO','DATA','FORNECIMENTO_RS','A1_RS','A2_RS','A3_RS','A3A_RS','A4_RS','AS_RS','BT_RS','MERCADO_BASE_TOTAL_RS','SUPRIMENTO_RS','LIVRES_A1_RS','DEMAIS_LIVRES_RS','DISTRIBUICAO_RS','GERADOR_RS','TOTAL_RS','RESIDENCIAL_RS','INDUSTRIAL_RS','COMERCIAL_RS','RURAL_RS','ILUMINACAO_RS','PODER_PUBLICO_RS','SERVICO_PUBLICO_RS','DEMAIS_RS'],index=index_maximo)

#Criação dataframe vazio
df_persas_capa = pd.DataFrame(data=[])
df_persas_receita = pd.DataFrame(data=[])



# df_persas_capa = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA')


# for aba in abas_receita:
#     try:
#         df_persas_receita = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = aba
#                                                                       ,nrows = 35
#                                                                       ,usecols = 'A:O')
        
#     except Exception as err:
#         print('Aba não disponível: ',err)


# df_persas_capa = df_persas_capa.astype('str')
# df_persas_receita = df_persas_receita.astype('str')


# linhas_capa = range(len(df_persas_capa.index))
# colunas_capa = range(len(df_persas_capa.columns))
# linhas_receita = range(len(df_persas_receita.index))
# colunas_receita = range(len(df_persas_receita.columns))



#%%Funções
#Função para extrair dados da 'Receita'
def extrair_receita(df_receita,df_persas_receita,index):
    for linha in linhas_receita:
        for coluna in colunas_receita:
            if 'FORNECIMENTO' in df_persas_receita.iloc[linha,coluna].upper():
                df_receita.at[index,'FORNECIMENTO_RS'] = df_persas_receita.iloc[linha,(coluna+2)]
            elif 'A1' in df_persas_receita.iloc[linha,coluna].upper():
                df_receita.at[index,'A1_RS'] = df_persas_receita.iloc[linha,(coluna+2)]
            elif 'A2' in df_persas_receita.iloc[linha,coluna].upper():
                df_receita.at[index,'A2_RS'] = df_persas_receita.iloc[linha,(coluna+2)]
            elif 'A3' in df_persas_receita.iloc[linha,coluna].upper():
                df_receita.at[index,'A3_RS'] = df_persas_receita.iloc[linha,(coluna+2)]
            if df_persas_receita.iloc[linha,coluna].upper() == 'A3A':
                df_receita.at[index,'A3A_RS'] = df_persas_receita.iloc[linha,(coluna+2)]
            elif df_persas_receita.iloc[linha,coluna].upper() == 'A4':
                df_receita.at[index,'A4_RS'] = df_persas_receita.iloc[linha,(coluna+2)]
            elif df_persas_receita.iloc[linha,coluna].upper() == 'AS':
                df_receita.at[index,'AS_RS'] = df_persas_receita.iloc[linha,(coluna+2)]
            elif ('BT' in df_persas_receita.iloc[linha,coluna].upper()) or (df_persas_receita.iloc[linha,coluna].upper() == 'B'):
                df_receita.at[index,'BT_RS'] = df_persas_receita.iloc[linha,(coluna+2)]
            if 'MERCADOBASE' in df_persas_receita.iloc[linha,coluna].upper():
                df_receita.at[index,'MERCADO_BASE_TOTAL_RS'] = df_persas_receita.iloc[linha,(coluna+2)]
            elif 'SUPRIMENTO' in df_persas_receita.iloc[linha,coluna].upper():
                df_receita.at[index,'SUPRIMENTO_RS'] = df_persas_receita.iloc[linha,(coluna+2)]
            elif ('LIVRES A1' in df_persas_receita.iloc[linha,coluna].upper()) or (df_persas_receita.iloc[linha,coluna].upper() == 'LIVREA1'):
                df_receita.at[index,'LIVRES_A1_RS'] = df_persas_receita.iloc[linha,(coluna+2)]
            elif ('LIVRES (DEMAIS)' in df_persas_receita.iloc[linha,coluna].upper()) or (df_persas_receita.iloc[linha,coluna].upper() == 'DEMAIS LIVRES'):
                df_receita.at[index,'DEMAIS_LIVRES_RS'] = df_persas_receita.iloc[linha,(coluna+2)]
            elif 'DISTRIBUIÇÃO' in df_persas_receita.iloc[linha,coluna].upper():
                df_receita.at[index,'DISTRIBUICAO_RS'] = df_persas_receita.iloc[linha,(coluna+2)]
            elif ('GERADOR' in df_persas_receita.iloc[linha,coluna].upper()) or ('GERAÇÃO' in df_persas_receita.iloc[linha,coluna].upper()):
                df_receita.at[index,'GERADOR_RS'] = df_persas_receita.iloc[linha,(coluna+2)]
            if df_persas_receita.iloc[linha,coluna].upper() == 'TOTAL':
                #Temos a string TOTAL' duplicado, não queremos o 'TOTAL' da linha 17 e coluna 5
                if linha == 17 and coluna == 5:
                    continue
                #'TOTAL' da receita desejado
                else:
                    df_receita.at[index,'TOTAL_RS'] = df_persas_receita.iloc[linha,(coluna+2)]
            
            
    for linha in linhas_receita:
        for coluna in colunas_receita:
            if df_persas_receita.iloc[linha,coluna].upper() == 'RESIDENCIAL':
                df_receita.at[index,'RESIDENCIAL_RS'] = df_persas_receita.iloc[linha,(coluna+2)]
            elif df_persas_receita.iloc[linha,coluna].upper() == 'INDUSTRIAL':
                df_receita.at[index,'INDUSTRIAL_RS'] = df_persas_receita.iloc[linha,(coluna+2)]
            elif 'COMERCIAL' in df_persas_receita.iloc[linha,coluna].upper():
                df_receita.at[index,'COMERCIAL_RS'] = df_persas_receita.iloc[linha,(coluna+2)]
            elif df_persas_receita.iloc[linha,coluna].upper() == 'RURAL':
                df_receita.at[index,'RURAL_RS'] = df_persas_receita.iloc[linha,(coluna+2)]
            elif 'ILUMINAÇÃO PÚBLICA' in df_persas_receita.iloc[linha,coluna].upper():
                df_receita.at[index,'ILUMINACAO_RS'] = df_persas_receita.iloc[linha,(coluna+2)]
            elif 'PODER PÚBLICO' in df_persas_receita.iloc[linha,coluna].upper():
                df_receita.at[index,'PODER_PUBLICO_RS'] = df_persas_receita.iloc[linha,(coluna+2)]
            elif 'SERVIÇO PÚBLICO' in df_persas_receita.iloc[linha,coluna].upper():
                df_receita.at[index,'SERVICO_PUBLICO_RS'] = df_persas_receita.iloc[linha,(coluna+2)]
            elif df_persas_receita.iloc[linha,coluna].upper() == 'DEMAIS':
                df_receita.at[index,'DEMAIS_RS'] = df_persas_receita.iloc[linha,(coluna+2)]


def distribuidora(df_receita,df_persas_capa,index):
    for linha in linhas_capa:
        for coluna in colunas_capa:
            #Determina o ANO
            if 'ANO DO PROCESSO' in df_persas_capa.iloc[linha,coluna].upper():
                df_receita.at[index,'ANO'] = df_persas_capa.iloc[linha,(coluna+1)]
                
            #Determina o SARI da Distribuidora
            elif 'SARI' in df_persas_capa.iloc[linha,coluna].upper():
                df_receita.at[index,'SARI'] = df_persas_capa.iloc[linha,(coluna+1)]
                
            #Determina a DATA
            elif 'REAJUSTE/REVISÃO SUGE' in df_persas_capa.iloc[linha,coluna].upper():
                df_receita.at[index,'DATA'] = df_persas_capa.iloc[linha,(coluna+1)].split(' ')[0]


    #Determina o EVENTO TARIFARIO
    for linha in linhas_capa:
        for coluna in colunas_capa:
            if 'REAJUSTE' in df_persas_capa.iloc[linha,coluna].upper():
                df_receita.at[index,'EVENTO_TARIFARIO'] = 'RTA'
                
            elif 'REVISÃO' in df_persas_capa.iloc[linha,coluna].upper():
                df_receita.at[index,'EVENTO_TARIFARIO'] = 'RTP'
                
            elif 'TARIFAS INICIAIS' in df_persas_capa.iloc[linha,coluna].upper():
                df_receita.at[index,'EVENTO_TARIFARIO'] = 'TI'
                

    #Determina o nome da DISTRIBUIDORA das PERSAS fora do layout padrão
    if (df_persas_capa.iloc[4,1].upper() == 'COOPERMILA') or (df_persas_capa.iloc[5,1].upper() == 'CERTREL'):
        df_receita.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[4,1].upper()
        
    elif (df_persas_capa.iloc[0,1].upper() == 'CERGAL'):
        df_receita.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[0,1].upper()
       
    #PERSAS no layout padrão
    else:
        for linha in linhas_capa:
            for coluna in colunas_capa:
                if df_persas_capa.iloc[linha,coluna].upper() == 'EMPRESA':
                    df_receita.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[linha,(coluna+1)].upper()

    #Determina a REGIAO
    if df_receita.at[index,'DISTRIBUIDORA'] == 'CERCOS':
        df_receita.at[index,'REGIAO'] = 'N/NE'
        
    else:
        df_receita.at[index,'REGIAO'] = 'S/SE/CO'

    #Determina a CHAVE
    df_receita.at[index,'CHAVE'] = df_receita.loc[index,'EVENTO_TARIFARIO']+df_receita.loc[index,'ANO']+df_receita.loc[index,'SARI']



#%%Inserção dos dados
#Abre a PERSAS de cada arquivo na pasta
for arquivo in arquivos:   
    try:
        df_persas_capa = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA')  

        for aba in abas_receita:
            try:
                df_persas_receita = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = aba
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
            df_persas_receita = df_persas_receita.astype('str')
    
            
            #Define o intervalo máximo de linhas e colunas do dataframe 
            linhas_capa = range(len(df_persas_capa.index))
            colunas_capa = range(len(df_persas_capa.columns))
            linhas_receita = range(len(df_persas_receita.index))
            colunas_receita = range(len(df_persas_receita.columns))

            
            
            #Função para extrair os dados
            distribuidora(df_receita,df_persas_capa,index)
            extrair_receita(df_receita,df_persas_receita,index)
            
            print('Extraiu o dado do arquivo: ',arquivo)
            print('Arquivos extraídos: ',round((index/len(arquivos)),2)*100,'%')
    
        except Exception as err:
            print('Não foi possível extrair os dados:',err)
    
        
    index = index + 1 #Passa para o próximo indice para conseguir dados da proxima SPARTA



#df_receita.to_excel(r'C:\Users\2018459\OneDrive - CPFL Energia S A\Área de Trabalho\RTP e RTA (permissionarias)\Tabelas\PERSAS_RECEITA.xlsx')


#%%Tratamento de dados
#Remover dados duplicados
df_receita = df_receita.drop_duplicates(subset = 'CHAVE',ignore_index = True)
df_receita = df_receita.dropna(axis=0,how='all')

#Tratamento dos dados
df_receita = df_receita.astype('str')
df_receita['FORNECIMENTO_RS'] = df_receita['FORNECIMENTO_RS'].replace('.',',').replace('nan',0).astype('float')
df_receita['A1_RS'] = df_receita['A1_RS'].replace('.',',').replace('nan',0).astype('float')
df_receita['A2_RS'] = df_receita['A2_RS'].replace('.',',').replace('nan',0).astype('float')
df_receita['A3_RS'] = df_receita['A3_RS'].replace('.',',').replace('nan',0).astype('float')
df_receita['A3A_RS'] = df_receita['A3A_RS'].replace('.',',').replace('nan',0).astype('float')
df_receita['A4_RS'] = df_receita['A4_RS'].replace('.',',').replace('nan',0).astype('float')
df_receita['AS_RS'] = df_receita['AS_RS'].replace('.',',').replace('nan',0).astype('float')
df_receita['BT_RS'] = df_receita['BT_RS'].replace('.',',').replace('nan',0).astype('float')
df_receita['MERCADO_BASE_TOTAL_RS'] = df_receita['MERCADO_BASE_TOTAL_RS'].replace('.',',').replace('nan',0).astype('float')
df_receita['SUPRIMENTO_RS'] = df_receita['SUPRIMENTO_RS'].replace('.',',').replace('nan',0).astype('float')
df_receita['LIVRES_A1_RS'] = df_receita['LIVRES_A1_RS'].replace('.',',').replace('nan',0).astype('float')
df_receita['DEMAIS_LIVRES_RS'] = df_receita['DEMAIS_LIVRES_RS'].replace('.',',').replace('nan',0).astype('float')
df_receita['DISTRIBUICAO_RS'] = df_receita['DISTRIBUICAO_RS'].replace('.',',').replace('nan',0).astype('float')
df_receita['GERADOR_RS'] = df_receita['GERADOR_RS'].replace('.',',').replace('nan',0).astype('float')
df_receita['TOTAL_RS'] = df_receita['TOTAL_RS'].replace('.',',').replace('nan',0).astype('float')
df_receita['RESIDENCIAL_RS'] = df_receita['RESIDENCIAL_RS'].replace('.',',').replace('nan',0).astype('float')
df_receita['INDUSTRIAL_RS'] = df_receita['INDUSTRIAL_RS'].replace('.',',').replace('nan',0).astype('float')
df_receita['COMERCIAL_RS'] = df_receita['COMERCIAL_RS'].replace('.',',').replace('nan',0).astype('float')
df_receita['RURAL_RS'] = df_receita['RURAL_RS'].replace('.',',').replace('nan',0).astype('float')
df_receita['ILUMINACAO_RS'] = df_receita['ILUMINACAO_RS'].replace('.',',').replace('nan',0).astype('float')
df_receita['PODER_PUBLICO_RS'] = df_receita['PODER_PUBLICO_RS'].replace('.',',').replace('nan',0).astype('float')
df_receita['SERVICO_PUBLICO_RS'] = df_receita['SERVICO_PUBLICO_RS'].replace('.',',').replace('nan',0).astype('float')
df_receita['DEMAIS_RS'] = df_receita['DEMAIS_RS'].replace('.',',').replace('nan',0).astype('float')
 
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
        sql = '''INSERT INTO ''' + tabela_oracle +''' VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26,:27,:28,:29,:30,:31)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        cursor.executemany(sql, dados_list)
    except Exception as err:
        print('Erro no Insert:', err)
    else:
        print('Carga executada com sucesso!')
        connection.commit() #Caso seja executado com sucesso, esse comando salva a base de dados
    finally:
        cursor.close()
        connection.close()



 