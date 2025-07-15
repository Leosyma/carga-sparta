# -*- coding: utf-8 -*-
"""
Created on Mon Apr 17 14:18:45 2023

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


index = 0 #Flag para inserção dos dados
# abas_resultado = ['Resumo','Resultado']
abas_mercado = ['RA0','Calc_Mercado']

#Nome da tabela Oracle onde será dada a carga
tabela_oracle = 'PERSAS_INDICES'
ano_oracle = "'2023'"


#%% Montar o dataframe da estrutura
#Defino o indice máximo que é igual o número de persas
index_maximo = list(range(0,len(arquivos)))

#Criação da estrutura do tabela NUC
df_indices = pd.DataFrame(data = [], columns=['CHAVE','EVENTO_TARIFARIO','ANO','SARI','DISTRIBUIDORA','REGIAO','DATA','IRT_ECONOMICO_PERCENT','IRT_FINANCEIRO_PERCENT','IRT_FINAN_ECON_PERCENT','EFEITO_MEDIO_AT_PERCENT','EFEITO_MEDIO_BT_PERCENT','EFEITO_TARIFA_AT_BT_PERCENT','TARIFA_RESIDEN_B1_RS_MWH','ICMS_PERCENT','PIS_PERCENT'],index=index_maximo)

#Criação dataframe vazio
df_persas_capa = pd.DataFrame(data=[])
df_persas_resultado_antiga = pd.DataFrame(data=[])
df_persas_resultado_recente = pd.DataFrame(data=[])



# df_persas_capa = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA')
    

# for aba_mercado in abas_mercado:
#     try:
#         df_persas_mercado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = aba_mercado)
        
#     except Exception as err:
#         print('Aba Mercado não encontrada:',err)
     
# #PERSAS 2012 e 2013
# try:
#     df_persas_resultado_antiga = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Resumo'
#                                                                     ,nrows=10)
    
# except Exception as err:
#     print('Aba Resultado não encontrada:',err)
    

# # #PERSAS Após 2013    
# try:
#     df_persas_resultado_recente = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Resultado'
#                                                                     ,nrows=10)
    
# except Exception as err:
#     print('Aba Resultado não encontrada:',err)
      
        
# linhas_capa = range(len(df_persas_capa.index))
# colunas_capa = range(len(df_persas_capa.columns))
# linhas_mercado = range(len(df_persas_mercado.index))
# colunas_mercado = range(len(df_persas_mercado.columns))
# linhas_resultado_antiga = range(len(df_persas_resultado_antiga.index))
# colunas_resultado_antiga = range(len(df_persas_resultado_antiga.columns))
# linhas_resultado_recente = range(len(df_persas_resultado_recente.index))
# colunas_resultado_recente = range(len(df_persas_resultado_recente.columns))

# df_persas_capa = df_persas_capa.astype('str')
# df_persas_mercado = df_persas_mercado.astype('str')
# df_persas_resultado_antiga = df_persas_resultado_antiga.astype('str')
# df_persas_resultado_recente = df_persas_resultado_recente.astype('str')


#%%Funções
#Função para extrair dados de 'Indices'
def extrair_indices(df_indices,df_persas_resultado_antiga,df_persas_resultado_recente,df_persas_mercado,index):
    #Dados da aba 'Resultado'
    #PERSAS 2012 e 2013
    if (df_indices.at[index,'ANO'] == '2012') or (df_indices.at[index,'ANO'] == '2013'):
        for linha in linhas_resultado_antiga:
            for coluna in colunas_resultado_antiga:
                if (df_persas_resultado_antiga.iloc[linha,coluna].upper() == 'ÍNDICE DE REPOSICIONAMENTO TARIFÁRIO') or ('ÍNDICE DE REAJUSTE TARIFÁRIO' in df_persas_resultado_antiga.iloc[linha,coluna].upper()) or ('IRT ECONOMICO' in df_persas_resultado_antiga.iloc[linha,coluna].upper()) or (df_persas_resultado_antiga.iloc[linha,coluna].upper() == 'VARIAÇÃO ECONÔMICA'):
                    df_indices.at[index,'IRT_ECONOMICO_PERCENT'] = df_persas_resultado_antiga.iloc[linha,(coluna+1)]
                elif 'COMPONENTES FINANCEIROS' in df_persas_resultado_antiga.iloc[linha,coluna].upper():
                    df_indices.at[index,'IRT_FINANCEIRO_PERCENT'] = df_persas_resultado_antiga.iloc[linha,(coluna+1)]
                elif ('ÍNDICE DE REPOSICIONAMENTO TARIFÁRIO COM FINANCEIROS' in df_persas_resultado_antiga.iloc[linha,coluna].upper()) or ('REAJUSTE MÉDIO COM FINANCEIROS' in df_persas_resultado_antiga.iloc[linha,coluna].upper()) or ('IRT ECONOMICO E FINANCEIRO' in df_persas_resultado_antiga.iloc[linha,coluna].upper()) or ('IRT COM FINANCEIROS' in df_persas_resultado_antiga.iloc[linha,coluna].upper()) or (df_persas_resultado_antiga.iloc[linha,coluna].upper() == 'VARIAÇÃO ECONÔMICA E FINANCEIRA'):
                    df_indices.at[index,'IRT_FINAN_ECON_PERCENT'] = df_persas_resultado_antiga.iloc[linha,(coluna+1)]
                elif 'ALTA TENSÃO' in df_persas_resultado_antiga.iloc[linha,coluna].upper():
                    df_indices.at[index,'EFEITO_MEDIO_AT_PERCENT'] = df_persas_resultado_antiga.iloc[linha,(coluna+1)]
                elif 'BAIXA TENSÃO' in df_persas_resultado_antiga.iloc[linha,coluna].upper():
                    df_indices.at[index,'EFEITO_MEDIO_BT_PERCENT'] = df_persas_resultado_antiga.iloc[linha,(coluna+1)]
                elif ('A + B' in df_persas_resultado_antiga.iloc[linha,coluna].upper()) or ('AT+BT' in df_persas_resultado_antiga.iloc[linha,coluna].upper()):
                    df_indices.at[index,'EFEITO_TARIFA_AT_BT_PERCENT'] = df_persas_resultado_antiga.iloc[linha,(coluna+1)]
                elif 'TARIFA B1' in df_persas_resultado_antiga.iloc[linha,coluna].upper():
                    df_indices.at[index,'TARIFA_RESIDEN_B1_RS_MWH'] = df_persas_resultado_antiga.iloc[linha,(coluna+1)]       
    
    #PERSAS Após 2013
    else:
        for linha in linhas_resultado_recente:
            for coluna in colunas_resultado_recente:
                if (df_persas_resultado_recente.iloc[linha,coluna].upper() == 'ÍNDICE DE REPOSICIONAMENTO TARIFÁRIO') or (df_persas_resultado_recente.iloc[linha,coluna].upper() == 'ÍNDICE DE REPOSICIONAMENTO TARIFÁRIO - IRT') or ('ÍNDICE DE REAJUSTE TARIFÁRIO' in df_persas_resultado_recente.iloc[linha,coluna].upper()) or (df_persas_resultado_recente.iloc[linha,coluna].upper() == 'IRT ECONOMICO') or (df_persas_resultado_recente.iloc[linha,coluna].upper() == 'VARIAÇÃO ECONÔMICA'):
                    df_indices.at[index,'IRT_ECONOMICO_PERCENT'] = df_persas_resultado_recente.iloc[linha,(coluna+1)]
                elif (df_persas_resultado_recente.iloc[linha,coluna].upper() == 'COMPONENTES FINANCEIROS') or (df_persas_resultado_recente.iloc[linha,coluna].upper() == 'COMPONENTES FINANCEIROS (%)'):
                    df_indices.at[index,'IRT_FINANCEIRO_PERCENT'] = df_persas_resultado_recente.iloc[linha,(coluna+1)]
                elif ('ÍNDICE DE REPOSICIONAMENTO TARIFÁRIO COM FINANCEIROS' in df_persas_resultado_recente.iloc[linha,coluna].upper()) or ('REAJUSTE MÉDIO COM FINANCEIROS' in df_persas_resultado_recente.iloc[linha,coluna].upper()) or ('IRT ECONOMICO E FINANCEIRO' in df_persas_resultado_recente.iloc[linha,coluna].upper()) or ('IRT COM FINANCEIROS' in df_persas_resultado_recente.iloc[linha,coluna].upper()) or (df_persas_resultado_recente.iloc[linha,coluna].upper() == 'VARIAÇÃO ECONÔMICA E FINANCEIRA'):
                    df_indices.at[index,'IRT_FINAN_ECON_PERCENT'] = df_persas_resultado_recente.iloc[linha,(coluna+1)]
                elif 'ALTA TENSÃO' in df_persas_resultado_recente.iloc[linha,coluna].upper():
                    df_indices.at[index,'EFEITO_MEDIO_AT_PERCENT'] = df_persas_resultado_recente.iloc[linha,(coluna+1)]
                elif 'BAIXA TENSÃO' in df_persas_resultado_recente.iloc[linha,coluna].upper():
                    df_indices.at[index,'EFEITO_MEDIO_BT_PERCENT'] = df_persas_resultado_recente.iloc[linha,(coluna+1)]
                elif ('A + B' in df_persas_resultado_recente.iloc[linha,coluna].upper()) or ('AT+BT' in df_persas_resultado_recente.iloc[linha,coluna].upper()):
                    df_indices.at[index,'EFEITO_TARIFA_AT_BT_PERCENT'] = df_persas_resultado_recente.iloc[linha,(coluna+1)]
                elif 'TARIFA B1' in df_persas_resultado_recente.iloc[linha,coluna].upper():
                    df_indices.at[index,'TARIFA_RESIDEN_B1_RS_MWH'] = df_persas_resultado_recente.iloc[linha,(coluna+1)]
      
                    
    #Dados da aba 'Mercado'
    for linha in linhas_mercado:
        for coluna in colunas_mercado:
            if 'ICMS' in df_persas_mercado.iloc[linha,coluna].upper():
                df_indices.at[index,'ICMS_PERCENT'] = df_persas_mercado.iloc[linha,(coluna+1)]
            elif 'PIS/COFINS' in df_persas_mercado.iloc[linha,coluna].upper():
                df_indices.at[index,'PIS_PERCENT'] = df_persas_mercado.iloc[linha,(coluna+1)]


def distribuidora(df_indices,df_persas_capa,index):
    for linha in linhas_capa:
        for coluna in colunas_capa:
            #Determina o ANO
            if 'ANO DO PROCESSO' in df_persas_capa.iloc[linha,coluna].upper():
                df_indices.at[index,'ANO'] = df_persas_capa.iloc[linha,(coluna+1)]
                
            #Determina o SARI da Distribuidora
            elif 'SARI' in df_persas_capa.iloc[linha,coluna].upper():
                df_indices.at[index,'SARI'] = df_persas_capa.iloc[linha,(coluna+1)]
                
            #Determina a DATA
            elif 'REAJUSTE/REVISÃO SUGE' in df_persas_capa.iloc[linha,coluna].upper():
                df_indices.at[index,'DATA'] = df_persas_capa.iloc[linha,(coluna+1)].split(' ')[0]


    #Determina o EVENTO TARIFARIO
    for linha in linhas_capa:
        for coluna in colunas_capa:
            if 'REAJUSTE' in df_persas_capa.iloc[linha,coluna].upper():
                df_indices.at[index,'EVENTO_TARIFARIO'] = 'RTA'
                
            elif 'REVISÃO' in df_persas_capa.iloc[linha,coluna].upper():
                df_indices.at[index,'EVENTO_TARIFARIO'] = 'RTP'
                
            elif 'TARIFAS INICIAIS' in df_persas_capa.iloc[linha,coluna].upper():
                df_indices.at[index,'EVENTO_TARIFARIO'] = 'TI'
                

    #Determina o nome da DISTRIBUIDORA das PERSAS fora do layout padrão
    if (df_persas_capa.iloc[4,1].upper() == 'COOPERMILA') or (df_persas_capa.iloc[4,1].upper() == 'CERTREL'):
        df_indices.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[4,1].upper()
        
    elif (df_persas_capa.iloc[0,1].upper() == 'CERGAL'):
        df_indices.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[0,1].upper()
       
    #PERSAS no layout padrão
    else:
        for linha in linhas_capa:
            for coluna in colunas_capa:
                if df_persas_capa.iloc[linha,coluna].upper() == 'EMPRESA':
                    df_indices.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[linha,(coluna+1)].upper()

    #Determina a REGIAO
    if df_indices.at[index,'DISTRIBUIDORA'] == 'CERCOS':
        df_indices.at[index,'REGIAO'] = 'N/NE'
        
    else:
        df_indices.at[index,'REGIAO'] = 'S/SE/CO'

    #Determina a CHAVE
    df_indices.at[index,'CHAVE'] = df_indices.loc[index,'EVENTO_TARIFARIO']+df_indices.loc[index,'ANO']+df_indices.loc[index,'SARI']



#%%Inserção dos dados
#Abre a PERSAS de cada arquivo na pasta
for arquivo in arquivos:   
    try:
        df_persas_capa = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA')  

        for aba_mercado in abas_mercado:
            try:
                df_persas_mercado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = aba_mercado)
                
            except Exception as err:
                print('Aba Mercado não encontrada:',err)
             
        #PERSAS 2012 e 2013
        try:
            df_persas_resultado_antiga = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Resumo'
                                                                            ,nrows=10)
            
        except Exception as err:
            print('Aba Resultado não encontrada:',err)
            

        # #PERSAS Após 2013    
        try:
            df_persas_resultado_recente = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Resultado'
                                                                            ,nrows=10)
            
        except Exception as err:
            print('Aba Resultado não encontrada:',err)
                
                                                                
    except:
        print('Aba não disponível na PERSAS', arquivo)
 
    #Rodamos as funções
    else:
        try:
            print('Leu o arquivo: ',arquivo)
        
            #Converte as tabelas para string, pois não é possível comparar string com valor NaN
            df_persas_capa = df_persas_capa.astype('str')
            df_persas_mercado = df_persas_mercado.astype('str')
            df_persas_resultado_antiga = df_persas_resultado_antiga.astype('str')
            df_persas_resultado_recente = df_persas_resultado_recente.astype('str')
    
            
            #Define o intervalo máximo de linhas e colunas do dataframe 
            linhas_capa = range(len(df_persas_capa.index))
            colunas_capa = range(len(df_persas_capa.columns))
            linhas_mercado = range(len(df_persas_mercado.index))
            colunas_mercado = range(len(df_persas_mercado.columns))
            linhas_resultado_antiga = range(len(df_persas_resultado_antiga.index))
            colunas_resultado_antiga = range(len(df_persas_resultado_antiga.columns))
            linhas_resultado_recente = range(len(df_persas_resultado_recente.index))
            colunas_resultado_recente = range(len(df_persas_resultado_recente.columns))
        
            
            #Função para extrair os dados
            distribuidora(df_indices,df_persas_capa,index)
            extrair_indices(df_indices,df_persas_resultado_antiga,df_persas_resultado_recente,df_persas_mercado,index)
            
            print('Extraiu o dado do arquivo: ',arquivo)
            print('Arquivos extraídos: ',round((index/len(arquivos)),2)*100,'%')
    
        except Exception as err:
            print('Não foi possível extrair os dados:',err)
    
        
    index = index + 1 #Passa para o próximo indice para conseguir dados da proxima SPARTA


# df_indices.to_excel(r'C:\Users\2018459\OneDrive - CPFL Energia S A\Área de Trabalho\RTP e RTA (permissionarias)\Tabelas\PERSAS_INDICES.xlsx')


#%%Tratamento de dados
#Remover dados duplicados
df_indices = df_indices.drop_duplicates(subset = 'CHAVE',ignore_index = True)
df_indices = df_indices.dropna(axis=0,how='all')

#Tratamento dos dados
df_indices = df_indices.astype('str')
df_indices['IRT_ECONOMICO_PERCENT'] = df_indices['IRT_ECONOMICO_PERCENT'].replace('.',',').replace('nan',0).astype('float')
df_indices['IRT_FINANCEIRO_PERCENT'] = df_indices['IRT_FINANCEIRO_PERCENT'].replace('.',',').replace('nan',0).astype('float')
df_indices['IRT_FINAN_ECON_PERCENT'] = df_indices['IRT_FINAN_ECON_PERCENT'].replace('.',',').replace('nan',0).astype('float')
df_indices['EFEITO_MEDIO_AT_PERCENT'] = df_indices['EFEITO_MEDIO_AT_PERCENT'].replace('.',',').replace('nan',0).replace('n.a.',0).astype('float')
df_indices['EFEITO_MEDIO_BT_PERCENT'] = df_indices['EFEITO_MEDIO_BT_PERCENT'].replace('.',',').replace('nan',0).replace('n.a.',0).astype('float')
df_indices['EFEITO_TARIFA_AT_BT_PERCENT'] = df_indices['EFEITO_TARIFA_AT_BT_PERCENT'].replace('.',',').replace('nan',0).replace('n.a.',0).astype('float')
df_indices['TARIFA_RESIDEN_B1_RS_MWH'] = df_indices['TARIFA_RESIDEN_B1_RS_MWH'].replace('.',',').replace('nan',0).astype('float')
df_indices['ICMS_PERCENT'] = df_indices['ICMS_PERCENT'].replace('.',',').replace('nan',0).astype('float')
df_indices['PIS_PERCENT'] = df_indices['PIS_PERCENT'].replace('.',',').replace('nan',0).astype('float')

# Define a data de atualização dos dados
data = datetime.today().strftime('%d/%m/%Y')
df_indices['DATA_ATUALIZA'] = data  

#%% Inserir dados no banco de dados

#Criar a lista para inserção no banco SQL com os dados da base editada
dados_list = df_indices.values.tolist()


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






