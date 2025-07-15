# -*- coding: utf-8 -*-
"""
Created on Thu May  4 11:04:52 2023

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
# arquivo ='PERSAS_CERIM_2014.xlsx'
arquivos = glob.glob(pasta)

abas_mercado = ['RA0','Calc_Mercado']
colunas_float=['TFSEE_RS','RGR_RS','CCC_RS','CDE_RS','CFURH_RS','ESS_EER_RS','PROINFA_RS','ONS_RS','PD_RS','ENCARGOS_RS','REDE_BASICA_RS','REDE_BASICA_FRONTEIRA_RS','REDE_BASICA_ONS_A2_RS','REDE_BASICA_EXPORT_A2_RS','CONEXAO_RS','MUST_ITAIPU_RS','TRANSPORTE_ITAIPU_RS','SISTEMA_DISTRIBUICAO_RS','TRANSPORTE_RS','ENERGIA_COMPRADA_RS','PARCELA_A_RS','PARCELA_B_RS','RA1_RS','ENERGIA_COMPRADA_MWH','FORNECIMENTO_SUPRIMENTO_MWH','FORNECIMENTO_MWH','SUPRIMENTO_MWH','CUSTO_MEDIO_RS_MWH','PERDA_NAO_TECNICA_PERCENT','PERDA_TECNICA_PERCENT','PERDA_DISTRIBUICAO_PERCENT','PERDA_REDE_BASICA_PERCENT','PERDAS_REGULATORIAS_MWH','PERDA_NAO_TECNICA_MWH','PERDA_TECNICA_MWH','PERDA_DISTRIBUICAO_MWH','PERDA_RB_DISTRIBUICAO_MWH','PERDA_RB_CATIVO_MWH']
index = 0 #Flag para inserção dos dados

#Nome da tabela Oracle onde será dada a carga
tabela_oracle = 'PERSAS_DRA'
ano_oracle = "'2023'"


#%% Montar o dataframe da estrutura
#Defino o indice máximo que é igual o número de persas
index_maximo = list(range(0,len(arquivos)))

#Criação da estrutura do tabela NUC
df_dra = pd.DataFrame(data = [], columns=['CHAVE','EVENTO_TARIFARIO','ANO','SARI','DISTRIBUIDORA','REGIAO','DATA','TFSEE_RS','RGR_RS','CCC_RS','CDE_RS','CFURH_RS','ESS_EER_RS','PROINFA_RS','ONS_RS','PD_RS','ENCARGOS_RS','REDE_BASICA_RS','REDE_BASICA_FRONTEIRA_RS','REDE_BASICA_ONS_A2_RS','REDE_BASICA_EXPORT_A2_RS','CONEXAO_RS','MUST_ITAIPU_RS','TRANSPORTE_ITAIPU_RS','SISTEMA_DISTRIBUICAO_RS','TRANSPORTE_RS','ENERGIA_COMPRADA_RS','PARCELA_A_RS','PARCELA_B_RS','RA1_RS','ENERGIA_COMPRADA_MWH','FORNECIMENTO_SUPRIMENTO_MWH','FORNECIMENTO_MWH','SUPRIMENTO_MWH','CUSTO_MEDIO_RS_MWH','PERDA_NAO_TECNICA_PERCENT','PERDA_TECNICA_PERCENT','PERDA_DISTRIBUICAO_PERCENT','PERDA_REDE_BASICA_PERCENT','PERDAS_REGULATORIAS_MWH','PERDA_NAO_TECNICA_MWH','PERDA_TECNICA_MWH','PERDA_DISTRIBUICAO_MWH','PERDA_RB_DISTRIBUICAO_MWH','PERDA_RB_CATIVO_MWH'],index=index_maximo)

#Criação dataframe vazio
df_persas_capa = pd.DataFrame(data=[])
df_persas_resultado = pd.DataFrame(data=[])


# df_persas_capa = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA')
    

# for aba_mercado in abas_mercado:
#     try:
#         df_persas_mercado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = aba_mercado)
        
#     except Exception as err:
#         print('Aba Mercado não encontrada:',err)
    

   

# df_persas_resultado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Resultado'
#                                                                      ,nrows=40
#                                                                      ,usecols='A:G')
    
        
# df_persas_energia = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Energia'
#                                                                ,nrows=18)



# linhas_capa = range(len(df_persas_capa.index))
# colunas_capa = range(len(df_persas_capa.columns))
# linhas_mercado = range(len(df_persas_mercado.index))
# colunas_mercado = range(len(df_persas_mercado.columns))
# linhas_resultado = range(len(df_persas_resultado.index))
# colunas_resultado = range(len(df_persas_resultado.columns))
# linhas_energia = range(len(df_persas_energia.index))
# colunas_energia = range(len(df_persas_energia.columns))


# df_persas_capa = df_persas_capa.astype('str')
# df_persas_mercado = df_persas_mercado.astype('str')
# df_persas_resultado = df_persas_resultado.astype('str')
# df_persas_energia = df_persas_energia.astype('str')


#%%Funções
#Função para extrair dados de 'DRP'
#Aba 'Resultado'
def extrair_resultado(df_dra,df_persas_resultado,index):
    #PERSAS 2012 e 2013
    if (df_dra.at[index,'ANO'] == '2012') or (df_dra.at[index,'ANO'] == '2013'):
        #Não extraimos nenhum dado das PERSAS de 2012 e 2013, pois elas não possuem dados de DRA
        print('Não extrai dados de DRA da aba Resultado das PERSAS de 2012 e 2013')
               
    #PERSAS Após 2013
    else:
        for linha_resultado in linhas_resultado:
            for coluna_resultado in colunas_resultado:
                if 'TFSEE' in df_persas_resultado.iloc[linha_resultado,coluna_resultado].upper():
                    df_dra.at[index,'TFSEE_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+1)]
                elif 'RGR' in df_persas_resultado.iloc[linha_resultado,coluna_resultado].upper():
                     df_dra.at[index,'RGR_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+1)]
                elif 'CCC' in df_persas_resultado.iloc[linha_resultado,coluna_resultado].upper():
                     df_dra.at[index,'CCC_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+1)]
                elif 'CDE' in df_persas_resultado.iloc[linha_resultado,coluna_resultado].upper():
                     df_dra.at[index,'CDE_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+1)]
                elif 'CFURH' in df_persas_resultado.iloc[linha_resultado,coluna_resultado].upper():
                     df_dra.at[index,'CFURH_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+1)]
                elif 'ESS' in df_persas_resultado.iloc[linha_resultado,coluna_resultado].upper():
                     df_dra.at[index,'ESS_EER_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+1)]
                elif 'PROINFA' in df_persas_resultado.iloc[linha_resultado,coluna_resultado].upper():
                     df_dra.at[index,'PROINFA_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+1)]
                elif df_persas_resultado.iloc[linha_resultado,coluna_resultado].upper() == 'ONS':
                    df_dra.at[index,'ONS_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+1)]
                elif 'P&D' in df_persas_resultado.iloc[linha_resultado,coluna_resultado].upper():
                     df_dra.at[index,'PD_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+1)]
                elif df_persas_resultado.iloc[linha_resultado,coluna_resultado].upper() == 'ENCARGOS':
                    df_dra.at[index,'ENCARGOS_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+1)]
                elif df_persas_resultado.iloc[linha_resultado,coluna_resultado].upper() == 'REDE BÁSICA':
                    df_dra.at[index,'REDE_BASICA_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+1)]
                elif df_persas_resultado.iloc[linha_resultado,coluna_resultado].upper() == 'REDE BÁSICA FRONTEIRA':
                    df_dra.at[index,'REDE_BASICA_FRONTEIRA_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+1)]
                elif 'REDE BÁSICA ONS' in df_persas_resultado.iloc[linha_resultado,coluna_resultado].upper():
                     df_dra.at[index,'REDE_BASICA_ONS_A2_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+1)]
                elif 'REDE BÁSICA EXPORT' in df_persas_resultado.iloc[linha_resultado,coluna_resultado].upper():
                    df_dra.at[index,'REDE_BASICA_EXPORT_A2_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+1)]
                elif df_persas_resultado.iloc[linha_resultado,coluna_resultado].upper() == 'CONEXÃO':
                    df_dra.at[index,'CONEXAO_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+1)]
                elif df_persas_resultado.iloc[linha_resultado,coluna_resultado].upper() == 'MUST ITAIPU':
                    df_dra.at[index,'MUST_ITAIPU_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+1)]
                elif df_persas_resultado.iloc[linha_resultado,coluna_resultado].upper() == 'TRANSPORTE DE ITAIPU':
                    df_dra.at[index,'TRANSPORTE_ITAIPU_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+1)]
                elif 'SISTEMA DE DISTRIBUIÇÃO' in df_persas_resultado.iloc[linha_resultado,coluna_resultado].upper():
                    df_dra.at[index,'SISTEMA_DISTRIBUICAO_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+1)]
                elif (df_persas_resultado.iloc[linha_resultado,coluna_resultado].upper() == 'TRANSPORTE') or (df_persas_resultado.iloc[linha_resultado,coluna_resultado].upper() == '@'):
                    df_dra.at[index,'TRANSPORTE_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+1)]
                elif df_persas_resultado.iloc[linha_resultado,coluna_resultado].upper() == 'ENERGIA':
                    df_dra.at[index,'ENERGIA_COMPRADA_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+1)]
                elif (df_persas_resultado.iloc[linha_resultado,coluna_resultado].upper() == 'VALOR DA PARCELA A'):
                    df_dra.at[index,'PARCELA_A_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+1)]
                elif (df_persas_resultado.iloc[linha_resultado,coluna_resultado].upper() == 'VALOR DA PARCELA B') or (df_persas_resultado.iloc[linha_resultado,coluna_resultado].upper() == 'VALOR DA PARCELA B PLEITEADA'):
                    df_dra.at[index,'PARCELA_B_RS'] = df_persas_resultado.iloc[linha_resultado,(coluna_resultado+1)]
                    

        df_dra.at[index,'RA1_RS'] = float(df_dra.at[index,'PARCELA_A_RS']) + float(df_dra.at[index,'PARCELA_B_RS'])


def extrair_energia(df_dra,df_persas_energia,index):
    #Aba 'Energia'
    #PERSAS 2012 e 2013
    if (df_dra.at[index,'ANO'] == '2012') or (df_dra.at[index,'ANO'] == '2013') or (df_dra.at[index,'ANO'] == '2022') or (df_dra.at[index,'ANO'] == '2023'):
        for linha_energia in linhas_energia:
            for coluna_energia in colunas_energia:
                if 'ENERGIA REQUERIDA' in df_persas_energia.iloc[linha_energia,coluna_energia].upper():
                    df_dra.at[index,'ENERGIA_COMPRADA_MWH'] = df_persas_energia.iloc[linha_energia,(coluna_energia+1)]
                elif (df_persas_energia.iloc[linha_energia,coluna_energia].upper() == 'ENERGIA DE FORNECIMENTO') or (df_persas_energia.iloc[linha_energia,coluna_energia].upper() == 'FORNECIMENTO'):
                    df_dra.at[index,'FORNECIMENTO_MWH'] = df_persas_energia.iloc[linha_energia,(coluna_energia+1)]
                elif df_persas_energia.iloc[linha_energia,coluna_energia].upper() == 'SUPRIMENTO':
                    df_dra.at[index,'SUPRIMENTO_MWH'] = df_persas_energia.iloc[linha_energia,(coluna_energia+1)]
                elif df_persas_energia.iloc[linha_energia,coluna_energia].upper() == 'TARIFA MÉDIA':
                    df_dra.at[index,'CUSTO_MEDIO_RS_MWH'] = df_persas_energia.iloc[linha_energia,(coluna_energia+1)]
                elif df_persas_energia.iloc[linha_energia,coluna_energia].upper() == 'PERDA NÃO TÉCNICA':
                    df_dra.at[index,'PERDA_NAO_TECNICA_MWH'] = df_persas_energia.iloc[linha_energia,(coluna_energia+1)]
                elif df_persas_energia.iloc[linha_energia,coluna_energia].upper() == 'PERDA TÉCNICA':
                    df_dra.at[index,'PERDA_TECNICA_MWH'] = df_persas_energia.iloc[linha_energia,(coluna_energia+1)]
                elif df_persas_energia.iloc[linha_energia,coluna_energia].upper() == 'PERDA REDE BÁSICA SOBRE DIST.':
                    df_dra.at[index,'PERDA_RB_DISTRIBUICAO_MWH'] = df_persas_energia.iloc[linha_energia,(coluna_energia+1)]
                elif (df_persas_energia.iloc[linha_energia,coluna_energia].upper() == 'PERDA REDE BÁSICA SOBRE MERCADO CAT.') or (df_persas_energia.iloc[linha_energia,coluna_energia].upper() == 'PERDA REDE BÁSICA SOBRE CAT.'):
                    df_dra.at[index,'PERDA_RB_CATIVO_MWH'] = df_persas_energia.iloc[linha_energia,(coluna_energia+1)]
                elif '% NÃO TÉCNICA' in df_persas_energia.iloc[linha_energia,coluna_energia].upper():
                    df_dra.at[index,'PERDA_NAO_TECNICA_PERCENT'] = df_persas_energia.iloc[linha_energia,(coluna_energia+1)]
                elif '% TÉCNICA' in df_persas_energia.iloc[linha_energia,coluna_energia].upper():
                    df_dra.at[index,'PERDA_TECNICA_PERCENT'] = df_persas_energia.iloc[linha_energia,(coluna_energia+1)]
                elif '% GLOBAL' in df_persas_energia.iloc[linha_energia,coluna_energia].upper():
                    df_dra.at[index,'PERDA_DISTRIBUICAO_PERCENT'] = df_persas_energia.iloc[linha_energia,(coluna_energia+1)]
                elif '% REDE BÁSICA' in df_persas_energia.iloc[linha_energia,coluna_energia].upper():
                    df_dra.at[index,'PERDA_REDE_BASICA_PERCENT'] = df_persas_energia.iloc[linha_energia,(coluna_energia+1)]
                    
        df_dra.at[index,'FORNECIMENTO_SUPRIMENTO_MWH'] = float(df_dra.at[index,'FORNECIMENTO_MWH']) + float(df_dra.at[index,'SUPRIMENTO_MWH'])
        df_dra.at[index,'PERDAS_REGULATORIAS_MWH'] = float(df_dra.at[index,'PERDA_NAO_TECNICA_MWH']) + float(df_dra.at[index,'PERDA_TECNICA_MWH']) + float(df_dra.at[index,'PERDA_RB_DISTRIBUICAO_MWH']) + float(df_dra.at[index,'PERDA_RB_CATIVO_MWH'])
                
    #PERSAS Antes de 2022 e 2023
    else:
        #Não extrai dados de Energia, pois não possuem dados de DRA
        print('Não extrai dados de DRA da aba Energia das PERSAS antes de 2022 e 2023')
                
            
def distribuidora(df_dra,df_persas_capa,index):
    for linha in linhas_capa:
        for coluna in colunas_capa:
            #Determina o ANO
            if 'ANO DO PROCESSO' in df_persas_capa.iloc[linha,coluna].upper():
                df_dra.at[index,'ANO'] = df_persas_capa.iloc[linha,(coluna+1)]
                
            #Determina o SARI da Distribuidora
            elif 'SARI' in df_persas_capa.iloc[linha,coluna].upper():
                df_dra.at[index,'SARI'] = df_persas_capa.iloc[linha,(coluna+1)]
                
            #Determina a DATA
            elif 'REAJUSTE/REVISÃO SUGE' in df_persas_capa.iloc[linha,coluna].upper():
                df_dra.at[index,'DATA'] = df_persas_capa.iloc[linha,(coluna+1)].split(' ')[0]


    #Determina o EVENTO TARIFARIO
    for linha in linhas_capa:
        for coluna in colunas_capa:
            if 'REAJUSTE' in df_persas_capa.iloc[linha,coluna].upper():
                df_dra.at[index,'EVENTO_TARIFARIO'] = 'RTA'
                
            elif 'REVISÃO' in df_persas_capa.iloc[linha,coluna].upper():
                df_dra.at[index,'EVENTO_TARIFARIO'] = 'RTP'
                
            elif 'TARIFAS INICIAIS' in df_persas_capa.iloc[linha,coluna].upper():
                df_dra.at[index,'EVENTO_TARIFARIO'] = 'TI'
                

    #Determina o nome da DISTRIBUIDORA das PERSAS fora do layout padrão
    if (df_persas_capa.iloc[4,1].upper() == 'COOPERMILA') or (df_persas_capa.iloc[4,1].upper() == 'CERTREL'):
        df_dra.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[4,1].upper()
        
    elif (df_persas_capa.iloc[0,1].upper() == 'CERGAL'):
        df_dra.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[0,1].upper()
       
    #PERSAS no layout padrão
    else:
        for linha in linhas_capa:
            for coluna in colunas_capa:
                if df_persas_capa.iloc[linha,coluna].upper() == 'EMPRESA':
                    df_dra.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[linha,(coluna+1)].upper()

    #Determina a REGIAO
    if df_dra.at[index,'DISTRIBUIDORA'] == 'CERCOS':
        df_dra.at[index,'REGIAO'] = 'N/NE'
        
    else:
        df_dra.at[index,'REGIAO'] = 'S/SE/CO'

    #Determina a CHAVE
    df_dra.at[index,'CHAVE'] = df_dra.loc[index,'EVENTO_TARIFARIO']+df_dra.loc[index,'ANO']+df_dra.loc[index,'SARI']



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


        df_persas_resultado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Resultado'
                                                                             ,nrows=40
                                                                             ,usecols='A:G')
            

                
        df_persas_energia = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Energia'
                                                                     ,nrows=18)
                
                                                                
    except:
        print('Aba não disponível na PERSAS', arquivo)
 
    #Rodamos as funções
    else:
        try:
            print('Leu o arquivo: ',arquivo)
        
            #Converte as tabelas para string, pois não é possível comparar string com valor NaN
            df_persas_capa = df_persas_capa.astype('str')
            df_persas_mercado = df_persas_mercado.astype('str')
            df_persas_resultado = df_persas_resultado.astype('str')
            df_persas_energia = df_persas_energia.astype('str')

    
            
            #Define o intervalo máximo de linhas e colunas do dataframe 
            linhas_capa = range(len(df_persas_capa.index))
            colunas_capa = range(len(df_persas_capa.columns))
            linhas_mercado = range(len(df_persas_mercado.index))
            colunas_mercado = range(len(df_persas_mercado.columns))
            linhas_resultado = range(len(df_persas_resultado.index))
            colunas_resultado = range(len(df_persas_resultado.columns))
            linhas_energia = range(len(df_persas_energia.index))
            colunas_energia = range(len(df_persas_energia.columns))
        
            
            #Função para extrair os dados
            distribuidora(df_dra,df_persas_capa,index)
            extrair_resultado(df_dra,df_persas_resultado,index)
            extrair_energia(df_dra,df_persas_energia,index)
            
            print('Extraiu o dado do arquivo: ',arquivo)
            print('Arquivos extraídos: ',round((index/len(arquivos)),2)*100,'%')
    
        except Exception as err:
            print('Não foi possível extrair os dados:',err)
    
        
    index = index + 1 #Passa para o próximo indice para conseguir dados da proxima SPARTA
    
    
    
#%%Tratamento de dados
#Remover dados duplicados e linhas nulas
df_dra = df_dra.drop_duplicates(subset = 'CHAVE',ignore_index = True)
df_dra = df_dra.dropna(axis=0,how='all')

#Tratamento dos dados
df_dra = df_dra.astype('str')
for coluna_float in colunas_float:
    df_dra[coluna_float] = df_dra[coluna_float].replace('.',',').replace('nan',0).astype('float')
  
# Define a data de atualização dos dados
data = datetime.today().strftime('%d/%m/%Y')
df_dra['DATA_ATUALIZA'] = data 
 
#%% Inserir dados no banco de dados

#Criar a lista para inserção no banco SQL com os dados da base editada
dados_list = df_dra.values.tolist()


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
        sql = '''INSERT INTO ''' + tabela_oracle +''' VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26,:27,:28,:29,:30,:31,:32,:33,:34,:35,:36,:37,:38,:39,:40,:41,:42,:43,:44,:45,:46)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        cursor.executemany(sql, dados_list)
    except Exception as err:
        print('Erro no Insert:', err)
    else:
        print('Carga executada com sucesso!')
        connection.commit() #Caso seja executado com sucesso, esse comando salva a base de dados
    finally:
        cursor.close()
        connection.close()

   
