# -*- coding: utf-8 -*-
"""
Created on Mon Jun 19 11:38:03 2023

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

colunas_str=['SUPRIDORA1','NIVEL_TENSAO1','SUPRIDORA2','NIVEL_TENSAO2','SUPRIDORA3','NIVEL_TENSAO3','SUPRIDORA4','NIVEL_TENSAO4','SUPRIDORA5','NIVEL_TENSAO5']
colunas_float=['MWH1','DESCONTO1_PERCENT','TARIFA1_RS_MWH','TARIFA_COBERTURA1_RS_MWH','DESCONTO_ANTIGO1_PERCENT','MWH2','DESCONTO2_PERCENT','TARIFA2_RS_MWH','TARIFA_COBERTURA2_RS_MWH','DESCONTO_ANTIGO2_PERCENT','MWH3','DESCONTO3_PERCENT','TARIFA3_RS_MWH','TARIFA_COBERTURA3_RS_MWH','DESCONTO_ANTIGO3_PERCENT','MWH4','DESCONTO4_PERCENT','TARIFA4_RS_MWH','TARIFA_COBERTURA4_RS_MWH','DESCONTO_ANTIGO4_PERCENT','MWH5','DESCONTO5_PERCENT','TARIFA5_RS_MWH','TARIFA_COBERTURA5_RS_MWH','DESCONTO_ANTIGO5_PERCENT']
index = 0 #Flag para inserção dos dados

#Nome da tabela Oracle onde será dada a carga
tabela_oracle = 'PERSAS_CUSTO_SUPRIMENTO'
ano_oracle = "'2023'"


#%% Montar o dataframe da estrutura
#Defino o indice máximo que é igual o número de persas
index_maximo = list(range(0,len(arquivos)))

#Criação da estrutura do tabela NUC
df_suprimento = pd.DataFrame(data = [], columns=['CHAVE','EVENTO_TARIFARIO','ANO','SARI','DISTRIBUIDORA','REGIAO','DATA','SUPRIDORA1','NIVEL_TENSAO1','MWH1','DESCONTO1_PERCENT','TARIFA1_RS_MWH','TARIFA_COBERTURA1_RS_MWH','DESCONTO_ANTIGO1_PERCENT','SUPRIDORA2','NIVEL_TENSAO2','MWH2','DESCONTO2_PERCENT','TARIFA2_RS_MWH','TARIFA_COBERTURA2_RS_MWH','DESCONTO_ANTIGO2_PERCENT','SUPRIDORA3','NIVEL_TENSAO3','MWH3','DESCONTO3_PERCENT','TARIFA3_RS_MWH','TARIFA_COBERTURA3_RS_MWH','DESCONTO_ANTIGO3_PERCENT','SUPRIDORA4','NIVEL_TENSAO4','MWH4','DESCONTO4_PERCENT','TARIFA4_RS_MWH','TARIFA_COBERTURA4_RS_MWH','DESCONTO_ANTIGO4_PERCENT','SUPRIDORA5','NIVEL_TENSAO5','MWH5','DESCONTO5_PERCENT','TARIFA5_RS_MWH','TARIFA_COBERTURA5_RS_MWH','DESCONTO_ANTIGO5_PERCENT'],index=index_maximo)

#Criação dataframe vazio
df_persas_capa = pd.DataFrame(data=[])
df_persas_suprimento = pd.DataFrame(data=[])


# df_persas_capa = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA')
    

# df_persas_suprimento = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Energia'
#                                                               ,header=21
#                                                               ,nrows=10
#                                                               ,usecols='A:J')


    

# linhas_capa = range(len(df_persas_capa.index))
# colunas_capa = range(len(df_persas_capa.columns))
# linhas_suprimento = range(len(df_persas_suprimento.index))
# colunas_suprimento = range(len(df_persas_suprimento.columns))



# df_persas_capa = df_persas_capa.astype('str')
# df_persas_suprimento = df_persas_suprimento.astype('str')


#%%Funções
# Função para extrair dados da Supridora
def extrair_supridora(df_suprimento,df_persas_suprimento,index):
    #PERSAS 2013
    if df_suprimento.at[index,'ANO'] == '2013':
        # SUPRIDORA 1
        for linha_suprimento in linhas_suprimento:
            for coluna_suprimento in colunas_suprimento:
                if ('EMPRESA' in df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper()):
                    df_suprimento.at[index,'SUPRIDORA1'] = df_persas_suprimento.iloc[(linha_suprimento+1),coluna_suprimento]
                elif ('NÍVEL' in df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper()) or ('SUBGRUPO' in df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper()):
                    df_suprimento.at[index,'NIVEL_TENSAO1'] = df_persas_suprimento.iloc[(linha_suprimento+1),coluna_suprimento]
                elif df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper() == 'MONTANTE':
                    df_suprimento.at[index,'MWH1'] = df_persas_suprimento.iloc[(linha_suprimento+1),coluna_suprimento]
                elif df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper() == 'DESCONTO':
                    df_suprimento.at[index,'DESCONTO1_PERCENT'] = df_persas_suprimento.iloc[(linha_suprimento+1),coluna_suprimento]
                elif df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper() == 'TARIFA SUP. SEM DESCONTO':
                    df_suprimento.at[index,'TARIFA1_RS_MWH'] = df_persas_suprimento.iloc[(linha_suprimento+1),coluna_suprimento]
    
    
    # Demais PERSAS
    else:
        # SUPRIDORA 1
        for linha_suprimento in linhas_suprimento:
            for coluna_suprimento in colunas_suprimento:
                if ('SUPRIDORA' in df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper()):
                    df_suprimento.at[index,'SUPRIDORA1'] = df_persas_suprimento.iloc[(linha_suprimento+1),coluna_suprimento]
                elif ('NÍVEL' in df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper()) or ('SUBGRUPO' in df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper()):
                    df_suprimento.at[index,'NIVEL_TENSAO1'] = df_persas_suprimento.iloc[(linha_suprimento+1),coluna_suprimento]
                elif df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper() == 'MWH':
                    df_suprimento.at[index,'MWH1'] = df_persas_suprimento.iloc[(linha_suprimento+1),coluna_suprimento]
                elif df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper() == 'DESCONTO':
                    df_suprimento.at[index,'DESCONTO1_PERCENT'] = df_persas_suprimento.iloc[(linha_suprimento+1),coluna_suprimento]
                elif df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper() == 'TARIFA SUP.':
                    df_suprimento.at[index,'TARIFA1_RS_MWH'] = df_persas_suprimento.iloc[(linha_suprimento+1),coluna_suprimento]
                elif df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper() == 'TARIFA SUP. DE COBERTURA':
                    df_suprimento.at[index,'TARIFA_COBERTURA1_RS_MWH'] = df_persas_suprimento.iloc[(linha_suprimento+1),coluna_suprimento]
                elif df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper() == 'DESCONTO ANTIGO':
                    df_suprimento.at[index,'DESCONTO_ANTIGO1_PERCENT'] = df_persas_suprimento.iloc[(linha_suprimento+1),coluna_suprimento]
                    
        # SUPRIDORA 2
        for linha_suprimento in linhas_suprimento:
            for coluna_suprimento in colunas_suprimento:
                if ('SUPRIDORA' in df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper()):
                    df_suprimento.at[index,'SUPRIDORA2'] = df_persas_suprimento.iloc[(linha_suprimento+2),coluna_suprimento]
                elif ('NÍVEL' in df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper()) or ('SUBGRUPO' in df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper()):
                    df_suprimento.at[index,'NIVEL_TENSAO2'] = df_persas_suprimento.iloc[(linha_suprimento+2),coluna_suprimento]
                elif df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper() == 'MWH':
                    df_suprimento.at[index,'MWH2'] = df_persas_suprimento.iloc[(linha_suprimento+2),coluna_suprimento]
                elif df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper() == 'DESCONTO':
                    df_suprimento.at[index,'DESCONTO2_PERCENT'] = df_persas_suprimento.iloc[(linha_suprimento+2),coluna_suprimento]
                elif df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper() == 'TARIFA SUP.':
                    df_suprimento.at[index,'TARIFA2_RS_MWH'] = df_persas_suprimento.iloc[(linha_suprimento+2),coluna_suprimento]
                elif df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper() == 'TARIFA SUP. DE COBERTURA':
                    df_suprimento.at[index,'TARIFA_COBERTURA2_RS_MWH'] = df_persas_suprimento.iloc[(linha_suprimento+2),coluna_suprimento]
                elif df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper() == 'DESCONTO ANTIGO':
                    df_suprimento.at[index,'DESCONTO_ANTIGO2_PERCENT'] = df_persas_suprimento.iloc[(linha_suprimento+2),coluna_suprimento]

        # SUPRIDORA 3
        for linha_suprimento in linhas_suprimento:
            for coluna_suprimento in colunas_suprimento:
                if ('SUPRIDORA' in df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper()):
                    df_suprimento.at[index,'SUPRIDORA3'] = df_persas_suprimento.iloc[(linha_suprimento+3),coluna_suprimento]
                elif ('NÍVEL' in df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper()) or ('SUBGRUPO' in df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper()):
                    df_suprimento.at[index,'NIVEL_TENSAO3'] = df_persas_suprimento.iloc[(linha_suprimento+3),coluna_suprimento]
                elif df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper() == 'MWH':
                    df_suprimento.at[index,'MWH3'] = df_persas_suprimento.iloc[(linha_suprimento+3),coluna_suprimento]
                elif df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper() == 'DESCONTO':
                    df_suprimento.at[index,'DESCONTO3_PERCENT'] = df_persas_suprimento.iloc[(linha_suprimento+3),coluna_suprimento]
                elif df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper() == 'TARIFA SUP.':
                    df_suprimento.at[index,'TARIFA3_RS_MWH'] = df_persas_suprimento.iloc[(linha_suprimento+3),coluna_suprimento]
                elif df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper() == 'TARIFA SUP. DE COBERTURA':
                    df_suprimento.at[index,'TARIFA_COBERTURA3_RS_MWH'] = df_persas_suprimento.iloc[(linha_suprimento+3),coluna_suprimento]
                elif df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper() == 'DESCONTO ANTIGO':
                    df_suprimento.at[index,'DESCONTO_ANTIGO3_PERCENT'] = df_persas_suprimento.iloc[(linha_suprimento+3),coluna_suprimento]

        # SUPRIDORA 4
        for linha_suprimento in linhas_suprimento:
            for coluna_suprimento in colunas_suprimento:
                if ('SUPRIDORA' in df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper()):
                    df_suprimento.at[index,'SUPRIDORA4'] = df_persas_suprimento.iloc[(linha_suprimento+4),coluna_suprimento]
                elif ('NÍVEL' in df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper()) or ('SUBGRUPO' in df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper()):
                    df_suprimento.at[index,'NIVEL_TENSAO4'] = df_persas_suprimento.iloc[(linha_suprimento+4),coluna_suprimento]
                elif df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper() == 'MWH':
                    df_suprimento.at[index,'MWH4'] = df_persas_suprimento.iloc[(linha_suprimento+4),coluna_suprimento]
                elif df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper() == 'DESCONTO':
                    df_suprimento.at[index,'DESCONTO4_PERCENT'] = df_persas_suprimento.iloc[(linha_suprimento+4),coluna_suprimento]
                elif df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper() == 'TARIFA SUP.':
                    df_suprimento.at[index,'TARIFA4_RS_MWH'] = df_persas_suprimento.iloc[(linha_suprimento+4),coluna_suprimento]
                elif df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper() == 'TARIFA SUP. DE COBERTURA':
                    df_suprimento.at[index,'TARIFA_COBERTURA4_RS_MWH'] = df_persas_suprimento.iloc[(linha_suprimento+4),coluna_suprimento]
                elif df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper() == 'DESCONTO ANTIGO':
                    df_suprimento.at[index,'DESCONTO_ANTIGO4_PERCENT'] = df_persas_suprimento.iloc[(linha_suprimento+4),coluna_suprimento]

        # SUPRIDORA 5
        for linha_suprimento in linhas_suprimento:
            for coluna_suprimento in colunas_suprimento:
                if ('SUPRIDORA' in df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper()):
                    df_suprimento.at[index,'SUPRIDORA5'] = df_persas_suprimento.iloc[(linha_suprimento+5),coluna_suprimento]
                elif ('NÍVEL' in df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper()) or ('SUBGRUPO' in df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper()):
                    df_suprimento.at[index,'NIVEL_TENSAO5'] = df_persas_suprimento.iloc[(linha_suprimento+5),coluna_suprimento]
                elif df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper() == 'MWH':
                    df_suprimento.at[index,'MWH5'] = df_persas_suprimento.iloc[(linha_suprimento+5),coluna_suprimento]
                elif df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper() == 'DESCONTO':
                    df_suprimento.at[index,'DESCONTO5_PERCENT'] = df_persas_suprimento.iloc[(linha_suprimento+5),coluna_suprimento]
                elif df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper() == 'TARIFA SUP.':
                    df_suprimento.at[index,'TARIFA5_RS_MWH'] = df_persas_suprimento.iloc[(linha_suprimento+5),coluna_suprimento]
                elif df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper() == 'TARIFA SUP. DE COBERTURA':
                    df_suprimento.at[index,'TARIFA_COBERTURA5_RS_MWH'] = df_persas_suprimento.iloc[(linha_suprimento+5),coluna_suprimento]
                elif df_persas_suprimento.iloc[linha_suprimento,coluna_suprimento].upper() == 'DESCONTO ANTIGO':
                    df_suprimento.at[index,'DESCONTO_ANTIGO5_PERCENT'] = df_persas_suprimento.iloc[(linha_suprimento+5),coluna_suprimento]



def distribuidora(df_suprimento,df_persas_capa,index):
    for linha in linhas_capa:
        for coluna in colunas_capa:
            #Determina o ANO
            if 'ANO DO PROCESSO' in df_persas_capa.iloc[linha,coluna].upper():
                df_suprimento.at[index,'ANO'] = df_persas_capa.iloc[linha,(coluna+1)]
                
            #Determina o SARI da Distribuidora
            elif 'SARI' in df_persas_capa.iloc[linha,coluna].upper():
                df_suprimento.at[index,'SARI'] = df_persas_capa.iloc[linha,(coluna+1)]
                
            #Determina a DATA
            elif 'REAJUSTE/REVISÃO SUGE' in df_persas_capa.iloc[linha,coluna].upper():
                df_suprimento.at[index,'DATA'] = df_persas_capa.iloc[linha,(coluna+1)].split(' ')[0]


    #Determina o EVENTO TARIFARIO
    for linha in linhas_capa:
        for coluna in colunas_capa:
            if 'REAJUSTE' in df_persas_capa.iloc[linha,coluna].upper():
                df_suprimento.at[index,'EVENTO_TARIFARIO'] = 'RTA'
                
            elif 'REVISÃO' in df_persas_capa.iloc[linha,coluna].upper():
                df_suprimento.at[index,'EVENTO_TARIFARIO'] = 'RTP'
                
            elif 'TARIFAS INICIAIS' in df_persas_capa.iloc[linha,coluna].upper():
                df_suprimento.at[index,'EVENTO_TARIFARIO'] = 'TI'
                

    #Determina o nome da DISTRIBUIDORA das PERSAS fora do layout padrão
    if (df_persas_capa.iloc[4,1].upper() == 'COOPERMILA') or (df_persas_capa.iloc[4,1].upper() == 'CERTREL'):
        df_suprimento.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[4,1].upper()
        
    elif (df_persas_capa.iloc[0,1].upper() == 'CERGAL'):
        df_suprimento.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[0,1].upper()
       
    #PERSAS no layout padrão
    else:
        for linha in linhas_capa:
            for coluna in colunas_capa:
                if df_persas_capa.iloc[linha,coluna].upper() == 'EMPRESA':
                    df_suprimento.at[index,'DISTRIBUIDORA'] = df_persas_capa.iloc[linha,(coluna+1)].upper()

    #Determina a REGIAO
    if df_suprimento.at[index,'DISTRIBUIDORA'] == 'CERCOS':
        df_suprimento.at[index,'REGIAO'] = 'N/NE'
        
    else:
        df_suprimento.at[index,'REGIAO'] = 'S/SE/CO'

    #Determina a CHAVE
    df_suprimento.at[index,'CHAVE'] = df_suprimento.loc[index,'EVENTO_TARIFARIO']+df_suprimento.loc[index,'ANO']+df_suprimento.loc[index,'SARI']



#%%Inserção dos dados
#Abre a PERSAS de cada arquivo na pasta
for arquivo in arquivos:   
    try:
        df_persas_capa = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA')
            

        df_persas_suprimento = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Energia'
                                                                      ,header=21
                                                                      ,nrows=10
                                                                      ,usecols='A:J')
                                                                
    except:
        print('Aba não disponível na PERSAS', arquivo)
 
    #Rodamos as funções
    else:
        try:
            print('Leu o arquivo: ',arquivo)
        
            #Converte as tabelas para string, pois não é possível comparar string com valor NaN
            df_persas_capa = df_persas_capa.astype('str')
            df_persas_suprimento = df_persas_suprimento.astype('str')

            
            #Define o intervalo máximo de linhas e colunas do dataframe 
            linhas_capa = range(len(df_persas_capa.index))
            colunas_capa = range(len(df_persas_capa.columns))
            linhas_suprimento = range(len(df_persas_suprimento.index))
            colunas_suprimento = range(len(df_persas_suprimento.columns))

            
            #Função para extrair os dados
            distribuidora(df_suprimento,df_persas_capa,index)
            extrair_supridora(df_suprimento,df_persas_suprimento,index)
            
            print('Extraiu o dado do arquivo: ',arquivo)
            print('Arquivos extraídos: ',round((index/len(arquivos)),2)*100,'%')
    
        except Exception as err:
            print('Não foi possível extrair os dados:',err)
    
        
    index = index + 1 #Passa para o próximo indice para conseguir dados da proxima SPARTA
    
  
#%%Tratamento de dados
#Remover dados duplicados
df_suprimento = df_suprimento.drop_duplicates(subset = 'CHAVE',ignore_index = True)
df_suprimento = df_suprimento.dropna(axis=0,how='all')

#Tratamento dos dados
df_suprimento = df_suprimento.astype('str')
for coluna_float in colunas_float:
    df_suprimento[coluna_float] = df_suprimento[coluna_float].replace('.',',').replace('nan',0).astype('float')

for coluna_str in colunas_str:
    df_suprimento[coluna_str] = df_suprimento[coluna_str].replace('nan','').replace('0','')

# Define a data de atualização dos dados
data = datetime.today().strftime('%d/%m/%Y')
df_suprimento['DATA_ATUALIZA'] = data 

#%% Inserir dados no banco de dados

#Criar a lista para inserção no banco SQL com os dados da base editada
dados_list = df_suprimento.values.tolist()


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
        sql = '''INSERT INTO ''' + tabela_oracle +''' VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26,:27,:28,:29,:30,:31,:32,:33,:34,:35,:36,:37,:38,:39,:40,:41,:42,:43)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        cursor.executemany(sql, dados_list)
    except Exception as err:
        print('Erro no Insert:', err)
    else:
        print('Carga executada com sucesso!')
        connection.commit() #Caso seja executado com sucesso, esse comando salva a base de dados
    finally:
        cursor.close()
        connection.close()
    



