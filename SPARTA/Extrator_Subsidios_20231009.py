# -*- coding: utf-8 -*-
"""
Created on Tue Feb 14 16:11:51 2023

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

#Caminho referencia
pasta = r"C:\Users\2018459\OneDrive - CPFL Energia S A\Documentos\Projetos\2023\BD RTP e RTA\RTP e RTA (concessionaria)\SPARTA\SPARTA 2023\*"
# pasta = r"C:\Users\2018459\OneDrive - CPFL Energia S A\Área de Trabalho\RTP e RTA (concessionaria)\SPARTA TESTE"

#Arquivos
#Como os dados das empresas estão em arquivos separados, listar todos que serão carregados conforme layout abaixo.
# arquivo ='SPARTA_Eletropaulo_2015.xlsx'
arquivos = glob.glob(pasta)


index = 0 #Flag para inserção dos dados

#Nome da tabela Oracle onde será dada a carga
tabela_oracle = 'SPARTA_SUBSIDIOS'
ano_oracle = "'2023'"



#%% Montar o dataframe da estrutura da aba 'SPARTA_NUC'
#Defino o indice máximo que é igual o número de SPARTA
index_maximo = list(range(0,len(arquivos)))

#Criação da estrutura do tabela NUC
df_subsidios = pd.DataFrame(data = [], columns=['CHAVE','EVENTO_TARIFARIO','ANO','ID','DISTRIBUIDORA','UF','DATA','PERIODO_TARIFARIO','CONTRATO','SUBSIDIO_CARGA_RS','SUBSIDIO_GERACAO_RS','SUBSIDIO_DISTRIBUICAO_RS','SUBSIDIO_AGUA_RS','SUBSIDIO_RURAL_RS','SUBSIDIO_IRRIGANTE_RS'],index=index_maximo) 

#Criação dataframe vazio
df_sparta_resultado = pd.DataFrame(data = [])
df_sparta_capa = pd.DataFrame(data=[])
df_sparta_mercado = pd.DataFrame(data=[])


# df_sparta_resultado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Resultado'
#                               ,header=12
#                               ,nrows=10
#                               ,usecols=[10,11])


#df_sparta_mercado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Mercado'
 #                             ,header=7
  #                            ,nrows=49
   #                           ,usecols=[1,2,3,4,5,6,7])



#%%Extração dos resultados
#Funções para extrair dados das SPARTA recentes
def determina_contrato(df_subsidios,df_sparta_mercado,index):
    #Se essa 'string' estiver presente nessa aba o contrato é NOVO
    if 'Percentual RI' in df_sparta_mercado.iloc[:,:].values:
          df_subsidios.at[index,'CONTRATO'] = 'NOVO' 
    #Caso contrário o contrato é ANTIGO      
    else:
          df_subsidios.at[index,'CONTRATO'] = 'ANTIGO'
          
        
def extrai_subsidios(df_sparta_resultado,df_subsidios,index):
    for linha in linhas_subsidios:  
        if 'CARGA' in df_sparta_resultado.iloc[linha,0].upper():
            df_subsidios.at[index,'SUBSIDIO_CARGA_RS'] = df_sparta_resultado.iloc[linha,1]
        elif 'GERAÇÃO' in df_sparta_resultado.iloc[linha,0].upper():
            df_subsidios.at[index,'SUBSIDIO_GERACAO_RS'] = df_sparta_resultado.iloc[linha,1]  
        elif 'DISTRIBUIÇÃO' in df_sparta_resultado.iloc[linha,0].upper():
            df_subsidios.at[index,'SUBSIDIO_DISTRIBUICAO_RS'] = df_sparta_resultado.iloc[linha,1]  
        elif 'ÁGUA' in df_sparta_resultado.iloc[linha,0].upper():
            df_subsidios.at[index,'SUBSIDIO_AGUA_RS'] = df_sparta_resultado.iloc[linha,1] 
        elif 'RURAL' in df_sparta_resultado.iloc[linha,0].upper():
            df_subsidios.at[index,'SUBSIDIO_RURAL_RS'] = df_sparta_resultado.iloc[linha,1] 
        elif 'IRRIGANTE' in df_sparta_resultado.iloc[linha,0].upper():
            df_subsidios.at[index,'SUBSIDIO_IRRIGANTE_RS'] = df_sparta_resultado.iloc[linha,1] 
          
          
def distribuidora(df_subsidios,df_sparta_capa,index):
    #Determina o ANO da SPARTA
    df_subsidios.at[index,'ANO'] = df_sparta_capa.iloc[8,1].strftime('%Y')
    
    #Determina o ID da Distribuidora
    df_subsidios.at[index,'ID'] = df_sparta_capa.iloc[1,1]
    
    #Determina o NOME da Distribuidora
    df_subsidios.at[index,'DISTRIBUIDORA'] = df_sparta_capa.iloc[0,0].upper()
    
    #Determina a DATA da SPARTA
    df_subsidios.at[index,'DATA'] = df_sparta_capa.iloc[8,1].strftime('%Y-%m-%d')
    
    #Determina o EVENTO TARIFARIO
    if 'Reajuste' in df_sparta_capa.iloc[0,1]:
        df_subsidios.at[index,'EVENTO_TARIFARIO'] = 'RTA'
    elif 'Revisão' in df_sparta_capa.iloc[0,1]:
        df_subsidios.at[index,'EVENTO_TARIFARIO'] = 'RTP'
    else:
        df_subsidios.at[index,'EVENTO_TARIFARIO'] = 'RTE'
        
    #Determina a CHAVE
    df_subsidios.at[index,'CHAVE'] = df_subsidios.loc[index,'EVENTO_TARIFARIO']+df_subsidios.loc[index,'ANO']+df_subsidios.loc[index,'ID']
    
    #Determina UF e PERIODO TARIFARIO
    #AES SUL
    if df_subsidios.loc[index,'ID'] == 'D01':
        df_subsidios.at[index,'UF'] = 'RS'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #AME
    if df_subsidios.loc[index,'ID'] == 'D02':
        df_subsidios.at[index,'UF'] = 'AM'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #AMPLA
    if df_subsidios.loc[index,'ID'] == 'D03':
        df_subsidios.at[index,'UF'] = 'RJ'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #BANDEIRANTE
    if df_subsidios.loc[index,'ID'] == 'D04':
        df_subsidios.at[index,'UF'] = 'SP'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #BOA VISTA
    if df_subsidios.loc[index,'ID'] == 'D05':
        df_subsidios.at[index,'UF'] = 'RR'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CAIUA
    if df_subsidios.loc[index,'ID'] == 'D06':
        df_subsidios.at[index,'UF'] = 'SP'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEA
    if df_subsidios.loc[index,'ID'] == 'D07':
        df_subsidios.at[index,'UF'] = 'AP'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CEAL
    if df_subsidios.loc[index,'ID'] == 'D08':
        df_subsidios.at[index,'UF'] = 'AL'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CEB
    if df_subsidios.loc[index,'ID'] == 'D09':
        df_subsidios.at[index,'UF'] = 'DF'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '4'
    
    #CEEE
    if df_subsidios.loc[index,'ID'] == 'D10':
        df_subsidios.at[index,'UF'] = 'RS'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
      
    #CELESC
    if df_subsidios.loc[index,'ID'] == 'D11':
        df_subsidios.at[index,'UF'] = 'SC'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CELG
    if df_subsidios.loc[index,'ID'] == 'D12':
        df_subsidios.at[index,'UF'] = 'GO'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CELPA
    if df_subsidios.loc[index,'ID'] == 'D13':
        df_subsidios.at[index,'UF'] = 'PA'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CELPE
    if df_subsidios.loc[index,'ID'] == 'D14':
        df_subsidios.at[index,'UF'] = 'PE'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CELTINS
    if df_subsidios.loc[index,'ID'] == 'D15':
        df_subsidios.at[index,'UF'] = 'TO'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEMAR
    if df_subsidios.loc[index,'ID'] == 'D16':
        df_subsidios.at[index,'UF'] = 'MA'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #EMT
    if df_subsidios.loc[index,'ID'] == 'D17':
        df_subsidios.at[index,'UF'] = 'MT'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEMIG-D
    if df_subsidios.loc[index,'ID'] == 'D18':
        df_subsidios.at[index,'UF'] = 'MG'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEPISA
    if df_subsidios.loc[index,'ID'] == 'D19':
        df_subsidios.at[index,'UF'] = 'PI'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CERON
    if df_subsidios.loc[index,'ID'] == 'D20':
        df_subsidios.at[index,'UF'] = 'RO'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CERR (CONFIMAR O PERIODO)
    if df_subsidios.loc[index,'ID'] == 'D21':
        df_subsidios.at[index,'UF'] = 'RR'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CFLO
    if df_subsidios.loc[index,'ID'] == 'D22':
        df_subsidios.at[index,'UF'] = 'PR'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CHESP
    if df_subsidios.loc[index,'ID'] == 'D23':
        df_subsidios.at[index,'UF'] = 'GO'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL JAGUARI
    if df_subsidios.loc[index,'ID'] == 'D24':
        df_subsidios.at[index,'UF'] = 'SP'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL MOCOCA
    if df_subsidios.loc[index,'ID'] == 'D25':
        df_subsidios.at[index,'UF'] = 'SP'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'        
    
    #CPFL Santa Cruz
    if df_subsidios.loc[index,'ID'] == 'D26':
        df_subsidios.at[index,'UF'] = 'SP'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CNEE
    if df_subsidios.loc[index,'ID'] == 'D27':
        df_subsidios.at[index,'UF'] = 'SP'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COCEL
    if df_subsidios.loc[index,'ID'] == 'D28':
        df_subsidios.at[index,'UF'] = 'PR'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COELBA
    if df_subsidios.loc[index,'ID'] == 'D29':
        df_subsidios.at[index,'UF'] = 'BA'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COELCE
    if df_subsidios.loc[index,'ID'] == 'D30':
        df_subsidios.at[index,'UF'] = 'CE'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #COOPERALIANÇA
    if df_subsidios.loc[index,'ID'] == 'D31':
        df_subsidios.at[index,'UF'] = 'SC'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COPEL
    if df_subsidios.loc[index,'ID'] == 'D32':
        df_subsidios.at[index,'UF'] = 'PR'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COSERN
    if df_subsidios.loc[index,'ID'] == 'D33':
        df_subsidios.at[index,'UF'] = 'RN'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Leste Paulista
    if df_subsidios.loc[index,'ID'] == 'D34':
        df_subsidios.at[index,'UF'] = 'SP'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Piratininga
    if df_subsidios.loc[index,'ID'] == 'D35':
        df_subsidios.at[index,'UF'] = 'SP'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CPFL Paulista
    if df_subsidios.loc[index,'ID'] == 'D36':
        df_subsidios.at[index,'UF'] = 'SP'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Sul Paulista
    if df_subsidios.loc[index,'ID'] == 'D37':
        df_subsidios.at[index,'UF'] = 'SP'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #DEMEI
    if df_subsidios.loc[index,'ID'] == 'D38':
        df_subsidios.at[index,'UF'] = 'RS'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #DME-PC
    if df_subsidios.loc[index,'ID'] == 'D39':
        df_subsidios.at[index,'UF'] = 'MG'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EBO
    if df_subsidios.loc[index,'ID'] == 'D40':
        df_subsidios.at[index,'UF'] = 'PB'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #EDEVP
    if df_subsidios.loc[index,'ID'] == 'D41':
        df_subsidios.at[index,'UF'] = 'SP'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EEB
    if df_subsidios.loc[index,'ID'] == 'D42':
        df_subsidios.at[index,'UF'] = 'SP'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EFLJC
    if df_subsidios.loc[index,'ID'] == 'D43':
        df_subsidios.at[index,'UF'] = 'SC'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EFLUL
    if df_subsidios.loc[index,'ID'] == 'D44':
        df_subsidios.at[index,'UF'] = 'SC'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ELEKTRO
    if df_subsidios.loc[index,'ID'] == 'D45':
        df_subsidios.at[index,'UF'] = 'SP'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELETROACRE
    if df_subsidios.loc[index,'ID'] == 'D46':
        df_subsidios.at[index,'UF'] = 'AC'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELETROCAR
    if df_subsidios.loc[index,'ID'] == 'D47':
        df_subsidios.at[index,'UF'] = 'RS'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ELETROPAULO
    if df_subsidios.loc[index,'ID'] == 'D48':
        df_subsidios.at[index,'UF'] = 'SP'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELFSM
    if df_subsidios.loc[index,'ID'] == 'D49':
        df_subsidios.at[index,'UF'] = 'ES'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EMG
    if df_subsidios.loc[index,'ID'] == 'D50':
        df_subsidios.at[index,'UF'] = 'MG'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EMS
    if df_subsidios.loc[index,'ID'] == 'D51':
        df_subsidios.at[index,'UF'] = 'MS'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ENF
    if df_subsidios.loc[index,'ID'] == 'D52':
        df_subsidios.at[index,'UF'] = 'RJ'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EPB
    if df_subsidios.loc[index,'ID'] == 'D53':
        df_subsidios.at[index,'UF'] = 'PB'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ESCELSA (CONFIRMAR PERIODO)
    if df_subsidios.loc[index,'ID'] == 'D54':
        df_subsidios.at[index,'UF'] = 'ES'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '3'
        
    #ESE
    if df_subsidios.loc[index,'ID'] == 'D55':
        df_subsidios.at[index,'UF'] = 'SE'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #FORCEL
    if df_subsidios.loc[index,'ID'] == 'D56':
        df_subsidios.at[index,'UF'] = 'PR'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #HIDROPAN
    if df_subsidios.loc[index,'ID'] == 'D57':
        df_subsidios.at[index,'UF'] = 'RS'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #IENERGIA
    if df_subsidios.loc[index,'ID'] == 'D58':
        df_subsidios.at[index,'UF'] = 'SC'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #JARI (CONFIRMAR PERIODO)
    if df_subsidios.loc[index,'ID'] == 'D59':
        df_subsidios.at[index,'UF'] = 'PA'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #LIGHT
    if df_subsidios.loc[index,'ID'] == 'D60':
        df_subsidios.at[index,'UF'] = 'RJ'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #MUX-Energia
    if df_subsidios.loc[index,'ID'] == 'D61':
        df_subsidios.at[index,'UF'] = 'RS'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
    
    #CPFL RGE
    if df_subsidios.loc[index,'ID'] == 'D62':
        df_subsidios.at[index,'UF'] = 'RS'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #SULGIPE
    if df_subsidios.loc[index,'ID'] == 'D63':
        df_subsidios.at[index,'UF'] = 'SE'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #UHENPAL
    if df_subsidios.loc[index,'ID'] == 'D64':
        df_subsidios.at[index,'UF'] = 'TO'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ESS
    if df_subsidios.loc[index,'ID'] == 'D65':
        df_subsidios.at[index,'UF'] = 'SP'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Santa Cruz
    if df_subsidios.loc[index,'ID'] == 'D66':
        df_subsidios.at[index,'UF'] = 'SP'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #RGE
    if df_subsidios.loc[index,'ID'] == 'D67':
        df_subsidios.at[index,'UF'] = 'RS'
        df_subsidios.at[index,'PERIODO_TARIFARIO'] = '5'

 
#%%Inserção dos dados
#Abre a SPARTA de cada arquivo na pasta
for arquivo in arquivos:   
    try:
        df_sparta_capa = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'CAPA'
                                       ,header = 5
                                       ,nrows = 14
                                       ,usecols = [1,2])   
    
        df_sparta_mercado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Mercado'
                                  ,header=7
                                  ,nrows=49
                                  ,usecols=[1,2,3,4,5,6,7])
                              
        df_sparta_resultado = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Resultado'
                              ,header=12
                              ,nrows=10
                              ,usecols=[10,11])
        
        print('Leu o arquivo: ',arquivo)

        
        #Mudamos o formato do dado para 'string', pois não conseguimos comparar strings com valores NaN
        df_sparta_resultado = df_sparta_resultado.astype('str')  

        
        #Função para extração dos dados da distribuidora e tipo de contrato
        determina_contrato(df_subsidios,df_sparta_mercado,index)
        distribuidora(df_subsidios,df_sparta_capa,index)
        
        
        #Determina o range de linhas do dataframe      
        linhas_subsidios = range(len(df_sparta_resultado.index))
        
        #Função para extrair os dados dos 'Subsidios'
        extrai_subsidios(df_sparta_resultado,df_subsidios,index)
        
        
        print('Extraiu o dado do arquivo: ',arquivo)
        print('Arquivos extraídos: ',round((index/len(arquivos)),2)*100,'%')
        index = index + 1 #Passa para o próximo indice para conseguir dados da proxima SPARTA
             
                          
    except:
        print('Aba não disponível na SPARTA', arquivo)
     
    

#%%Tratamento de dados 
#Remover dados duplicados e linhas nulas
df_subsidios = df_subsidios.drop_duplicates(subset = 'CHAVE',ignore_index = True)
df_subsidios = df_subsidios.dropna(axis=0,how='all')     


#Limpeza e Tratamento dos dados
df_subsidios = df_subsidios.astype(str)
df_subsidios['PERIODO_TARIFARIO'] = df_subsidios['PERIODO_TARIFARIO'].astype(int)
df_subsidios['SUBSIDIO_CARGA_RS'] = df_subsidios['SUBSIDIO_CARGA_RS'].replace('nan','0').astype(float).replace('.',',')
df_subsidios['SUBSIDIO_GERACAO_RS'] = df_subsidios['SUBSIDIO_GERACAO_RS'].replace('nan','0').astype(float).replace('.',',')
df_subsidios['SUBSIDIO_DISTRIBUICAO_RS'] = df_subsidios['SUBSIDIO_DISTRIBUICAO_RS'].replace('nan','0').astype(float).replace('.',',')
df_subsidios['SUBSIDIO_AGUA_RS'] = df_subsidios['SUBSIDIO_AGUA_RS'].replace('nan','0').astype(float).replace('.',',')
df_subsidios['SUBSIDIO_RURAL_RS'] = df_subsidios['SUBSIDIO_RURAL_RS'].replace('nan','0').astype(float).replace('.',',')
df_subsidios['SUBSIDIO_IRRIGANTE_RS'] = df_subsidios['SUBSIDIO_IRRIGANTE_RS'].replace('nan','0').astype(float).replace('.',',')

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
        sql = '''INSERT INTO ''' + tabela_oracle +''' VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        cursor.executemany(sql, dados_list)
    except Exception as err:
        print('Erro no Insert:', err)
    else:
        print('Carga executada com sucesso!')
        connection.commit() #Caso seja executado com sucesso, esse comando salva a base de dados
    finally:
        cursor.close()
        connection.close()




