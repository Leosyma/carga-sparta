# -*- coding: utf-8 -*-
"""
Created on Fri Feb 24 11:37:54 2023

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
tabela_oracle = 'SPARTA_FINANCEIRO'
ano_oracle = "'2023'"



#%% Montar o dataframe da estrutura da aba 'SPARTA_NUC'
#Defino o indice máximo que é igual o número de SPARTA
index_maximo = list(range(0,len(arquivos)))

#Criação da estrutura do tabela NUC
df_financeiro = pd.DataFrame(data = [], columns=['CHAVE','EVENTO_TARIFARIO','ANO','ID','DISTRIBUIDORA','UF','DATA','PERIODO_TARIFARIO','CONTRATO','DOLAR_MEDIO_RS','CVA_TOTAL_RS','SALDO_CVA_TOTAL_RS','NEUTRALIDADE_TOTAL_RS','SOBRECONTRATACAO_TOTAL_RS','PREVISAO_RISCO_HIDROLOGICO_TOTAL_RS','REVERSAO_RISCO_HIDROLOGICO_TOTAL_RS','ESCASSEZ_HIDRICA_TOTAL_RS','CREDITOS_PIS_RS','TOTAL_FINANCEIRO_RS'],index=index_maximo) 

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
 
# #Traz as componentes financeiras separadas   
# df_sparta_financeiro = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Financeiros'
#                               ,header=8
#                               ,usecols=[2,3,4,5,6])

# #Traz somente o valor total das componentes financeiras
# df_sparta_financeiro_total = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Financeiros'
#                               ,header=6
#                               ,nrows = 1
#                               ,usecols=[2,3,4])

# df_sparta_bd = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'BD'
#                               ,header=51
#                               ,nrows = 1
#                               ,usecols=[6,7])


#%%Extração dos resultados
#Funções para extrair dados das SPARTA recentes
def determina_contrato(df_financeiro,df_sparta_mercado,index):
    #Se essa 'string' estiver presente nessa aba o contrato é NOVO
    if 'Percentual RI' in df_sparta_mercado.iloc[:,:].values:
          df_financeiro.at[index,'CONTRATO'] = 'NOVO' 
    #Caso contrário o contrato é ANTIGO      
    else:
          df_financeiro.at[index,'CONTRATO'] = 'ANTIGO'
 
#Função para extrair as componentes financeiras das SPARTA RECENTE  
def extrai_financeiro(df_sparta_financeiro,df_sparta_financeiro_total,df_sparta_bd,df_financeiro,index):
    cva_total = 0  #Variável para salvar o valor total de CVA
    cva_saldo_total = 0  #Variável para salvar o valor total de CVA Saldo a compensar
    neutralidade_total = 0  #Variável para salvar o valor total de Neutralidade
    sobrecontratacao_total = 0  #Variável para salvar o valor total de Sobrecontratação
    previsao_risco_total = 0  #Variável para salvar o valor total de Previsão de Risco Hidrológico
    reversao_risco_total = 0  #Variável para salvar o valor total de Reversão de Risco Hidrológico
    escassez_hidrica_total = 0  #Variável para salvar o valor total de Escassez Hídrica
    creditos_pis_total = 0  #Variável para salvar o valor total de Créditos PIS
    #Passa por todas as linhas do dataframe
    for linha in linhas:
        #Seleciona qual componente financeira queremos pela coluna 'Tipo' do dataframe e salvamos o valor dela na variável correspondente
        if (df_sparta_financeiro.loc[linha,'Tipo'].upper() == 'CVA'):
            cva_total += df_sparta_financeiro.loc[linha,'Valor']
        elif (df_sparta_financeiro.loc[linha,'Tipo'].upper() == 'CVA SALDO A COMPENSAR'):
            cva_saldo_total += df_sparta_financeiro.loc[linha,'Valor']
        elif (df_sparta_financeiro.loc[linha,'Tipo'].upper() == 'NEUTRALIDADE'):
            neutralidade_total += df_sparta_financeiro.loc[linha,'Valor']
        elif df_sparta_financeiro.loc[linha,'Tipo'].upper() == 'SOBRECONTRATAÇÃO':
            sobrecontratacao_total += df_sparta_financeiro.loc[linha,'Valor']
        elif df_sparta_financeiro.loc[linha,'Tipo'].upper() == 'PREVISÃO DE RISCO HIDROLÓGICO':
            previsao_risco_total += df_sparta_financeiro.loc[linha,'Valor']
        elif df_sparta_financeiro.loc[linha,'Tipo'].upper() == 'REVERSÃO DE RISCO HIDROLÓGICO':
            reversao_risco_total += df_sparta_financeiro.loc[linha,'Valor']
        elif 'ESCASSEZ' in df_sparta_financeiro.loc[linha,'Nome do Financeiro'].upper():
            escassez_hidrica_total += df_sparta_financeiro.loc[linha,'Valor']
        elif 'PIS' in df_sparta_financeiro.loc[linha,'Tipo'].upper():
            creditos_pis_total += df_sparta_financeiro.loc[linha,'Valor']
        df_financeiro.at[index,'CVA_TOTAL_RS'] = cva_total  #Insere o valor do 'cva_total' no dataframe
        df_financeiro.at[index,'SALDO_CVA_TOTAL_RS'] = cva_saldo_total  #Insere o valor do 'cva_saldo_total' no dataframe
        df_financeiro.at[index,'NEUTRALIDADE_TOTAL_RS'] = neutralidade_total  #Insere o valor do 'neutralidade_total' no dataframe
        df_financeiro.at[index,'SOBRECONTRATACAO_TOTAL_RS'] = sobrecontratacao_total  #Insere o valor do 'sobrecontratacao_total' no dataframe
        df_financeiro.at[index,'PREVISAO_RISCO_HIDROLOGICO_TOTAL_RS'] = previsao_risco_total  #Insere o valor do 'previsao_risco_total' no dataframe
        df_financeiro.at[index,'REVERSAO_RISCO_HIDROLOGICO_TOTAL_RS'] = reversao_risco_total  #Insere o valor do 'reversao_risco_total' no dataframedf_financeiro.at[index,'CVA_TOTAL_RS'] = cva_total  #Insere o valor do 'cva_total' no dataframe
        df_financeiro.at[index,'ESCASSEZ_HIDRICA_TOTAL_RS'] = escassez_hidrica_total  #Insere o valor do 'escassez_hidrica_total' no dataframe
        df_financeiro.at[index,'CREDITOS_PIS_RS'] = creditos_pis_total  #Insere o valor do 'creditos_pis_total' no dataframe
        
    #Determina o valor total das componentes financeiras    
    df_financeiro.at[index,'TOTAL_FINANCEIRO_RS'] = df_sparta_financeiro_total.iloc[0,1] 
     
    #Determina o valor do dólar médio
    df_financeiro.at[index,'DOLAR_MEDIO_RS'] = df_sparta_bd.iloc[0,1]

 
#Função para extrair as componentes financeiras das SPARTA ANTIGA   
def extrai_financeiro_sparta_antiga(df_sparta_financeiro,df_sparta_financeiro_total,df_sparta_bd,df_financeiro,index):
    cva_total = 0  #Variável para salvar o valor total de CVA
    cva_saldo_total = 0  #Variável para salvar o valor total de CVA Saldo a compensar
    neutralidade_total = 0  #Variável para salvar o valor total de Neutralidade
    #Passa por todas as linhas do dataframe
    for linha in linhas:
        try:
            #Seleciona qual componente financeira queremos pela coluna 'Tipo' do dataframe e salvamos o valor dela na variável correspondente
            if (df_sparta_financeiro.loc[linha,'Nome do Financeiro'].startswith('CVA')):
                cva_total += float(str(df_sparta_financeiro.loc[linha,'Valor']))
            elif (df_sparta_financeiro.loc[linha,'Nome do Financeiro'].startswith('Saldo')):
                cva_saldo_total += float(str(df_sparta_financeiro.loc[linha,'Valor']))
            elif (df_sparta_financeiro.loc[linha,'Nome do Financeiro'].startswith('Neutralidade')):
                neutralidade_total += float(str(df_sparta_financeiro.loc[linha,'Valor']))

        except:
            print('Erro!!!')

        df_financeiro.at[index,'CVA_TOTAL_RS'] = cva_total  #Insere o valor do 'cva_total' no dataframe
        df_financeiro.at[index,'SALDO_CVA_TOTAL_RS'] = cva_saldo_total  #Insere o valor do 'cva_saldo_total' no dataframe
        df_financeiro.at[index,'NEUTRALIDADE_TOTAL_RS'] = neutralidade_total  #Insere o valor do 'neutralidade_total' no dataframe

        
    #Determina o valor total das componentes financeiras    
    df_financeiro.at[index,'TOTAL_FINANCEIRO_RS'] = df_sparta_financeiro_total.iloc[0,1] 
     
    #Determina o valor do dólar médio
    df_financeiro.at[index,'DOLAR_MEDIO_RS'] = df_sparta_bd.iloc[0,1]

       
def distribuidora(df_financeiro,df_sparta_capa,index):
    #Determina o ANO da SPARTA
    df_financeiro.at[index,'ANO'] = df_sparta_capa.iloc[8,1].strftime('%Y')
    
    #Determina o ID da Distribuidora
    df_financeiro.at[index,'ID'] = df_sparta_capa.iloc[1,1]
    
    #Determina o NOME da Distribuidora
    df_financeiro.at[index,'DISTRIBUIDORA'] = df_sparta_capa.iloc[0,0].upper()
    
    #Determina a DATA da SPARTA
    df_financeiro.at[index,'DATA'] = df_sparta_capa.iloc[8,1].strftime('%Y-%m-%d')
    
    #Determina o EVENTO TARIFARIO
    if 'Reajuste' in df_sparta_capa.iloc[0,1]:
        df_financeiro.at[index,'EVENTO_TARIFARIO'] = 'RTA'
    elif 'Revisão' in df_sparta_capa.iloc[0,1]:
        df_financeiro.at[index,'EVENTO_TARIFARIO'] = 'RTP'
    else:
        df_financeiro.at[index,'EVENTO_TARIFARIO'] = 'RTE'
        
    #Determina a CHAVE
    df_financeiro.at[index,'CHAVE'] = df_financeiro.loc[index,'EVENTO_TARIFARIO']+df_financeiro.loc[index,'ANO']+df_financeiro.loc[index,'ID']
    
    #Determina UF e PERIODO TARIFARIO
    #AES SUL
    if df_financeiro.loc[index,'ID'] == 'D01':
        df_financeiro.at[index,'UF'] = 'RS'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #AME
    if df_financeiro.loc[index,'ID'] == 'D02':
        df_financeiro.at[index,'UF'] = 'AM'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #AMPLA
    if df_financeiro.loc[index,'ID'] == 'D03':
        df_financeiro.at[index,'UF'] = 'RJ'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #BANDEIRANTE
    if df_financeiro.loc[index,'ID'] == 'D04':
        df_financeiro.at[index,'UF'] = 'SP'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #BOA VISTA
    if df_financeiro.loc[index,'ID'] == 'D05':
        df_financeiro.at[index,'UF'] = 'RR'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CAIUA
    if df_financeiro.loc[index,'ID'] == 'D06':
        df_financeiro.at[index,'UF'] = 'SP'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEA
    if df_financeiro.loc[index,'ID'] == 'D07':
        df_financeiro.at[index,'UF'] = 'AP'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CEAL
    if df_financeiro.loc[index,'ID'] == 'D08':
        df_financeiro.at[index,'UF'] = 'AL'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CEB
    if df_financeiro.loc[index,'ID'] == 'D09':
        df_financeiro.at[index,'UF'] = 'DF'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '4'
    
    #CEEE
    if df_financeiro.loc[index,'ID'] == 'D10':
        df_financeiro.at[index,'UF'] = 'RS'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
      
    #CELESC
    if df_financeiro.loc[index,'ID'] == 'D11':
        df_financeiro.at[index,'UF'] = 'SC'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CELG
    if df_financeiro.loc[index,'ID'] == 'D12':
        df_financeiro.at[index,'UF'] = 'GO'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CELPA
    if df_financeiro.loc[index,'ID'] == 'D13':
        df_financeiro.at[index,'UF'] = 'PA'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CELPE
    if df_financeiro.loc[index,'ID'] == 'D14':
        df_financeiro.at[index,'UF'] = 'PE'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CELTINS
    if df_financeiro.loc[index,'ID'] == 'D15':
        df_financeiro.at[index,'UF'] = 'TO'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEMAR
    if df_financeiro.loc[index,'ID'] == 'D16':
        df_financeiro.at[index,'UF'] = 'MA'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #EMT
    if df_financeiro.loc[index,'ID'] == 'D17':
        df_financeiro.at[index,'UF'] = 'MT'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEMIG-D
    if df_financeiro.loc[index,'ID'] == 'D18':
        df_financeiro.at[index,'UF'] = 'MG'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CEPISA
    if df_financeiro.loc[index,'ID'] == 'D19':
        df_financeiro.at[index,'UF'] = 'PI'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CERON
    if df_financeiro.loc[index,'ID'] == 'D20':
        df_financeiro.at[index,'UF'] = 'RO'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CERR (CONFIMAR O PERIODO)
    if df_financeiro.loc[index,'ID'] == 'D21':
        df_financeiro.at[index,'UF'] = 'RR'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CFLO
    if df_financeiro.loc[index,'ID'] == 'D22':
        df_financeiro.at[index,'UF'] = 'PR'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CHESP
    if df_financeiro.loc[index,'ID'] == 'D23':
        df_financeiro.at[index,'UF'] = 'GO'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL JAGUARI
    if df_financeiro.loc[index,'ID'] == 'D24':
        df_financeiro.at[index,'UF'] = 'SP'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL MOCOCA
    if df_financeiro.loc[index,'ID'] == 'D25':
        df_financeiro.at[index,'UF'] = 'SP'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'        
    
    #CPFL Santa Cruz
    if df_financeiro.loc[index,'ID'] == 'D26':
        df_financeiro.at[index,'UF'] = 'SP'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CNEE
    if df_financeiro.loc[index,'ID'] == 'D27':
        df_financeiro.at[index,'UF'] = 'SP'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COCEL
    if df_financeiro.loc[index,'ID'] == 'D28':
        df_financeiro.at[index,'UF'] = 'PR'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COELBA
    if df_financeiro.loc[index,'ID'] == 'D29':
        df_financeiro.at[index,'UF'] = 'BA'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COELCE
    if df_financeiro.loc[index,'ID'] == 'D30':
        df_financeiro.at[index,'UF'] = 'CE'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #COOPERALIANÇA
    if df_financeiro.loc[index,'ID'] == 'D31':
        df_financeiro.at[index,'UF'] = 'SC'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COPEL
    if df_financeiro.loc[index,'ID'] == 'D32':
        df_financeiro.at[index,'UF'] = 'PR'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #COSERN
    if df_financeiro.loc[index,'ID'] == 'D33':
        df_financeiro.at[index,'UF'] = 'RN'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Leste Paulista
    if df_financeiro.loc[index,'ID'] == 'D34':
        df_financeiro.at[index,'UF'] = 'SP'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Piratininga
    if df_financeiro.loc[index,'ID'] == 'D35':
        df_financeiro.at[index,'UF'] = 'SP'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #CPFL Paulista
    if df_financeiro.loc[index,'ID'] == 'D36':
        df_financeiro.at[index,'UF'] = 'SP'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Sul Paulista
    if df_financeiro.loc[index,'ID'] == 'D37':
        df_financeiro.at[index,'UF'] = 'SP'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #DEMEI
    if df_financeiro.loc[index,'ID'] == 'D38':
        df_financeiro.at[index,'UF'] = 'RS'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #DME-PC
    if df_financeiro.loc[index,'ID'] == 'D39':
        df_financeiro.at[index,'UF'] = 'MG'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EBO
    if df_financeiro.loc[index,'ID'] == 'D40':
        df_financeiro.at[index,'UF'] = 'PB'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #EDEVP
    if df_financeiro.loc[index,'ID'] == 'D41':
        df_financeiro.at[index,'UF'] = 'SP'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EEB
    if df_financeiro.loc[index,'ID'] == 'D42':
        df_financeiro.at[index,'UF'] = 'SP'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EFLJC
    if df_financeiro.loc[index,'ID'] == 'D43':
        df_financeiro.at[index,'UF'] = 'SC'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EFLUL
    if df_financeiro.loc[index,'ID'] == 'D44':
        df_financeiro.at[index,'UF'] = 'SC'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ELEKTRO
    if df_financeiro.loc[index,'ID'] == 'D45':
        df_financeiro.at[index,'UF'] = 'SP'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELETROACRE
    if df_financeiro.loc[index,'ID'] == 'D46':
        df_financeiro.at[index,'UF'] = 'AC'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELETROCAR
    if df_financeiro.loc[index,'ID'] == 'D47':
        df_financeiro.at[index,'UF'] = 'RS'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ELETROPAULO
    if df_financeiro.loc[index,'ID'] == 'D48':
        df_financeiro.at[index,'UF'] = 'SP'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ELFSM
    if df_financeiro.loc[index,'ID'] == 'D49':
        df_financeiro.at[index,'UF'] = 'ES'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EMG
    if df_financeiro.loc[index,'ID'] == 'D50':
        df_financeiro.at[index,'UF'] = 'MG'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EMS
    if df_financeiro.loc[index,'ID'] == 'D51':
        df_financeiro.at[index,'UF'] = 'MS'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ENF
    if df_financeiro.loc[index,'ID'] == 'D52':
        df_financeiro.at[index,'UF'] = 'RJ'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #EPB
    if df_financeiro.loc[index,'ID'] == 'D53':
        df_financeiro.at[index,'UF'] = 'PB'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '4'
        
    #ESCELSA (CONFIRMAR PERIODO)
    if df_financeiro.loc[index,'ID'] == 'D54':
        df_financeiro.at[index,'UF'] = 'ES'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '3'
        
    #ESE
    if df_financeiro.loc[index,'ID'] == 'D55':
        df_financeiro.at[index,'UF'] = 'SE'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #FORCEL
    if df_financeiro.loc[index,'ID'] == 'D56':
        df_financeiro.at[index,'UF'] = 'PR'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #HIDROPAN
    if df_financeiro.loc[index,'ID'] == 'D57':
        df_financeiro.at[index,'UF'] = 'RS'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #IENERGIA
    if df_financeiro.loc[index,'ID'] == 'D58':
        df_financeiro.at[index,'UF'] = 'SC'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #JARI (CONFIRMAR PERIODO)
    if df_financeiro.loc[index,'ID'] == 'D59':
        df_financeiro.at[index,'UF'] = 'PA'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #LIGHT
    if df_financeiro.loc[index,'ID'] == 'D60':
        df_financeiro.at[index,'UF'] = 'RJ'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #MUX-Energia
    if df_financeiro.loc[index,'ID'] == 'D61':
        df_financeiro.at[index,'UF'] = 'RS'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
    
    #CPFL RGE
    if df_financeiro.loc[index,'ID'] == 'D62':
        df_financeiro.at[index,'UF'] = 'RS'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #SULGIPE
    if df_financeiro.loc[index,'ID'] == 'D63':
        df_financeiro.at[index,'UF'] = 'SE'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #UHENPAL
    if df_financeiro.loc[index,'ID'] == 'D64':
        df_financeiro.at[index,'UF'] = 'TO'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #ESS
    if df_financeiro.loc[index,'ID'] == 'D65':
        df_financeiro.at[index,'UF'] = 'SP'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #CPFL Santa Cruz
    if df_financeiro.loc[index,'ID'] == 'D66':
        df_financeiro.at[index,'UF'] = 'SP'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'
        
    #RGE
    if df_financeiro.loc[index,'ID'] == 'D67':
        df_financeiro.at[index,'UF'] = 'RS'
        df_financeiro.at[index,'PERIODO_TARIFARIO'] = '5'

         
 
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
                              
        #Traz as componentes financeiras separadas   
        df_sparta_financeiro = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Financeiros'
                                      ,header=8
                                      ,usecols=[2,3,4,5,6])

        #Traz somente o valor total das componentes financeiras
        df_sparta_financeiro_total = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'Financeiros'
                                      ,header=6
                                      ,nrows = 1
                                      ,usecols=[2,3,4])

        df_sparta_bd = pd.read_excel(os.path.join(pasta,arquivo),sheet_name = 'BD'
                                      ,header=51
                                      ,nrows = 1
                                      ,usecols=[6,7]) 

        print('Leu o arquivo: ',arquivo)

        
        #Dropa todas as linhas que possuem algum valor NaN
        df_sparta_financeiro = df_sparta_financeiro.dropna(axis=0) 
        df_sparta_financeiro = df_sparta_financeiro.reset_index(drop=True)  #reseta o indice depois de dropar as linhas NaN

        
        #Função para extração dos dados da distribuidora e tipo de contrato
        determina_contrato(df_financeiro,df_sparta_mercado,index)
        distribuidora(df_financeiro,df_sparta_capa,index)
        
        
        #Define o intervalo máximo de linhas do dataframe 
        linhas = range(len(df_sparta_financeiro.index))
        
        #Função para extrair os dados de 'Financeiro'
        if 19 in df_sparta_financeiro.iloc[:,:].values:
            extrai_financeiro_sparta_antiga(df_sparta_financeiro,df_sparta_financeiro_total,df_sparta_bd,df_financeiro,index)
        else:
            extrai_financeiro(df_sparta_financeiro,df_sparta_financeiro_total,df_sparta_bd,df_financeiro,index)
        
        
        print('Extraiu o dado do arquivo: ',arquivo)
        print('Arquivos extraídos: ',round((index/len(arquivos)),2)*100,'%')
        index = index + 1 #Passa para o próximo indice para conseguir dados da proxima SPARTA
                                  
                      
                       
    except:
        print('Aba não disponível na SPARTA', arquivo)
    
    

#%%Tratamento de dados 
#Remover dados duplicados e linhas nulas
df_financeiro = df_financeiro.drop_duplicates(subset = 'CHAVE',ignore_index = True)    
df_financeiro = df_financeiro.dropna(axis=0,how='all')

#Limpeza e Tratamento dos dados
df_financeiro = df_financeiro.astype(str)
df_financeiro['PERIODO_TARIFARIO'] = df_financeiro['PERIODO_TARIFARIO'].astype(int)
df_financeiro['DOLAR_MEDIO_RS'] = df_financeiro['DOLAR_MEDIO_RS'].replace('nan','0').astype(float).replace('.',',')
df_financeiro['CVA_TOTAL_RS'] = df_financeiro['CVA_TOTAL_RS'].replace('nan','0').astype(float).replace('.',',')
df_financeiro['SALDO_CVA_TOTAL_RS'] = df_financeiro['SALDO_CVA_TOTAL_RS'].replace('nan','0').astype(float).replace('.',',')
df_financeiro['NEUTRALIDADE_TOTAL_RS'] = df_financeiro['NEUTRALIDADE_TOTAL_RS'].replace('nan','0').astype(float).replace('.',',')
df_financeiro['SOBRECONTRATACAO_TOTAL_RS'] = df_financeiro['SOBRECONTRATACAO_TOTAL_RS'].replace('nan','0').astype(float).replace('.',',')
df_financeiro['PREVISAO_RISCO_HIDROLOGICO_TOTAL_RS'] = df_financeiro['PREVISAO_RISCO_HIDROLOGICO_TOTAL_RS'].replace('nan','0').astype(float).replace('.',',')
df_financeiro['REVERSAO_RISCO_HIDROLOGICO_TOTAL_RS'] = df_financeiro['REVERSAO_RISCO_HIDROLOGICO_TOTAL_RS'].replace('nan','0').astype(float).replace('.',',')
df_financeiro['ESCASSEZ_HIDRICA_TOTAL_RS'] = df_financeiro['ESCASSEZ_HIDRICA_TOTAL_RS'].replace('nan','0').astype(float).replace('.',',')
df_financeiro['CREDITOS_PIS_RS'] = df_financeiro['CREDITOS_PIS_RS'].replace('nan','0').astype(float).replace('.',',')
df_financeiro['TOTAL_FINANCEIRO_RS'] = df_financeiro['TOTAL_FINANCEIRO_RS'].replace('nan','0').astype(float).replace('.',',')

# Define a data de atualização dos dados
data = datetime.today().strftime('%d/%m/%Y')
df_financeiro['DATA_ATUALIZA'] = data

#%% Inserir dados no banco de dados

#Criar a lista para inserção no banco SQL com os dados da base editada
dados_list = df_financeiro.values.tolist()


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
        sql = '''INSERT INTO ''' + tabela_oracle +''' VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20)''' #Deve ser igual ao número de colunas da tabela do banco de dados
        cursor.executemany(sql, dados_list)
    except Exception as err:
        print('Erro no Insert:', err)
    else:
        print('Carga executada com sucesso!')
        connection.commit() #Caso seja executado com sucesso, esse comando salva a base de dados
    finally:
        cursor.close()
        connection.close()


