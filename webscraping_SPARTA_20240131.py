# -*- coding: utf-8 -*-
"""
Created on Mon Jul 31 12:02:46 2023

@author: 2018459
"""

#%% Bibliotecas
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
import urllib
import requests
import wget
import urllib.request
from unidecode import unidecode
import re
import pandas as pd
import numpy as np
import time
import glob
import os
import cx_Oracle


#%% Definição do caminho para extração dos dados

#Site de onde serão extraídos os dados
site = 'https://www2.aneel.gov.br/aplicacoes_liferay/tarifa/'

#Local onde está localizado o chromedriver
chromedriver =  r"C:\Users\2018459\OneDrive - CPFL Energia S A\Documentos\Chromedriver\versao 120\chromedriver.exe"

# Abre o navegador
options = webdriver.ChromeOptions()
service = ChromeService(executable_path=chromedriver)
navegador = webdriver.Chrome(service=service, options=options)


#%% Acessando o site
# Vai até o site definido
navegador.get(site)

# Função para selecionar todos os processos de 2023
def sparta_2023():
    # Selecionando os campos
    # Seleciona 'Concessionária de Distribuição' no campo 'Categoria do Agente'
    navegador.find_element('xpath','/html/body/table/tbody/tr[2]/td[1]/select/option[4]').click()
    time.sleep(3)
    
    # Seleciona 'Todos' no campo 'Agente'
    navegador.find_element('xpath','/html/body/table/tbody/tr[2]/td[2]/select/option[2]').click()
    time.sleep(3)
    
    # Seleciona 'Todos' no campo 'Tipo de Processo'
    navegador.find_element('xpath','/html/body/table/tbody/tr[2]/td[3]/select/option[2]').click()
    time.sleep(3)
    
    # Seleciona '2023' no campo 'Ano'
    navegador.find_element('xpath','/html/body/table/tbody/tr[2]/td[4]/select/option[13]').click()
    time.sleep(3)
    
    # Clica no botão 'Procurar'
    navegador.find_element('xpath','/html/body/table/tbody/tr[2]/td[5]/input').click()
    time.sleep(3)
    
    
# Função para selecionar somente os processos de 'Revisão'
def sparta_rtp():
    # Selecionando os campos
    # Seleciona 'Concessionária de Distribuição' no campo 'Categoria do Agente'
    navegador.find_element('xpath','/html/body/table/tbody/tr[2]/td[1]/select/option[4]').click()
    time.sleep(3)
    
    # Seleciona 'Todos' no campo 'Agente'
    navegador.find_element('xpath','/html/body/table/tbody/tr[2]/td[2]/select/option[2]').click()
    time.sleep(3)
    
    # Seleciona 'Revisão' no campo 'Tipo de Processo'
    navegador.find_element('xpath','/html/body/table/tbody/tr[2]/td[3]/select/option[5]').click()
    time.sleep(3)
    
    # Seleciona '2023' no campo 'Ano'
    navegador.find_element('xpath','/html/body/table/tbody/tr[2]/td[4]/select/option[2]').click()
    time.sleep(3)
    
    # Clica no botão 'Procurar'
    navegador.find_element('xpath','/html/body/table/tbody/tr[2]/td[5]/input').click()
    time.sleep(3)
    
def sparta_rte():
    # Selecionando os campos
    # Seleciona 'Concessionária de Distribuição' no campo 'Categoria do Agente'
    navegador.find_element('xpath','/html/body/table/tbody/tr[2]/td[1]/select/option[4]').click()
    time.sleep(3)
    
    # Seleciona 'Todos' no campo 'Agente'
    navegador.find_element('xpath','/html/body/table/tbody/tr[2]/td[2]/select/option[2]').click()
    time.sleep(3)
    
    # Seleciona 'Revisão Extraordinária' no campo 'Tipo de Processo'
    navegador.find_element('xpath','/html/body/table/tbody/tr[2]/td[3]/select/option[3]').click()
    time.sleep(3)
    
    # Seleciona '2023' no campo 'Ano'
    navegador.find_element('xpath','/html/body/table/tbody/tr[2]/td[4]/select/option[2]').click()
    time.sleep(3)
    
    # Clica no botão 'Procurar'
    navegador.find_element('xpath','/html/body/table/tbody/tr[2]/td[5]/input').click()
    time.sleep(3)
    
    
# Função para selecionar somente os processos de 'Reajuste'
def sparta_rta():
    # Selecionando os campos
    # Seleciona 'Concessionária de Distribuição' no campo 'Categoria do Agente'
    navegador.find_element('xpath','/html/body/table/tbody/tr[2]/td[1]/select/option[4]').click()
    time.sleep(3)
    
    # Seleciona 'Todos' no campo 'Agente'
    navegador.find_element('xpath','/html/body/table/tbody/tr[2]/td[2]/select/option[2]').click()
    time.sleep(3)
    
    # Seleciona 'Reajuste' no campo 'Tipo de Processo'
    navegador.find_element('xpath','/html/body/table/tbody/tr[2]/td[3]/select/option[8]').click()
    time.sleep(3)
    
    # Seleciona '2023' no campo 'Ano'
    navegador.find_element('xpath','/html/body/table/tbody/tr[2]/td[4]/select/option[2]').click()
    time.sleep(3)
    
    # Clica no botão 'Procurar'
    navegador.find_element('xpath','/html/body/table/tbody/tr[2]/td[5]/input').click()
    time.sleep(3)


# Extrai links do site
def extrai_link(urls_sparta):
    # Extrai todos os links presentes na pasta
    links = navegador.find_elements(By.TAG_NAME,'a')
    for link in links:
        # print(link.get_attribute('href'))
        # Filtra somente os links referentes a SPARTA
        if 'SPARTA' in link.get_attribute('href').upper():
            urls_sparta.append(link.get_attribute('href'))
            print(link.get_attribute('href'))


# Limpa os arquivos na pasta
def limpa_arquivos(pasta_download):
    filelist = glob.glob(pasta_download + '\*')
    for f in filelist:
        os.remove(f)
   
    
# Faz o download das SPARTA
def download_sparta(urls_sparta,pasta_download):
    for url in urls_sparta:
        response = requests.get(url) # Faz a requisição da url
        arquivo = urllib.parse.unquote(url) # Converte a url sem caractere especial
        arquivo = arquivo.split('/')[-1] # Quebra a url pela '/' e traz somenteo nome do arquivo
        with open(os.path.join(pasta_download,arquivo),mode="wb") as file: # Salva o arquivo referente a url na pasta
            file.write(response.content)
    
    
#%% Rodamos as funções
# Download dos processos de 'Revisão'
# Pasta para salvar os arquivos
pasta_download = r'C:\Users\2018459\OneDrive - CPFL Energia S A\Documentos\Projetos\2023\BD RTP e RTA\RTP e RTA (concessionaria)\SPARTA\RTP'

# Variavel para guardar os links da SPARTA
urls_sparta = []

# Acessa o site dos processos RTP
sparta_rtp()

# Extração dos links das SPARTA
extrai_link(urls_sparta)

# Acessa o site dos processos RTE
sparta_rte()

# Extração dos links das SPARTA
extrai_link(urls_sparta)

# Deleta os arquivos na pasta
limpa_arquivos(pasta_download)

# Salva as SPARTA na pasta
download_sparta(urls_sparta,pasta_download)

# Limpa a variável
urls_sparta = []



#%% Rodamos as funções
# Download dos processos de 'Revisão'
# Pasta para salvar os arquivos
pasta_download = r'C:\Users\2018459\OneDrive - CPFL Energia S A\Documentos\Projetos\2023\BD RTP e RTA\RTP e RTA (concessionaria)\SPARTA\RTA'

# Variavel para guardar os links da SPARTA
urls_sparta = []

# Acessa o site dos processos RTA
sparta_rta()

# Extração dos links das SPARTA
extrai_link(urls_sparta)

# Deleta os arquivos na pasta
limpa_arquivos(pasta_download)

# Salva as SPARTA na pasta
download_sparta(urls_sparta,pasta_download)

# Limpa a variável
urls_sparta = []









