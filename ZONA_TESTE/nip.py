# Importando bibliotecas

import pandas as pd
import unidecode
import os
import shutil
import webdriver_manager
from datetime import date
import time
import pyodbc as pyodbc
import pyautogui
import csv
from selenium import webdriver
from selenium.webdriver.common.by import By

#qw?YJ%w

# <<<<<<<<<<<<<<<<<<<Donwload das NIP do site da ANS e Tratativas dos arquivos em CSV>>>>>>>>>>>>>>>>>>>

# login e senha

usuario = ("402.086.558-24")
senha = ("UnihospS@ude123")

                          # começo do código

# Define o cabeçalho do usuário com a versão correta do Chrome
user_agente = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, como Gecko) Chrome/120.0.6099.71 Safari/537.36'

# Inicializa o driver do Chrome
navegador = webdriver.Chrome()

# Abre a página de NIP
navegador.get("https://www2.ans.gov.br/nip_operadora/?target=975e076c781bbe7b91c24a195d7ae003a2fd781f6d2f5d9b97a53cbcb1196626")

time.sleep(6)

# Realizando o login e senha

usuarioLogin = navegador.find_element(By.XPATH, '//*[@id="input-mask"]')
usuarioLogin.send_keys(usuario)
time.sleep(2)

usuarioSenha = navegador.find_element(By.XPATH, '//*[@id="mod-login-password"]')
usuarioSenha.send_keys(senha)
time.sleep(1.5)

botao_login = navegador.find_element(By.XPATH, '//*[@id="botao"]').click()
time.sleep(4)

# Fim do login e senha


# Baixando informações

# Guia Aguardadndo Resposta
excel_aguardando_resposta = navegador.find_element(By.XPATH, '/html/body/div[2]/div/div/form/div[5]/div[1]/div[2]/table/tbody/tr[10]/td/div/div/div[1]/div[2]/div/a/img').click()
time.sleep(10)

# entrando na guia Em Andamento

guia_em_andamento = navegador.find_element(By.XPATH, '/html/body/div[2]/div/div/form/div[5]/div[1]/div[2]/table/tbody/tr[10]/td/div/ul/li[2]/a').click()
time.sleep(18)

excel_em_andamento = navegador.find_element(By.XPATH, '/html/body/div[2]/div/div/form/div[5]/div[1]/div[2]/table/tbody/tr[10]/td/div/div/div[2]/div[2]/div/a/img').click()
time.sleep(10)

# Entrando na guia - Finalizadas Não Resolvidas

guia_finalizadas_nao_resolvidas = navegador.find_element(By.XPATH, '/html/body/div[2]/div/div/form/div[5]/div[1]/div[2]/table/tbody/tr[10]/td/div/ul/li[3]/a').click()
time.sleep(18)

time.sleep(4)
excel_finalizadas_nao_resolvidas = navegador.find_element(By.XPATH, '/html/body/div[2]/div/div/form/div[5]/div[1]/div[2]/table/tbody/tr[10]/td/div/div/div[3]/div[2]/div/a/img').click()
time.sleep(18)

time.sleep(6)
# Entrando na guia - Finalizadas

time.sleep(6)
guia_finalizadas = navegador.find_element(By.XPATH, '/html/body/div[2]/div/div/form/div[5]/div[1]/div[2]/table/tbody/tr[10]/td/div/ul/li[4]/a').click()
time.sleep(6)

excel_finalizadas = navegador.find_element(By.XPATH, '/html/body/div[2]/div/div/form/div[5]/div[1]/div[2]/table/tbody/tr[10]/td/div/div/div[4]/div[2]/div/a/img').click()
time.sleep(18)

# Entrando na guia - Aguandando Documentos de Classificação Residual

guia_aguardando_documentos = navegador.find_element(By.XPATH, '/html/body/div[2]/div/div/form/div[5]/div[1]/div[2]/table/tbody/tr[10]/td/div/ul/li[5]/a').click()
time.sleep(8)

excel_aguardando_documentos = navegador.find_element(By.XPATH, '/html/body/div[2]/div/div/form/div[5]/div[1]/div[2]/table/tbody/tr[10]/td/div/div/div[5]/div[2]/div/a/img').click()
time.sleep(8)

navegador.close()


# fim dos Downloads



caminho = "C:\\Users\\wendel.goncalves\\Downloads"
caminhoDestino = "C:\\ARQUIVOS_SQL\\ARQUIVO_NIP"
caminhoBackup = "S:\\DW_BI_UNIHOSP\\REGULATORIO\\BACKUP\\2023"

# obter a data atual em um formato específico
data_atual = date.today().strftime('%Y-%m-%d_')

ListaArquivo = os.listdir(caminho)
for arquivo in ListaArquivo: 
    if 'demandas_' in arquivo:
        read_file = pd.read_excel(caminho + '\\' + arquivo)


        # Remove os acentos de todas as colunas do DataFrame
        read_file = read_file.applymap(lambda x: unidecode.unidecode(str(x)) if isinstance(x, str) else x)

        # Remove os campos que contem ; e substitui por , para não quebrar em coluna o CSV
        read_file.replace(';', ',', regex=True, inplace=True)

        # Envia o arquivo para o caminho salvando em CSV e separador por ;
        read_file.to_csv(caminhoDestino + '\\' + arquivo + '.csv', index=None, header=True, sep=';')
        print('Arquivos: ', arquivo)

        # Move o arquivo
        shutil.move(os.path.join(caminho, arquivo), os.path.join(caminhoBackup, data_atual + arquivo))


time.sleep(5)

# Configuração da conexão com o SQL Server

server = 'PC-IN02\SQLUNIHOSP'
database = 'DB_DAILY'
username = 'operacional.unihosp'
password = 'PBWunihosp2023@$.'
driver = 'ODBC Driver 17 for SQL Server'

# String de conexão
conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Conectar ao SQL Server
cnxn = pyodbc.connect(conn_str)

# Criar um cursor para executar comandos SQL
cursor = cnxn.cursor()

# Nome do job a ser iniciado
job_name = 'JOB_BI_IMPORTA_NIP'

# Comando para iniciar o job
command = f'EXEC msdb.dbo.sp_start_job @job_name = N\'{job_name}\''

# Executar o comando para iniciar o job
cursor.execute(command)

# Confirmar as alterações
cnxn.commit()

# Consultar o status do job
query = f"EXEC msdb.dbo.sp_help_job @job_name = N'{job_name}'"

# Executar a consulta
cursor.execute(query)

# Obter o resultado da consulta
result = cursor.fetchall()

# Imprimir o resultado
for row in result:
    print(row)

# Fechar o cursor e a conexão
server = 'PC-IN02\SQLUNIHOSP'
cursor.close()

# <<<<<<<<<<<<<<<<<<< FIM: Donwload das NIP do site da ANS e Tratativas dos arquivos em CSV>>>>>>>>>>>>>>>>>>>



# <<<<<<<<<<<<<<<<<<INICIO: Download do Excel 365 Input/preenchimento Regunlatório>>>>>>>>>>>>>>>>>>


time.sleep(5)

# login e senha do Excel 365

usuarioINPUT = ("unihosp26@unihospsaude.onmicrosoft.com")
senhaINPUT = ("Bia052430")

# começo do códigos

# Define o cabeçalho do usuário com a versão correta do Chrome
user_agente = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, como Gecko) Chrome/118.0.0.0 Safari/537.36'

# Configuração das opções do Chrome
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--start-maximized')  # Inicializa o Chrome maximizado

# Inicializa o driver do Chrome com as opções configuradas
navegadorInput = webdriver.Chrome(options=chrome_options)

# Abre a página de NIP
navegadorInput.get("https://unihospsaude-my.sharepoint.com/:x:/g/personal/unihosp26_unihospsaude_onmicrosoft_com/EYe4xSYuq9FOprmVIYfNfKIBEHKwqPMthtBDxOfzgzvSpQ?e=bCCjHa")


# Realizando o login e senha
"""
usuarioLoginINPUT = navegadorINPUT.find_element(By.XPATH, '/html/body/div/form[1]/div/div/div[2]/div[1]/div/div/div/div/div[1]/div[3]/div/div/div/div[2]/div[2]/div/input[1]')
usuarioLoginINPUT.send_keys(usuarioINPUT)
time.sleep(2)

clicando_avancarINPUT = navegadorINPUT.find_element(By.XPATH,'/html/body/div/form[1]/div/div/div[2]/div[1]/div/div/div/div/div[1]/div[3]/div/div/div/div[4]/div/div/div/div/input').click()
time.sleep(3)

usuarioSenhaINPUT = navegadorINPUT.find_element(By.XPATH, '/html/body/div/form[1]/div/div/div[2]/div[1]/div/div/div/div/div/div[3]/div/div[2]/div/div[3]/div/div[2]/input')
usuarioSenhaINPUT.send_keys(senhaINPUT)
time.sleep(2)

botao_loginINPUT = navegadorINPUT.find_element(By.XPATH, '/html/body/div/form[1]/div/div/div[2]/div[1]/div/div/div/div/div/div[3]/div/div[2]/div/div[4]/div[2]/div/div/div/div/input').click()
time.sleep(3)

continuar_conectadoINPUT = navegadorINPUT.find_element(By.XPATH, '/html/body/div/form/div/div/div[2]/div[1]/div/div/div/div/div/div[3]/div/div[2]/div/div[3]/div[2]/div/div/div[2]/input').click()


# Fim do login e senha

time.sleep(8)

# Realizando o download do arquivo

# Clicando em Arquivo e fazendo o Download
pyautogui.click(x=52, y=186)
time.sleep(0.5)

pyautogui.click(x=68, y=275)
time.sleep(0.5)

pyautogui.click(x=349, y=245)

"""
time.sleep(8)
# Realizando o download do arquivo com clique no teclado

# Clique ALT
pyautogui.keyDown("alt")
time.sleep(1.5)
pyautogui.keyUp("alt")

# Clique A
pyautogui.keyDown("a")
time.sleep(1.5)
pyautogui.keyUp("a")

# Pressione a tecla "S"
pyautogui.keyDown("s")
time.sleep(1.5)
pyautogui.keyUp("s")

# Pressione a tecla "T"
pyautogui.keyDown("t")
time.sleep(1.5)
pyautogui.keyUp("t")

#fehcando o ChromeX
time.sleep(5)
navegadorInput.close()

# fim do download do arquivo


# Salvando o arquivo no C da maquina transformando-o em CSV


caminhoINPUT = "C:\\Users\\wendel.goncalves\\Downloads"
caminhoDestinoINPUT = "C:\\ARQUIVOS_SQL\\ARQUIVO_NIP"
caminhoBackupINPUT = "C:\\BACKUP_EXCEL_365"

data_atualINPUT = date.today().strftime('%Y-%m-%d ')


#Listando os arquivos do Download
ListaArquivoINPUT = os.listdir(caminhoINPUT)


pd.set_option('display.float_format', '{:.17f}'.format)  # Configurar opção de exibição de float_format

for arquivoINPUT in ListaArquivoINPUT:
    if 'BASE INPUT NIP V1.0.1' in arquivoINPUT:
        xls_file = pd.read_excel(caminhoINPUT + '\\' + arquivoINPUT, sheet_name='INPUT NIP - ANS')

        df = xls_file
        # Definir a primeira linha como cabeçalho
        df.columns = df.iloc[0]

        df = df[1:]  # Remover a primeira linha (antigo cabeçalho)

        # Remover os acentos de todas as colunas do DataFrame
        df = df.applymap(lambda x: unidecode.unidecode(str(x)) if isinstance(x, str) else x)

        # Formatar as colunas de datas
        colunas_data = ['DT NOTICACAO']
        for coluna in colunas_data:
            df[coluna] = pd.to_datetime(df[coluna], dayfirst=True)

        Coluna_dtClassificacao = ['DT DE CLASSIFICAÇÃO']
        for coluna in Coluna_dtClassificacao:
            df[coluna] = pd.to_datetime(df[coluna], errors='coerce').dt.date

        # Ajustando para que não vire numero cinetifico
        df['Nº PROC ADM'] = df['Nº PROC ADM'].fillna('').astype(object)

        # Retirando palavras indesejadas
        colunas_str = ['NATUREZA', 'N_EXECUCAO_FISCAL', 'DEFESA_RECURSO_ADM']
        for coluna in colunas_str:
            df[coluna] = df[coluna].astype(str).str.replace('_x000D_', '', case=False).replace('nan', '')

        # Formatar a coluna MULTA_BASE como valor decimal
        #df['MULTA_BASE'] = df['MULTA_BASE'].astype(str).str.replace('.', '').str.replace(',', '').str.replace('nan', '')

        # Salvar o arquivo CSV com separador ";"
        csv_path = caminhoDestinoINPUT + '\\' + arquivoINPUT + '.csv'
        df.to_csv(csv_path, index=None, header=True, sep=';', quoting=csv.QUOTE_NONNUMERIC)

        # Configurar opção de exibição de float_format
        pd.set_option('display.float_format', '{:.17f}'.format)

        # Mover o novo arquivo para o diretório de backup
        shutil.move(os.path.join(caminhoINPUT, arquivoINPUT),os.path.join(caminhoBackupINPUT, data_atualINPUT + arquivoINPUT))

        print('Arquivo:', arquivoINPUT)

# Fim da transaformação do arquivo e salvando no diretório C da máquina



# Configuração da conexão com o SQL Server

server = 'PC-IN02\SQLUNIHOSP'
database = 'DB_DAILY'
username = 'operacional.unihosp'
password = 'PBWunihosp2023@$.'
driver = 'ODBC Driver 17 for SQL Server'

# String de conexão
conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Conectar ao SQL Server
cnxn = pyodbc.connect(conn_str)

# Criar um cursor para executar comandos SQL
cursorPRC = cnxn.cursor()

# Executar uma stored procedure de importação da tabela TB_INPUT para o Banco
etapa1 = 'PRC_BI_IMPORTA_INPUT_NIP'
cursorPRC.execute(f"EXEC {etapa1}")

#Executar uma stored procedure para atualizar novas NIP's para TB_INPUT
etapa2 = 'PRC_BI_PREENCHIMENTO_TB_INPUT'
cursorPRC.execute(f"EXEC {etapa2}")

#Executar uma store procedure para atualizar a VIEW de NIP do PBI
etapa3 = 'PRC_BI_VW_NIP'
cursorPRC.execute(f"EXEC {etapa3}")


# Confirmar as alterações (caso a stored procedure faça alterações no banco)
cnxn.commit()

# Fechar o cursor e a conexão
cursorPRC.close()
cnxn.close()

#  <<<<<<<<<<<<<<<<<<< FIM: Download Excel 365 Tabela INPUT/Preenchida pelo regulatório>>>>>>>>>>>>>>>>>>>





