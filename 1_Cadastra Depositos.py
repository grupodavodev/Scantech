#05.07.22 - Consulta e cadastro de depositos-lojas na scantech
#contato scantech Rafael - orientacoes em 04-07-22
#https://scanntech.cloud.xwiki.com/xwiki/wiki/di/view/apis/api-stock/

import cx_Oracle
import os
from datetime import datetime,timedelta
import requests
import json
from dotenv import load_dotenv 
load_dotenv() #Carregar variaveis 
#os.getenv("iURL_BASE_WIKIMEE_INTERNA") 

iAMBIENTE_SCANTECH = "prd"

if iAMBIENTE_SCANTECH == "qas":
    iCOMPANY_CODE = "73750"
    iTOKEN = "ZGktZGF2by1sa2oxOiZjJThCM29K" #user: di-davo-lkj1   |    senha: &c%8B3oJ
    iURL_BASE = "http://test.parceiro.scanntech.com/api-stock"
else:
    iCOMPANY_CODE = "39061"
    iTOKEN = "ZGktZGF2by01ZmsyOiNmMS1FMWhM" #user: di-davo-5fk2   |    senha: #f1-E1hL
    iURL_BASE = "http://parceiro.scanntech.com/api-stock"
    

#LOG
iFORMATNAMELOG = datetime.now().strftime('%Y_%m_%d')
iNAMEARQLOG = "_Integra_Scantech_Filiais"
iSTATUSLOG = 2 #2=Gera log FULL     1=gera log de erros e avisos        0=nao gera

#diretorios
if os.name == 'nt': #windows
    dirLOGREQUEST = "//nas/dbxprd/PRD/LOG/SCANTECH/"    
else:
    os.environ['ORACLE_HOME'] = "/usr/lib/oracle/19.6/client64"    
    dirLOGREQUEST = "//dbx/PRD/LOG/SCANTECH/"

#Conexao Oracle
try:    
    myCONNORA = cx_Oracle.connect('davo/d4v0@davoprd') #conexao com o Oracle
    myCONNORA.autocommit = True
    curORA = myCONNORA.cursor() #execucoes Oracle
    try:
        curORA.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ',.' ")
        curORA.execute("alter session set nls_date_format = 'DD/MM/YYYY'")    
    except cx_Oracle.DatabaseError as e_sql: 
        print("Erro : " + str(e_sql))
except:
    print("Erro ao comunicar com Oracle em: " + str(iFORMATNAMELOG))
    exit()
    pass


#GeraLog
def gera_Log(iTEXTOLOG): 
    data_hora_atual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  
    try:
        arquivo = open(dirLOGREQUEST + str(iFORMATNAMELOG) + iNAMEARQLOG  +  '.log', 'a')
        arquivo.writelines(iTEXTOLOG + " \n | " + data_hora_atual + '\n')
        arquivo.close()
    except IndexError:        
        arquivo.close()
        print ("Erro ao criar arquivo txt em: " + str(dirLOGREQUEST) + str(iNAMEARQLOG) + ".log")

def consultaLOJAS():
    url = str(iURL_BASE) + "/v2/companies/" + str(iCOMPANY_CODE) + "/store-warehouses?limit=50"
    payload={}
    headers = {
    'accept': '*/*',
    'Authorization': 'Basic ' + str(iTOKEN)
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    print("ambiente Scantech: " + str(iAMBIENTE_SCANTECH))
    print("status_code: " + str(response.status_code))
    if response.status_code != 200:
        print("erro ao consultar dados!")
        print(response.text)
    else:
        jRETORNO    = json.loads(response.text) 
        print("consultando depositos(lojas) cadastradas:")
        for itens in jRETORNO['store_warehouses']:
            print(itens)

def consultaLOJAS_DAVO():
    iQUERY = (" " +
    "         SELECT t.tip_codigo, " +
    "         t.tip_digito, " +
    "         t.tip_nome_fantasia, " +
    "         t.tip_regiao " +
    "     FROM rms.aa2ctipo t " +
    "     WHERE t.tip_loj_cli = 'L' " +
    "     AND t.tip_natureza = 'LS' " +
    "     AND t.tip_regiao in (2, " +
    "                         8) " +    
    " ")
    if iAMBIENTE_SCANTECH == "qas":
        iQUERY += " and t.tip_codigo in (1,7) " #so tem loja 1 e 7 na homologacao

    iLISTA_LOJAS = []
    try:
        for iITEMS in curORA.execute(iQUERY).fetchall():
            iLISTA_LOJAS.append((iITEMS[0],iITEMS[1],str(iITEMS[2]).replace("  ",""),iITEMS[3]))
        return iLISTA_LOJAS
    except cx_Oracle.DatabaseError as e_sql: 
        if iSTATUSLOG > 0:
            gera_Log("Erro : " + str(e_sql))

def cadastraLOJAS_SCANTECH():
    iLISTA_LOJAS = consultaLOJAS_DAVO()
    url = str(iURL_BASE) + "/v2/companies/" + str(iCOMPANY_CODE) + "/store-warehouses"
    headers = {
        'accept': '*/*',
        'Content-Type': 'application/json',
        'Authorization': 'Basic ' + str(iTOKEN)
        }
    for itens in iLISTA_LOJAS:
        payload = json.dumps({
        "description": "" + str(itens[2]) + "",
        "id": "" + str(itens[0]) + "",
        "store_id": "" + str(itens[0]) + ""
        })
        
        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code not in (200,208):
            print("erro ao cadastrar nova loja!")
            print(url)
            print(headers)
            print(payload)
            print(response.status_code)
            print(response.text)
            exit()
        else:
            
            iTXT = "loja: " + str(itens[2]) + " cadastrada com sucesso! (ambiente scantech: " + str(iAMBIENTE_SCANTECH) + " )"
            print(iTXT)
            if iSTATUSLOG > 1:                
                gera_Log(iTXT)
            

##para consultar os depositos(lojas) cadastradas
consultaLOJAS()

#para cadastrar novas lojas
#cadastraLOJAS_SCANTECH()
