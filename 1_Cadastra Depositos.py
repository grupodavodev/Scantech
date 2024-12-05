#05.07.22 - Consulta e cadastro de depositos-lojas na scantech
#contato scantech Rafael - orientacoes em 04-07-22
#https://scanntech.cloud.xwiki.com/xwiki/wiki/di/view/apis/api-stock/operations/warehouses/

import cx_Oracle
import os
from datetime import datetime,timedelta
import requests
import json
from dotenv import load_dotenv 
load_dotenv() 

import sys
sys.path.append(os.getenv("iDIRLIBEXTRA_WIN") if os.name == 'nt' else os.getenv("iDIRLIBEXTRA_LINUX"))
from logging_config import setup_logger #log padrao
logger = setup_logger(app_name=os.path.basename(__file__).replace('.py', ''), project_name=os.getenv("iNAMEPROJECTLOG"))

iAMBIENTE_SCANTECH = "prd"
iPASS_BDORA = os.getenv("iORA_PASS") 
iUSER_BDORA = os.getenv("iORA_USER") 
iHOST_BDORA = os.getenv("iORA_HOST") 

if iAMBIENTE_SCANTECH == "qas":
    iCOMPANY_CODE = os.getenv("iSCAN_COMPANYCODE_HML") 
    iTOKEN = os.getenv("iSCAN_TOKEN_HML") 
    iURL_BASE = os.getenv("iSCAN_BASEURL_HML") 
else:
    iCOMPANY_CODE = os.getenv("iSCAN_COMPANYCODE_PRD") 
    iTOKEN = os.getenv("iSCAN_TOKEN_PRD") 
    iURL_BASE = os.getenv("iSCAN_BASEURL_PRD") 

if os.name != 'nt': 
    os.environ['ORACLE_HOME'] =   os.getenv("iORACLEHOME_LINUX") 

#Conexao Oracle
try:    
    myCONNORA = cx_Oracle.connect(f"{iUSER_BDORA}/{iPASS_BDORA}@{iHOST_BDORA}") 
    myCONNORA.autocommit = True
    curORA = myCONNORA.cursor() #execucoes Oracle
    try:
        curORA.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ',.' ")
        curORA.execute("alter session set nls_date_format = 'DD/MM/YYYY'")    
    except cx_Oracle.DatabaseError as e_sql: 
        logger.error(f"{e_sql}")
except Exception as e:
    logger.error(f"{e}")
    print(f"{e}")
    exit()

def consultaLOJAS_SCAN():
    logger.info(f"Funcao, retorna as lojas cadastradas")
    iLISTA = []
    url = f"{iURL_BASE}/v2/companies/{iCOMPANY_CODE}/store-warehouses?limit=100"
    payload={}
    headers = {
                'accept': '*/*',
                'Authorization': f'Basic {iTOKEN}'
                }
    response = requests.request("GET", url, headers=headers, data=payload)
    if response.status_code != 200:
        print(response.text)
        logger.error(f"{url}")
        logger.error(f"{headers}")
        logger.error(f"{response.status_code}")
        logger.error(f"{response.text}")        
    else:
        jRETORNO    = json.loads(response.text) 
        for itens in jRETORNO['store_warehouses']:
            logger.debug(f"{itens}")
            iLISTA.append((itens["id"], itens["store_id"], itens["description"]))
    return iLISTA

def consultaLOJAS_DAV():
    logger.info(f"Funcao, retorna as lojas")
    iQUERY = (f"""
            SELECT t.tip_codigo,
                t.tip_digito,
                t.tip_nome_fantasia,
                t.tip_regiao
            FROM   rms.aa2ctipo t
            WHERE  t.tip_loj_cli = 'L'
                AND t.tip_natureza = 'LS'
                AND t.tip_regiao IN ( 2, 8, 3) 
              """)
    if iAMBIENTE_SCANTECH == "qas":
        iQUERY += " and t.tip_codigo in (1,7) " #so tem loja 1 e 7 na homologacao
    logger.debug(f"{iQUERY}")
    iLISTA = []
    try:
        for iITEMS in curORA.execute(iQUERY).fetchall():
            iLISTA.append((iITEMS[0],iITEMS[1],str(iITEMS[2]).replace("  ",""),iITEMS[3]))        
    except cx_Oracle.DatabaseError as e_sql: 
        logger.error(f"{e_sql}")
    logger.debug(f"{iLISTA}")
    return iLISTA

def cadastraLOJAS_SCANTECH():
    iLISTA_LOJAS_DAV = consultaLOJAS_DAV()
    iLISTA_LOJAS_SCAN = consultaLOJAS_SCAN()
    url = f"{iURL_BASE}/v2/companies/{iCOMPANY_CODE}/store-warehouses"
    headers = {
            'accept': '*/*',
            'Content-Type': 'application/json',
            'Authorization': 'Basic ' + str(iTOKEN)
            }
    for itens in iLISTA_LOJAS_DAV:
        iJAEXISTECADASTRO = "N"
        for lojascan in iLISTA_LOJAS_SCAN:
            if str(lojascan[1]) == str(itens[0]): 
                iJAEXISTECADASTRO = "S"
                logger.debug(f"Ja existe cadastro loja {itens[0]}. Nao enviara novo cadastro!")
        if iJAEXISTECADASTRO == "N":     
            logger.debug(f"Nao existe cadastro loja {itens[0]}, enviando cadastro...")    
            payload = json.dumps({
                                "description": str(itens[2]),
                                "id": str(itens[0]),
                                "store_id": str(itens[0])
                                })  
            response = requests.request("POST", url, headers=headers, data=payload)
            if response.status_code not in (200,208):
                logger.warning(f"{url}")
                logger.warning(f"{payload}")
                logger.warning(f"{response.status_code}")
                logger.warning(f"{response.text}")
            else:
                logger.debug(f"Sucesso ao cadastro loja {itens[2]} (ambiente scantech: {iAMBIENTE_SCANTECH})")
                logger.warning(f"{response.text}")


#PARA CADASTRAR NOVOS DEPOSITOS
    #NECESSARIO TER CADASTRO PREVIO DE 'locales' NA SCANTECH. SCAN CADASTRA A LOJA E DA O OK, PARA QUE SEJA POSSIVEL CADASTRAR O 'DEPOSITO' DA LOJA
cadastraLOJAS_SCANTECH()

##RETORNO ESTOQUE DE LOJAS CADASTRADAS NA SCANTECH
#print(consultaLOJAS_SCAN())

#RETORNA LOJAS DA REDE
#print(consultaLOJAS_DAV)