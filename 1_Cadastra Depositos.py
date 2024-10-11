#05.07.22 - Consulta e cadastro de depositos-lojas na scantech
#contato scantech Rafael - orientacoes em 04-07-22
#https://scanntech.cloud.xwiki.com/xwiki/wiki/di/view/apis/api-stock/

import cx_Oracle
import os
from datetime import datetime,timedelta
import requests
import json
from dotenv import load_dotenv 
load_dotenv() 
import logging


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
    

#LOG
if os.name == 'nt': 
    dirLOGREQUEST = os.getenv("iDIRLOG_WIN") 
else:
    os.environ['ORACLE_HOME'] =   os.getenv("iORACLEHOME_LINUX") 
    dirLOGREQUEST = os.getenv("iDIRLOG_LINUX") 
iNAMEARQLOG = os.getenv("iNOMEARQLOG") 
iEXTENSAO_LOG = os.getenv("iEXTENSAO_LOG") 
logging.basicConfig(
    filename=f"{dirLOGREQUEST}{iNAMEARQLOG}_{datetime.now().strftime('%d%m%Y')}{iEXTENSAO_LOG}",  # Nome do arquivo de log
    format='%(asctime)s - [PID:%(process)d] -  %(levelname)s - %(funcName)s - %(message)s ',  # Formato da mensagem de log
    level=logging.DEBUG  # Nivel minimo de log que sera registrado
)

#Conexao Oracle
try:    
    myCONNORA = cx_Oracle.connect(f"{iUSER_BDORA}/{iPASS_BDORA}@{iHOST_BDORA}") 
    myCONNORA.autocommit = True
    curORA = myCONNORA.cursor() #execucoes Oracle
    try:
        curORA.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ',.' ")
        curORA.execute("alter session set nls_date_format = 'DD/MM/YYYY'")    
    except cx_Oracle.DatabaseError as e_sql: 
        logging.error(f"{e_sql}")
except Exception as e:
    logging.error(f"{e}")
    print(f"{e}")
    exit()

def consultaLOJAS_SCAN():
    logging.info(f"Funcao, retorna as lojas cadastradas")
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
        logging.error(f"{url}")
        logging.error(f"{headers}")
        logging.error(f"{response.status_code}")
        logging.error(f"{response.text}")        
    else:
        jRETORNO    = json.loads(response.text) 
        for itens in jRETORNO['store_warehouses']:
            logging.debug(f"{itens}")
            iLISTA.append((itens["id"], itens["store_id"], itens["description"]))
    return iLISTA

def consultaLOJAS_DAV():
    logging.info(f"Funcao, retorna as lojas")
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
    logging.debug(f"{iQUERY}")
    iLISTA = []
    try:
        for iITEMS in curORA.execute(iQUERY).fetchall():
            iLISTA.append((iITEMS[0],iITEMS[1],str(iITEMS[2]).replace("  ",""),iITEMS[3]))        
    except cx_Oracle.DatabaseError as e_sql: 
        logging.error(f"{e_sql}")
    logging.debug(f"{iLISTA}")
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
                logging.debug(f"Ja existe cadastro loja {itens[0]}. Nao enviara novo cadastro!")
        if iJAEXISTECADASTRO == "N":     
            logging.debug(f"Nao existe cadastro loja {itens[0]}, enviando cadastro...")    
            payload = json.dumps({
                                "description": str(itens[2]),
                                "id": str(itens[0]),
                                "store_id": str(itens[0])
                                })  
            response = requests.request("POST", url, headers=headers, data=payload)
            if response.status_code not in (200,208):
                logging.warning(f"{url}")
                logging.warning(f"{payload}")
                logging.warning(f"{response.status_code}")
                logging.warning(f"{response.text}")
            else:
                logging.debug(f"Sucesso ao cadastro loja {itens[2]} (ambiente scantech: {iAMBIENTE_SCANTECH})")
                logging.warning(f"{response.text}")


#PARA CADASTRAR NOVOS DEPOSITOS
    #NECESSARIO TER CADASTRO PREVIO DE 'locales' NA SCANTECH. SCAN CADASTRA A LOJA E DA O OK, PARA QUE SEJA POSSIVEL CADASTRAR O 'DEPOSITO' DA LOJA
cadastraLOJAS_SCANTECH()

##RETORNO ESTOQUE DE LOJAS CADASTRADAS NA SCANTECH
#print(consultaLOJAS_SCAN())

#RETORNA LOJAS DA REDE
#print(consultaLOJAS_DAV)