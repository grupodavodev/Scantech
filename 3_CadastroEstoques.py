#05.07.22 - cadastro de estoques na scantech
#contato scantech Rafael - orientacoes em 04-07-22
#https://scanntech.cloud.xwiki.com/xwiki/wiki/di/view/apis/api-stock/operations/stock/

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
iENVIA_ESTQ_NEGATIVO = "S"  #03.11.23
iPASS_BDORA = os.getenv("iORA_PASS") 
iUSER_BDORA = os.getenv("iORA_USER") 
iHOST_BDORA = os.getenv("iORA_HOST") 
iMAXITENS_PAYLOAD = 400

if iAMBIENTE_SCANTECH == "qas":
    iCOMPANY_CODE = os.getenv("iSCAN_COMPANYCODE_HML") 
    iTOKEN = os.getenv("iSCAN_TOKEN_HML") 
    iURL_BASE = os.getenv("iSCAN_BASEURL_HML") 
else:
    iCOMPANY_CODE = os.getenv("iSCAN_COMPANYCODE_PRD") 
    iTOKEN = os.getenv("iSCAN_TOKEN_PRD") 
    iURL_BASE = os.getenv("iSCAN_BASEURL_PRD") 
    

if os.name == 'nt': 
    iPRINT_ACOMPANHAMENTO = "S" 
    iPRINT_LOG = "S"
else:
    os.environ['ORACLE_HOME'] =   os.getenv("iORACLEHOME_LINUX") 
    iPRINT_ACOMPANHAMENTO = "N"
    iPRINT_LOG = "S"

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

def atualizaESTOQUE(iLISTA_ESTQ, iCODLOJA_SDIG):
    logger.info(f"Funcao, efetivamente envia o estoque na API")
    iDICT = {}
    iDICT.update({ "stock": iLISTA_ESTQ })
    data_set = (iDICT )
    payload = json.dumps(data_set)
    headers = {
                'accept': '*/*',
                'Authorization': 'Basic ' + str(iTOKEN),
                'Content-Type': 'application/json'
                }
    url = str(iURL_BASE) + "/v2/companies/" + str(iCOMPANY_CODE) + "/warehouses/" + str(iCODLOJA_SDIG) + "/stock"
    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code == 200:
            logger.debug(f"{payload}")
            logger.debug(f"{response.text}")
        else:
            logger.warning(f"{url}")
            logger.warning(f"{headers}")
            logger.warning(f"{payload}")
            logger.warning(f"{response.status_code}")
            logger.warning(f"{response.text}")
    except Exception as e:
        logger.error(f"{e}")    

def sendESTOQUE_RMS(iCODLOJA_SDIG):
    logger.info(f"Funcao, capta estoques por loja. Parametro iCODLOJA_SDIG: {iCODLOJA_SDIG}")
    iQUERY = (f"""
                SELECT To_char(sysdate-1, 'yyyy-mm-dd')
                    AS
                    data_ontem,
                    To_char(rms.Rms7to_date(rms.Rms6to_rms7(e.get_dt_ult_ent)), 'yyyy-mm-dd')
                    AS
                    data_ult_compra,
                    i.git_codigo_ean13,
                    i.git_descricao,
                    e.get_cod_produto,
                    (e.get_estoque - e.get_qtd_pend_vda) as get_estoque,
                    t.tip_codigo
                FROM   rms.aa2cestq e
                    JOIN rms.aa3citem i
                        ON ( i.git_cod_item = Trunc(e.get_cod_produto / 10)
                            
                            )
                    JOIN rms.aa2ctipo t
                        ON ( t.tip_codigo = Trunc(e.get_cod_local / 10)
                             )
                WHERE  1 > 0
                    AND Trunc(e.get_cod_local / 10) = {iCODLOJA_SDIG}
                and (rms.rms6to_rms7(e.get_dt_ult_fat) >= rms.dateto_rms7(sysdate-60)
                        or
                        rms.Rms6to_rms7(e.get_dt_ult_ent)>= rms.dateto_rms7(sysdate-60)
                        )
                    and e.get_dt_ult_ent is not null
                     and e.get_dt_ult_ent > 0
              """)
    iLISTA_ESTQ = []
    try:
        logger.debug(f"{iQUERY}")
        iCONTADOR = 0    
        iTOTAL_ENVIADO = 0
        iLISTA_RESULTORACLE = curORA.execute(iQUERY).fetchall()
        iTOTAL_RESULTS = len(iLISTA_RESULTORACLE)
        logger.debug(f"Iniciando envio de {iTOTAL_RESULTS} itens para a loja: {iCODLOJA_SDIG}")
        if iPRINT_LOG == "S": print(f"Iniciando envio de {iTOTAL_RESULTS} itens para a loja: {iCODLOJA_SDIG}")
        for iITEMS in iLISTA_RESULTORACLE:
            iTOTAL_ENVIADO += 1
            iDICT_EST = { "date" : iITEMS[0],
                        "last_purchase_date" : iITEMS[1],  
                        "product_barcode" : iITEMS[2] ,  
                        "product_description" : iITEMS[3],
                        "product_id" : iITEMS[4],
                        "quantity" : iITEMS[5],
                        "sales_channel_id" : str(iCODLOJA_SDIG) 
                            }
            iLISTA_ESTQ.append(iDICT_EST)
            iCONTADOR += 1

            #atualiza com um payload maximo 
            if iCONTADOR >= iMAXITENS_PAYLOAD:
                atualizaESTOQUE(iLISTA_ESTQ, iCODLOJA_SDIG)
                logger.debug(f"enviado {len(iLISTA_ESTQ)} | total enviado: { iTOTAL_ENVIADO } de um total de itens: { iTOTAL_RESULTS}")
                if iPRINT_ACOMPANHAMENTO == "S": print(f"enviado {len(iLISTA_ESTQ)} | total enviado: { iTOTAL_ENVIADO } de um total de itens: { iTOTAL_RESULTS}")
                iCONTADOR = 0
                iLISTA_ESTQ = []
        
        #repassa ultima vez caso tenha sobra da execucao acima
        if iCONTADOR > 0:
            atualizaESTOQUE(iLISTA_ESTQ, iCODLOJA_SDIG)
        return iLISTA_ESTQ
    except cx_Oracle.DatabaseError as e_sql: 
        logger.error(f"{e_sql}")

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

def start_envia_estoques_lojas():
    logger.info(f"Funcao, envia estoque por filial cadastrada na Scantech")
    iLISTALOJAS_SCAN = consultaLOJAS_SCAN()
    for lojascan in iLISTALOJAS_SCAN:
        iIDLOJADAV = lojascan[1]
        logger.debug(f"{lojascan}")
        logger.debug(f"Iniciando envio da loja: {iIDLOJADAV}")
        print(f"Iniciando envio da loja: {iIDLOJADAV}")
        sendESTOQUE_RMS(iIDLOJADAV)

start_envia_estoques_lojas()