#05.07.22 - cadastro de embalagens na scantech
#apenas quando o item tem multiplos EANs
#contato scantech Rafael - orientacoes em 04-07-22
#https://scanntech.cloud.xwiki.com/xwiki/wiki/di/view/apis/api-stock/operations/packages/

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
iMAXITENS_PAYLOAD = 400

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

def consultaLOJAS_DAV():
    logger.info(f"Funcao, consultando lojas DAV")
    iQUERY = (""" 
            SELECT t.tip_codigo,
                t.tip_digito,
                t.tip_nome_fantasia,
                t.tip_regiao
            FROM   rms.aa2ctipo t
            WHERE  t.tip_loj_cli = 'L'
                AND t.tip_natureza = 'LS'
                AND t.tip_regiao IN ( 2, 8 ) 
                        """)
    if iAMBIENTE_SCANTECH == "qas":
        iQUERY += " and t.tip_codigo in (1,7) " #so tem loja 1 e 7 na homologacao
    iLISTA = []
    try:
        for iITEMS in curORA.execute(iQUERY).fetchall():
            iLISTA.append(iITEMS[0])        
    except cx_Oracle.DatabaseError as e_sql: 
        logger.error(f"{e_sql}")
    return iLISTA

def atualizaEMBALAGENS(iLISTA_EMB,iLISTA_LOJAS):
    logger.info(f"Funcao, envia cadastro de embalagens / loja")
    iDICT = {}
    iDICT.update({ "packages": iLISTA_EMB })    
    data_set = (iDICT )
    for loja in iLISTA_LOJAS:
        payload = json.dumps(data_set)
        headers = {
                    'accept': '*/*',
                    'Authorization': 'Basic ' + str(iTOKEN),
                    'Content-Type': 'application/json'
                    }
        url = f"{iURL_BASE}/v2/companies/{iCOMPANY_CODE}/warehouses/{loja}/packages/batch"
        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code == 200:
            logger.debug(f"{payload}")
            logger.debug(f"{response.text}")
        else:
            logger.warning(f"{url}")
            logger.warning(f"{payload}")
            logger.warning(f"{headers}")
            logger.warning(f"{response.status_code}")
            logger.warning(f"{response.text}")

def buscaEMB_RMS():
    logger.info(f"Funcao, envia cadastro de embalagens / loja")
    iQUERY = (""" SELECT e.ean_cod_ean,
                        e.ean_cod_pro_alt,
                        e.ean_emb_venda,
                        e.ean_tpo_emb_venda,
                        i.git_codigo_ean13,
                        i.git_descricao,
                        Trim(i.git_descricao)
                        || ' '
                        || e.ean_tpo_emb_venda
                        || ' '
                        || e.ean_emb_venda
                    FROM   rms.aa3ccean e
                        JOIN rms.aa3citem i
                            ON ( i.git_cod_item = Trunc(e.ean_cod_pro_alt / 10)
                                AND i.git_estq_atual > 0 )
                    WHERE  e.ean_pdv = 'S'
                        AND e.ean_emb_venda > 1
                        AND e.ean_tpo_emb_venda IN ( 'CX', 'FD', 'PC', 'CA' )
                    ORDER  BY e.ean_cod_pro_alt,
                            i.git_estq_atual DESC 
                                """)    
    iLISTA_EMB = []
    iCONTADOR = 0 
    iCOD_OLD = 0   
    iTOTAL_ENVIADO = 0
    iLISTA_LOJAS = consultaLOJAS_DAV()
    logger.debug(f"{iQUERY}")
    for iITEMS in curORA.execute(iQUERY).fetchall():
        iTOTAL_ENVIADO += 1
        iDICT_EMB = { "content_product_barcode" : iITEMS[4],
                    "content_product_description" : iITEMS[5],  
                    "content_product_id" : iITEMS[1] ,  
                    "content_quantity" : iITEMS[2],
                    "product_barcode" : iITEMS[0],
                    "product_description" : iITEMS[6],
                    "product_id" : iITEMS[1]
                        }
        if iITEMS[1] != iCOD_OLD:
            iCOD_OLD = iITEMS[1]
            iLISTA_EMB.append(iDICT_EMB)
            iCONTADOR += 1

        #atualiza com um payload maximo 
        if iCONTADOR >= iMAXITENS_PAYLOAD:
            atualizaEMBALAGENS(iLISTA_EMB,iLISTA_LOJAS)            
            print(f"enviado: {len(iLISTA_EMB)} | total enviado: {iTOTAL_ENVIADO}")
            logger.debug(f"enviado: {len(iLISTA_EMB)} | total enviado: {iTOTAL_ENVIADO}")
            iLISTA_EMB = []
            iCONTADOR = 0

    #repassa ultima vez caso tenha sobra da execucao acima
    if iCONTADOR > 0:
        atualizaEMBALAGENS(iLISTA_EMB,iLISTA_LOJAS)
    logger.debug("Finalizou leitura de estoque das lojas")

buscaEMB_RMS()
#print(i)
