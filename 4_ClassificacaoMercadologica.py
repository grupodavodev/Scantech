#17.07.23 envio semanal de clas mercadologica para scanntech
#https://scanntech.cloud.xwiki.com/xwiki/wiki/di/view/apis/api-products/operations/products-classifications/

import cx_Oracle
import os
from datetime import datetime,timedelta
import requests
import json
import concurrent.futures
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
max_threads = 5 #envio de threads, maximo de threads simultaneas

if iAMBIENTE_SCANTECH == "qas":
    iCOMPANY_CODE = os.getenv("iSCAN_COMPANYCODE_HML") 
    iTOKEN = os.getenv("iSCAN_TOKEN_HML") 
    iURL_BASE = os.getenv("iSCAN_BASEURL_CLASSMERC_HML") 
    iSCAN_NUMREDEARVMERC = os.getenv("iSCAN_NUMREDE_ARVMERCADOLOGICA") 
else:
    iCOMPANY_CODE = os.getenv("iSCAN_COMPANYCODE_PRD") 
    iTOKEN = os.getenv("iSCAN_TOKEN_PRD") 
    iURL_BASE = os.getenv("iSCAN_BASEURL_CLASSMERC_PRD") 
    iSCAN_NUMREDEARVMERC = os.getenv("iSCAN_NUMREDE_ARVMERCADOLOGICA") 


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

def captaINF_CLASS():
    logger.info(f"Funcao, para capturar informacoes das classificacoes mercadologicas")
    iQUERY = ("""
                SELECT 
                E.EAN_COD_EAN
                ,E.EAN_COD_PRO_ALT
                ,TRIM(T.TAB_CONTEUDO)
                ,TRIM(I.GIT_DESCRICAO)
                ,TRIM(NVL(D.DET_MARCA, ' '))
                ,'' AS FABR
                ,'' AS CONTEUD
                , ( SELECT TRIM(TAB.TAB_CONTEUDO) 
                        FROM RMS.AA2CTABE TAB
                    WHERE TAB.TAB_CODIGO = 16
                        AND to_number(trim(TAB.TAB_ACESSO)) = i.GIT_DEPTO
                        AND ROWNUM = 1
                    ) as depto
                , ( SELECT TRIM(CM1.NCC_DESCRICAO) descricao
                    FROM RMS.AA3CNVCC CM1
                    WHERE CM1.NCC_DEPARTAMENTO = i.GIT_DEPTO
                    AND CM1.NCC_SECAO        = i.GIT_SECAO
              AND i.GIT_SECAO > 0
                    AND CM1.NCC_GRUPO        = 0
                    AND CM1.NCC_SUBGRUPO     = 0
                    AND ROWNUM               = 1
                ) as Sec
                , (  SELECT TRIM(CM1.NCC_DESCRICAO) descricao
                    FROM RMS.AA3CNVCC CM1
                    WHERE CM1.NCC_DEPARTAMENTO = i.GIT_DEPTO
                        AND CM1.NCC_SECAO        = i.GIT_SECAO
                        AND CM1.NCC_GRUPO        = i.GIT_GRUPO
              AND i.GIT_GRUPO > 0
                        AND CM1.NCC_SUBGRUPO     = 0
                        AND ROWNUM               = 1
                ) as Grupo
                ,( SELECT TRIM(CM1.NCC_DESCRICAO) descricao
                    FROM RMS.AA3CNVCC CM1
                    WHERE CM1.NCC_DEPARTAMENTO = i.GIT_DEPTO
                    AND CM1.NCC_SECAO        = i.GIT_SECAO
                    AND CM1.NCC_GRUPO        = i.GIT_GRUPO
                    AND CM1.NCC_SUBGRUPO     = i.GIT_SUBGRUPO
              AND i.GIT_SUBGRUPO > 0
                    AND ROWNUM               = 1
                ) as subgrupo
                ,( SELECT TRIM(CM1.NCC_DESCRICAO) descricao
                    FROM RMS.AA3CNVCC CM1
                    WHERE CM1.NCC_DEPARTAMENTO = i.GIT_DEPTO
                    AND CM1.NCC_SECAO        = i.GIT_SECAO
                    AND CM1.NCC_GRUPO        = i.GIT_GRUPO
                    AND CM1.NCC_SUBGRUPO     = i.GIT_SUBGRUPO
                    AND CM1.NCC_CATEGORIA     = i.GIT_CATEGORIA
              AND i.GIT_CATEGORIA > 0
                    AND ROWNUM               = 1
                ) as categoria
                FROM RMS.AA3CCEAN E 
                    JOIN RMS.AA3CITEM I ON (I.GIT_COD_ITEM = TRUNC(E.EAN_COD_PRO_ALT/10) )
                    JOIN RMS.AA1DITEM D ON (D.DET_COD_ITEM = I.GIT_COD_ITEM)
                    JOIN RMS.AA2CTABE T ON (T.TAB_CODIGO = 001 AND TRIM(T.TAB_ACESSO) = LPAD(I.GIT_COMPRADOR, 3, '0') )
                WHERE 1 > 0
                    AND I.Git_Estq_Atual > 0
                    AND I.GIT_DEPTO IN (1, 2, 3)
              """)
    logger.debug(f"{iQUERY}")
    iLISTA_ITENS = []    
    for iITEMS in curORA.execute(iQUERY).fetchall():
        try:
            iLISTA_ITENS.append(( iITEMS[0], iITEMS[1], iITEMS[2], iITEMS[3], iITEMS[4], iITEMS[5], iITEMS[6],
                                  iITEMS[7], iITEMS[8], iITEMS[9], iITEMS[10], iITEMS[11],))
        except:
            return iLISTA_ITENS
    logger.debug(f"Total de registros: {len(iLISTA_ITENS)}")
    return iLISTA_ITENS


def enviaSCANNTECH(iEAN, iCOD, iCOMPR, iDESC, iMARCA, iFABR, iCONT, iNIV1, iNIV2, iNIV3, iNIV4, iNIV5, iCONTADOR):
    logger.info(f"Funcao,  para enviar informacoes para a Scanntech")
    url =  f"{iURL_BASE}/redes/{iSCAN_NUMREDEARVMERC}/estructuramercadologica/"
    #print(url)
    payload = json.dumps({
                            "barra": str(iEAN),
                            "codigoInterno": str(iCOD),
                            "comprador": str(iCOMPR),
                            "descripcion": str(iDESC),
                            "marca": iMARCA,
                            "fabricante": iFABR,
                            "contenido": iCONT,
                            "nivel1": iNIV1,
                            "nivel2": iNIV2,
                            "nivel3": iNIV3,
                            "nivel4": iNIV4,
                            "nivel5": iNIV5
                            })
    headers = {
                'accept': '*/*',
                'Content-Type': 'application/json',
                'Authorization': 'Basic ' + str(iTOKEN)
                }
    logger.debug(f"contador={iCONTADOR} url: {url}")
    logger.debug(f"contador={iCONTADOR} headers: {headers}")
    logger.debug(f"contador={iCONTADOR} payload: {headers}")
    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code != 200:
            logger.debug(f"contador={iCONTADOR} url: {url}")
            logger.debug(f"contador={iCONTADOR} headers: {headers}")
            logger.debug(f"contador={iCONTADOR} payload: {payload}")
            logger.debug(f"contador={iCONTADOR} response.status_code: {response.status_code}")
            logger.debug(f"contador={iCONTADOR} response.text: {response.text}")
        else:
            logger.debug(f"contador={iCONTADOR} payload: {payload}")
            logger.debug(f"contador={iCONTADOR} response.text: {response.text}")
            logger.debug(f"contador={iCONTADOR} response.status_code: {response.status_code}")
    except Exception as e:
        logger.error(f"{e}")    

def geraENVIO_CLASMERC():
    iLISTA_ITENSCLASS = captaINF_CLASS()    
    iCONTADOR = 1
    iTOTAL_ITENS = len(iLISTA_ITENSCLASS)
    with concurrent.futures.ThreadPoolExecutor(max_threads) as executor:
        futures = []
        for itens in iLISTA_ITENSCLASS:
            iCONTADOR += 1
            future = executor.submit(enviaSCANNTECH, itens[0], itens[1], itens[2], itens[3], itens[4],
                                     itens[5], itens[6], itens[7], itens[8], itens[9], itens[10], itens[11], iCONTADOR)
            futures.append(future)

        # Aguarda todas as threads terminarem
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # Isso lancara uma excecao se ocorreu um erro na thread
            except Exception as e:
                print(f"Erro na thread: {e}")

geraENVIO_CLASMERC()