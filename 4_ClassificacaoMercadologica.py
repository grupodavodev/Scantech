#17.07.23 envio semanal de clas mercadologica para scanntech
#contato jsantos@scanntech.com - email 

import cx_Oracle
import os
from datetime import datetime,timedelta
import requests
import json
import concurrent.futures

iAMBIENTE_SCANTECH = "prd"

if iAMBIENTE_SCANTECH == "qas":
    iCOMPANY_CODE = "358"
    iTOKEN = "ZGktZGF2by1sa2oxOiZjJThCM29K" #user: di-davo-lkj1   |    senha: &c%8B3oJ
    iURL_BASE = "https://test-parceiro.scanntech.com/api-mercado/api-mercado/api/v1"  
else:
    iCOMPANY_CODE = "328"
    iTOKEN = "ZGktZGF2by01ZmsyOiNmMS1FMWhM" #user: di-davo-5fk2   |    senha: #f1-E1hL
    iURL_BASE = "http://parceiro.scanntech.com/api-mercado/api-mercado/api/v1" 

    

#LOG
iFORMATNAMELOG = datetime.now().strftime('%Y_%m_%d')
iNAMEARQLOG = "_Integra_Scantech_ClasMerc"
iSTATUSLOG = 2 #2=Gera log FULL     1=gera log de erros e avisos        0=nao gera

#diretorios
if os.name == 'nt': #windows
    dirLOGREQUEST = "//nas/dbxprd/PRD/LOG/SCANTECH/"   
    iPRINT_ACOMPANHAMENTO = "S" 
    iPRINT_LOG = "S"
else:
    os.environ['ORACLE_HOME'] = "/usr/lib/oracle/19.6/client64"    
    dirLOGREQUEST = "//dbx/PRD/LOG/SCANTECH/"
    iPRINT_ACOMPANHAMENTO = "N"
    iPRINT_LOG = "S"

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

# Funcao para capturar informacoes das classificacoes mercadologicas
def captaINF_CLASS():
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
    gera_Log("iQUERY: " + str(iQUERY))
    iLISTA_ITENS = []    
    for iITEMS in curORA.execute(iQUERY).fetchall():
        try:
            iLISTA_ITENS.append(( iITEMS[0], iITEMS[1], iITEMS[2], iITEMS[3], iITEMS[4], iITEMS[5], iITEMS[6],
                                  iITEMS[7], iITEMS[8], iITEMS[9], iITEMS[10], iITEMS[11],))
        except:
            return iLISTA_ITENS
    return iLISTA_ITENS

# Funcao para enviar informacoes para a Scanntech
def enviaSCANNTECH(iEAN, iCOD, iCOMPR, iDESC, iMARCA, iFABR, iCONT, iNIV1, iNIV2, iNIV3, iNIV4, iNIV5, iCONTADOR):
    url = iURL_BASE + "/redes/328/estructuramercadologica/"
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
    gera_Log("contador=" + str(iCONTADOR) + " " + "url: " + str(url))
    gera_Log("contador=" + str(iCONTADOR) + " " + "headers: " + str(headers))
    gera_Log("contador=" + str(iCONTADOR) + " " + "payload: " + str(payload))
    response = requests.request("POST", url, headers=headers, data=payload)
    gera_Log("contador=" + str(iCONTADOR) + " " + "response.status_code: " + str(response.status_code))
    gera_Log("contador=" + str(iCONTADOR) + " " + "response.text: " + str(response.text))

## Funcao para gerar o envio da classe mercadologica                                
#def geraENVIO_CLASMERC():   
#    iLISTA_ITENSCLASS = captaINF_CLASS()
#    iTOTAL_ITENS = len(iLISTA_ITENSCLASS)
#    iCONTADOR = 1
#    for itens in iLISTA_ITENSCLASS:
#        gera_Log(f"Enviando: {iCONTADOR} de {iTOTAL_ITENS} ")
#        enviaSCANNTECH(itens[0], itens[1], itens[2], itens[3], itens[4], itens[5], itens[6], itens[7], 
#                       itens[8], itens[9], itens[10], itens[11])
#        iCONTADOR += 1

def geraENVIO_CLASMERC():
    iLISTA_ITENSCLASS = captaINF_CLASS()

    # Limita o numero maximo de threads concorrentes para 5
    max_threads = 5
    iCONTADOR = 1
    iTOTAL_ITENS = len(iLISTA_ITENSCLASS)
    with concurrent.futures.ThreadPoolExecutor(max_threads) as executor:
        futures = []
        for itens in iLISTA_ITENSCLASS:
            iCONTADOR += 1
            gera_Log(f"Enviando: {iCONTADOR} de {iTOTAL_ITENS} ")
            future = executor.submit(enviaSCANNTECH, itens[0], itens[1], itens[2], itens[3], itens[4],
                                     itens[5], itens[6], itens[7], itens[8], itens[9], itens[10], itens[11], iCONTADOR)
            futures.append(future)

        # Aguarda todas as threads terminarem
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # Isso lancara uma excecao se ocorreu um erro na thread
            except Exception as e:
                print(f"Erro na thread: {e}")

# Chamada da funcao para gerar o envio das classes mercadologicas
geraENVIO_CLASMERC()