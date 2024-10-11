#05.07.22 - cadastro de embalagens na scantech
#apenas quando o item tem multiplos EANs
#contato scantech Rafael - orientacoes em 04-07-22

import cx_Oracle
import os
from datetime import datetime,timedelta
import requests
import json


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
iNAMEARQLOG = "_Integra_Scantech_Emb"
iSTATUSLOG = 2 #2=Gera log FULL     1=gera log de erros e avisos        0=nao gera

iMAXITENS_PAYLOAD = 400

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
            iLISTA_LOJAS.append(iITEMS[0])
        return iLISTA_LOJAS
    except cx_Oracle.DatabaseError as e_sql: 
        if iSTATUSLOG > 0:
            gera_Log("Erro : " + str(e_sql))

def atualizaEMBALAGENS(iLISTA_EMB,iLISTA_LOJAS):
    iDICT = {}
    iDICT.update({ "packages": iLISTA_EMB })
    
    data_set = (iDICT )

    #iJSON_TRATADO = json.dumps(data_set, ensure_ascii=False, indent=4, sort_keys=True)
    #print(iJSON_TRATADO)

    for loja in iLISTA_LOJAS:
        payload = json.dumps(data_set)
        headers = {
                    'accept': '*/*',
                    'Authorization': 'Basic ' + str(iTOKEN),
                    'Content-Type': 'application/json'
                    }
        url = str(iURL_BASE) + "/v2/companies/" + str(iCOMPANY_CODE) + "/warehouses/" + str(loja) + "/packages/batch"
        if iSTATUSLOG > 1:
                gera_Log("url: " + str(url))
        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code == 200:
            if iSTATUSLOG > 1:
                gera_Log("payload: " + str(payload))
                gera_Log("response.text: " + str(response.text))
        else:
            if iSTATUSLOG > 1:
                gera_Log("url: " + str(url))
                gera_Log("payload: " + str(payload))
                gera_Log("headers: " + str(headers))
                gera_Log("response.status_code: " + str(response.status_code))
                gera_Log("response.text: " + str(response.text))


def buscaEMB_RMS():
    iQUERY = (" " + 
    "             SELECT e.ean_cod_ean, " + 
    "             e.ean_cod_pro_alt, " + 
    "             e.ean_emb_venda, " + 
    "             e.ean_tpo_emb_venda, " + 
    "             i.git_codigo_ean13, " + 
    "             i.git_descricao, " + 
    "             trim(i.git_descricao) || ' ' || e.ean_tpo_emb_venda || ' ' || e.ean_emb_venda " + 
    "         FROM rms.aa3ccean e " + 
    "         JOIN rms.aa3citem i ON (i.git_cod_item = trunc(e.ean_cod_pro_alt/10) " + 
    "                                 AND i.Git_Estq_Atual > 0) " + 
    "         WHERE e.ean_pdv = 'S' " + 
    "         AND e.ean_emb_venda > 1 " + 
    "         AND e.ean_tpo_emb_venda in ('CX', " + 
    "                                     'FD', " + 
    "                                     'PC', " + 
    "                                     'CA') " +  #"         AND rownum < 3  " + 
    "         order by e.ean_cod_pro_alt, i.git_estq_atual desc     " +
    " ")
    if iSTATUSLOG > 0:
        gera_Log("iQUERY: " + str(iQUERY))
    iLISTA_EMB = []
    iCONTADOR = 0 
    iCOD_OLD = 0   
    iTOTAL_ENVIADO = 0
    iLISTA_LOJAS = consultaLOJAS_DAVO()
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
            print("enviado + " + str(len(iLISTA_EMB)) + " total enviado: " + str(iTOTAL_ENVIADO))
            iLISTA_EMB = []
            iCONTADOR = 0

    #repassa ultima vez caso tenha sobra da execucao acima
    if iCONTADOR > 0:
        atualizaEMBALAGENS(iLISTA_EMB,iLISTA_LOJAS)
    if iSTATUSLOG > 0:
        gera_Log("Finalizou leitura de estoque das lojas")

buscaEMB_RMS()
