#05.07.22 - cadastro de estoques na scantech
#contato scantech Rafael - orientacoes em 04-07-22

import cx_Oracle
import os
from datetime import datetime,timedelta
import requests
import json


iAMBIENTE_SCANTECH = "prd"
iENVIA_ESTQ_NEGATIVO = "S"  #03.11.23

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
iNAMEARQLOG = "_Integra_Scantech_Estq"
iSTATUSLOG = 2 #2=Gera log FULL     1=gera log de erros e avisos        0=nao gera

iMAXITENS_PAYLOAD = 400



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

def atualizaESTOQUE(iLISTA_ESTQ, iCODLOJA_SDIG):
    iDICT = {}
    iDICT.update({ "stock": iLISTA_ESTQ })

    data_set = (iDICT )
    #iJSON_TRATADO = json.dumps(data_set, ensure_ascii=False, indent=4, sort_keys=True)
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
    except Exception as e:
        gera_Log("Erro: " + str(e))
        pass




    
    
    
    



def buscaESTOQUE_RMS(iCODLOJA_SDIG):
    iQUERY = (f"""
                SELECT To_char(sysdate, 'yyyy-mm-dd')
                    AS
                    data_hoje,
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
        if iSTATUSLOG > 0:
            gera_Log("iQUERY: " + str(iQUERY))
        iCONTADOR = 0    
        iTOTAL_ENVIADO = 0
        iLISTA_RESULTORACLE = curORA.execute(iQUERY).fetchall()
        iTOTAL_RESULTS = len(iLISTA_RESULTORACLE)
        if iPRINT_LOG == "S":
            print("Inciando envio de " + str(iTOTAL_RESULTS) + " itens para a loja: " + str(iCODLOJA_SDIG))
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
                if iPRINT_ACOMPANHAMENTO == "S":
                    print("enviado + " + str(len(iLISTA_ESTQ)) + " total enviado: " + str(iTOTAL_ENVIADO) + " de um total de itens: " + str(iTOTAL_RESULTS))
                iCONTADOR = 0
                iLISTA_ESTQ = []
        
        #repassa ultima vez caso tenha sobra da execucao acima
        if iCONTADOR > 0:
            atualizaESTOQUE(iLISTA_ESTQ, iCODLOJA_SDIG)

        if iSTATUSLOG > 0:
            gera_Log("Finalizou leitura de estoque das lojas")
        return iLISTA_ESTQ
    except cx_Oracle.DatabaseError as e_sql: 
        if iSTATUSLOG > 0:
            gera_Log("Erro : " + str(e_sql))

def filtraLOJAS_START_ESTOQUE():
    iQUERY = (f"""
                SELECT t.tip_codigo,
                    t.tip_digito,
                    t.tip_nome_fantasia,
                    t.tip_regiao
                FROM   rms.aa2ctipo t
                WHERE 1 > 0 
                and (t.tip_codigo in (205, 221)
                      OR  (t.tip_loj_cli = 'L'
                          AND t.tip_natureza = 'LS'
                          AND t.tip_regiao IN ( 2, 8 ) )
                    )
              """)
    if iAMBIENTE_SCANTECH == "qas":
        " and t.tip_codigo in (1,7) " 

    try:
        if iSTATUSLOG > 0:
            gera_Log("Iniciando o envio de estoque por filial: " + str(iQUERY))
        for iITEMS in curORA.execute(iQUERY).fetchall():
            if iSTATUSLOG > 0:
                gera_Log("Iniciando filial: " + str(iITEMS[0]))
            buscaESTOQUE_RMS(iITEMS[0])
    except cx_Oracle.DatabaseError as e_sql: 
        if iSTATUSLOG > 0:
            gera_Log("Erro : " + str(e_sql))

filtraLOJAS_START_ESTOQUE()