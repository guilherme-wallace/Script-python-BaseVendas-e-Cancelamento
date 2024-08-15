from configparser import Error
from os import terminal_size
import mysql.connector as MySQL
from mysql.connector.errors import IntegrityError
import requests as Request
import os.path as Path
import json as JSON
import re 
from datetime import date, datetime
from requests.models import REDIRECT_STATI
from dadosDaConexao import *

listaDeCondominios: str = Path.join(Path.dirname(__file__), 'listaDeCondominios.csv')

condominiumList: list = []

RBX = MySQL.connect(host=hostRBX, user=userRBX, password=passwordRBX, database=databaseRBX, auth_plugin='mysql_native_password')

IVP = MySQL.connect(host=hostIVP, user=userIVP, password=passwordIVP, database=databaseIVP, auth_plugin='mysql_native_password') 

cursorRBX = RBX.cursor()

cursorIVP = IVP.cursor()

def sale (codigo: int) -> None:
    queryColocaIVPSale = "insert into sale (contractId, clientId, groupId, addressId, operation, technologyId, \
researchId, datetime, salespersonId, observation,) values ({contractId}, {codigo}, {groupId}, {addressId}, \
{operation}, {technologyId}, {researchId}, {datetime}, {salespersonId}, {observation})"
    cursorRBX.execute(f'SELECT Contratos.PacoteID, Clientes.grupo FROM isupergaus.Contratos \
                        join isupergaus.Clientes on Contratos.Cliente = Clientes.Codigo \
                        WHERE Contratos.Cliente = {codigo} GROUP BY Contratos.PacoteID;')
    dadosDaQuerry: list = cursorRBX.fetchall()
    contract: str = dadosDaQuerry[0][0]
    groupId: int = dadosDaQuerry[0][1]
    if contract == "":
        contract = "Não tem contrato"
    #print(Contract, ' ', groupId)
    elif len(dadosDaQuerry) > 1:
        print(f'Ha clientes que possue mais de 1 contrado -> código: {codigo}')
    else:
        try:
            print(codigo, " ", contract, " ", groupId)
        except IntegrityError as error:
            if error:
                print("Deu algum erro")
            else:
                raise error

VENDAS: str = Path.join(Path.dirname(__file__), 'VENDASjaneiro.csv')
with open(VENDAS, 'r') as file:
    try:
        while (line := file.readline().rstrip()):
            values: list[str] = line.split(';')
            codigo: int

            for i, value in enumerate(values):
                if i == 0:
                    try:
                        codigo = value[:value.index('_')]
                    except ValueError:
                        codigo = value

            sale(codigo)

    except Exception as error:
        print(error)
'''
CANCELAMENTO: str = Path.join(Path.dirname(__file__), 'CANCELAMENTOjaneiro.csv')
with open(CANCELAMENTO, 'r') as file:
    try:
        while (line := file.readline().rstrip()):
            values: list[str] = line.split(';')
            codigo: int
            #contract
            endDate: 'str | None'

            for i, value in enumerate(values):
                if i == 1:
                    try:
                        codigo = value[:value.index('_')]
                    except ValueError:
                        codigo = value
                
            sale(codigo)

    except Exception as error:
        print(error)

print(CodeListContract)
with open(MaisDe1Contrato, 'w') as file:
    print('Códigos que possuem mais de 1 contrato')
    file.writelines(CodeListContract)
    file.close
'''