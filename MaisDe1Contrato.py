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

MaisDe1Contrato: str = Path.join(Path.dirname(__file__), 'MaisDe1ContratoJaneiro.csv')

CodeListContract: list = []

RBX = MySQL.connect(host=hostRBX, user=userRBX, password=passwordRBX, database=databaseRBX, auth_plugin='mysql_native_password')

IVP = MySQL.connect(host=hostIVP, user=userIVP, password=passwordIVP, database=databaseIVP, auth_plugin='mysql_native_password') 

cursorRBX = RBX.cursor()

cursorIVP = IVP.cursor()

def saleContract(codigo: int, endDate: 'str | None', time: 'str | None') -> None:
    queryColocaIVPContract ="insert into saleContract (contractId, name, bandwidth, cost, startDate, endDate) values ('{contractId}', '{name}', \
{bandwidth}, {cost}, '{startDate}', {endDate})"
    cursorRBX.execute(f'SELECT Contratos.PacoteID as Pacote, PlanosPacotes.Descricao, \
				sum(Contratos.ValorPlano) as Valor, Contratos.Inicio FROM isupergaus.Contratos \
                JOIN isupergaus.PlanosPacotes ON PlanosPacotes.Codigo = Contratos.Pacote \
                WHERE Contratos.Cliente = {codigo} GROUP BY Contratos.PacoteID;')
    listaDeContratos:list = cursorRBX.fetchall()

    if len(listaDeContratos) == 0:
        print(f'Este código: {codigo} não tem contrato')
    elif len(listaDeContratos) > 1:
        #print(f'Ha clientes que possue mais de 1 contrado -> código: {codigo}')
        CodeListContract.append(f'{codigo},\n')
    else:
        bandwidth: int = int(re.search(r'\d{2,3}(?=M)', listaDeContratos[0][1])[0])
        valor: float = format(listaDeContratos[0][2], ".2f")
        nameBandwidth: str =f'{bandwidth} Mbps'
        if endDate is not None:
            endDate = datetime.strptime(endDate, '%d/%m/%Y').date()
        
        dataInicio: str = listaDeContratos[0][3]

        if time is not None:
            dataInicio = f'{dataInicio} {time}'

        try:
            cursorIVP.execute(queryColocaIVPContract.format(contractId = listaDeContratos[0][0], name = nameBandwidth, bandwidth = bandwidth, 
            cost = valor, startDate = dataInicio, endDate = f"'{endDate}'" if endDate else 'NULL'))
            #IVP.commit()
            
            
        except IntegrityError as error:
            if error.errno == 1062:
                print(f"saleContract: Contrato duplicado = Código: {codigo}, contrato: {listaDeContratos[0][0]}")
            else:
                raise error


'''
VENDAS: str = Path.join(Path.dirname(__file__), 'VENDASjaneiro.csv')
with open(VENDAS, 'r') as file:
    try:
        while (line := file.readline().rstrip()):
            values: list[str] = line.split(';')
            codigo: int
            #contract
            time: 'str | None'

            for i, value in enumerate(values):
                if i == 0:
                    try:
                        codigo = value[:value.index('_')]
                    except ValueError:
                        codigo = value
                if i == 2:
                    time = value

            saleContract(codigo, None , time)

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
                if i == 0:
                    endDate = value
                elif i == 1:
                    try:
                        codigo = value[:value.index('_')]
                    except ValueError:
                        codigo = value
                
            saleContract(codigo, endDate, None)

    except Exception as error:
        print(error)

print(CodeListContract)
with open(MaisDe1Contrato, 'w') as file:
    print('Códigos que possuem mais de 1 contrato')
    file.writelines(CodeListContract)
    file.close
