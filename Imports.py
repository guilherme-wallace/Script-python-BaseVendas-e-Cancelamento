#from configparser import Error
#from os import terminal_size
from typing import List
import mysql.connector as MySQL
#from mysql.connector import connection
from mysql.connector.errors import IntegrityError
#import requests as Request
import os.path as Path
#import json as JSON
import re 
from datetime import date, datetime
from requests.models import REDIRECT_STATI
from Erros import *
from dadosDaConexao import *

ImportsLog: str = Path.join(Path.dirname(__file__), 'ImportsLog.log')
ListLog: list = []

RBX = MySQL.connect(host=hostRBX, user=userRBX, password=passwordRBX, database=databaseRBX, auth_plugin='mysql_native_password')

IVP = MySQL.connect(host=hostIVP, user=userIVP, password=passwordIVP, database=databaseIVP, auth_plugin='mysql_native_password') 

cursorRBX = RBX.cursor()

cursorIVP = IVP.cursor()

def saleAddress(codigo: int, complement: str) -> None:
    queryColocaIVPAddress ="insert into saleAddress (postalCodeId, number, complement) values ('{postalCodeId}', {number}, {complement})"
    cursorRBX.execute(f'select Clientes.Numero, Clientes.CEP from isupergaus.Clientes where Clientes.Codigo = {codigo};')
    listaEnderecos: list = cursorRBX.fetchone()
    numero: int = listaEnderecos[0]
    complement: str
    addressId: int = None

    if numero == 0:
        numero: str = str('NULL')

    try:
        cursorIVP.execute(queryColocaIVPAddress.format(postalCodeId = listaEnderecos[1], number = numero, 
        complement = f"'{complement}'" if complement else 'NULL'))
        #IVP.commit()
        addressId = cursorIVP.lastrowid
        #print(addressId)
        return addressId
    except IntegrityError as error:
        if error.errno == 1452:
            raise AddressCepException(f'saleAddress em erro = Código: {codigo}, Cep: {listaEnderecos[1]}')
        else:
            print('Erro não identificado no saleAddress', error)
            raise error

def saleResearch(codigo: int, howMet: 'str | None', reason: 'str | None', satisfaction: 'str | None',
handout: 'str | None', facebook: 'str | None', instagram: 'str | None') -> None:
    queryColocaIVPResearch ="insert into saleResearch (howMetId, reasonId, serviceProviderId, satisfactionId, handout, facebook, \
instagram) values ({howMetId}, {reasonId}, {serviceProviderId}, {satisfactionId}, {handout}, {facebook}, {instagram})"
    howMet: 'str | None'
    reason: 'str | None'
    serviceProvider: 'str | None' = 'NULL'
    satisfaction: 'str | None'
    handout: 'str | None'
    facebook: 'str | None'
    instagram: 'str | None'
    researchId: int = None

    #Motivo
    if not howMet:
        howMet = 'NULL'
    elif howMet == "Indicação":
        howMet: int = 2
    elif howMet == "Já foi cliente":
        howMet: int = 1
    elif howMet == "Panfleto":
        howMet: int = 5
    elif howMet == "Mala direta":
        howMet: int = 2
    elif howMet == "Indique um Amigo":
        howMet: int = 2
    elif howMet == "Internet":
        howMet: int = 3
    elif howMet == "Facebook":
        howMet: int = 8
    elif howMet == "Carro InterVip":
        howMet: int = 7
    elif howMet == "Site Intervip":
        howMet: int = 3
    elif howMet == "WhatsApp":
        howMet: str = "ERROR"
    elif howMet == "Elevador (Elemídia)":
        howMet: str = "ERROR"
    elif howMet == "Instagram":
        howMet: int = 9

    #Reason
    if not reason:
        reason = 'NULL'
    elif reason == "Mudança de endereço":
        reason = 18
    elif reason == "Viagem":
        reason = 19
    elif reason == "Mudou-se para casa":
        reason = 18
    elif reason == "Intermitencia no Serviço":
        reason = 15
    elif reason == "Não Utiliza":
        reason = 21
    elif reason == "Sem Acesso":
        reason = 17
    elif reason == "Nagios":
        reason = 17
    elif reason == "Dificuldade Financeira":
        reason = 13
    elif reason == "Lentidão no Acesso":
        reason = 16
    elif reason == "Boleto":
        reason = 11
    elif reason == "Demora no atendimento":
        reason = 12
    elif reason == "Encerramento da Empresa":
        reason = 14
    elif reason == "Acha o plano caro":
        reason = 10
    elif reason == "Sem Computador":
        reason = 21
    #ServiceProvider
    if reason == "Assinou Outra":
        reason = 20
        serviceProvider = 42
    elif reason == "Assinou VIVO":
        reason = 20
        serviceProvider = 36
    elif reason == "Assinou NET":
        reason = 20
        serviceProvider = 23
    elif reason == "Assinou EBR NET":
        reason = 20
        serviceProvider = 27
    elif reason == "Assinou Interless":
        reason = 29
        serviceProvider = 31
    elif reason == "Assinou InterPrime":
        reason = 20
        serviceProvider = 28
    elif reason == "Assinou GVT":
        reason = 20
        serviceProvider = 36

    #Satisfaction
    if not satisfaction:
        satisfaction = 'NULL'
    elif satisfaction == "SATISFEITO":
        satisfaction: int = 37
    elif satisfaction == "INSATISFEITO COM O SERVIÇO":
        satisfaction: int = 39
    elif satisfaction == "INSATISFEITO COM ATENDIMENTO/SERVIÇO":
        satisfaction: int = 40
    elif satisfaction == "INSATISFEITO COM O ATENDIMENTO":
        satisfaction: int = 38
    
    if not handout:
        handout: int = 0
    elif handout == "NAO":
        handout: int = 0 
    elif handout == "SIM":
        handout: int = 1 
    
    if not facebook:
        facebook: int = 0
    elif facebook == "NAO":
        facebook: int = 0 
    elif facebook == "SIM":
        facebook: int = 1 

    if not instagram:
        instagram: int = 0
    elif instagram == "NAO":
        instagram: int = 0 
    elif instagram == "SIM":
        instagram: int = 1 

    try:
        cursorIVP.execute(queryColocaIVPResearch.format(howMetId = howMet, reasonId = reason, serviceProviderId = serviceProvider, 
        satisfactionId = satisfaction, handout = handout, facebook = facebook, instagram = instagram))
        #IVP.commit()
        researchId = cursorIVP.lastrowid
        #print(researchId)
        return researchId
            
    except IntegrityError as error:
        if error:
            raise ResearchErrorException(f'Erro ao inserir saleResearch do cliente {codigo}')
        else:
            print('Erro não identificado no saleResearch', error)
            raise error

def Contracts(codigo: int) -> str:
    cursorRBX.execute(f'SELECT Contratos.PacoteID as Pacote FROM isupergaus.Contratos \
                JOIN isupergaus.PlanosPacotes ON PlanosPacotes.Codigo = Contratos.Pacote \
                WHERE Contratos.Cliente = "{codigo}" GROUP BY Contratos.PacoteID;')
    listaDeContratos:list = cursorRBX.fetchall()

    if len(listaDeContratos) == 0:
        raise ContractNotFoundException(f'O código {codigo} não tem contrato.')
    
    if len(listaDeContratos) > 1:
        #raise MoreThanOneContractException(list(listaDeContratos[0]), f'O código {codigo} tem mais de um contrato.')
        chosenContract = 'Escolha inválida'
        print(listaDeContratos)
        chosenContractIndex: int = int(input('Escolha o index do contrato correto: '))
        
        for numberOfContract, otherContract in enumerate(listaDeContratos):
            if numberOfContract == chosenContractIndex:
                chosenContract = otherContract[0]
        
        while chosenContract == 'Escolha inválida':
            print('Index escolhido inválido! Por favor selecione outro.')
            chosenContractIndex: int = int(input('Escolha o index do contrato correto: '))
            for numberOfContract, otherContract in enumerate(listaDeContratos):
                if numberOfContract == chosenContractIndex:
                    chosenContract = otherContract[0]
        
        print(f'Contrato escolhido: {chosenContract}')
        return chosenContract

    else:
        return listaDeContratos[0][0]

def saleContract(codigo: int, contract: str, endDate: 'str | None', time: 'str | None') -> None:
    queryColocaIVPContract ="insert into saleContract (contractId, name, bandwidth, cost, startDate, endDate) values ('{contractId}', '{name}', \
{bandwidth}, {cost}, '{startDate}', {endDate})"
    cursorRBX.execute(f'SELECT PlanosPacotes.Descricao, sum(Contratos.ValorPlano) as Valor, Contratos.Inicio \
                FROM isupergaus.Contratos JOIN isupergaus.PlanosPacotes \
                ON PlanosPacotes.Codigo = Contratos.Pacote \
                WHERE Contratos.PacoteID = "{contract}"')
    listaDeContratos:list = cursorRBX.fetchall()
    bandwidth: int = int(re.search(r'\d{2,3}(?=M)', listaDeContratos[0][0])[0])
    valor: float = format(listaDeContratos[0][1], ".2f")
    nameBandwidth: str =f'{bandwidth} Mbps'
    dataInicio: str = listaDeContratos[0][2]

    if endDate is not None:
        endDate = datetime.strptime(endDate, '%d/%m/%Y').date()
    if time is not None:
        dataInicio = f'{dataInicio} {time}'

    try:
        cursorIVP.execute(queryColocaIVPContract.format(contractId = contract, name = nameBandwidth, bandwidth = bandwidth, 
        cost = valor, startDate = dataInicio, endDate = f"'{endDate}'" if endDate else 'NULL'))
        #IVP.commit()
    except IntegrityError as error:
        if error.errno == 1062:
            raise DuplicateContractException(contract, f'Contrato duplicado = Código: {codigo}, contrato: {contract}')
        else:
            print('Erro não identificado no SaleContract', error)
            raise error

def sale (codigo: int, contract: str, addressId: int, operation: str, researchId: int, dateSpreadsheet: str, time: 'str | None', salesperson: 'str | None', observation: 'str | None') -> None:
    querryColocaIVPSale = "insert into sale (contractId, clientId, groupId, addressId, operation, technologyId, \
researchId, datetime, salespersonId, observation) values ('{contractId}', {clientId}, {groupId}, {addressId}, \
'{operation}', {technologyId}, {researchId}, '{datetime}', {salespersonId}, '{observation}')"
    cursorRBX.execute(f'SELECT grupo FROM isupergaus.Clientes WHERE Clientes.Codigo = {codigo}')
    dadosDaQuerryRBX: list = list(cursorRBX.fetchone())
    cursorIVP.execute(f'select technologyId from block where groupId = {dadosDaQuerryRBX[0]}')
    dadosDaQuerryIVP: list = cursorIVP.fetchall()

    if dadosDaQuerryIVP == []:
        raise BlockHasNoTechnologyException((f'Cliente {codigo}, pertece ao grupo {dadosDaQuerryRBX[0]}, e não possui bloco.'))
    
    if time is not None:
        dateSpreadsheet = f'{datetime.strptime(dateSpreadsheet, "%d/%m/%Y").date()} {time}'
    else:
        dateSpreadsheet = f'{datetime.strptime(dateSpreadsheet, "%d/%m/%Y").date()}'

    if operation == "NOVO":
        operation = "V"
    elif operation == "REATIVAÇAO":
        operation = "R"
    elif operation == "Cancelado":
        operation = "C"
    elif operation == "Suspenso":
        operation = "S"
    elif operation == "Retido":
        operation = "T"
    
    if observation == "":
        observation = "-"

    if salesperson == "":
        raise SalespersonIsNoneException(f'O código {codigo} não tem vendedor')
    elif salesperson == "LUANA MATHIAS COITINHO":
        salesperson: int = int(68)
    elif salesperson == "DOUGLAS AZEVEDO CARVALHO CAVALCANTE":
        salesperson: int = int(34)
    elif salesperson == "JULIO CESAR GOMES DA SILVA":
        salesperson: int = int(58)
    elif salesperson == "MYRIÃ RODRIGUES":
        salesperson: int = int(89)
    elif salesperson == "MATHEUS PEREIRA SEBASTIÃO PEREIRA":
        salesperson: int = int(80)
    elif salesperson == "VANDERLEI FERNANDES DA COSTA JUNIOR":
        salesperson: int = int(108)
    elif salesperson == "AMANDA QUININO DA SILVA":
        salesperson: int = int(9)
    elif salesperson == "MATHEUS BARROS BOLDT":
        salesperson: int = int(79)
    elif salesperson == "FABIO HENRIQUE RIBEIRO SOARES":
        salesperson: int = int(120)
    elif salesperson == "PAULO RICARDO DE OLIVEIRA":
        salesperson: int = int(96)
    elif salesperson == "FRANCIELE DAS NEVES":
        salesperson: int = int(118)
    elif salesperson == "WELITON DE OLIVEIRA GONZAGA":
        salesperson: int = int(121)

    try:
        cursorIVP.execute(querryColocaIVPSale.format(contractId = contract, clientId = codigo, groupId = dadosDaQuerryRBX[0], addressId = addressId, \
operation = operation, technologyId = dadosDaQuerryIVP[0][0], researchId = researchId, datetime = dateSpreadsheet, salespersonId = salesperson, \
observation = observation))
    except IntegrityError as error:
        if error:
            print(error, " ", codigo)
        else:
            print('Erro não identificado no Sale', error )
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
            #addres
            complement: 'str | None'
            #research
            howMet: 'str | None'
            handout: str = 'NAO'
            facebook: str
            instagram:str
            #sale
            operation: str
            dateSpreadsheet: str
            salesperson: 'str | None'
            observation: 'str | None'

            for i, value in enumerate(values):
                if i == 0:
                    try:
                        codigo = value[:value.index('_')]
                    except ValueError:
                        codigo = value
                if i == 2:
                    time = value
                if i == 9:
                    complement = value
                if i == 16:
                    howMet = value
                if i == 22:
                    facebook = value
                if i == 23:
                    instagram = value
                if i == 7:
                    operation = value
                if i == 1:
                    dateSpreadsheet = value
                if i == 12:
                    salesperson = value
                if i == 20:
                    observation = value
            
            contract: str = ''
            try:
                contract= Contracts(codigo)
                saleContract(codigo, contract, None , time)
            except DuplicateContractException as e:
                pass
            except ScriptException as e:
                ListLog.append(f'|> {e.message} \n')
            
            if contract != '':
                try:
                    sale(codigo, contract, saleAddress(codigo, complement), operation, saleResearch(codigo, howMet , None, None, handout, facebook, instagram), 
                    dateSpreadsheet, time, salesperson, observation)
                except ScriptException as e:
                    ListLog.append(f'{e.message} \n')
            
    except Exception as error:
        print(f'erro: {error} no cliente {codigo}')
    
    finally:
        IVP.commit()
        IVP.close()
        print("Tabela de vendas inserida com sucesso!")
'''
CANCELAMENTO: str = Path.join(Path.dirname(__file__), 'CANCELAMENTOjaneiro.csv')
with open(CANCELAMENTO, 'r') as file:
    try:
        while (line := file.readline().rstrip()):
            values: list[str] = line.split(';')
            codigo: int
            #contract
            endDate: 'str | None'
            #addres
            complement: 'str | None'
            #research
            reason: 'str | None'
            satisfaction: 'str | None'
            handout: 'str | None' = 'NULL'
            facebook: 'str | None' = 'NULL'
            instagram: 'str | None' = 'NULL'
            #sale
            operation: str
            dateSpreadsheet: 'str | None'
            salesperson: 'str | None'
            observation: 'str | None'

            for i, value in enumerate(values):
                if i == 0:
                    endDate = value
                    dateSpreadsheet = value
                elif i == 1:
                    try:
                        codigo = value[:value.index('_')]
                    except ValueError:
                        codigo = value
                if i == 7:
                    complement = value
                if i == 3:
                    reason = value
                if i == 12:
                    satisfaction = value
                if i == 4:
                    operation = value
                if i == 11:
                    salesperson = value
                if i == 14:
                    observation = value
                
            #saleContract(codigo, endDate, None)
            #saleAddress(codigo, complement)
            #saleResearch(codigo, None , reason, satisfaction, handout, facebook, instagram)
            
            contract: str = ''
            try:
                contract= Contracts(codigo)
                saleContract(codigo, contract, endDate , None)
            except DuplicateContractException as e:
                pass
            except ScriptException as e:
                ListLog.append(f'|> {e.message} \n')
            
            if contract != '':
                try:
                    sale(codigo, contract, saleAddress(codigo, complement), operation, saleResearch(codigo, None , reason, satisfaction, handout, 
                    facebook, instagram), dateSpreadsheet, None, salesperson, observation)
                except ScriptException as e:
                    ListLog.append(f'{e.message} \n')

    except Exception as error:
        print(f'erro: {error} no cliente {codigo}')
    
    finally:
        IVP.commit()
        IVP.close()
        print("Tabela de cancelamento inserida com sucesso!")

with open(ImportsLog, 'w') as file:
    print('Log criado')
    file.writelines(ListLog)
    file.close