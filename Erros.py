class ScriptException(Exception):
    message: str

    def __init__(self, message):
        super().__init__()
        self.message = message

class ContractException(ScriptException):
    contracts: list

    def __init__(self, contracts, message):
        super().__init__(message)
        self.contracts = contracts

class ContractNotFoundException(ContractException):
    def __init__(self, message):
        super().__init__(None, message)

class MoreThanOneContractException(ContractException):
    def __init__(self, contracts, message):
        super().__init__(contracts, message)

class DuplicateContractException(ContractException):
    def __init__(self, contracts, message):
        super().__init__(contracts, message)

class SalespersonIsNoneException(ScriptException):
    def __init__(self,message):
        super().__init__(message)

class AddressCepException(ScriptException):
    def __init__(self,message):
        super().__init__(message)

class ResearchErrorException(ScriptException):
    def __init__(self,message):
        super().__init__(message)

class BlockHasNoTechnologyException(ScriptException):
    def __init__(self,message):
        super().__init__(message)