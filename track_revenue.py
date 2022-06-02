from web3 import Web3
from web3.datastructures import AttributeDict
from web3.exceptions import TransactionNotFound
from web3.middleware import geth_poa_middleware
from web3.logs import DISCARD
from eth_account import Account
import json
import requests
import time
import logging
import websocket as ws
from datetime import datetime
import sys

#To RUN, in console type: python3 track_revenue.py START_BLOCK END_BLOCK VALIDATOR_ADDRESS
#For example:             python3 track_revenue.py 28500000 29000000 0xb95d435df3f8b2a8d8b9c2b7c8766c9ae6ed8cc9
#Note that the validator address includes the 0x prefix but has no quotation marks around it. It can be Checksum or all lower case - either works. 

#POLYGONSCAN API KEY#
API_KEY = 'get_api_key_from_signing_up_on_polygonscan.com'

#PROFIT RECEIVING ADDRESS#
#(WHERE YOU STORE YOUR NON-GAS TOKENS BETWEEN TRADE - PROBABLY YOUR SMART CONTRACT)#
ACCOUNT = Web3.toChecksumAddress('0x000000000000')

#EOA LIST#
#yes, all of them...#
EOA_LIST = [
    Web3.toChecksumAddress('0x11111'),
    Web3.toChecksumAddress('0x22222'),
    Web3.toChecksumAddress('0x33333'),
    Web3.toChecksumAddress('0xetc...'),
]

#SETTINGS#
blocksIncrementPerSearch = 100000
transactionsToSearch = 10000
sleepBetweenSearches = 0.25 #in seconds, to prevent timeout
isArchiveNode = False  #If using an archive node, set this as True for more accurate USD net profit reporting

#OUTPUT FILE#
outputFile = 'validatorRevenueTracking.json'

#LOCAL / CONNECTION SETTINGS#
#THIS PROGRAM WILL NOT WORK WITHOUT erc20.json and uniswapLPAbi.json in your ABI direction
ABI_DIRECTORY = "./abi"  #folder where erc20.json (erc20 ABI) and uniswapLPAbi.json (ABI for uniswap V2 liquidity pools) are stored

#WEB3PY CONNECTION#
ipc_provider = '/data/ipc/bor/bor.ipc'
web3 = Web3(Web3.IPCProvider(ipc_provider))
web3.middleware_onion.inject(geth_poa_middleware, layer=0)

#WEBSOCKET CONNECTION#
global bpcrWebsocket
bpcrWebsocket = ws.create_connection("ws://0.0.0.0:8545")

#CONSTANTS#
MATIC_ADDRESS = Web3.toChecksumAddress('0x0000000000000000000000000000000000001010')
WMATIC_ADDRESS = Web3.toChecksumAddress('0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270')
CHI_ADDRESS = Web3.toChecksumAddress("0x0000000000004946c0e9F43F4Dee607b0eF1fA1c")
WETH_ADDRESS = Web3.toChecksumAddress('0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619')
WBTC_ADDRESS = Web3.toChecksumAddress('0x1BFD67037B42Cf73acF2047067bd4F2C47D9BfD6')
USDC_ADDRESS = Web3.toChecksumAddress('0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174')
USDT_ADDRESS = Web3.toChecksumAddress('0xc2132D05D31c914a87C6611C10748AEb04B58e8F')
DAI_ADDRESS = Web3.toChecksumAddress('0x8f3cf7ad23cd3cadbd9735aff958023239c6a063')
AAVE_ADDRESS = Web3.toChecksumAddress('0xD6DF932A45C0f255f85145f286eA0b292B21C90B')
CRV_ADDRESS = Web3.toChecksumAddress('0x172370d5cd63279efa6d502dab29171933a610af')
UST_ADDRESS = Web3.toChecksumAddress('0x692597b009d13c4049a947cab2239b7d6517875f')
MAI_ADDRESS = Web3.toChecksumAddress('0xa3fa99a148fa48d14ed51d610c367c61876997f1')
LINK_ADDRESS = Web3.toChecksumAddress('0x53e0bca35ec356bd5dddfebbd1fc0fd03fabad39')
QUICKSWAP_ADDRESS = Web3.toChecksumAddress('0x831753DD7087CaC61aB5644b308642cc1c33Dc13')
UNI_ADDRESS = Web3.toChecksumAddress('0xb33eaad8d922b1083446dc23f610c2567fb5180f')
SNX_ADDRESS = Web3.toChecksumAddress('0x50b728d8d964fd00c2d0aad81718b71311fef68a')
DPI_ADDRESS = Web3.toChecksumAddress('0x85955046df4668e1dd369d2de9f3aeb98dd2a369')

TOKEN_TO_DECIMALS_DICT = {
    str(WMATIC_ADDRESS).lower(): 18,
    str(WETH_ADDRESS).lower(): 18,
    str(WBTC_ADDRESS).lower(): 6,
    str(USDC_ADDRESS).lower(): 6,
    str(USDT_ADDRESS).lower(): 6,
    str(MAI_ADDRESS).lower(): 18,
    str(DAI_ADDRESS).lower(): 18,
    str(AAVE_ADDRESS).lower(): 18,
    str(CRV_ADDRESS).lower(): 18,
    str(UST_ADDRESS).lower(): 18,
    str(LINK_ADDRESS).lower(): 18,
    str(QUICKSWAP_ADDRESS).lower(): 18,
    str(UNI_ADDRESS).lower(): 18,
    str(SNX_ADDRESS).lower(): 18,
    str(DPI_ADDRESS).lower(): 18,
}

TOKEN_ADDRESS_TO_NAME_DICT = {
    str(WMATIC_ADDRESS): 'WMATIC',
    str(WETH_ADDRESS): 'WETH',
    str(WBTC_ADDRESS): 'WBTC',
    str(USDC_ADDRESS): 'USDC',
    str(USDT_ADDRESS): 'USDT',
    str(MAI_ADDRESS): 'MAI',
    str(DAI_ADDRESS): 'DAI',
    str(AAVE_ADDRESS): 'AAVE',
    str(CRV_ADDRESS): 'CRV',
    str(UST_ADDRESS): 'UST',
    str(LINK_ADDRESS): 'LINK',
    str(QUICKSWAP_ADDRESS): 'QUICKSWAP',
    str(UNI_ADDRESS): 'UNI',
    str(SNX_ADDRESS): 'SNX',
    str(DPI_ADDRESS): 'DPI',
}

STABLE_TOKEN_LIST = [
    Web3.toChecksumAddress('0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174'),
    Web3.toChecksumAddress('0xc2132D05D31c914a87C6611C10748AEb04B58e8F'),
    Web3.toChecksumAddress('0x8f3cf7ad23cd3cadbd9735aff958023239c6a063'),
]

QUICKSWAP_USDC_ROUTE = {
    WMATIC_ADDRESS: ["0x6e7a5fafcec6bb1e78bae2a1f0b612012bf14827"],
    WETH_ADDRESS: ["0x853ee4b2a13f8a742d64c8f088be7ba2131f670d"],
    WBTC_ADDRESS: ["0xf6a637525402643b0654a54bead2cb9a83c8b498"],
    CRV_ADDRESS: ["0x8982d71337003cd172198739238ada0d5d0bd2fe"],
    QUICKSWAP_ADDRESS: ["0x1f1e4c845183ef6d50e9609f16f6f9cae43bc9cb"],
    AAVE_ADDRESS: ["0x90bc3e68ba8393a3bf2d79309365089975341a43", "0x853ee4b2a13f8a742d64c8f088be7ba2131f670d"],
    SNX_ADDRESS: ["0x6f0f154edbc4468034f4b4fefec5b6c636dc4600", "0xf04adbf75cdfc5ed26eea4bbbb991db002036bdd"],
    LINK_ADDRESS: ["0x5ca6ca6c3709e1e6cfe74a50cf6b2b6ba2dadd67", "0x1f1e4c845183ef6d50e9609f16f6f9cae43bc9cb"],
    DPI_ADDRESS: ["0x9f77ef7175032867d26e75d2fa267a6299e3fb57", "0x853ee4b2a13f8a742d64c8f088be7ba2131f670d"],
    UNI_ADDRESS: ["0x75ee48f37d0b8e3899b53695622932dc3c052e41"],
}

#ABIs#
with open(f"{ABI_DIRECTORY}/uniswapLPAbi.json") as f:
        uniswap_lp_abi = json.load(f)

with open(f"{ABI_DIRECTORY}/erc20.json") as f:
        erc20_abi = json.load(f)
        erc20_contract = web3.eth.contract(abi=erc20_abi)
  

#SYS INPUT#
assert len(sys.argv) == 4, f"Usage: {sys.argv[0]} START_BLOCK  END_BLOCK  VALIDATOR"
startBlock = int(sys.argv[1])
endBlock = int(sys.argv[2])
validator_to_track = str(sys.argv[3])

#BASE FUNCTIONS#
def updateUniV2Reserves(web3, poolAddress):
    storage1 = str(web3.toHex(web3.eth.get_storage_at(poolAddress, 8)))[10:]
    return web3.toInt(hexstr='0x' + storage1[28:]), web3.toInt(hexstr='0x' + storage1[:28])

def updateArchivedUniV2Reserves(web3, poolAddress, blockNumber):
    storage1 = str(web3.toHex(web3.eth.get_storage_at(poolAddress, 8, block_identifier=blockNumber)))[10:]
    return web3.toInt(hexstr='0x' + storage1[28:]), web3.toInt(hexstr='0x' + storage1[:28])

def findUniTokens(web3, poolAddress):
    return web3.toChecksumAddress('0x' + str(web3.toHex(web3.eth.get_storage_at(poolAddress, 6)))[26:]), web3.toChecksumAddress('0x' + str(web3.toHex(web3.eth.get_storage_at(poolAddress, 7)))[26:])

def tokenDecimals(_address):
    return int(TOKEN_TO_DECIMALS_DICT[str(_address).lower()])

def getMaticPrice(web3):
    global currentMaticPrice
    quickswapWmaticUsdc = web3.toChecksumAddress('0x6e7a5fafcec6bb1e78bae2a1f0b612012bf14827')
    _maticReserves, _usdcReserves = updateUniV2Reserves(web3, quickswapWmaticUsdc)
    _maticReservesFormatted = _maticReserves / ( 10 ** 18)
    _usdcReservesFormatted = _usdcReserves / ( 10 ** 6)
    currentMaticPrice = _usdcReservesFormatted / _maticReservesFormatted

def getArchivedMaticPrice(web3, blockNumber):
    quickswapWmaticUsdc = web3.toChecksumAddress('0x6e7a5fafcec6bb1e78bae2a1f0b612012bf14827')
    _maticReserves, _usdcReserves = updateArchivedUniV2Reserves(web3, quickswapWmaticUsdc, blockNumber)
    _maticReservesFormatted = _maticReserves / ( 10 ** 18)
    _usdcReservesFormatted = _usdcReserves / ( 10 ** 6)
    return _usdcReservesFormatted / _maticReservesFormatted

#DATA GATHERING FUNCTIONS#
def getValidator(web3, bpcrWebsocket, blockNumber):
    validatorRequest={
        "jsonrpc": "2.0",
        "method": "bor_getAuthor",
        "params": [web3.toHex(blockNumber)],
        "id": 1,
    }
    x = bpcrWebsocket.send(json.dumps(validatorRequest)) 
    y = json.loads(bpcrWebsocket.recv())

    if y["result"] == None:
        bpcrWebsocket.close()
        bpcrWebsocket.connect("ws://0.0.0.0:8545")
        x = bpcrWebsocket.send(json.dumps(validatorRequest)) 
        y = json.loads(bpcrWebsocket.recv())
    return y["result"]

def get_transaction_hashes(_eoa):
    try:
        currentBlock = int(web3.eth.blockNumber)
        payload = {
            'module': 'account',
            'action': 'txlist',
            'address': str(_eoa),
            'startblock': int(query_data[_eoa]['startBlock']),
            'endblock': int(query_data[_eoa]['endBlock']),
            'sort': 'desc',
            'apikey': API_KEY
        }
        r = requests.get('https://api.polygonscan.com/api', params=payload)
        result = r.json()['result']

        transaction_hash_list = []
        for entry in result:
            if int(entry['nonce']) > int(query_data[_eoa]['startNonce']):
                transaction_hash_list.append(entry['hash'])

        query_data[_eoa]['startBlock'] = int(query_data[_eoa]['endBlock'] + 1)

        if query_data[_eoa]['startBlock'] + blocksIncrementPerSearch > int(endBlock):
            query_data[_eoa]['endBlock'] = int(endBlock)

        else:
            query_data[_eoa]['endBlock'] = query_data[_eoa]['startBlock'] + blocksIncrementPerSearch 

        query_data[_eoa]['startNonce'] = int(result[0]['nonce'])

        return transaction_hash_list
    except:
        return []

#PROFIT ASSESSMENT FUNCTIONS#
def mark_to_usd(web3, block_number, _token_address, amount):
    """
    input a token address and integer amount
    returns a float amount of dollars
    """
    def addressIfNotAddress(a):
        return web3.toChecksumAddress(a) if not web3.isAddress(a) else a

    token_address = addressIfNotAddress(_token_address)

    if token_address in STABLE_TOKEN_LIST:
        return amount / (10** tokenDecimals(token_address))

    last_token = token_address
    last_amount = amount
    pool_path = QUICKSWAP_USDC_ROUTE[token_address]
    for _pool_address in pool_path:
        pool_address = web3.toChecksumAddress(_pool_address)
        if isArchiveNode == True:
            r0, r1 = updateArchivedUniV2Reserves(web3, pool_address, block_number)
        else:
            r0, r1 = updateUniV2Reserves(web3, pool_address)
        t0, t1 = findUniTokens(web3, pool_address)
        r_in = r0 if t0 == last_token else r1
        r_out = r1 if t0 == last_token else r0
        last_token = t0 if t0 != last_token else t1
        last_amount = last_amount * r_out//r_in
    return last_amount / (10 ** 6)

def token_deltas_from_tx(tx_hash, tx, blockNumber):
    receipt = web3.eth.get_transaction_receipt(tx_hash)
    erc20_logs = [
        log
        for log in erc20_contract.events.Transfer().processReceipt(receipt, errors=DISCARD)
        if log.address != CHI_ADDRESS
    ]
    
    foundTokenOut = False
    foundTokenIn = False
    for erc20_log in erc20_logs:
        if erc20_log.args["from"].endswith(ACCOUNT):
            tokenOutAmount = erc20_log.args["value"]
            foundTokenOut = True
            tokenOut = erc20_log.address

        elif erc20_log.args["to"].endswith(ACCOUNT):
            tokenInAmount = erc20_log.args["value"]
            foundTokenIn = True
            tokenIn = erc20_log.address

    if foundTokenOut == True and foundTokenIn == True:
        if tokenOut == tokenIn:
            token_delta = tokenInAmount - tokenOutAmount
            reporting_token = tokenIn
        elif tokenOut in STABLE_TOKEN_LIST and tokenIn == MATIC_ADDRESS:
            token_delta = 0
            reporting_token = 'gas'
        elif tokenOut == WMATIC_ADDRESS and tokenIn == MATIC_ADDRESS:
            token_delta = 0
            reporting_token = 'gas'
        elif tokenOut in STABLE_TOKEN_LIST and tokenIn in STABLE_TOKEN_LIST:
            token_delta_formated = (tokenInAmount / (10 ** tokenDecimals(tokenIn))) - (tokenOutAmount / (10 ** tokenDecimals(tokenOut)))
            reporting_token = tokenIn
            token_delta = token_delta_formated * (10 ** tokenDecimals(tokenIn))
        else:
            token_delta = 0
            reporting_token = 'unknown'
    
    elif foundTokenIn == True:
        token_delta = tokenInAmount
        reporting_token = tokenIn

    else:
        token_delta = 0
        reporting_token = 'fail'

    gas_cost = receipt.gasUsed * tx.gasPrice

    return reporting_token, token_delta, gas_cost

def process_profit(web3, tx_hash, tx, validator, blockNumber):
    reporting_token, token_delta, gas_cost = token_deltas_from_tx(tx_hash, tx, blockNumber)
    if validator not in profitTrackingDict:
        profitTrackingDict[validator] = {}
    if 'gas_cost_in_USD' not in profitTrackingDict[validator]:
        profitTrackingDict[validator]['gas_cost_in_USD'] = mark_to_usd(web3, blockNumber, WMATIC_ADDRESS, gas_cost)
    else:
        profitTrackingDict[validator]['gas_cost_in_USD'] += mark_to_usd(web3, blockNumber, WMATIC_ADDRESS, gas_cost)
    if token_delta != 0:
        if TOKEN_ADDRESS_TO_NAME_DICT[reporting_token] not in profitTrackingDict[validator]:
            profitTrackingDict[validator][TOKEN_ADDRESS_TO_NAME_DICT[reporting_token]] = token_delta / (10 ** tokenDecimals(reporting_token))
        else:
            profitTrackingDict[validator][TOKEN_ADDRESS_TO_NAME_DICT[reporting_token]] += token_delta / (10 ** tokenDecimals(reporting_token))
    
        gasCostUSD = mark_to_usd(web3, blockNumber, WMATIC_ADDRESS, gas_cost)
        tokenReturnUSD = mark_to_usd(web3, blockNumber, reporting_token, token_delta)
        netProfitUSD = tokenReturnUSD - gasCostUSD
    else:
        gasCostUSD = mark_to_usd(web3, blockNumber, WMATIC_ADDRESS, gas_cost)
        netProfitUSD = 0 - gasCostUSD

    if 'net_profit_in_USD' not in profitTrackingDict[validator]:
        profitTrackingDict[validator]['net_profit_in_USD'] = netProfitUSD
    else:
        profitTrackingDict[validator]['net_profit_in_USD'] += netProfitUSD

    if 'transaction_count' not in profitTrackingDict[validator]:
        profitTrackingDict[validator]['transaction_count'] = 1
    else:
        profitTrackingDict[validator]['transaction_count'] += 1

def monitor_transactions_segment(web3, bpcrWebsocket, eoa):
    new_transaction_hash_list = get_transaction_hashes(eoa)
    if len(new_transaction_hash_list) > 0:
        for tx_hash in new_transaction_hash_list:
            tx = web3.eth.get_transaction(tx_hash)
            _blockNumber = int(tx.blockNumber)
            validator = getValidator(web3, bpcrWebsocket, _blockNumber)
            process_profit(web3, tx_hash, tx, validator, _blockNumber)
        return True
    else:
        return False

#PROFIT REPORTING FUNCTIONS#
def build_query_data_dict(eoa_list):
    for eoa in eoa_list:
        if eoa not in query_data.keys():
            query_data[eoa] = {}
        
        if int(startBlock) + blocksIncrementPerSearch > int(endBlock):
            _endBlock = int(endBlock)
        else:
            _endBlock = int(startBlock) + blocksIncrementPerSearch 
        
        query_data[eoa] = {
            'startBlock': int(startBlock),
            'startNonce': int(web3.eth.get_transaction_count(ACCOUNT)) - transactionsToSearch,
            'endBlock': _endBlock
        }

def format_profit_tracking_dict():
    initial_start_block = startBlock
    tokenReturnSummaryDict = {}
    cumulativeNetProfitUSD = 0
    for validator in profitTrackingDict.keys():
        if len(list(profitTrackingDict[validator].keys())) > 0:
            foundNetProfit = False
            foundTransactionCount = False
            foundGasCost = False
            processedRatios = False
            for _token in profitTrackingDict[validator].keys():
                if _token == 'transaction_count':
                    foundTransactionCount = True
                    transactionCount = profitTrackingDict[validator]['transaction_count']
                    continue
                elif _token == 'net_profit_in_USD':
                    foundNetProfit = True
                    netProfitUSD = profitTrackingDict[validator]['net_profit_in_USD']
                    cumulativeNetProfitUSD += netProfitUSD
                    continue
                elif _token == 'gas_cost_in_USD':
                    foundGasCost = True
                    if 'gas_cost_in_USD' not in tokenReturnSummaryDict.keys():
                        tokenReturnSummaryDict['gas_cost_in_USD'] = profitTrackingDict[validator]['gas_cost_in_USD']
                    else:
                        tokenReturnSummaryDict['gas_cost_in_USD'] += profitTrackingDict[validator]['gas_cost_in_USD']
                    gasCostUSD = profitTrackingDict[validator]['gas_cost_in_USD'] * currentMaticPrice
                    continue
                elif _token == 'n/a':
                    continue
                elif _token == 'gas':
                    continue
                elif _token == 'unknown':
                    continue
                elif _token == 'fail':
                    continue
                else:
                    if _token not in tokenReturnSummaryDict.keys():
                        tokenReturnSummaryDict[_token] = profitTrackingDict[validator][_token]
                    else:
                        tokenReturnSummaryDict[_token] += profitTrackingDict[validator][_token]
                    continue

    profitTrackingDict['SUMMARY'] = {}
    profitTrackingDict['SUMMARY']['netProfitUSD'] = cumulativeNetProfitUSD
    profitTrackingDict['SUMMARY']['startBlock'] = initial_start_block
    profitTrackingDict['SUMMARY']['endBlock'] = endBlock
    profitTrackingDict['SUMMARY'].update(tokenReturnSummaryDict)

    #print('')
    #print('--SUMMARY_FOR_ALL_VALIDATORS-------')
    #print(profitTrackingDict['SUMMARY'])
    #print('-----------------------------------')
    #print('')

    with open(outputFile,'w') as validatorProfitTrackingJson:
        json.dump(profitTrackingDict, validatorProfitTrackingJson, indent=2)



def main():
    currentBlock = int(web3.eth.blockNumber)
    print('currentBlock:',int(web3.eth.blockNumber))
    print('startBlock:',int(startBlock))
    
    getMaticPrice(web3)
    
    print('currentMaticPrice:',round(currentMaticPrice,4))
    print('processing...')
    build_query_data_dict(EOA_LIST)
    for eoa in EOA_LIST:
        print('processing for EOA',eoa)
        while True:
            print('processing from block',query_data[eoa]['startBlock'])
            monitor_transactions_segment(web3, bpcrWebsocket, eoa)
            if int(endBlock + 1) == query_data[eoa]['startBlock']:
                break
            print('processed to block',query_data[eoa]['startBlock'])
            time.sleep(sleepBetweenSearches)

        print('processed to block',query_data[eoa]['endBlock'])
        
    format_profit_tracking_dict()
    
    try:
        vabrv = str(validator_to_track)[:7]
        print('')
        print(f'--- SUMMARY FOR {vabrv} -----------')
        print(f'From block {startBlock} to {endBlock}')
        print('')
        _dict = profitTrackingDict[str(validator_to_track).lower()]
        try:
            print(f'Net Profit:  ${round(_dict["net_profit_in_USD"],2)}   (USD)')
        except:
            print('error finding net_profit_in_USD')
        
        try:
            print(f'Gas Cost:  ${round(_dict["gas_cost_in_USD"],2)}   (USD)')
        except:
            print('error finding gas_cost_in_USD')

        try:
            print('Total Transaction Count:  ', _dict['transaction_count'])
        except:
            print('error finding transaction_count')


        print('')
        print('Token Values Earned:')
        for _k, _v in _dict.items():
            if _k == 'transaction_count' or _k == 'gas_cost_in_USD' or _k == 'net_profit_in_USD':
                continue
            elif _k == 'WBTC' or _k == 'WETH' or _k == 'AAVE':
                print(f'  {_k}:  {round(_v,6)}')
            else:
                print(f'  {_k}:  {round(_v,2)}')
        print('-----------------------------------')
    except:
        print(f'validator {validator_to_track} not found in revenue stream between block {startBlock} and {endBlock}')

#DEFINE DICTIONARIES
query_data = {}
validatorRevenueRate = {}
profitTrackingDict = {}
        
#MODULE MEANT TO BE RUN ALONE, NOT IMPORTED:
main()
