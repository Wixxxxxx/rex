import requests
import pandas as pd
import numpy as np
import json
import time
import datetime
from anchorpy import Idl, Program, Provider
from base58 import b58decode
from solders.pubkey import Pubkey
from solana.rpc.async_api import AsyncClient

from pprint import pprint
import concurrent.futures
from dotenv import load_dotenv
import os
load_dotenv()

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

WHIRLPOOL_PROGRAM_ID = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"
WHIRLPOOL_ADDRESS = "CSP4RmB6kBHkKGkyTnzt9zYYXDA8SbZ5Do5WfZcjqjE4"
SWAP_METHODS = {"swap", "swap_v2", "two_hop_swap", "two_hop_swap_v2"}

HELIUS_API_KEY= os.getenv("Helius_API_Key")

def fetch_account_balance(token_vault_address: str):
    url = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTokenAccountBalance",
        "params": [token_vault_address]
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }
    response = requests.post(url, json=payload, headers=headers)
    response_json = response.json()
    return response_json

def fetch_account_balance_parallel(token_vault_addresses: list[str]):
    balances = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = {executor.submit(fetch_account_balance, addr): addr for addr in token_vault_addresses}

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if 'result' in result:
                balances[futures[future]] = {
                    'balance': result['result']['value']['uiAmount'],
                    'slot': result['result']['context']['slot']
                }
    return balances

def fetch_signatures_for_address(address: str, limit: int = 1000, before=None):
    url = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    params = {"limit": limit}

    if before:
        params["before"] = before

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSignaturesForAddress",
        "params": [f"{address}", params]
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }
    response = requests.post(url, json=payload, headers=headers)
    response_json = response.json()
    return response_json

def fetch_signature_hist_by_block_time(address: str, end_block_time: int, lookback: int = 14, limit: int = 1000):
    """
    Fetch transaction signatures from (end_block_time - lookback days) to end_block_time
    """
    start_block_time = end_block_time - lookback * 24 * 60 * 60
    before = None
    txs = {"slot": [], "block_time": [], "tx_signature": []}
    attempts = 0

    print(f"Fetching signatures from {datetime.datetime.fromtimestamp(start_block_time).strftime('%Y-%m-%d %H:%M:%S')} to {datetime.datetime.fromtimestamp(end_block_time).strftime('%Y-%m-%d %H:%M:%S')}")

    while True:
        if not before:
            print("Called API with no signature")
            response = fetch_signatures_for_address(address, limit=limit)
        else:
            print(f"Called API with signature {before}")
            response = fetch_signatures_for_address(address, limit=limit, before=before)

        result = response.get('result', None)

        if not result:
            print(f"No results returned. Response: {response}")
            time.sleep(3)
            attempts += 1
            if attempts > 3:
                print("Too many failed attempts. Breaking...")
                break
            continue

        attempts = 0
        batch = result
        keep_fetching = True
        valid_transactions_in_batch = 0

        for tx in batch:
            tx_block_time = tx.get("blockTime")

            if tx["err"] or not tx_block_time:
                continue

            if tx_block_time > end_block_time:
                continue

            if tx_block_time < start_block_time:
                keep_fetching = False
                break

            txs["slot"].append(tx["slot"])
            txs["block_time"].append(tx_block_time)
            txs["tx_signature"].append(tx["signature"])
            valid_transactions_in_batch += 1

        print(f'Added {valid_transactions_in_batch} valid transactions from this batch')

        if batch:
            oldest_time = min([tx["blockTime"] for tx in batch if tx.get("blockTime")])
            oldest_time_ui = datetime.datetime.fromtimestamp(oldest_time).strftime('%Y-%m-%d %H:%M:%S')
            print(f"Oldest transaction in batch: {oldest_time_ui}")

        if not keep_fetching:
            print("Reached desired start block time!")
            break

        if len(batch) < limit:
            print("Fetched all available transactions!")
            break

        before = batch[-1]['signature']
        print(f"Next pagination signature: {before}")
        time.sleep(1)

    print(f"Total valid transactions collected: {len(txs['slot'])}")
    return pd.DataFrame(txs)

def fetch_raw_transaction(signature: str, max_retries: int = 3):
    """Fetch raw transaction with retry logic and error handling"""
    url = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            signature,
            {
                "encoding": "jsonParsed",
                "commitment": "finalized",
                "maxSupportedTransactionVersion": 0
            }
        ]
    }
    headers = {"accept": "application/json", "content-type": "application/json"}

    for attempt in range(max_retries):
        try:
            res = requests.post(url, json=payload, headers=headers)
            res.raise_for_status()  # Raise an exception for bad status codes
            result = res.json()

            # Check if the result contains an error
            if 'error' in result:
                print(f"API error for signature {signature}: {result['error']}")
                return None

            # Check if result is None (transaction not found)
            if not result.get('result'):
                print(f"Transaction not found: {signature}")
                return None

            return result

        except requests.exceptions.RequestException as e:
            print(f"Request failed for signature {signature}, attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                print(f"Failed to fetch transaction {signature} after {max_retries} attempts")
                return None
        except Exception as e:
            print(f"Unexpected error for signature {signature}: {e}")
            return None

def setup_program(idl_file: str, program_pubkey: str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"):
    idl = Idl.from_json(open(idl_file).read())
    client = AsyncClient("https://api.mainnet-beta.solana.com")
    provider = Provider(client, None)
    program = Program(idl, Pubkey.from_string(program_pubkey), provider)
    return program

def decode_instruction(data_b58: str, program):
    try:
        data = b58decode(data_b58)
        ix = program.coder.instruction.parse(data)
        return ix.name
    except Exception as e:
        print(f"Failed to decode instruction: {e}")
        return None

def get_pre_post_balances(tx_res, token_mint_a, token_mint_b, pool_address):
    tx_pre_post_balances = {
        'pre_balance_a': 0.0,
        'pre_balance_b': 0.0,
        'post_balance_a': 0.0,
        'post_balance_b': 0.0,
        'decimals_a': 0,
        'decimals_b': 0
    }

    if not tx_res or 'result' not in tx_res:
        return tx_pre_post_balances

    meta = tx_res.get('result', {}).get('meta', {})
    pre_token_balances = meta.get('preTokenBalances', [])
    post_token_balances = meta.get('postTokenBalances', [])

    for bal in pre_token_balances:
        if bal.get('owner') == pool_address:
            mint = bal.get('mint')
            token = bal.get('uiTokenAmount', {})
            if mint == token_mint_a:
                tx_pre_post_balances['pre_balance_a'] = token.get('uiAmount', 0.0) or 0.0
                tx_pre_post_balances['decimals_a'] = token.get('decimals', 0) or 0
            elif mint == token_mint_b:
                tx_pre_post_balances['pre_balance_b'] = token.get('uiAmount', 0.0) or 0.0
                tx_pre_post_balances['decimals_b'] = token.get('decimals', 0) or 0

    for bal in post_token_balances:
        if bal['owner'] == pool_address:
            mint = bal.get('mint')
            token = bal.get('uiTokenAmount', {})
            if bal['mint'] == token_mint_a:
                tx_pre_post_balances['post_balance_a'] = token.get('uiAmount', 0.0) or 0.0
                tx_pre_post_balances['decimals_a'] = token.get('decimals', 0) or 0
            elif bal['mint'] == token_mint_b:
                tx_pre_post_balances['post_balance_b'] = token.get('uiAmount', 0.0) or 0.0
                tx_pre_post_balances['decimals_b'] = token.get('decimals', 0) or 0

    return tx_pre_post_balances

def get_swap_events_inner(res, pool_address, token_vault_a, token_vault_b, program):
    swap_events = []
    num_swaps = 0

    # Check if response is valid
    if not res or 'result' not in res or not res['result']:
        return swap_events, num_swaps

    # Check if meta and innerInstructions exist
    meta = res['result'].get('meta', {})
    inner_instructions = meta.get('innerInstructions', [])

    if not inner_instructions:
        return swap_events, num_swaps

    for inner in inner_instructions:
        ixs = inner.get("instructions", [])

        for idx, ix in enumerate(ixs):
            program_id = ix.get('programId', None)
            data = ix.get('data', None)
            accounts = ix.get('accounts', None)

            # detect swap
            if program_id == WHIRLPOOL_PROGRAM_ID and accounts and data:
                if pool_address in accounts:
                    ix_call = decode_instruction(data, program)

                    if ix_call and ix_call in SWAP_METHODS:
                        num_swaps += 1

                        swap = {}
                        vaultA_detected = False
                        vaultB_detected = False

                        # if swap detected loop through next inner instructions
                        for next_ix in ixs[idx+1:]:
                            parsed = next_ix.get('parsed', None)

                            if next_ix.get('programId') != "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" or not parsed:
                                continue

                            info = parsed.get('info', None)
                            if not info:
                                continue

                            src, dst = info.get('source'), info.get('destination')

                            if 'tokenAmount' in info:
                                amount = int(info['tokenAmount']['amount'])
                            elif 'amount' in info:
                                amount = int(info['amount'])
                            else:
                                continue

                            if not src or not dst:
                                continue

                            if token_vault_a in [src, dst]:
                                vaultA_detected = True
                                swap['token_amount_a'] = amount
                            elif token_vault_b in [src, dst]:
                                vaultB_detected = True
                                swap['token_amount_b'] = amount

                            if vaultA_detected and vaultB_detected:
                                swap_events.append(swap)
                                break

    return swap_events, num_swaps

def get_swap_events_outer(res, pool_address, token_vault_a, token_vault_b, program):
    swap_events = []
    num_swaps = 0

    # Check if response is valid
    if not res or 'result' not in res or not res['result']:
        return swap_events, num_swaps

    # Check if transaction and message exist
    transaction = res['result'].get('transaction', {})
    message = transaction.get('message', {})
    instructions = message.get('instructions', [])

    if not instructions:
        return swap_events, num_swaps

    meta = res['result'].get('meta', {})
    inner_instructions = meta.get('innerInstructions', [])

    for idx, ix in enumerate(instructions):
        program_id = ix.get('programId', None)
        data = ix.get('data', None)
        accounts = ix.get('accounts', None)

        if program_id == WHIRLPOOL_PROGRAM_ID and accounts and data:
            if pool_address in accounts:
                ix_call = decode_instruction(data, program)

                if ix_call and ix_call in SWAP_METHODS:
                    num_swaps += 1
                    for inner_ix in inner_instructions:
                        if inner_ix.get('index') != idx:
                            continue

                        swap = {}
                        vaultA_detected = False
                        vaultB_detected = False

                        for transfer in inner_ix.get('instructions', []):
                            parsed = transfer.get('parsed', None)

                            if transfer.get('programId') != "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" or not parsed:
                                continue

                            info = parsed.get('info', None)
                            if not info:
                                continue

                            src, dst = info.get('source'), info.get('destination')

                            if 'tokenAmount' in info:
                                amount = int(info['tokenAmount']['amount'])
                            elif 'amount' in info:
                                amount = int(info['amount'])
                            else:
                                continue

                            if not src or not dst:
                                continue

                            if token_vault_a in [src, dst]:
                                vaultA_detected = True
                                swap['token_amount_a'] = amount
                            elif token_vault_b in [src, dst]:
                                vaultB_detected = True
                                swap['token_amount_b'] = amount

                            if vaultA_detected and vaultB_detected:
                                swap_events.append(swap)
                                break

    return swap_events, num_swaps

def create_dataset_for_pool_by_block_time(pool_address: str, token_mint_a: str, token_mint_b: str,
                                         token_vault_a: str, token_vault_b: str, end_block_time: int,
                                         program_idl: str = "whirlpool.json", lookback: int = 14):
    """
    Create dataset for a pool using block_time-based filtering instead of signature-based pagination
    """
    # Fetch transactions within the specified time range
    txs_df = fetch_signature_hist_by_block_time(pool_address, end_block_time, lookback)

    if txs_df.empty:
        print("No transactions found in the specified time range!")
        return txs_df

    # Setup df columns
    txs_df['token_mint_a'] = token_mint_a
    txs_df['token_mint_b'] = token_mint_b
    txs_df['token_vault_a'] = token_vault_a
    txs_df['token_vault_b'] = token_vault_b

    txs_df['num_swaps'] = 0
    txs_df['token_amount_a'] = 0
    txs_df['token_amount_b'] = 0

    txs_df['pre_balance_a'] = 0.0
    txs_df['pre_balance_b'] = 0.0
    txs_df['post_balance_a'] = 0.0
    txs_df['post_balance_b'] = 0.0

    txs_df['decimals_a'] = 0
    txs_df['decimals_b'] = 0

    batch_num = 1

    # Setup program from json to decode transactions
    program = setup_program(program_idl)

    print("Transaction collection complete :)")
    print(f"Total transactions: {txs_df.shape[0]}")

    failed_transactions = 0

    for idx, row in txs_df.iterrows():
        # Fetch transaction with error handling
        res = fetch_raw_transaction(row['tx_signature'])
        print(f"Processing transaction {idx+1}/{len(txs_df)}: {row['tx_signature']}")

        if not res:
            print(f"Skipping failed transaction: {row['tx_signature']}")
            failed_transactions += 1
            continue

        try:
            # Fetch swap amounts
            swap_events_inner, num_swaps_inner = get_swap_events_inner(res, pool_address,
                                                                      row['token_vault_a'],
                                                                      row['token_vault_b'], program)
            swap_events_outer, num_swaps_outer = get_swap_events_outer(res, pool_address,
                                                                      row['token_vault_a'],
                                                                      row['token_vault_b'], program)

            if swap_events_inner:
                token_amount_a = sum([int(swap.get('token_amount_a', 0)) for swap in swap_events_inner])
                token_amount_b = sum([int(swap.get('token_amount_b', 0)) for swap in swap_events_inner])

                txs_df.loc[idx, 'token_amount_a'] += token_amount_a
                txs_df.loc[idx, 'token_amount_b'] += token_amount_b
                txs_df.loc[idx, 'num_swaps'] += num_swaps_inner

            if swap_events_outer:
                token_amount_a = sum([int(swap.get('token_amount_a', 0)) for swap in swap_events_outer])
                token_amount_b = sum([int(swap.get('token_amount_b', 0)) for swap in swap_events_outer])

                txs_df.loc[idx, 'token_amount_a'] += token_amount_a
                txs_df.loc[idx, 'token_amount_b'] += token_amount_b
                txs_df.loc[idx, 'num_swaps'] += num_swaps_outer

            # Get pre and post token balances
            pre_post_balances = get_pre_post_balances(res, row['token_mint_a'], row['token_mint_b'], pool_address)
            if pre_post_balances:
                txs_df.loc[idx, 'pre_balance_a'] = float(pre_post_balances['pre_balance_a'])
                txs_df.loc[idx, 'pre_balance_b'] = float(pre_post_balances['pre_balance_b'])
                txs_df.loc[idx, 'post_balance_a'] = float(pre_post_balances['post_balance_a'])
                txs_df.loc[idx, 'post_balance_b'] = float(pre_post_balances['post_balance_b'])
                txs_df.loc[idx, 'decimals_a'] = int(pre_post_balances['decimals_a'])
                txs_df.loc[idx, 'decimals_b'] = int(pre_post_balances['decimals_b'])

        except Exception as e:
            print(f"Error processing transaction {row['tx_signature']}: {e}")
            failed_transactions += 1
            continue

        time.sleep(0.2)

        if (idx + 1) % 5000 == 0:
            path = f"pool_data_batch_{batch_num}.csv"
            txs_df.iloc[:idx+1].to_csv(path, index=False)
            print(f"✅ Saved progress to {path}")
            batch_num += 1

    print("Transaction processing complete :)")
    print(f"Failed transactions: {failed_transactions}")

    # Save final dataset
    final_path = f"pool_data_final.csv"
    txs_df.to_csv(final_path, index=False)
    print(f"✅ Final dataset saved to {final_path}")

    return txs_df