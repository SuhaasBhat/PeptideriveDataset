import pandas as pd
import requests
import asyncio
import aiohttp
import time
from tqdm.asyncio import tqdm
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-o', '--output_dir', help="The directory to store output and intermediates", required=True)
args = parser.parse_args()

limit = asyncio.Semaphore(2)
missed_pdbs = open(f"../data/{args.output_dir}/missed_pdbs.txt", "a+") 

async def get_entity_info(entry_id, entity_id, session):
    ###sequence, polymer type, type
    polymer_entity_url = f'https://data.rcsb.org/rest/v1/core/polymer_entity/{entry_id}/{entity_id}'
    try:
        async with limit, session.request('GET', url=polymer_entity_url) as response:
            response.raise_for_status()
            resp = await response.json()
            sequence = resp['entity_poly']['pdbx_seq_one_letter_code_can'] ##this avoids the parentheses headache
            polymer_type = resp['entity_poly']['rcsb_entity_polymer_type']
            type = resp['entity_poly']['type']
            return sequence, polymer_type, type

    except Exception as e:
        tqdm.write(f"Error {e} on entity {entity_id} of PDB {entry_id}.")
        if ("429" in str(e)) or ("rcsb" in str(e)):
            missed_pdbs.write(f"{entry_id}\n")
        return None


async def get_auth_id(entry_id, asym_id, session):
    chain_url = f"https://data.rcsb.org/rest/v1/core/polymer_entity_instance/{entry_id}/{asym_id}"
    try:
        async with limit, session.request('GET', url=chain_url) as response:
            response.raise_for_status()
            resp = await response.json()
            auth_chain_id = resp['rcsb_polymer_entity_instance_container_identifiers']['auth_asym_id']
            return auth_chain_id
    
    except Exception as e:
        tqdm.write(f"Error {e} on chain {asym_id} of PDB {entry_id}")
        if ("429" in str(e)) or ("rcsb" in str(e)):
            missed_pdbs.write(f"{entry_id}\n")
        return None


async def get_resolution(entry_id, session):
    entry_url = f"https://data.rcsb.org/rest/v1/core/entry/{entry_id}"
    try:
        async with limit, session.request('GET', url=entry_url) as response:
            response.raise_for_status()
            resp = await response.json()
            resolution = resp['pdbx_vrpt_summary']['pdbresolution']
            return resolution

    except Exception as e:
        tqdm.write(f"Error {e} on PDB {entry_id}")
        if ("429" in str(e)) or ("rcsb" in str(e)):
            missed_pdbs.write(f"{entry_id}\n")
        return None

async def get_row_data(idx, row, session):
    auth_chain_id_1 = await get_auth_id(row['entry_id'], row['chain_1'], session)
    auth_chain_id_2 = await get_auth_id(row['entry_id'], row['chain_2'], session)
    seq_1, poly_type_1, type_1 = await get_entity_info(row['entry_id'], row['entity_id_1'], session)
    seq_2, poly_type_2, type_2 = await get_entity_info(row['entry_id'], row['entity_id_2'], session)
    resolution = await get_resolution(row['entry_id'], session)
    return (idx, auth_chain_id_1, auth_chain_id_2, seq_1, seq_2, poly_type_1, poly_type_2, type_1, type_2, resolution)


async def main(interim_df): 
    async with aiohttp.ClientSession() as session:        
        tasks = [asyncio.create_task(get_row_data(idx, row, session)) for idx, row in interim_df.iterrows()]
        outputs = [await f for f in tqdm(asyncio.as_completed(tasks), total=len(tasks))] 
    return outputs

interim_df = pd.read_csv(f'../data/{args.output_dir}/RCSB_DATASET_interim.csv') ##depending on memory limitations, could do list of dicts
print(interim_df.head())

outputs = asyncio.run(main(interim_df))

##there's probably a better way to do this
for output_tuple in tqdm(outputs):
    interim_df.loc[output_tuple[0], ['auth_id_1', 'auth_id_2', 'seq_1', 'seq_2','poly_type_1', 'poly_type_2', 'entity_type_1', 'entity_type_2', 'resolution']] = output_tuple[1:]
    #print(output_tuple)

interim_df.to_csv(f'../data/{args.output_dir}/RCSB_DATASET_final.csv')
