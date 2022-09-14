import pandas as pd
import requests
import asyncio
import aiohttp
from tqdm.asyncio import tqdm
import pickle
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-o', '--output_dir', help="The directory to store output and intermediates", required=True)
args = parser.parse_args()

with open(f"../data/{args.output_dir}/entry_to_assembly_ids_dict.pkl", "rb") as fp:   #unpickling
    entry_to_assembly_ids_dict = pickle.load(fp)

missed_pdbs = open(f"../data/{args.output_dir}/numchains_missed_pdbs.txt", "a") 

limit = asyncio.Semaphore(2)

async def get_num_chains(entry_id, assembly_id, session):
    assembly_url = f"https://data.rcsb.org/rest/v1/core/assembly/{entry_id}/{assembly_id}"
    try:
        async with limit, session.request('GET', url=assembly_url) as response:
            response.raise_for_status()
            resp = await response.json()
            num_chains = resp['rcsb_assembly_info']['polymer_entity_instance_count_protein']
            return (entry_id, assembly_id, num_chains)
        
    except Exception as e:
        tqdm.write(f"Error {e} on assembly {assembly_id} of PDB {entry_id}.")
        if ("429" in str(e)) or ("rcsb" in str(e)):
            missed_pdbs.write(f"{entry_id}\n")


async def main(entry_to_assembly_ids_dict): 
    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.create_task(get_num_chains(entry_id, assembly_id, session)) \
                 for entry_id in entry_to_assembly_ids_dict.keys() \
                 for assembly_id in entry_to_assembly_ids_dict[entry_id].keys()]

        entry_assembly_chains_tuples_list = [await f for f in tqdm(asyncio.as_completed(tasks), total=len(tasks))] 
        
    return entry_assembly_chains_tuples_list

entry_assembly_chains_tuples_list = asyncio.run(main(entry_to_assembly_ids_dict))


ea_to_chains_dict = {}
for eac_tuple in entry_assembly_chains_tuples_list:
    if eac_tuple is not None:
        key = str(eac_tuple[0]) + '_' + str(eac_tuple[1])
        ea_to_chains_dict.update({key:eac_tuple[2]})
    


with open(f"../data/{args.output_dir}/ea_to_chains_dict.pkl", "wb") as fp:   #Pickling
    pickle.dump(ea_to_chains_dict, fp)
