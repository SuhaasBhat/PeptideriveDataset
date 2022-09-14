import pandas as pd
import requests
import asyncio
import aiohttp
import time
from tqdm.asyncio import tqdm
import pickle
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-o', '--output_dir', help="The directory to store output and intermediates", required=True)
args = parser.parse_args()

with open(f"../data/{args.output_dir}/entry_to_assembly_ids_dict.pkl", "rb") as fp:   #unpickling
    entry_to_assembly_ids_dict = pickle.load(fp)

missed_pdbs = open(f"../data/{args.output_dir}/missed_pdbs.txt", "a") 

limit = asyncio.Semaphore(2)

async def populate_assembly_interface_ids(entry_id, assembly_id, session):
    assembly_url = f"https://data.rcsb.org/rest/v1/core/assembly/{entry_id}/{assembly_id}"
    try:
        async with limit, session.request('GET', url=assembly_url) as response:
            response.raise_for_status()
            resp = await response.json()
            interface_ids = resp['rcsb_assembly_container_identifiers']['interface_ids']
            tqdm.write(f"Found an interface for pdb {entry_id}")
            return (entry_id, assembly_id, interface_ids)
        
    except Exception as e:
        tqdm.write(f"Error {e} on assembly {assembly_id} of PDB {entry_id}.")
        if ("429" in str(e)) or ("rcsb" in str(e)):
            missed_pdbs.write(f"{entry_id}\n")


async def main(entry_to_assembly_ids_dict): 
    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.create_task(populate_assembly_interface_ids(entry_id, assembly_id, session)) \
                 for entry_id in entry_to_assembly_ids_dict.keys() \
                 for assembly_id in entry_to_assembly_ids_dict[entry_id].keys()]
        #tasks = []
        #counter = 0
        #for entry_id in entry_to_assembly_ids_dict.keys():
        #    for assembly_id in tqdm(entry_to_assembly_ids_dict[entry_id].keys()):
        #        counter += 1
        #        tasks.append(asyncio.create_task(populate_assembly_interface_ids(entry_id, assembly_id, session)))
        #        if counter==999:
        #            counter=0
        #            await asyncio.sleep(10)

        entry_assembly_interface_tuples_list = [await f for f in tqdm(asyncio.as_completed(tasks), total=len(tasks))] 
        
    return entry_assembly_interface_tuples_list
        

eai_tuples_list = asyncio.run(main(entry_to_assembly_ids_dict))

for eai_tuple in eai_tuples_list: 
    if eai_tuple is not None:
        entry_to_assembly_ids_dict[eai_tuple[0]][eai_tuple[1]].extend(eai_tuple[2])

with open(f"../data/{args.output_dir}/entry_to_assembly_ids_to_interface_ids_dict.pkl", "wb") as fp:   #Pickling
    pickle.dump(entry_to_assembly_ids_dict, fp)


