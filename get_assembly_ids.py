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


with open("../data/input/every_pdb.pkl", "rb") as fp:   # Unpickling
    every_pdb = pickle.load(fp)

missed_pdbs = open(f"../data/{args.output_dir}/missed_pdbs.txt", "a") 

###get every set of assembly ids for each pdb entry
###dict from entry to dict {assembly ids:[]} ###could do ndarray to be more clever, but less readable
limit = asyncio.Semaphore(2)

async def get_entry_assembly_ids(entry_id, session):
    entry_url = f"https://data.rcsb.org/rest/v1/core/entry/{entry_id}"
    #await asyncio.sleep(0.01) #nonblocking - does it do anything?
    #time.sleep(0.01) #blocking - seems to completely break everything. just sequentially waiting for 0.01 seconds for each
    try:
        async with limit, session.request('GET', url=entry_url) as response:
            response.raise_for_status()
            resp = await response.json()
            assembly_ids = resp['rcsb_entry_container_identifiers']['assembly_ids']
            assembly_id_dict = {a_id:[] for a_id in assembly_ids}
            return (entry_id,assembly_id_dict)
        
    except Exception as e:
        tqdm.write(f"Error {e} on PDB {entry_id}.")
        if ("429" in str(e)) or ("rcsb" in str(e)):
            missed_pdbs.write(f"{entry_id}\n")


async def main(pdb_id_list):
    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.create_task(get_entry_assembly_ids(pdb_id, session)) for pdb_id in pdb_id_list] #comprehension, can't add sleep though.
        #tasks = []
        #for idx, pdb_id in enumerate(tqdm(pdb_id_list)):
        #    tasks.append(asyncio.create_task(get_entry_assembly_ids(pdb_id, session)))
            #if (idx % 999) == 0:
                #if idx != 0:
                    #await asyncio.sleep(10) #nonblocking - does it do anything?

        entry_assembly_tuples_list = [await f for f in tqdm(asyncio.as_completed(tasks), total=len(tasks))] 
    entry_assembly_tuples_list = list(filter(None, entry_assembly_tuples_list))
    return {k:v for (k,v) in entry_assembly_tuples_list}

entry_to_assembly_ids_dict = asyncio.run(main(every_pdb))

with open(f"../data/{args.output_dir}/entry_to_assembly_ids_dict.pkl", "wb") as fp:   #Pickling
    pickle.dump(entry_to_assembly_ids_dict, fp)
    print("successfully dumped")

