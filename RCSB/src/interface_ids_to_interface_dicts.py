import pandas as pd
import requests
import asyncio
import aiohttp
import time
from tqdm.asyncio import tqdm
import pickle
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-o', '--output_dir', help="The basename of the directory to store output and intermediates", required=True)
args = parser.parse_args()

with open(f"../data/{args.output_dir}/entry_to_assembly_ids_to_interface_ids_dict.pkl", "rb") as fp:   #unpickling
    entry_to_assembly_ids_to_interface_ids_dict = pickle.load(fp)

missed_pdbs = open(f"../data/{args.output_dir}/missed_pdbs.txt", "a") 

limit = asyncio.Semaphore(2)

###for each interface of each assembly of each pdb entry, add it to the entry's list of dicts
###partner asym ID, partner entity ID, composition, chain

async def get_interface_dict(entry_id, assembly_id, interface_id, session):
    interface_url = f"https://data.rcsb.org/rest/v1/core/interface/{entry_id}/{assembly_id}/{interface_id}"
    try:
        async with limit, session.request('GET', url=interface_url) as response:
            response.raise_for_status()
            resp = await response.json()

            interface_area = resp['rcsb_interface_info']['interface_area']
            interface_composition = resp['rcsb_interface_info']['polymer_composition']
            if (interface_composition != 'Protein (only)'):
                ###only want protein-protein interfaces, don't waste compute time on anything else
                raise Exception("not protein-protein")
                
            interface_character = resp['rcsb_interface_info']['interface_character']
            interface_partner_asym_ids = (resp['rcsb_interface_partner'][0]['interface_partner_identifier']['asym_id'], \
                                          resp['rcsb_interface_partner'][1]['interface_partner_identifier']['asym_id'])
            interface_partner_entity_ids = (resp['rcsb_interface_partner'][0]['interface_partner_identifier']['entity_id'], \
                                          resp['rcsb_interface_partner'][1]['interface_partner_identifier']['entity_id'])
            interface_dict = {'entry_id':entry_id, 'assembly_id':assembly_id, \
                                              'interface_id':interface_id, \
                                              'chain_1':interface_partner_asym_ids[0],\
                                              'chain_2':interface_partner_asym_ids[1],\
                                              'composition':interface_composition, 'area':interface_area,\
                                              'character':interface_character, \
                                              'entity_id_1':interface_partner_entity_ids[0], \
                                              'entity_id_2':interface_partner_entity_ids[1]
                             }
            return interface_dict
    
    except Exception as e:
        tqdm.write(f"Error {e} on interface {interface_id} of assembly {assembly_id} of PDB {entry_id}.")
        if ("429" in str(e)) or ("rcsb" in str(e)):
            missed_pdbs.write(f"{entry_id}\n")

        


async def main(entry_to_assembly_ids_to_interface_ids_dict): 
    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.create_task(get_interface_dict(entry_id, assembly_id, interface_id, session)) \
                 for entry_id in entry_to_assembly_ids_to_interface_ids_dict.keys() \
                 for assembly_id in entry_to_assembly_ids_to_interface_ids_dict[entry_id].keys() \
                 for interface_id in entry_to_assembly_ids_to_interface_ids_dict[entry_id][assembly_id]]

        #tasks = []
        #counter=0
        #for entry_id in entry_to_assembly_ids_to_interface_ids_dict.keys():
        #    for assembly_id in entry_to_assembly_ids_to_interface_ids_dict[entry_id].keys():
        #        for interface_id in entry_to_assembly_ids_to_interface_ids_dict[entry_id][assembly_id]:
        #            counter += 1
        #            tasks.append(asyncio.create_task(get_interface_dict(entry_id, assembly_id, interface_id, session)))
        #            if counter==1000:
        #                counter=0
        #                await asyncio.sleep(10)


        interface_dict_list = [await f for f in tqdm(asyncio.as_completed(tasks), total=len(tasks))] 
        
    return interface_dict_list

interface_dict_list = asyncio.run(main(entry_to_assembly_ids_to_interface_ids_dict))

with open(f'../data/{args.output_dir}/interface_dict_list.pkl', "wb") as fp:
    pickle.dump(interface_dict_list, fp)
