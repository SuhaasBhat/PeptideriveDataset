###this step might need a lot of ram
import pandas as pd
import requests
from tqdm import tqdm
import pickle
import resource
import argparse
import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument('-o', '--output_dir', help="The directory to store output and intermediates", required=True)
parser.add_argument('--binding_area_cluster_radius', type=int, help="The radius (in Angstroms squared) that we cluster interface surface area by. For proteins, default 100.", default=100)
args = parser.parse_args()

def lexsort_df(interfaces_df): ##doesn't seem any faster :(
    ##sorts ascending by assembly id, descending by area
    col_name_dict = {col_name:interfaces_df.columns.get_loc(col_name) for col_name in interfaces_df.columns}
    ind_col_dict = {v:k for k, v in col_name_dict.items()}
    arr = interfaces_df.apply(pd.to_numeric, errors='ignore').to_numpy() 
    interfaces_df =  pd.DataFrame(arr[np.lexsort((-arr[:, col_name_dict["area"]], arr[:, col_name_dict["assembly_id"]]))])
    interfaces_df = interfaces_df.rename(columns = ind_col_dict)
    return interfaces_df


def filter_df_by_area_clustering(interfaces_dict_list_for_entry):  
    cluster_centers = []
    interfaces_df = pd.DataFrame(interfaces_dict_list_for_entry) ##setting the variable in the function frees up ram

    ##should be able to cluster on area separately, and then just filter such that we have entity ids for each cluster
    for index, row in interfaces_df.iterrows():
        area = int(row['area'])
        is_in_clusters_list = [((area <= center+args.binding_area_cluster_radius) and (area >= center-args.binding_area_cluster_radius)) for center in cluster_centers]

        if (any(is_in_clusters_list)):
            cluster = is_in_clusters_list.index(True) + 1
            interfaces_df.loc[index, 'cluster'] = cluster
        else:
            cluster_centers.append(area)
            interfaces_df.loc[index, 'cluster'] = cluster_centers.index(area) + 1

    ##ensure that the entity ids are in ascending order       
    ## YOU CAN'T DO THIS, IT FUCKS UP THE CHAIN IDS
    ##YOU NEED TO SWITCH THE CHAIN IDS AS WELL
    #for idx, row in interfaces_df.iterrows(): 
    #    if int(row['entity_id_1']) > int(row['entity_id_2']):
    #        high_val = row.at['entity_id_1']
    #        high_chain = row.at['chain_id_1']
    #        interfaces_df.loc[idx, 'entity_id_1'] = row.at['entity_id_2']
    #        interfaces_df.loc[idx, 'entity_id_2'] = high_val
    ###these two rows will fix the mismatch error
    #        interfaces_df.loc[idx, 'chain_id_1'] = row.at['chain_id_1']
    #        interfaces_df.loc[idx, 'chain_id_2'] = high_chain
                
            
    #now, sort by assembly id & area, so that we prioritize getting entries from the same assembly, but get the highest area therein.
    #interfaces_df = interfaces_df.sort_values(['assembly_id', 'area'], ascending=[True, False], inplace=False) #get highest area of each interface class, per assembly
    ##sorting with numpy instead

    interfaces_df = lexsort_df(interfaces_df)
    interfaces_df = interfaces_df.drop_duplicates(['entry_id', 'entity_id_1', 'entity_id_2', 'cluster'], inplace=False)

    #tqdm.write(f"Max usage in the function is {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}")

    return interfaces_df.to_dict('records') ##returns list of dicts, for each row



with open(f'../data/{args.output_dir}/interface_dict_list.pkl', "rb") as fp:
    interface_dict_list = pickle.load(fp)

with open("../data/input/every_pdb.pkl", "rb") as fp:   
    every_pdb = pickle.load(fp) ##datatype list

filtered_list_of_interface_dicts = []
interface_dict_list = list(filter(None, interface_dict_list))

###for each of entry set of the list of dicts, we need to sequentially transform into dataframes, and filter
for pdb_id in tqdm(every_pdb):
    #get list of interfaces for one entry
    interfaces_dict_list_for_entry = [interface_dict for interface_dict in interface_dict_list if interface_dict['entry_id']==pdb_id] 
    if interfaces_dict_list_for_entry != []:
        filtered_list_of_interface_dicts.extend(filter_df_by_area_clustering(interfaces_dict_list_for_entry))


filtered_interfaces_df = pd.DataFrame(filtered_list_of_interface_dicts)  ##this could still be too big
filtered_interfaces_df.sort_values('entry_id', inplace=True) ##this could also use even more memory

tqdm.write(f"Max memory usage in the program is {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}")


filtered_interfaces_df.to_csv(f"../data/{args.output_dir}/RCSB_DATASET_interim.csv")
tqdm.write("Completed transformation.")

###Now, want to pull sequences, auth identities, and possibly resolution.
