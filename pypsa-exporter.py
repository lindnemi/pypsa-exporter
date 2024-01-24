# -*- coding: utf-8 -*-
"""
Script to convert networks from PyPSA-Eur to data format used in the
Ariadne database

Thanks to @martavp. Adapted from https://github.com/martavp/pypsa-eur-sec-to-ipcc/tree/main
"""
#%%
import pypsa
import pandas as pd
from itertools import product
from functools import reduce
import os
import snakemake as sm
from pathlib import Path
#%%

project_dir = "/home/micha/git/pypsa-ariadne/"
snakefile = project_dir + "/workflow/Snakefile"

os.chdir(project_dir)

workflow = sm.Workflow(snakefile, overwrite_configfiles=[], rerun_triggers=[])
#%%

# Raises an exception but sucessfully reads the config
try:
    workflow.include(snakefile)
except e:
    print(e)
finally:
    assert workflow.config != {}
    print("Caught error, config read successfully.")
    config=workflow.config

#%%
    
# official template
template_path = Path(__file__).parent /  "2023-03-16_template_Ariadne.xlsx" 

# Metadata
model = "PyPSA-Ariadne v" + config['version']

keys, values = zip(*config['scenario'].items())
permutations_dicts = [dict(zip(keys, v)) for v in product(*values)]
scenarios = []
for scenario_i in permutations_dicts:
    scenarios.append("elec_s{simpl}_{clusters}_l{ll}_{opts}_{sector_opts}_".format(**scenario_i))
scenario = scenarios[0]

#%%

output_folder = project_dir +'results/'

years = config['scenario']['planning_horizons']

# %%
# A mapping of variable names to the corresponding units, extracted from the template
var2unit = pd.read_excel(
    template_path, 
    sheet_name="variable_definitions",
    index_col="Variable",
)["Unit"]
 
#%%
def get_cap(df, label, region):
    MW2GW = 0.001

    # CAREFUL! FOR CHPs this return electrical capacity
    if "CHP" in "label":
        print("Warning: Returning electrical capacity of the CHP, not thermal.")
    if df.index.name == "Link":
        return (( 
            MW2GW * 
            df.efficiency.filter(like=label).filter(like=region) *
            df.p_nom_opt.filter(like=label).filter(like=region)
        ).sum())
    if df.index.name == "Generator":
        return MW2GW * df.p_nom_opt.filter(like=label).filter(like=region).sum() 
    if df.index.name == "StorageUnit":
        return MW2GW * df.p_nom_opt.filter(like=label).filter(like=region).sum()
    else:
        raise Exception("Received unexpected DataFrame.")
    


def get_ariadne_var(n, region):


    var = {}

    # var["Capacity|Electricity|Biomass|Gases and Liquids"] = 

    var["Capacity|Electricity|Biomass|Solids"] = \
        get_cap(n.links, "solid biomass CHP")
    var["Capacity|Electricity|Biomass|w/ CCS"] = \
        get_cap(n.links, "solid biomass CHP CC") 
    
    # var["Capacity|Electricity|Biomass|w/o CCS"] = 

    var["Capacity|Electricity|Biomass"] = \
        var["Capacity|Electricity|Biomass|Solids"]

    var["Capacity|Electricity|Coal|Hard Coal"] = \
        get_cap(n.links, "coal", region)                                                  


    var["Capacity|Electricity|Coal|Lignite"] = \
        get_cap(n.links, "lignite", region)
    
    # var["Capacity|Electricity|Coal|Hard Coal|w/ CCS"] = 
    # var["Capacity|Electricity|Coal|Hard Coal|w/o CCS"] = 
    # var["Capacity|Electricity|Coal|Lignite|w/ CCS"] = 
    # var["Capacity|Electricity|Coal|Lignite|w/o CCS"] = 
    # var["Capacity|Electricity|Coal|w/ CCS"] = 
    # var["Capacity|Electricity|Coal|w/o CCS"] = 

    var["Capacity|Electricity|Coal"] = \
        var["Capacity|Electricity|Coal|Lignite"] + \
        var["Capacity|Electricity|Coal|Hard Coal"]

    
    var["Capacity|Electricity|Gas|CC"] = \
        get_cap(n.links, "CCGT", region)
    
    var["Capacity|Electricity|Gas|OC"] = \
        get_cap(n.links, "OCGT", region)
    
    var["Capacity|Electricity|Gas|w/ CCS"] =  \
        get_cap(n.links, "gas CHP CC", region)  
    
    # var["Capacity|Electricity|Gas|CC|w/ CCS"] =
    # var["Capacity|Electricity|Gas|CC|w/o CCS"] =      
    # var["Capacity|Electricity|Gas|w/o CCS"] = 
    # Q: Are all OC and CC plants without CCS?

    var["Capacity|Electricity|Gas"] = \
        var["Capacity|Electricity|Gas|OC"] + \
        var["Capacity|Electricity|Gas|CC"] + \
        get_cap(n.links, "gas CHP", region)

    # var["Capacity|Electricity|Geothermal"] = 
    # ! Not implemented

    var["Capacity|Electricity|Hydro"] = \
        get_cap(n.generators, "ror", region) \
        + get_cap(n.storage_units, 'hydro', region)


    # var["Capacity|Electricity|Hydrogen|CC"] = 
    # var["Capacity|Electricity|Hydrogen|OC"] = 
    # Q: What about retrofitted gas power plants?
    # Q: Are all vars in the Network object, regardless of the params?

    var["Capacity|Electricity|Hydrogen|FC"] = \
        get_cap(n.generators, "H2 Fuel Cell", region)

    var["Capacity|Electricity|Hydrogen"] = \
        var["Capacity|Electricity|Hydrogen|FC"]

    # var["Capacity|Electricity|Non-Renewable Waste"] = 

    # var["Capacity|Electricity|Nuclear"] = 
    # Q: why is there nuclear AND uranium capacity? 
    # Q: why are there nuclear generators AND links?

    # var["Capacity|Electricity|Ocean"] = 

    # var["Capacity|Electricity|Oil|w/ CCS"] = 
    # var["Capacity|Electricity|Oil|w/o CCS"] = 
    var["Capacity|Electricity|Oil"] = \
        get_cap(n.links, "oil", region)

    # var["Capacity|Electricity|Other"] = 

    # var["Capacity|Electricity|Peak Demand"] = 

    var["Capacity|Heat|Solar thermal"] = \
        get_cap(n.generators, "solar thermal", region)
    var["Capacity|Electricity|Solar|PV|Rooftop"] = \
        get_cap(n.generators, "solar rooftop", region)
    var["Capacity|Electricity|Solar|PV|Open Field"] = \
        get_cap(n.generators, "solar", region) \
        - var["Capacity|Electricity|Solar|PV|Rooftop"] \
        - var["Capacity|Heat|Solar thermal"]
    var["Capacity|Electricity|Solar|PV"] = \
        var["Capacity|Electricity|Solar|PV|Open Field"] \
        + var["Capacity|Electricity|Solar|PV|Rooftop"]
    
    # var["Capacity|Electricity|Solar|CSP"] = 


    var["Capacity|Electricity|Solar"] = var["Capacity|Electricity|Solar|PV"]
    
    # var["Capacity|Electricity|Storage Converter"] = 
    # var["Capacity|Electricity|Storage Converter|CAES"] = 
    # var["Capacity|Electricity|Storage Converter|Hydro Dam Reservoir"] = 
    # var["Capacity|Electricity|Storage Converter|Pump Hydro"] = 
    # var["Capacity|Electricity|Storage Converter|Stationary Batteries"] = 
    # var["Capacity|Electricity|Storage Converter|Vehicles"] = 
    # var["Capacity|Electricity|Storage Reservoir"] = 
    # var["Capacity|Electricity|Storage Reservoir|CAES"] = 
    # var["Capacity|Electricity|Storage Reservoir|Hydro Dam Reservoir"] = 
    # var["Capacity|Electricity|Storage Reservoir|Pump Hydro"] = 
    # var["Capacity|Electricity|Storage Reservoir|Stationary Batteries"] = 
    # var["Capacity|Electricity|Storage Reservoir|Vehicles"] = 
    # var["Capacity|Electricity|Transmissions Grid"] =

    # var["Capacity|Electricity"] =   

    var["Capacity|Electricity|Wind|Offshore"] = \
        get_cap(n.generators, "offwind", region)
    var["Capacity|Electricity|Wind|Onshore"] = \
        get_cap(n.generators, "onwind", region) 
    var["Capacity|Electricity|Wind"] = \
        var["Capacity|Electricity|Wind|Offshore"] + \
        var["Capacity|Electricity|Wind|Onshore"]
    
    return var


# uses the global variables model, scenario and var2unit. For now.
def get_data(year):
    n = pypsa.Network(f"results/{config["run"]["name"]}/postnetworks/{scenario}{year}.nc")
    
    var = get_ariadne_var(n, "DE")

    data = []
    for v in var:
        data.append([
            model, 
            scenario,
            "DE",
            v,
            var2unit[v],
            var[v],
        ])

    tab = pd.DataFrame(
        data, 
        columns=["Model", "Scenario", "Region", "Variable", "Unit", year]
    )

    return tab

# %%


yearly_dfs = map(get_data, years)

df = reduce(
    lambda left, right: pd.merge(
        left, 
        right, 
        on=["Model", "Scenario", "Region", "Variable", "Unit"]), 
    yearly_dfs
)


df.to_excel(
    "/home/micha/git/pypsa-exporter/pypsa_output.xlsx",
    index=False
)

# %%
# costs = pd.read_csv(
#     f"results/{config["run"]["name"]}/csvs/costs.csv",
#     index_col=[0,1,2], 
#     names=["variable", "capital", "type", *years],
# )
# "2005", "2010", "2015", "2020", "2025", "2030", "2035", 
# "2040", "2045", "2050", "2060", "2070", "2080", "2090", "2100"])
n = pypsa.Network(f"results/{config["run"]["name"]}/postnetworks/{scenario}{2040}.nc")

# %%
