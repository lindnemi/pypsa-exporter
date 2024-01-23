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
vars2unit = pd.read_excel(
    template_path, 
    sheet_name="variable_definitions",
    index_col="Variable",
)["Unit"]
 
#%%

def get_ariadne_vars(n, region):
    cap = n.statistics.supply(comps=["Generator"])

    MW2GW = 0.001
    vars = {}

    # vars["Capacity|Electricity"] = 
    # vars["Capacity|Electricity|Biomass"] = 
    # vars["Capacity|Electricity|Biomass|Gases and Liquids"] = 
    # vars["Capacity|Electricity|Biomass|Solids"] = 
    # vars["Capacity|Electricity|Biomass|w/ CCS"] = 
    # vars["Capacity|Electricity|Biomass|w/o CCS"] = 
    # vars["Capacity|Electricity|Coal"] = 
    # vars["Capacity|Electricity|Coal|Hard Coal"] = 
    # vars["Capacity|Electricity|Coal|Hard Coal|w/ CCS"] = 
    # vars["Capacity|Electricity|Coal|Hard Coal|w/o CCS"] = 
    # vars["Capacity|Electricity|Coal|Lignite"] = 
    # vars["Capacity|Electricity|Coal|Lignite|w/ CCS"] = 
    # vars["Capacity|Electricity|Coal|Lignite|w/o CCS"] = 
    # vars["Capacity|Electricity|Coal|w/ CCS"] = 
    # vars["Capacity|Electricity|Coal|w/o CCS"] = 
    # vars["Capacity|Electricity|Gas"] = 
    # vars["Capacity|Electricity|Gas|CC"] = 
    # vars["Capacity|Electricity|Gas|CC|w/ CCS"] = 
    # vars["Capacity|Electricity|Gas|CC|w/o CCS"] = 
    # vars["Capacity|Electricity|Gas|OC"] = 
    # vars["Capacity|Electricity|Gas|w/ CCS"] = 
    # vars["Capacity|Electricity|Gas|w/o CCS"] = 
    # vars["Capacity|Electricity|Geothermal"] = 
    # vars["Capacity|Electricity|Hydro"] = 
    # vars["Capacity|Electricity|Hydrogen"] = 
    # vars["Capacity|Electricity|Hydrogen|CC"] = 
    # vars["Capacity|Electricity|Hydrogen|FC"] = 
    # vars["Capacity|Electricity|Hydrogen|OC"] = 
    # vars["Capacity|Electricity|Non-Renewable Waste"] = 
    # vars["Capacity|Electricity|Nuclear"] = 
    # vars["Capacity|Electricity|Ocean"] = 
    # vars["Capacity|Electricity|Oil"] = 
    # vars["Capacity|Electricity|Oil|w/ CCS"] = 
    # vars["Capacity|Electricity|Oil|w/o CCS"] = 
    # vars["Capacity|Electricity|Other"] = 
    # vars["Capacity|Electricity|Peak Demand"] = 
    vars["Capacity|Heat|Solar thermal"] = \
         MW2GW * n.generators.p_nom_opt \
        .filter(like='solar thermal').filter(like=region).sum()
    vars["Capacity|Electricity|Solar|PV|Rooftop"] = \
        MW2GW * n.generators.p_nom_opt \
        .filter(like='solar rooftop').filter(like=region).sum()
    vars["Capacity|Electricity|Solar|PV|Open Field"] = \
        MW2GW * n.generators.p_nom_opt \
        .filter(like='solar').filter(like=region).sum() \
        - vars["Capacity|Electricity|Solar|PV|Rooftop"] \
        - vars["Capacity|Heat|Solar thermal"]
    vars["Capacity|Electricity|Solar|PV"] = \
        vars["Capacity|Electricity|Solar|PV|Open Field"] \
        + vars["Capacity|Electricity|Solar|PV|Rooftop"]
    # vars["Capacity|Electricity|Solar|CSP"] = 
    vars["Capacity|Electricity|Solar"] = vars["Capacity|Electricity|Solar|PV"]
    
    # vars["Capacity|Electricity|Storage Converter"] = 
    # vars["Capacity|Electricity|Storage Converter|CAES"] = 
    # vars["Capacity|Electricity|Storage Converter|Hydro Dam Reservoir"] = 
    # vars["Capacity|Electricity|Storage Converter|Pump Hydro"] = 
    # vars["Capacity|Electricity|Storage Converter|Stationary Batteries"] = 
    # vars["Capacity|Electricity|Storage Converter|Vehicles"] = 
    # vars["Capacity|Electricity|Storage Reservoir"] = 
    # vars["Capacity|Electricity|Storage Reservoir|CAES"] = 
    # vars["Capacity|Electricity|Storage Reservoir|Hydro Dam Reservoir"] = 
    # vars["Capacity|Electricity|Storage Reservoir|Pump Hydro"] = 
    # vars["Capacity|Electricity|Storage Reservoir|Stationary Batteries"] = 
    # vars["Capacity|Electricity|Storage Reservoir|Vehicles"] = 
    # vars["Capacity|Electricity|Transmissions Grid"] =
     
    vars["Capacity|Electricity|Wind|Offshore"] = \
        MW2GW * n.generators.p_nom_opt \
        .filter(like='offwind').filter(like=region).sum() 
    vars["Capacity|Electricity|Wind|Onshore"] = \
        MW2GW * n.generators.p_nom_opt \
        .filter(like='onwind').filter(like=region).sum()   
    vars["Capacity|Electricity|Wind"] = \
        vars["Capacity|Electricity|Wind|Offshore"] + \
        vars["Capacity|Electricity|Wind|Onshore"]
    
    return vars


# uses the global variables model, scenario and vars2unit. For now.
def get_data(year):
    n = pypsa.Network(f"results/{config["run"]["name"]}/postnetworks/{scenario}{year}.nc")
    
    vars = get_ariadne_vars(n, "DE")

    data = []
    for var in vars:
        data.append([
            model, 
            scenario,
            "DE",
            var,
            vars2unit[var],
            vars[var],
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
