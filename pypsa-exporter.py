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
from pathlib import Path
from _utils import *
from _getters import *
import yaml


# Defining global varibales

TWh2PJ = 3.6
MWh2TJ = 3.6e-3 
MW2GW = 1e-3
t2Mt = 1e-6

#%%

project_dir = "/home/micha/git/pypsa-ariadne/"
snakefile = project_dir + "/workflow/Snakefile"
configfile = project_dir + "results/240216-365H-higherelectrolysis/config.yaml"
os.chdir(project_dir)

with open(configfile) as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

# Set this manually when reusing old models
#%%
    
# official template
template_path = Path(__file__).parent /  "2024-01-31_template_Ariadne.xlsx" 

# Metadata
model = "PyPSA-Ariadne v" + config['version']

keys, values = zip(*config['scenario'].items())
permutations_dicts = [dict(zip(keys, v)) for v in product(*values)]
scenarios = []
for scenario_i in permutations_dicts:
    scenarios.append(
        "elec_s{simpl}_{clusters}_l{ll}_{opts}_{sector_opts}_".format(
            **scenario_i
        )
    )
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

def get_ariadne_var(n, industry_demand, energy_totals, region):

    var = pd.concat([
        get_capacities_electricity(n,region),
        get_capacities_heat(n,region),
        get_capacities_other(n,region),
        get_primary_energy(n, region),
        get_secondary_energy(n, region),
        #get_final_energy(n, region, industry_demand, energy_totals),
        #get_prices(n,region), 
        #get_emissions
    ])


    return var


# uses the global variables model, scenario and var2unit. For now.
def get_data(year):
    print("Evaluating year ", year, ".", sep="")
    n = pypsa.Network(f"results/{config['run']['name']}/postnetworks/{scenario}{year}.nc")
    industry_demand = pd.read_csv(
        "resources/industrial_energy_demand_elec_s{simpl}_{clusters}_{year}.csv".format(
            year=year, 
            **permutations_dicts[0],
        ), 
        index_col="TWh/a (MtCO2/a)",
    ).multiply(TWh2PJ)
    industry_demand.index.name = "bus"
    energy_totals = pd.read_csv(
    "resources/energy_totals.csv",
    index_col=0,
).multiply(TWh2PJ)
    var = get_ariadne_var(n, industry_demand, energy_totals, "DE")

    data = []
    for v in var.index:
        try:
            unit = var2unit[v]
        except KeyError:
            print("Warning: Variable '", v, "' not in Ariadne Database")
            unit = "NA"

        data.append([
            model, 
            scenario,
            "DE",
            v,
            unit,
            var[v],
        ])

    tab = pd.DataFrame(
        data, 
        columns=["Model", "Scenario", "Region", "Variable", "Unit", year]
    )

    return tab

# %%
# costs = pd.read_csv(
#     f"results/{config["run"]["name"]}/csvs/costs.csv",
#     index_col=[0,1,2], 
#     names=["variable", "capital", "type", *years],
# )
# "2005", "2010", "2015", "2020", "2025", "2030", "2035", 


# "2040", "2045", "2050", "2060", "2070", "2080", "2090", "2100"])
n = n20 = pypsa.Network(f"results/{config['run']['name']}/postnetworks/{scenario}{2020}.nc")


n30 = pypsa.Network(f"results/{config['run']['name']}/postnetworks/{scenario}{2030}.nc")

n40 = pypsa.Network(f"results/{config['run']['name']}/postnetworks/{scenario}{2040}.nc")

n50 = pypsa.Network(f"results/{config['run']['name']}/postnetworks/{scenario}{2050}.nc")

kwargs = {
    'groupby': n.statistics.groupers.get_name_bus_and_carrier,
    'nice_names': False,
}
year=2020
region="DE"
industry_demand = pd.read_csv(
    "resources/industrial_energy_demand_elec_s{simpl}_{clusters}_{year}.csv".format(
        year=year, 
        **permutations_dicts[0],
    ), 
    index_col="TWh/a (MtCO2/a)",
).multiply(TWh2PJ)

energy_totals = pd.read_csv(
    "resources/energy_totals.csv",
    index_col=0,
).multiply(TWh2PJ)
# %%


yearly_dfs = map(get_data, years)

df = reduce(
    lambda left, right: pd.merge(
        left, 
        right, 
        on=["Model", "Scenario", "Region", "Variable", "Unit"]), 
    yearly_dfs
) # directly use pd.merge?


df.to_excel(
    "/home/micha/git/pypsa-exporter/pypsa_output.xlsx",
    index=False
)
# !: Check for integer zeros in the xlsx-file. They may indicate missing
# technologies

# # %% OLD
 
# It's important to also regard the bus carrier "co2 stored", for this to
# work correctly. Even if I fix it, it might break again with the 
# # introduction of "co2 sequestered"

# def get_total_co2_supply(n, region):
#     return t2Mt * n.statistics.supply(
#         bus_carrier="co2",
#         groupby=n.statistics.groupers.get_name_bus_and_carrier
#     ).filter(like=region).groupby("carrier").sum().sum()

# def sum_co2_supply(n, carrier, region):
#     # If a technology is not built, it does not show up in n.statistics
#     # What to do in this case?
#     if type(carrier) == list:
#         return sum([sum_co2_supply(n, car, region) for car in carrier])
    
#     stats = n.statistics.supply(
#         bus_carrier="co2",
#         groupby=n.statistics.groupers.get_name_bus_and_carrier
#     ).filter(like=region).groupby("carrier").sum()

#     if carrier not in stats.index:
#         print("Warning, ", carrier, " not found, maybe not built!")
#         return 0
#     return t2Mt * stats[carrier]

# # !!! This mistakenly got the charger, not the discharger capacity                                     
# def get_converter_capacity(n, label, region):
#     _idx = n.links.bus0.isin(n.stores[n.stores.carrier == label].bus)
#     df = n.links[_idx].filter(like=region, axis=0)
#     return MW2GW * df.p_nom_opt.multiply(df.efficiency).sum()
# %%
# n50.links_t["p0"][n50.links[n50.links.carrier=="DAC"].index].sum().sum()
# n.statistics.energy_balance(aggregate_bus=False).xs("co2 atmosphere", level="bus").groupby("carrier").sum().sort_values()
# %%
