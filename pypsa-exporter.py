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
from _utils import *
from _getters import *


# Defining global varibales

TWh2PJ = 3.6
MWh2TJ = 3.6e-3 
MW2GW = 1e-3
t2Mt = 1e-6

#%%

project_dir = "/home/julian-geis/repos/pypsa-ariadne/"
snakefile = project_dir + "/workflow/Snakefile"

os.chdir(project_dir)

workflow = sm.Workflow(snakefile, overwrite_configfiles=[], rerun_triggers=[])
#%%

# Raises an exception but sucessfully reads the config
try:
    workflow.include(snakefile)
except IndentationError as e:
    print(e)
except NameError as ne:
    print(ne)
finally:
    assert workflow.config != {}
    print("Caught error, config read successfully.")
    config=workflow.config

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

def get_ariadne_var(n, industry_demand, region):

    var = {}

    var.update(get_ariadne_capacities(n, region))
    
    ## Emissions

    var["Carbon Sequestration|BECCS"] = \
        sum_co2(
            n,
            [
                "biogas to gas",
                "solid biomass for industry CC",
                "biogas to gas CC",
                "urban central solid biomass CHP CC",
            ],
            region,
        )     

    var["Carbon Sequestration|DACCS"] = \
        sum_co2(n, "DAC", region)


    var["Emissions|CO2"] = \
        get_total_co2(n, region) 

    # ! LULUCF should also be subtracted, we get from REMIND, 
    # TODO how to add it here?
    
    # Make sure these values are about right
    # var["Emissions|CO2|Energy and Industrial Processes"] = \  
    # var["Emissions|CO2|Industrial Processes"] = \ 
    # var["Emissions|CO2|Energy"] = \   
    # var["Emissions|CO2|Energy incl Bunkers"] = \  
    # var["Emissions|CO2|Energy|Demand"] = \    
    # var["Emissions|CO2|Energy incl Bunkers|Demand"] = \
       
    # var["Emissions|CO2|Energy|Demand|Industry"] = \
    #     sum_co2(
    #         n,
    #         "naphtha for industry",
    #         region,
    #     )
    # Q: these are emissions through burning of plastic waste!!! 


    var["Emissions|CO2|Energy|Demand|Residential and Commercial"] = \
        sum_co2(
            n,
            [
                *n.links.carrier.filter(like="oil boiler").unique(),
                *n.links.carrier.filter(like="gas boiler").unique(),
                # matches "gas CHP CC" as well
                *n.links.carrier.filter(like="gas CHP").unique(),
            ],
            region
        )
    # Q: are the gas CHPs for Residential and Commercial demand??
    # Q: Also residential elec demand!

    var["Emissions|CO2|Energy|Demand|Transportation"] = \
        sum_co2(n, "land transport oil", region)
  
    var["Emissions|CO2|Energy|Demand|Bunkers|Aviation"] = \
        sum_co2(n, "kerosene for aviation", region)
    
    var["Emissions|CO2|Energy|Demand|Bunkers|Navigation"] = \
        sum_co2(n, ["shipping oil", "shipping methanol"], region)
    
    var["Emissions|CO2|Energy|Demand|Bunkers"] = \
        var["Emissions|CO2|Energy|Demand|Bunkers|Aviation"] + \
        var["Emissions|CO2|Energy|Demand|Bunkers|Navigation"]
    
    var["Emissions|CO2|Energy|Demand|Other Sector"] = \
        sum_co2(n, "agriculture machinery oil", region)
    
    
    var["Emissions|Gross Fossil CO2|Energy|Supply|Electricity"] = \
        sum_co2(n,
            [
                "OCGT",
                "CCGT",
                "coal",
                "lignite",
                "oil",
                "urban central gas CHP",
                "urban central gas CHP CC",
            ], 
            region,
        )
    
    var["Emissions|CO2|Energy|Supply|Electricity"] = (
        var["Emissions|Gross Fossil CO2|Energy|Supply|Electricity"]
        + sum_co2(n, "urban central solid biomass CHP CC", region)
    )

    # Q: Where should I add the CHPs?
    # ! According to Ariadne Database in the Electricity

    var["Emissions|CO2|Energy|Supply|Heat"] = \
        sum_co2(n,
            [
                *n.links.carrier.filter(like="oil boiler").unique(),
                *n.links.carrier.filter(like="gas boiler").unique(),
            ], 
            region,
        )

    var["Emissions|CO2|Energy|Supply|Electricity and Heat"] = \
        var["Emissions|CO2|Energy|Supply|Heat"] + \
        var["Emissions|CO2|Energy|Supply|Electricity"]

    # var["Emissions|CO2|Supply|Non-Renewable Waste"] = \   
    var["Emissions|CO2|Energy|Supply|Hydrogen"] = \
        sum_co2(n,
            [
                "SMR",
                "SMR CC",
            ], 
            region,
        )
    
    #var["Emissions|CO2|Energy|Supply|Gases"] = \
    var["Emissions|CO2|Supply|Non-Renewable Waste"] = \
        sum_co2(n, "naphtha for industry", region)
    # Q: These are plastic combustino emissions, not Gases. 
    # What then are gases?

    var["Emissions|CO2|Energy|Supply|Liquids"] = \
        sum_co2(
            n,
            [
                "agriculture machinery oil",
                "kerosene for aviation",
                "shipping oil",
                "shipping methanol",
                "land transport oil"
            ],
            region
        )
    # Q: Some things show up on Demand as well as Supply side

    var["Emissions|CO2|Energy|Supply|Liquids and Gases"] = \
        var["Emissions|CO2|Energy|Supply|Liquids"]
        # var["Emissions|CO2|Energy|Supply|Gases"] + \
    
    var["Emissions|CO2|Energy|Supply"] = \
        var["Emissions|CO2|Energy|Supply|Liquids and Gases"] + \
        var["Emissions|CO2|Energy|Supply|Hydrogen"] + \
        var["Emissions|CO2|Energy|Supply|Electricity and Heat"]
    # var["Emissions|CO2|Energy|Supply|Other Sector"] = \   
    # var["Emissions|CO2|Energy|Supply|Solids"] = \ 
    # TODO Add (negative) BECCS emissions! (Use "co2 stored" and "co2 sequestered")


    ## Primary Energy

    var["Primary Energy|Oil|Heat"] = \
        sum_link_input(
            n,
            n.links.carrier.filter(like="oil boiler").unique().tolist(),
            region,
        )

    
    var["Primary Energy|Oil|Electricity"] = \
        sum_link_input(n, "oil", region)
    
    var["Primary Energy|Oil"] = (
        var["Primary Energy|Oil|Electricity"] 
        + var["Primary Energy|Oil|Heat"] 
        + sum_link_input(
            n,
            [
                "land transport oil",
                "agriculture machinery oil",
                "shipping oil",
                "kerosene for aviation",
                "naphtha for industry"
            ],
            region,
        )
    )   
    # n.statistics.withdrawal(bus_carrier="oil")

    var["Primary Energy|Gas|Heat"] = \
        sum_link_input(
            n,
            n.links.carrier.filter(like="gas boiler").unique().tolist(),
            region,
        )
    
    var["Primary Energy|Gas|Electricity"] = \
        sum_link_input(
            n,
            [
                'CCGT',
                'OCGT',
                'urban central gas CHP',
                'urban central gas CHP CC',
            ],
            region,
        )
    # Adding the CHPs to electricity, see also Capacity|Electricity|Gas
    # Q: pypsa to iamc SPLITS the CHPS. Should we do the same?
    
    var["Primary Energy|Gas"] = (
        var["Primary Energy|Gas|Heat"]
        + var["Primary Energy|Gas|Electricity"]
        + sum_link_input(
            n,
            [
                'gas for industry', 
                'gas for industry CC',
            ],
            region,
        )
    )

    # ! There are CC sub-categories that could be used



    var["Primary Energy|Coal|Electricity"] = \
        sum_link_input(n, "coal", region)
    
    var["Primary Energy|Coal"] = (
        var["Primary Energy|Coal|Electricity"] 
        # + sum_load(n, "coal for industry", region)
    )
    # Q: It's strange to sum a load here, probably wrong (and 0 anyways)


    var["Primary Energy|Fossil"] = (
        var["Primary Energy|Coal"]
        + var["Primary Energy|Gas"]
        + var["Primary Energy|Oil"]
    )

    var["Secondary Energy|Electricity|Coal|Hard Coal"] = \
        sum_link_output(n, "coal", region)
    

    var["Secondary Energy|Electricity|Coal"] = (
        var["Secondary Energy|Electricity|Coal|Hard Coal"] 
        + sum_link_output(n, "lignite", region)
    )
    
    var["Secondary Energy|Electricity|Oil"] = \
        sum_link_output(n, "oil", region)
    
    var["Secondary Energy|Electricity|Gas"] = \
        sum_link_output(
            n,
            [
                'CCGT',
                'OCGT',
                'urban central gas CHP',
                'urban central gas CHP CC',
            ],
            region,
        )

    var["Secondary Energy|Electricity|Biomass"] = \
        sum_link_output(
            n,
            [
                'urban central solid biomass CHP',
                'urban central solid biomass CHP CC',
            ],
            region,
        )
    # ! Biogas to gas should go into this category
    # How to do that? (trace e.g., biogas to gas -> CCGT)
    # If so: Should double counting with |Gas be avoided?

    var["Secondary Energy|Electricity|Fossil"] = (
        var["Secondary Energy|Electricity|Gas"]
        + var["Secondary Energy|Electricity|Oil"]
        + var["Secondary Energy|Electricity|Coal"]
    )

    var["Secondary Energy|Electricity|Hydro"] = (
        sum_storage_unit_output(n, ["PHS", "hydro"], region)
        + sum_generator_output(n, "ror", region)
    )
    # Q: PHS produces negative electricity, because of storage losses
    # Q: Should it be considered here??
    var["Secondary Energy|Heat|Gas"] = (
        sum_link_output(
            n,
            n.links.carrier.filter(like="oil boiler").unique().tolist(),
            region,
        ) 
        + sum_link_output(
            n,
            [
                'urban central gas CHP',
                'urban central gas CHP CC',
            ],
            region,
            port="p2"
        ) 
    )
    # Q: Make sure to provide a comprehensive list of boilers


    var["Secondary Energy|Hydrogen|Electricity"] = \
        sum_link_output(n, 'H2 Electrolysis', region)
    # Q: correct units?
    
    var["Secondary Energy|Hydrogen|Gas"] = \
        sum_link_output(n, ["SMR", "SMR CC"], region)
    # Q: Why does SMR not consume heat or electricity?


    var["Secondary Energy|Liquids|Hydrogen"] = \
        sum_link_output(
            n,
            [
                "methanolisation",
                "Fischer-Tropsch",
            ],
            region,
        )
    
    var["Secondary Energy|Gases|Hydrogen"] = \
        sum_link_output(
            n,
            [
                "Sabatier",
            ],
            region,
        )
    
    var["Secondary Energy|Gases|Biomass"] = \
        sum_link_output(
            n,
            [
                "biogas to gas",
                "biogas to gas CC",
            ],
            region,
        )
    # Q: biogas to gas is an EU Bus and gets filtered out by "region"
    # Fixed by biomass_spatial: True



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
    )
    industry_demand.index.name = "bus"
    var = get_ariadne_var(n, industry_demand, "DE")

    data = []
    for v in var:
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


yearly_dfs = map(get_data, years)

df = reduce(
    lambda left, right: pd.merge(
        left, 
        right, 
        on=["Model", "Scenario", "Region", "Variable", "Unit"]), 
    yearly_dfs
) # directly use pd.merge?


df.to_excel(
    "/home/julian-geis/repos/pypsa-exporter/pypsa_output.xlsx",
    index=False
)
# !: Check for integer zeros in the xlsx-file. They may indicate missing
# technologies

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

region="DE"
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
