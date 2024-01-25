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

# Defining global varibales

MW2GW = 0.001
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

## Electricity

def get_capacity(_df, label, region):
    if type(label) == list:
        return sum(map(lambda lab: get_capacity(_df, lab, region), label))
    # Would be nice to have an explicit column for the region, not just implicit
    # location derived from the index name
    df = _df[_df.carrier==label].filter(like=region, axis=0)
    if "CHP" in label:
        print("Warning: Returning electrical capacity of the CHP, not thermal.")
    if df.index.name == "Link":
        return MW2GW * df.p_nom_opt.multiply(df.efficiency).sum()
    elif df.index.name == "Generator":
        return MW2GW * df.p_nom_opt.sum() 
    elif df.index.name == "StorageUnit":
        return MW2GW * df.p_nom_opt.sum()
    else:
        raise Exception("Received unexpected DataFrame.")
    

def get_converter_capacity(n, label, region):
    _idx = n.links.bus0.isin(n.stores[n.stores.carrier == label].bus)
    df = n.links[_idx].filter(like=region, axis=0)
    return MW2GW * df.p_nom_opt.multiply(df.efficiency).sum()

def get_reservoir_capacity(_df, label, region):
    df = _df[_df.carrier == label].filter(like=region, axis=0)
    if df.index.name == "Store":
        return MW2GW * df.e_nom_opt.sum()
    elif df.index.name == "StorageUnit":
        return MW2GW * df.p_nom_opt.multiply(df.max_hours).sum()
    else:
        raise Exception("Received unexpected DataFrame.")
    
def get_line_capacity(n, region):
    AC_capacity = (
        0.5 * (
            n.lines.bus0.str.contains(region).astype(float) + 
            n.lines.bus1.str.contains(region).astype(float)
        ) * n.lines.length.multiply(n.lines.s_nom_opt)
    ).sum()

    _DC_links = n.links[n.links.carrier == "DC"]
    # Drop all reversed links
    DC_links = _DC_links[~_DC_links.index.str.contains("-reversed")]
    DC_capacity = (
        0.5 * (
            DC_links.bus0.str.contains(region).astype(float) + 
            DC_links.bus1.str.contains(region).astype(float)
        ) * DC_links.length.multiply(DC_links.p_nom_opt)
    ).sum()

    return MW2GW * (AC_capacity + DC_capacity)
#%%

## Heat

def get_capacity2(_df, label, region):
    if type(label) == list:
        return sum(map(lambda lab: get_capacity2(_df, lab, region), label))

    df = _df[_df.carrier==label].filter(like=region, axis=0)
    if df.index.name == "Link":
        return MW2GW * df.p_nom_opt.multiply(df.efficiency2).sum()
    else:
        raise Exception("Received unexpected DataFrame.")

#%%

def get_ariadne_var(n, region):

    var = {}

    ## Capacity | Electricity

    # var["Capacity|Electricity|Biomass|Gases and Liquids"] =
    # direct biogas plants are not implemented,
    # biogas gets upgraded to gas 
    # Disregarding biomass -> biogas -> gas -> electricity


    var["Capacity|Electricity|Biomass|w/ CCS"] = \
        get_capacity(n.links, 'urban central solid biomass CHP CC', region) 
    var["Capacity|Electricity|Biomass|w/o CCS"] = \
        get_capacity(n.links, 'urban central solid biomass CHP', region) 

    var["Capacity|Electricity|Biomass|Solids"] = \
        var["Capacity|Electricity|Biomass|w/ CCS"] + \
        var["Capacity|Electricity|Biomass|w/o CCS"]

    var["Capacity|Electricity|Biomass"] = \
        var["Capacity|Electricity|Biomass|Solids"]


    var["Capacity|Electricity|Coal|Hard Coal"] = \
        get_capacity(n.links, "coal", region)                                                  

    var["Capacity|Electricity|Coal|Lignite"] = \
        get_capacity(n.links, "lignite", region)
    
    # var["Capacity|Electricity|Coal|Hard Coal|w/ CCS"] = 
    # var["Capacity|Electricity|Coal|Hard Coal|w/o CCS"] = 
    # var["Capacity|Electricity|Coal|Lignite|w/ CCS"] = 
    # var["Capacity|Electricity|Coal|Lignite|w/o CCS"] = 
    # var["Capacity|Electricity|Coal|w/ CCS"] = 
    # var["Capacity|Electricity|Coal|w/o CCS"] = 
    # !? CCS for coal Implemented, but not activated, should we use it?
    # > config: coal_cc

    var["Capacity|Electricity|Coal"] = \
        var["Capacity|Electricity|Coal|Lignite"] + \
        var["Capacity|Electricity|Coal|Hard Coal"]

    # var["Capacity|Electricity|Gas|CC|w/ CCS"] =
    # var["Capacity|Electricity|Gas|CC|w/o CCS"] =  
    # ! Not implemented, rarely used   

    var["Capacity|Electricity|Gas|CC"] = \
        get_capacity(n.links, "CCGT", region)
    
    var["Capacity|Electricity|Gas|OC"] = \
        get_capacity(n.links, "OCGT", region)
    
    var["Capacity|Electricity|Gas|w/ CCS"] =  \
        get_capacity(n.links, "urban central gas CHP CC", region)  
    
    var["Capacity|Electricity|Gas|w/o CCS"] =  \
        get_capacity(n.links, "urban central gas CHP", region) + \
        var["Capacity|Electricity|Gas|CC"] + \
        var["Capacity|Electricity|Gas|OC"]
    

    var["Capacity|Electricity|Gas"] = \
        var["Capacity|Electricity|Gas|w/ CCS"] + \
        var["Capacity|Electricity|Gas|w/o CCS"]

    # var["Capacity|Electricity|Geothermal"] = 
    # ! Not implemented

    var["Capacity|Electricity|Hydro"] = \
        get_capacity(n.generators, "ror", region) \
        + get_capacity(n.storage_units, 'hydro', region) \
        + get_capacity(n.storage_units, 'PHS', region)

    # var["Capacity|Electricity|Hydrogen|CC"] = 
    # ! Not implemented
    # var["Capacity|Electricity|Hydrogen|OC"] = 
    # Q: "H2-turbine"
    # Q: What about retrofitted gas power plants? -> Lisa

    var["Capacity|Electricity|Hydrogen|FC"] = \
        get_capacity(n.generators, "H2 Fuel Cell", region)

    var["Capacity|Electricity|Hydrogen"] = \
        var["Capacity|Electricity|Hydrogen|FC"]

    # var["Capacity|Electricity|Non-Renewable Waste"] = 
    # ! Not implemented

    # var["Capacity|Electricity|Nuclear"] = 
    # Q: why is there nuclear AND uranium capacity? 
    # Q: why are there nuclear generators AND links?
    # ! Use only generators once model is updated

    # var["Capacity|Electricity|Ocean"] = 
    # ! Not implemented

    # var["Capacity|Electricity|Oil|w/ CCS"] = 
    # var["Capacity|Electricity|Oil|w/o CCS"] = 
    # ! Not implemented

    var["Capacity|Electricity|Oil"] = \
        get_capacity(n.links, "oil", region)
    
    # ! Probably this varibale should be in the Heat part of the script
    # Filtering for multiple values is possible with the .isin(.) method

    
    var["Capacity|Electricity|Solar|PV|Rooftop"] = \
        get_capacity(n.generators, "solar rooftop", region)
    
    var["Capacity|Electricity|Solar|PV|Open Field"] = \
        get_capacity(n.generators, "solar", region) 

    var["Capacity|Electricity|Solar|PV"] = \
        var["Capacity|Electricity|Solar|PV|Open Field"] \
        + var["Capacity|Electricity|Solar|PV|Rooftop"]
    
    # var["Capacity|Electricity|Solar|CSP"] = 
    # ! not implemented

    var["Capacity|Electricity|Solar"] = var["Capacity|Electricity|Solar|PV"]

    # var["Capacity|Electricity|Storage Converter|CAES"] = 
    # ! Not implemented

    var["Capacity|Electricity|Storage Converter|Hydro Dam Reservoir"] = \
        get_capacity(n.storage_units, 'hydro', region)
    
    var["Capacity|Electricity|Storage Converter|Pump Hydro"] = \
        get_capacity(n.storage_units, "PHS", region)

    var["Capacity|Electricity|Storage Converter|Stationary Batteries"] = \
        get_converter_capacity(n, "battery", region) + \
        get_converter_capacity(n, "home battery", region)

    var["Capacity|Electricity|Storage Converter|Vehicles"] = \
        get_converter_capacity(n, "Li ion", region)
    
    var["Capacity|Electricity|Storage Converter"] = \
        var["Capacity|Electricity|Storage Converter|Hydro Dam Reservoir"] + \
        var["Capacity|Electricity|Storage Converter|Pump Hydro"] + \
        var["Capacity|Electricity|Storage Converter|Stationary Batteries"] + \
        var["Capacity|Electricity|Storage Converter|Vehicles"] 
    

    # var["Capacity|Electricity|Storage Reservoir|CAES"] =
    # ! Not implemented
     
    var["Capacity|Electricity|Storage Reservoir|Hydro Dam Reservoir"] = \
        get_reservoir_capacity(n.storage_units, "hydro", region)

    var["Capacity|Electricity|Storage Reservoir|Pump Hydro"] = \
        get_reservoir_capacity(n.storage_units, "PHS", region)
    
    var["Capacity|Electricity|Storage Reservoir|Stationary Batteries"] = \
        get_reservoir_capacity(n.stores, "battery", region) + \
        get_reservoir_capacity(n.stores, "home battery", region)
    
    var["Capacity|Electricity|Storage Reservoir|Vehicles"] = \
        get_reservoir_capacity(n.stores, "Li ion", region)

    var["Capacity|Electricity|Storage Reservoir"] = \
        var["Capacity|Electricity|Storage Reservoir|Hydro Dam Reservoir"] + \
        var["Capacity|Electricity|Storage Reservoir|Pump Hydro"] + \
        var["Capacity|Electricity|Storage Reservoir|Stationary Batteries"] + \
        var["Capacity|Electricity|Storage Reservoir|Vehicles"]
    
    var["Capacity|Electricity|Wind|Offshore"] = \
        get_capacity(n.generators, "offwind", region) + \
        get_capacity(n.generators, "offwind-ac", region) + \
        get_capacity(n.generators, "offwind-dc", region)
    # take care of "offwind" -> "offwind-ac"/"offwind-dc"

    var["Capacity|Electricity|Wind|Onshore"] = \
        get_capacity(n.generators, "onwind", region)
    
    var["Capacity|Electricity|Wind"] = \
        var["Capacity|Electricity|Wind|Offshore"] + \
        var["Capacity|Electricity|Wind|Onshore"]

    var["Capacity|Electricity"] = \
        var["Capacity|Electricity|Wind"] + \
        var["Capacity|Electricity|Solar"] + \
        var["Capacity|Electricity|Oil"] + \
        var["Capacity|Electricity|Coal"] + \
        var["Capacity|Electricity|Gas"] + \
        var["Capacity|Electricity|Biomass"] +\
        var["Capacity|Electricity|Hydro"] + \
        var["Capacity|Electricity|Hydrogen"]

    var["Capacity|Electricity|Transmissions Grid"] = \
        get_line_capacity(n, region)
    
    # var["Capacity|Electricity|Peak Demand"] = 
    # ???

    # var["Capacity|Electricity|Other"] = 
    # ???

    ## Capacity | Heat

    var["Capacity|Heat|Solar thermal"] = \
    get_capacity(n.generators, 'residential rural solar thermal', region) + \
    get_capacity(n.generators, 'services rural solar thermal', region) + \
    get_capacity(n.generators, 'residential urban decentral solar thermal', region) + \
    get_capacity(n.generators, 'services urban decentral solar thermal', region) + \
    get_capacity(n.generators, 'urban central solar thermal', region)

    # !!! Missing in the Ariadne database
    #
    # var["Capacity|Heat|Biomass|w/ CCS"] = \
    #     get_capacity2(n.links, 'urban central solid biomass CHP CC', region) 
    # var["Capacity|Heat|Biomass|w/o CCS"] = \
    #     get_capacity2(n.links, 'urban central solid biomass CHP', region)
    # var["Capacity|Heat|Biomass"] = \
    #     var["Capacity|Heat|Biomass|w/ CCS"] + \
    #     var["Capacity|Heat|Biomass|w/o CCS"]
    # FURTHER ADD THE BIOMASS BOILERS!
    #
    # !!! Missing in the Ariadne database

    # We could be much more detailed for the heat sector (as for electricity)
    # if desired by Ariadne
    # Capacity|Heat|Electricity
    # 'residential rural resistive heater',
    # 'services rural resistive heater',
    # 'residential urban decentral resistive heater',
    # 'services urban decentral resistive heater',
    # 'urban central resistive heater'

    # Capacity|Heat
    var["Capacity|Heat|Gas"] = get_capacity(
        n.links,
        [
            'residential rural gas boiler', 
            'services rural gas boiler',
            'residential urban decentral gas boiler',
            'services urban decentral gas boiler', 
            'urban central gas boiler'
        ],
        region
    ) + get_capacity2(
        n.links, 
        [
            "urban central gas CHP",
            "urban central gas CHP CC"
        ],
        region
    )

    
    # var["Capacity|Heat|Geothermal"] =
    # ! Not implemented 
    var["Capacity|Heat|Heat pump"] = \
        get_capacity(
            n.links,
            [
                'residential rural ground heat pump',
                'services rural ground heat pump',
                'residential urban decentral air heat pump',
                'services urban decentral air heat pump',
                'urban central air heat pump', 
            ],
            region
        )

    var["Capacity|Heat|Oil"] = \
        get_capacity(n.links, "urban central oil boiler", region)
    # var["Capacity|Heat|Storage Converter"] =
    # var["Capacity|Heat|Storage Reservoir"] =

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
region="DE"
# %%
 