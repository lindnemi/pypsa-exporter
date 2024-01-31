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
t2Mt = 1e-6
#%%

project_dir = "/home/micha/git/pypsa-ariadne/"
snakefile = project_dir + "/workflow/Snakefile"

os.chdir(project_dir)

workflow = sm.Workflow(snakefile, overwrite_configfiles=[], rerun_triggers=[])
#%%

# Raises an exception but sucessfully reads the config
try:
    workflow.include(snakefile)
except IndentationError as e:
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
 

#%% CO2

def get_total_co2(n, region):
    return t2Mt * n.statistics.supply(
        bus_carrier="co2",
        groupby=n.statistics.groupers.get_name_bus_and_carrier
    ).filter(like=region).groupby("carrier").sum().sum()


def get_co2(n, carrier, region):
    # If a technology is not built, it does not show up in n.statistics
    # What to do in this case?
    if type(carrier) == list:
        return sum([get_co2(n, car, region) for car in carrier])
    
    stats = n.statistics.supply(
        bus_carrier="co2",
        groupby=n.statistics.groupers.get_name_bus_and_carrier
    ).filter(like=region).groupby("carrier").sum()

    if carrier not in stats.index:
        print("Warning, ", carrier, " not found, maybe not built!")
        return 0
    return t2Mt * stats[carrier]

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
    

# !!! This mistakenly get the charger, not the discharger capacity                                     
# def get_converter_capacity(n, label, region):
#     _idx = n.links.bus0.isin(n.stores[n.stores.carrier == label].bus)
#     df = n.links[_idx].filter(like=region, axis=0)
#     return MW2GW * df.p_nom_opt.multiply(df.efficiency).sum()

def get_reservoir_capacity(_df, label, region):
    if type(label) == list:
        return sum(map(lambda lab: get_reservoir_capacity(_df, lab, region), label))

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


# This has to be defined differently to avoid code duplication


def get_capacityN(_df, label, region, N=2):
    if type(label) == list:
        return sum(map(lambda lab: get_capacityN(_df, lab, region, N=N), label))

    df = _df[_df.carrier==label].filter(like=region, axis=0)
    if df.index.name == "Link":
        return MW2GW * df.p_nom_opt.multiply(df["efficiency{}".format(N)]).sum()
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
    # ? CCS for coal Implemented, but not activated, should we use it?
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
        get_capacity(n.links, "battery discharger", region) + \
        get_capacity(n.links, "home battery discharger", region)

    var["Capacity|Electricity|Storage Converter|Vehicles"] = \
        get_capacity(n.links, "V2G", region)
    
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
    #  We could be much more detailed for the heat sector (as for electricity)
    # if desired by Ariadne
    #
    var["Capacity|Heat|Biomass|w/ CCS"] = \
        get_capacityN(n.links, 'urban central solid biomass CHP CC', region) 
    var["Capacity|Heat|Biomass|w/o CCS"] = \
        get_capacityN(n.links, 'urban central solid biomass CHP', region)
    var["Capacity|Heat|Biomass"] = \
        var["Capacity|Heat|Biomass|w/ CCS"] + \
        var["Capacity|Heat|Biomass|w/o CCS"] + \
        get_capacity(
            n.links,
            [
                'residential rural biomass boiler',
                'services rural biomass boiler',
                'residential urban decentral biomass boiler',
                'services urban decentral biomass boiler',
            ],
            region,
        )
    
    var["Capacity|Heat|Electricity"] = \
        get_capacity(
            n.links,
            [
                'residential rural resistive heater',
                'services rural resistive heater',
                'residential urban decentral resistive heater',
                'services urban decentral resistive heater',
                'urban central resistive heater',
            ],
            region,
        )
    
    var["Capacity|Heat|H2"] = \
        get_capacityN(
            n.links,
            [
                'H2 Electrolysis', 
                'H2 Fuel Cell',
            ],
            region,
        )
    
    var["Capacity|Heat|Processes"] = \
        get_capacityN(
            n.links,
            [
                'Sabatier', 
                'Fischer-Tropsch', 
                # 'DAC' 
            ],
            region,
            N=3,
        ) + \
        get_capacityN(
            n.links,
            'methanolisation',
            region,
            N=4,
        )
    # Q: Why has DAC negative heat efficiency? Should it be excluded then? 
    # ! The DAC process needs heat, it should not be included 
    # !!! Missing in the Ariadne database




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
    ) + get_capacityN(
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
    # TODO new technologies like rural air heat pump should be added!

    var["Capacity|Heat|Oil"] = \
        get_capacity(n.links, "urban central oil boiler", region)
    
    var["Capacity|Heat|Storage Converter"] = \
        get_capacity(
            n.links,
            [
                'residential rural water tanks discharger',
                'services rural water tanks discharger',
                'residential urban decentral water tanks discharger',
                'services urban decentral water tanks discharger',
                'urban central water tanks discharger',
            ],
            region,
        )

    var["Capacity|Heat|Storage Reservoir"] = \
        get_reservoir_capacity(
            n.stores,
            [
                'residential rural water tanks',
                'services rural water tanks',
                'residential urban decentral water tanks',
                'services urban decentral water tanks',
                'urban central water tanks',
            ],
            region
        )



    # !!! New technologies get added as we develop the model.
    # It would be helpful to have some double-checking, e.g.,
    # by asserting that every technology gets added,
    # or by computing the total independtly of the subtotals, 
    # and summing the subcategories to compare to the total

    # n.links.carrier[n.links.bus1.str.contains("heat")].unique()
    # ^ same for other buses

    # TODO check for typos
    
    var["Capacity|Heat"] = (
        var["Capacity|Heat|Solar thermal"] +
        var["Capacity|Heat|Electricity"] +
        var["Capacity|Heat|Biomass"] +
        var["Capacity|Heat|Oil"] +
        var["Capacity|Heat|Gas"] +
        var["Capacity|Heat|Processes"] +
        var["Capacity|Heat|H2"] +
        var["Capacity|Heat|Heat pump"]
    )
    # Q: Should heat capacity exclude storage converters (just like for elec)
    
    var["Emissions|CO2"] = get_total_co2(n, region)
    # Make sure these values are about right
    # var["Emissions|CO2|Energy and Industrial Processes"] = \  
    # var["Emissions|CO2|Industrial Processes"] = \ 
    # var["Emissions|CO2|Energy"] = \   
    # var["Emissions|CO2|Energy incl Bunkers"] = \  
    # var["Emissions|CO2|Energy|Demand"] = \    
    # var["Emissions|CO2|Energy incl Bunkers|Demand"] = \
       
    var["Emissions|CO2|Energy|Demand|Industry"] = \
        get_co2(
            n,
            "naphtha for industry",
            region,
        )

    var["Emissions|CO2|Energy|Demand|Residential and Commercial"] = \
        get_co2(
            n,
            [
                "residential rural gas boiler",
                "residential rural oil boiler",
                "residential urban decentral gas boiler",
                "residential urban decentral oil boiler",
                "services rural gas boiler",
                "services rural oil boiler",
                "services urban decentral gas boiler",
                "services urban decentral oil boiler",
                "urban central gas CHP",
                "urban central gas CHP CC",
                "urban central gas boiler",
                "urban central oil boiler",
                "urban decentral gas boiler",
                "urban decentral oil boiler",
            ],
            region
        )
    # Q: are the gas CHPs for Residential and Commercial demand??

    var["Emissions|CO2|Energy|Demand|Transportation"] = \
        get_co2(n, "land transport oil", region)
  
    var["Emissions|CO2|Energy|Demand|Bunkers|Aviation"] = \
        get_co2(n, "kerosene for aviation", region)
    
    var["Emissions|CO2|Energy|Demand|Bunkers|Navigation"] = \
        get_co2(n, ["shipping oil", "shipping methanol"], region)
    # Q: Is Methanol a fuel, or a shipped good?
    
    var["Emissions|CO2|Energy|Demand|Bunkers"] = \
        var["Emissions|CO2|Energy|Demand|Bunkers|Aviation"] + \
        var["Emissions|CO2|Energy|Demand|Bunkers|Navigation"]
    
    var["Emissions|CO2|Energy|Demand|Other Sector"] = \
        get_co2(n, "agriculture machinery oil", region)
    
    
    var["Emissions|CO2|Energy|Supply|Electricity"] = \
        get_co2(n,
            [
                "Combined-Cycle Gas",
                "Open-Cycle Gas",
                "coal",
                "lignite",
                "oil",
                "urban central gas CHP",
                "urban central gas CHP CC",
            ], 
            region,
        )
    # Q: Where should I add the CHPs?
    # According to Ariadne Database in the Electricity

    var["Emissions|CO2|Energy|Supply|Heat"] = \
        get_co2(n,
            [
                "residential rural gas boiler",
                "residential rural oil boiler",
                "residential urban decentral gas boiler",
                "residential urban decentral oil boiler",
                "services rural gas boiler",
                "services rural oil boiler",
                "services urban decentral gas boiler",
                "services urban decentral oil boiler",
                "urban central gas boiler",
                "urban central oil boiler",
                "urban decentral gas boiler",
                "urban decentral oil boiler",
            ], 
            region,
        )

    var["Emissions|CO2|Energy|Supply|Electricity and Heat"] = \
        var["Emissions|CO2|Energy|Supply|Heat"] + \
        var["Emissions|CO2|Energy|Supply|Electricity"]

    # var["Emissions|CO2|Supply|Non-Renewable Waste"] = \   
    var["Emissions|CO2|Energy|Supply|Hydrogen"] = \
        get_co2(n,
            [
                "SMR",
                "SMR CC",
            ], 
            region,
        )
    
    var["Emissions|CO2|Energy|Supply|Gases"] = \
        get_co2(n, "naphtha for industry", region)
    var["Emissions|CO2|Energy|Supply|Liquids"] = \
        get_co2(
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
        var["Emissions|CO2|Energy|Supply|Gases"] + \
        var["Emissions|CO2|Energy|Supply|Liquids"] 
    
    var["Emissions|CO2|Energy|Supply"] = \
        var["Emissions|CO2|Energy|Supply|Liquids and Gases"] + \
        var["Emissions|CO2|Energy|Supply|Hydrogen"] + \
        var["Emissions|CO2|Energy|Supply|Electricity and Heat"]
    # var["Emissions|CO2|Energy|Supply|Other Sector"] = \   
    # var["Emissions|CO2|Energy|Supply|Solids"] = \ 
    # TODO Add (negative) BECCS emissions! (Use "co2 stored" and "co2 sequestered")
    return var


# uses the global variables model, scenario and var2unit. For now.
def get_data(year):
    n = pypsa.Network(f"results/{config['run']['name']}/postnetworks/{scenario}{year}.nc")
    
    var = get_ariadne_var(n, "DE")

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
n = pypsa.Network(f"results/{config['run']['name']}/postnetworks/{scenario}{2030}.nc")
region="DE"
# # %% OLD
 

# def get_total_co2(n, region):
#     # including international bunker fuels and negative emissions
#     df = n.links.filter(like=region, axis=0)

#     co2 = 0
#     for port in [col[3:] for col in df if col.startswith("bus")]:
#         links = df.index[df[f"bus{port}"] == "co2 atmosphere"]
#         if port == "0":
#             co2 += -1. * n.links_t["p0"][links].multiply(
#                 n.snapshot_weightings.generators,
#                 axis=0,
#             ).values.sum()
#         else:
#             co2 += n.links_t[f"p{port}"][links].multiply(
#                 n.snapshot_weightings.generators,
#                 axis=0,
#             ).values.sum()
#     return t2Mt * co2

# %%
