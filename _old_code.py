from _utils import *
import pandas as pd

TWh2PJ = 3.6
MWh2TJ = 3.6e-3 
MW2GW = 1e-3
t2Mt = 1e-6


def get_ariadne_capacities(n, region):
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
    # Q: CCS for coal Implemented, but not activated, should we use it?
    # !: No, because of Kohleausstieg
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
        + get_capacity(n.storage_units, 'hydro', region)

    # var["Capacity|Electricity|Hydrogen|CC"] = 
    # ! Not implemented
    # var["Capacity|Electricity|Hydrogen|OC"] = 
    # Q: "H2-turbine"
    # Q: What about retrofitted gas power plants? -> Lisa

    var["Capacity|Electricity|Hydrogen|FC"] = \
        get_capacity(n.links, "H2 Fuel Cell", region)

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
    # Q: It seems that Li ion has been merged into battery storage??

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
    # ! Use a filter


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
                # 'DAC' # consumes heat
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

    # !!! Missing in the Ariadne database




    # Capacity|Heat
    var["Capacity|Heat|Gas"] = get_capacity(
        n.links,
        n.links.carrier.filter(like="gas boiler").unique().tolist(),
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
    # use a filter!

    var["Capacity|Heat|Oil"] = \
        get_capacity(
            n.links, 
            [
                "rural oil boiler",
                'urban decentral oil boiler',
                "urban central oil boiler",
            ],
            region,
        )
    # Q: Check list for completeness!

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
    
    
    
    return var
