from _utils import *

MWh2GJ = 3.6
TWh2PJ = 3.6
MWh2PJ = 3.6e-6

#n.statistics.withdrawal(bus_carrier="land transport oil", groupby=groupby, aggregate_time=False).filter(like="DE1 0",axis=0)

# convert EURXXXX to EUR2020
def get_ariadne_prices(n, region):
    var = {}
    groupby = n.statistics.groupers.get_name_bus_and_carrier

    nodal_flows = n.statistics.withdrawal(
        bus_carrier="low voltage", 
        groupby=groupby,
        aggregate_time=False,
    ).filter(
        like=region,
        axis=0,
    ).query( # Take care to exclude everything else at this bus
        "not carrier.str.contains('agriculture')"
         "& not carrier.str.contains('industry')"
         "& not carrier.str.contains('urban central')"
    ).groupby("bus").sum().T 

    nodal_prices = n.buses_t.marginal_price[nodal_flows.columns] 

    # electricity price at the final level in the residential sector. Prices should include the effect of carbon prices.
    var["Price|Final Energy|Residential|Electricity"] = \
        nodal_flows.mul(
            nodal_prices
        ).sum().div(
            nodal_flows.sum()
        ).div(MWh2GJ) # TODO should this be divided by MWh2GJ ???

    return var

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
        + get_capacity(n.storage_units, 'hydro', region) \
        + get_capacity(n.storage_units, 'PHS', region)

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



def get_ariadne_final_energy(n, region, industry_demand):
    var = {}


    var["Final Energy|Industry excl Non-Energy Use|Electricity"] = \
        sum_load(n, "industry electricity", region)
    
    var["Final Energy|Industry excl Non-Energy Use|Heat"] = \
        sum_load(n, "low-temperature heat for industry", region)
    
    # var["Final Energy|Industry excl Non-Energy Use|Solar"] = \
    # !: Included in |Heat

    # var["Final Energy|Industry excl Non-Energy Use|Geothermal"] = \
    # Not implemented

    var["Final Energy|Industry excl Non-Energy Use|Gases"] = TWh2PJ * (
        industry_demand[
            ["methane"]
        ].filter(like=region, axis=0).values.sum()
    )
    # ! "gas for industry" is not regionally resolved
    # !!! probably this value is too low because instant electrification
    # in 2020 / 2025 is assumed
    
    
    # var["Final Energy|Industry excl Non-Energy Use|Power2Heat"] = \
    # Q: misleading description

    var["Final Energy|Industry excl Non-Energy Use|Hydrogen"] = \
        sum_load(n, "H2 for industry", region)
    

    var["Final Energy|Industry excl Non-Energy Use|Liquids"] = \
        sum_load(n, "naphtha for industry", region)
    

    # var["Final Energy|Industry excl Non-Energy Use|Other"] = \

    var["Final Energy|Industry excl Non-Energy Use|Solids"] = TWh2PJ * (
        industry_demand[
            ["coal", "coke", "solid biomass"]
        ].filter(like=region, axis=0).values.sum()
    )
        
    # var["Final Energy|Industry excl Non-Energy Use|Non-Metallic Minerals"] = \
    # var["Final Energy|Industry excl Non-Energy Use|Chemicals"] = \
    # var["Final Energy|Industry excl Non-Energy Use|Steel"] = \
    # var["Final Energy|Industry excl Non-Energy Use|Steel|Primary"] = \
    # var["Final Energy|Industry excl Non-Energy Use|Steel|Secondary"] = \
    # var["Final Energy|Industry excl Non-Energy Use|Pulp and Paper"] = \
    # var["Final Energy|Industry excl Non-Energy Use|Food and Tobacco"] = \
    # var["Final Energy|Industry excl Non-Energy Use|Non-Ferrous Metals"] = \
    # var["Final Energy|Industry excl Non-Energy Use|Engineering"] = \
    # var["Final Energy|Industry excl Non-Energy Use|Vehicle Construction"] = \
    # Q: Most of these could be found somewhere, but are model inputs!




    # Final energy is delivered to the consumers
    low_voltage_electricity = n.statistics.withdrawal(
        bus_carrier="low voltage", 
        groupby=n.statistics.groupers.get_name_bus_and_carrier,
    ).filter(
        like=region,
        axis=0,
    ).groupby("carrier").sum()
    
    var["Final Energy|Residential and Commercial|Electricity"] = \
        MWh2PJ * low_voltage_electricity[
            # carrier does not contain one of the following three substrings
            ~low_voltage_electricity.index.str.contains(
                "urban central|industry|agriculture"
            )
        ].sum()
    

    # urban decentral heat and rural heat are delivered as different forms of energy
    # (gas, oil, biomass, ...)
    urban_decentral_heat_withdrawal = n.statistics.withdrawal(
        bus_carrier="urban decentral heat", 
        groupby=n.statistics.groupers.get_name_bus_and_carrier,
    ).filter(
        like=region,
        axis=0,
    ).groupby("carrier").sum()

    urban_decentral_heat_residential_and_commercial_fraction = (
        urban_decentral_heat_withdrawal["urban decentral heat"] 
        / urban_decentral_heat_withdrawal.sum()
    )

    urban_decentral_heat_supply = n.statistics.supply(
        bus_carrier="urban decentral heat", 
        groupby=n.statistics.groupers.get_name_bus_and_carrier,
    ).filter(
        like=region,
        axis=0,
    ).groupby("carrier").sum()

    rural_heat_withdrawal = n.statistics.withdrawal(
        bus_carrier="rural heat", 
        groupby=n.statistics.groupers.get_name_bus_and_carrier,
    ).filter(
        like=region,
        axis=0,
    ).groupby("carrier").sum()

    rural_heat_residential_and_commercial_fraction = (
        rural_heat_withdrawal["rural heat"] 
        / rural_heat_withdrawal.sum()
    )

    rural_heat_supply = n.statistics.supply(
        bus_carrier="rural heat", 
        groupby=n.statistics.groupers.get_name_bus_and_carrier,
    ).filter(
        like=region,
        axis=0,
    ).groupby("carrier").sum()

    # Dischargers probably should not be considered, to avoid double counting

    #var["Final Energy|Residential and Commercial|Gases"] = \


    # Only urban central directly delivers heat
    var["Final Energy|Residential and Commercial|Heat"] = \
        sum_load(n, "urban central heat", region)
    # var["Final Energy|Residential and Commercial|Hydrogen"] = \
    # var["Final Energy|Residential and Commercial|Liquids"] = \
    # var["Final Energy|Residential and Commercial|Other"] = \
    # var["Final Energy|Residential and Commercial|Solids"] = \
    # var["Final Energy|Residential and Commercial|Solids|Biomass"] = \
    # var["Final Energy|Residential and Commercial|Solids|Coal"] = \
    # Q: Everything else seems to be not implemented

    var["Final Energy|Residential and Commercial"] = (
        var["Final Energy|Residential and Commercial|Electricity"]
        + var["Final Energy|Residential and Commercial|Heat"]
    )

    # var["Final Energy|Transportation|Other"] = \

    var["Final Energy|Transportation"] = \
        sum_load(
            n,
            [
                "land transport oil",
                "land transport EV",
                "land transport fuel cell",
                "shipping oil", 
                "shipping methanol",
                # "H2 for shipping" # not used
                "kerosene for aviation",            
            ],
            region
        )
    # !!! From every use of shipping and aviation carriers, we should find a way
    # to separate domestic from international contributions


    
    var["Final Energy|Agriculture|Electricity"] = \
        sum_load(n, "agriculture electricity", region)
    var["Final Energy|Agriculture|Heat"] = \
        sum_load(n, "agriculture heat", region)
    var["Final Energy|Agriculture|Liquids"] = \
        sum_load(n, "agriculture machinery oil", region)
    # var["Final Energy|Agriculture|Gases"] = \
    var["Final Energy|Agriculture"] = (
        var["Final Energy|Agriculture|Electricity"]
        + var["Final Energy|Agriculture|Heat"]
        + var["Final Energy|Agriculture|Liquids"]
    )
    return var