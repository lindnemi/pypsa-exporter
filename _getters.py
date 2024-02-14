from _utils import *
import pandas as pd

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

def get_capacities_heat(n, region):
    var = pd.Series()

    kwargs = {
        'groupby': n.statistics.groupers.get_name_bus_and_carrier,
        'nice_names': False,
    }

    var = pd.Series()

    capacities_heat = n.statistics.optimal_capacity(
        bus_carrier=[
            "urban central heat",
            "urban decentral heat",
            "rural heat"
        ],
        **kwargs,
    ).filter(like=region).groupby("carrier").sum().multiply(MW2GW)


    var["Capacity|Heat|Solar thermal"] = \
        capacities_heat.filter(like="solar thermal").sum()

    # !!! Missing in the Ariadne database
    #  We could be much more detailed for the heat sector (as for electricity)
    # if desired by Ariadne
    #
    var["Capacity|Heat|Biomass|w/ CCS"] = \
        capacities_heat.get('urban central solid biomass CHP CC') 
    var["Capacity|Heat|Biomass|w/o CCS"] = \
        capacities_heat.get('urban central solid biomass CHP') \
        +  capacities_heat.filter(like="biomass boiler").sum()
    
    var["Capacity|Heat|Biomass"] = \
        var["Capacity|Heat|Biomass|w/ CCS"] + \
        var["Capacity|Heat|Biomass|w/o CCS"]

    assert var["Capacity|Heat|Biomass"] == \
        capacities_heat.filter(like="biomass").sum()
    
    var["Capacity|Heat|Resistive heater"] = \
        capacities_heat.filter(like="resistive heater").sum()
    

    
    var["Capacity|Heat|Processes"] = \
        capacities_heat.get([
            "Fischer-Tropsch",
            "H2 Electrolysis",
            "H2 Fuel Cell",
            "Sabatier",
            "methanolisation"
        ]) 

    # !!! Missing in the Ariadne database




    # Capacity|Heat
    var["Capacity|Heat|Gas"] = \
        capacities_heat.filter(like="gas boiler").sum() \
        + capacities_heat.filter(like="gas CHP").sum()



    
    # var["Capacity|Heat|Geothermal"] =
    # ! Not implemented 

    var["Capacity|Heat|Heat pump"] = \
        capacities_heat.filter(like="heat pump").sum()


    var["Capacity|Heat|Oil"] = \
        capacities_heat.filter(like="oil boiler").sum()

    var["Capacity|Heat|Storage Converter"] = \
        capacities_heat.filter(like="water tanks discharger").sum()

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


def get_capacities_other(n, region):
    var = pd.Series()
    return var

def get_capacities_electricity(n, region):

    kwargs = {
        'groupby': n.statistics.groupers.get_name_bus_and_carrier,
        'nice_names': False,
    }

    var = pd.Series()

    capacities_electricity = n.statistics.optimal_capacity(
        bus_carrier=["AC", "low voltage"],
        **kwargs,
    ).filter(like=region).groupby("carrier").sum().drop( 
        # transmission capacities
        ["AC", "DC", "electricity distribution grid"],
    ).multiply(MW2GW)

    var["Capacity|Electricity|Biomass|w/ CCS"] = \
        capacities_electricity.get('urban central solid biomass CHP CC')
    
    var["Capacity|Electricity|Biomass|w/o CCS"] = \
        capacities_electricity.get('urban central solid biomass CHP')

    var["Capacity|Electricity|Biomass|Solids"] = \
        var[[
            "Capacity|Electricity|Biomass|w/ CCS",
            "Capacity|Electricity|Biomass|w/o CCS",
        ]].sum()

    # Ariadne does no checks, so we implement our own?
    assert var["Capacity|Electricity|Biomass|Solids"] == \
        capacities_electricity.filter(like="solid biomass").sum()

    var["Capacity|Electricity|Biomass"] = \
        var["Capacity|Electricity|Biomass|Solids"]


    var["Capacity|Electricity|Coal|Hard Coal"] = \
        capacities_electricity.get('coal')                                              

    var["Capacity|Electricity|Coal|Lignite"] = \
        capacities_electricity.get('lignite')
    
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
        var[[
            "Capacity|Electricity|Coal|Lignite",
            "Capacity|Electricity|Coal|Hard Coal",
        ]].sum()

    # var["Capacity|Electricity|Gas|CC|w/ CCS"] =
    # var["Capacity|Electricity|Gas|CC|w/o CCS"] =  
    # ! Not implemented, rarely used   

    var["Capacity|Electricity|Gas|CC"] = \
        capacities_electricity.get('CCGT')
    
    var["Capacity|Electricity|Gas|OC"] = \
        capacities_electricity.get('OCGT')
    
    var["Capacity|Electricity|Gas|w/ CCS"] =  \
        capacities_electricity.get('urban central gas CHP CC')  
    
    var["Capacity|Electricity|Gas|w/o CCS"] =  \
        capacities_electricity.get('urban central gas CHP') + \
        var[[
            "Capacity|Electricity|Gas|CC",
            "Capacity|Electricity|Gas|OC",
        ]].sum()
    

    var["Capacity|Electricity|Gas"] = \
        var[[
            "Capacity|Electricity|Gas|w/ CCS",
            "Capacity|Electricity|Gas|w/o CCS",
        ]].sum()

    # var["Capacity|Electricity|Geothermal"] = 
    # ! Not implemented

    var["Capacity|Electricity|Hydro"] = \
        capacities_electricity.get(['ror', 'hydro']).sum()
    # Q!: Not counting PHS here, because it is a true storage,
    # as opposed to hydro
     
    # var["Capacity|Electricity|Hydrogen|CC"] = 
    # ! Not implemented
    # var["Capacity|Electricity|Hydrogen|OC"] = 
    # Q: "H2-turbine"
    # Q: What about retrofitted gas power plants? -> Lisa

    var["Capacity|Electricity|Hydrogen|FC"] = \
        capacities_electricity.get("H2 Fuel Cell")

    var["Capacity|Electricity|Hydrogen"] = \
        var["Capacity|Electricity|Hydrogen|FC"]

    # var["Capacity|Electricity|Non-Renewable Waste"] = 
    # ! Not implemented

    var["Capacity|Electricity|Nuclear"] = \
        capacities_electricity.get("nuclear")

    # var["Capacity|Electricity|Ocean"] = 
    # ! Not implemented

    # var["Capacity|Electricity|Oil|w/ CCS"] = 
    # var["Capacity|Electricity|Oil|w/o CCS"] = 
    # ! Not implemented

    var["Capacity|Electricity|Oil"] = \
        capacities_electricity.get("oil")


    var["Capacity|Electricity|Solar|PV|Rooftop"] = \
        capacities_electricity.get("solar rooftop")
    
    var["Capacity|Electricity|Solar|PV|Open Field"] = \
        capacities_electricity.get("solar") 

    var["Capacity|Electricity|Solar|PV"] = \
        var[[
            "Capacity|Electricity|Solar|PV|Open Field",
            "Capacity|Electricity|Solar|PV|Rooftop",
        ]].sum()
    
    # var["Capacity|Electricity|Solar|CSP"] = 
    # ! not implemented

    var["Capacity|Electricity|Solar"] = \
        var["Capacity|Electricity|Solar|PV"]
    
    var["Capacity|Electricity|Wind|Offshore"] = \
        capacities_electricity.get(
            ["offwind", "offwind-ac", "offwind-dc"]
        ).sum()
    # !: take care of "offwind" -> "offwind-ac"/"offwind-dc"

    var["Capacity|Electricity|Wind|Onshore"] = \
        capacities_electricity.get("onwind")
    
    var["Capacity|Electricity|Wind"] = \
        capacities_electricity.filter(like="wind").sum()
    
    assert var["Capacity|Electricity|Wind"] == \
        var[[
            "Capacity|Electricity|Wind|Offshore",
            "Capacity|Electricity|Wind|Onshore",
        ]].sum()


    # var["Capacity|Electricity|Storage Converter|CAES"] = 
    # ! Not implemented

    var["Capacity|Electricity|Storage Converter|Hydro Dam Reservoir"] = \
        capacities_electricity.get('hydro')
    
    var["Capacity|Electricity|Storage Converter|Pump Hydro"] = \
        capacities_electricity.get('PHS')

    var["Capacity|Electricity|Storage Converter|Stationary Batteries"] = \
        capacities_electricity.get("battery discharger") + \
        capacities_electricity.get("home battery discharger")

    var["Capacity|Electricity|Storage Converter|Vehicles"] = \
        capacities_electricity.get("V2G")
    
    var["Capacity|Electricity|Storage Converter"] = \
        var[[
            "Capacity|Electricity|Storage Converter|Hydro Dam Reservoir",
            "Capacity|Electricity|Storage Converter|Pump Hydro",
            "Capacity|Electricity|Storage Converter|Stationary Batteries",
            "Capacity|Electricity|Storage Converter|Vehicles",
        ]].sum()
    

    storage_capacities = n.statistics.optimal_capacity(
        storage=True,
        **kwargs,
    ).filter(like=region).groupby("carrier").sum().multiply(MW2GW)
    # var["Capacity|Electricity|Storage Reservoir|CAES"] =
    # ! Not implemented
     
    var["Capacity|Electricity|Storage Reservoir|Hydro Dam Reservoir"] = \
        storage_capacities.get("hydro")

    var["Capacity|Electricity|Storage Reservoir|Pump Hydro"] = \
        storage_capacities.get("PHS")
    
    var["Capacity|Electricity|Storage Reservoir|Stationary Batteries"] = \
        storage_capacities.get(["battery", "home battery"]).sum()
    
    var["Capacity|Electricity|Storage Reservoir|Vehicles"] = \
        storage_capacities.get("Li ion") 

    var["Capacity|Electricity|Storage Reservoir"] = \
        var[[
            "Capacity|Electricity|Storage Reservoir|Hydro Dam Reservoir",
            "Capacity|Electricity|Storage Reservoir|Pump Hydro",
            "Capacity|Electricity|Storage Reservoir|Stationary Batteries",
            "Capacity|Electricity|Storage Reservoir|Vehicles",
        ]].sum()


    var["Capacity|Electricity"] = \
            var[[
            "Capacity|Electricity|Wind",
            "Capacity|Electricity|Solar",
            "Capacity|Electricity|Oil",
            "Capacity|Electricity|Coal",
            "Capacity|Electricity|Gas",
            "Capacity|Electricity|Biomass",
            "Capacity|Electricity|Hydro",
            "Capacity|Electricity|Hydrogen",
            "Capacity|Electricity|Nuclear",
            ]].sum()

    # Test if we forgot something
    _drop_idx = [
        col for col in [
            "PHS",
            "battery discharger",
            "home battery discharger",
            "V2G",
        ] if col in capacities_electricity.index
    ]
    assert abs(
        var["Capacity|Electricity"]
        - capacities_electricity.drop(_drop_idx).sum()
    ) < 10e-10
    
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