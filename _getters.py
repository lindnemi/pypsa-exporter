from _utils import *
import pandas as pd
from numpy import isclose

MWh2GJ = 3.6
TWh2PJ = 3.6
MWh2PJ = 3.6e-6

#n.statistics.withdrawal(bus_carrier="land transport oil", groupby=groupby, aggregate_time=False).filter(like="DE1 0",axis=0)

# first look at final energy then get variables from presentation
# convert EURXXXX to EUR2020
def get_ariadne_prices(n, region):
    var = {}

    nodal_flows_lw = get_nodal_flows(
        n, "low voltage", "DE",
        query = "not carrier.str.contains('agriculture')"
                "& not carrier.str.contains('industry')"
                "& not carrier.str.contains('urban central')"
            )

    nodal_prices_lw = n.buses_t.marginal_price[nodal_flows_lw.columns] 

    # electricity price at the final level in the residential sector. Prices should include the effect of carbon prices.
    var["Price|Final Energy|Residential|Electricity"] = \
        nodal_flows_lw.mul(
            nodal_prices_lw
        ).sum().div(
            nodal_flows_lw.sum()
        ).div(MWh2GJ) # TODO should this be divided by MWh2GJ ???
    
    # vars: Tier 1, Category: energy(price)

    nodal_flows_bm = get_nodal_flows(
        n, "solid biomass", "DE")
    nodal_prices_bm = n.buses_t.marginal_price[nodal_flows_bm.columns]

    var["Price|Primary Energy|Biomass"] = \
        nodal_flows_bm.mul(
            nodal_prices_bm
        ).sum().div(
            nodal_flows_bm.sum()
        ).div(MWh2GJ)
    
    # Price|Primary Energy|Coal
    # is coal also lignite?
    nodal_flows_coal = get_nodal_flows(
        n, "coal", "DE")
    nodal_prices_coal = n.buses_t.marginal_price[nodal_flows_coal.columns]

    var["Price|Primary Energy|Coal"] = \
        nodal_flows_coal.mul(
            nodal_prices_coal
        ).sum().div(
            nodal_flows_coal.sum()
        ).div(MWh2GJ)
    
    # Price|Primary Energy|Gas
    nodal_flows_gas = get_nodal_flows(
        n, "gas", "DE")
    nodal_prices_gas = n.buses_t.marginal_price[nodal_flows_gas.columns]

    var["Price|Primary Energy|Gas"] = \
        nodal_flows_gas.mul(
            nodal_prices_gas
        ).sum().div(
            nodal_flows_gas.sum()
        ).div(MWh2GJ)
    
    # Price|Primary Energy|Oil
    nodal_flows_oil = get_nodal_flows(
        n, "oil", "DE")
    nodal_prices_oil = n.buses_t.marginal_price[nodal_flows_oil.columns]

    var["Price|Primary Energy|Oil"] = \
        nodal_flows_oil.mul(
            nodal_prices_oil
        ).sum().div(
            nodal_flows_oil.sum()
        ).div(MWh2GJ)
    
    # Price|Secondary Energy|Electricity
    # Price|Secondary Energy|Gases|Natural Gas
    # Price|Secondary Energy|Gases|Hydrogen
    # Price|Secondary Energy|Gases|Biomass
    # Price|Secondary Energy|Gases|Efuel
    # Price|Secondary Energy|Hydrogen



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
        capacities_electricity.get('coal', 0)                                              

    var["Capacity|Electricity|Coal|Lignite"] = \
        capacities_electricity.get('lignite', 0)
    
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
        pd.Series({
            c: capacities_electricity.get(c) 
            for c in ["ror", "hydro"]
        }).sum()
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
        capacities_electricity.get("nuclear", 0)

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
        capacities_electricity.get("V2G", 0)
    
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
        pd.Series({
            c: storage_capacities.get(c) 
            for c in ["battery", "home battery"]
        }).sum()
    
    var["Capacity|Electricity|Storage Reservoir|Vehicles"] = \
        storage_capacities.get("Li ion", 0) 

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
    assert isclose(
        var["Capacity|Electricity"],
        capacities_electricity.drop(_drop_idx).sum(),
    )
    
    return var

def get_capacities_heat(n, region):

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
    ).filter(like=region).groupby("carrier").sum().drop(
        ["urban central heat vent"]
    ).multiply(MW2GW)


    var["Capacity|Heat|Solar thermal"] = \
        capacities_heat.filter(like="solar thermal").sum()
    # TODO Ariadne DB distinguishes between Heat and Decentral Heat!
    # We should probably change all capacities here?!

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

    assert isclose(
        var["Capacity|Heat|Biomass"],
        capacities_heat.filter(like="biomass").sum()
    )
    
    var["Capacity|Heat|Resistive heater"] = \
        capacities_heat.filter(like="resistive heater").sum()
    
    var["Capacity|Heat|Processes"] = \
        pd.Series({c: capacities_heat.get(c) for c in [
                "Fischer-Tropsch",
                "H2 Electrolysis",
                "H2 Fuel Cell",
                "Sabatier",
                "methanolisation",
        ]}).sum()

    # !!! Missing in the Ariadne database

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

    storage_capacities = n.statistics.optimal_capacity(
        storage=True,
        **kwargs,
    ).filter(like=region).groupby("carrier").sum().multiply(MW2GW)

    var["Capacity|Heat|Storage Reservoir"] = \
        storage_capacities.filter(like="water tanks").sum()

    # Q: New technologies get added as we develop the model.
    # It would be helpful to have some double-checking, e.g.,
    # by asserting that every technology gets added,
    # or by computing the total independtly of the subtotals, 
    # and summing the subcategories to compare to the total
    # !: For now, check the totals by summing in two different ways
    
    var["Capacity|Heat"] = (
        var["Capacity|Heat|Solar thermal"] +
        var["Capacity|Heat|Resistive heater"] +
        var["Capacity|Heat|Biomass"] +
        var["Capacity|Heat|Oil"] +
        var["Capacity|Heat|Gas"] +
        var["Capacity|Heat|Processes"] +
        #var["Capacity|Heat|Hydrogen"] +
        var["Capacity|Heat|Heat pump"]
    )

    assert isclose(
        var["Capacity|Heat"],
        capacities_heat[
            # exclude storage converters (i.e., dischargers)
            ~capacities_heat.index.str.contains("discharger")
        ].sum()
    )

    return var


def get_capacities_other(n, region):
    kwargs = {
        'groupby': n.statistics.groupers.get_name_bus_and_carrier,
        'nice_names': False,
    }

    var = pd.Series()

    capacities_h2 = n.statistics.optimal_capacity(
        bus_carrier="H2",
        **kwargs,
    ).filter(
        like=region
    ).groupby("carrier").sum().multiply(MW2GW)

    var["Capacity|Hydrogen|Gas|w/ CCS"] = \
        capacities_h2.get("SMR CC")
    
    var["Capacity|Hydrogen|Gas|w/o CCS"] = \
        capacities_h2.get("SMR")
    
    var["Capacity|Hydrogen|Gas"] = \
        capacities_h2.filter(like="SMR").sum()
    
    assert var["Capacity|Hydrogen|Gas"] == \
        var["Capacity|Hydrogen|Gas|w/ CCS"] + \
        var["Capacity|Hydrogen|Gas|w/o CCS"] 
    
    var["Capacity|Hydrogen|Electricity"] = \
        capacities_h2.get("H2 Electrolysis", 0)

    var["Capacity|Hydrogen"] = (
        var["Capacity|Hydrogen|Electricity"]
        + var["Capacity|Hydrogen|Gas"]
    )
    assert isclose(
        var["Capacity|Hydrogen"],
        capacities_h2.reindex([
            "H2 Electrolysis",
            "SMR",
            "SMR CC",
        ]).sum(), # if technology not build, reindex returns NaN
    )

    storage_capacities = n.statistics.optimal_capacity(
        storage=True,
        **kwargs,
    ).filter(like=region).groupby("carrier").sum().multiply(MW2GW)

    var["Capacity|Hydrogen|Reservoir"] = \
        storage_capacities.get("H2")



    capacities_gas = n.statistics.optimal_capacity(
        bus_carrier="gas",
        **kwargs,
    ).filter(
        like=region
    ).groupby("carrier").sum().drop(
        # Drop Import (Generator, gas), Storage (Store, gas), 
        # and Transmission capacities
        ["gas", "gas pipeline", "gas pipeline new"]
    ).multiply(MW2GW)

    var["Capacity|Gases|Hydrogen"] = \
        capacities_gas.get("Sabatier", 0)
    
    var["Capacity|Gases|Biomass"] = \
        capacities_gas.reindex([
            "biogas to gas",
            "biogas to gas CC",
        ]).sum()

    var["Capacity|Gases"] = (
        var["Capacity|Gases|Hydrogen"] +
        var["Capacity|Gases|Biomass"] 
    )

    assert isclose(
        var["Capacity|Gases"],
        capacities_gas.sum(),
    )


    capacities_liquids = n.statistics.optimal_capacity(
        bus_carrier=["oil", "methanol"],
        **kwargs,
    ).filter(
        like=region
    ).groupby("carrier").sum().multiply(MW2GW)

    var["Capacity|Liquids|Hydrogen"] = \
        capacities_liquids.get("Fischer-Tropsch") + \
        capacities_liquids.get("methanolisation", 0)
    
    var["Capacity|Liquids"] = var["Capacity|Liquids|Hydrogen"]

    assert isclose(
        var["Capacity|Liquids"], capacities_liquids.sum(),
    )

    return var 

def get_primary_energy(n, region):
    kwargs = {
        'groupby': n.statistics.groupers.get_name_bus_and_carrier,
        'nice_names': False,
    }

    var = pd.Series()

    EU_oil_supply = n.statistics.supply(bus_carrier="oil")
    oil_fossil_fraction = (
        EU_oil_supply.get("Generator").get("oil")
        / EU_oil_supply.sum()
    )
    
    oil_usage = n.statistics.withdrawal(
        bus_carrier="oil", 
        **kwargs
    ).filter(
        like=region
    ).groupby(
        "carrier"
    ).sum().multiply(oil_fossil_fraction).multiply(MWh2PJ)

        ## Primary Energy

    var["Primary Energy|Oil|Heat"] = \
        oil_usage.filter(like="oil boiler").sum()

    
    var["Primary Energy|Oil|Electricity"] = \
        oil_usage.get("oil")
    # This will get the oil store as well, but it should be 0
    
    var["Primary Energy|Oil"] = (
        var["Primary Energy|Oil|Electricity"] 
        + var["Primary Energy|Oil|Heat"] 
        + oil_usage.reindex(
            [
                "land transport oil",
                "agriculture machinery oil",
                "shipping oil",
                "kerosene for aviation",
                "naphtha for industry"
            ],
        ).sum()
    )   
    assert isclose(var["Primary Energy|Oil"], oil_usage.sum())

    # !! TODO since gas is now regionally resolved we 
    # compute the reginoal gas supply 
    regional_gas_supply = n.statistics.supply(
        bus_carrier="gas", 
        **kwargs,
    ).filter(
        like=region
    ).groupby(
        ["component", "carrier"]
    ).sum().drop([
        "Store",
        ("Link", "gas pipeline"),
        ("Link", "gas pipeline new"),
    ])

    gas_fossil_fraction = (
        regional_gas_supply.get("Generator").get("gas")
        / regional_gas_supply.sum()
    )
    # Eventhough biogas gets routed through the EU gas bus,
    # it should be counted separately as Primary Energy|Biomass
    gas_usage = n.statistics.withdrawal(
        bus_carrier="gas", 
        **kwargs,
    ).filter(
        like=region
    ).groupby(
        ["component", "carrier"],
    ).sum().drop([
        "Store",
        ("Link", "gas pipeline"),
        ("Link", "gas pipeline new"),
    ]).groupby(
        "carrier"
    ).sum().multiply(gas_fossil_fraction).multiply(MWh2PJ)

    var["Primary Energy|Gas|Heat"] = \
        gas_usage.filter(like="gas boiler").sum()
    
    var["Primary Energy|Gas|Electricity"] = \
        gas_usage.reindex(
            [
                'CCGT',
                'OCGT',
                'urban central gas CHP',
                'urban central gas CHP CC',
            ],
        ).sum()
    # Adding the CHPs to electricity, see also Capacity|Electricity|Gas
    # Q: pypsa to iamc SPLITS the CHPS. TODO Should we do the same?

    var["Primary Energy|Gas|Hydrogen"] = \
        gas_usage.filter(like="SMR").sum()
    
    var["Primary Energy|Gas"] = (
        var["Primary Energy|Gas|Heat"]
        + var["Primary Energy|Gas|Electricity"]
        + var["Primary Energy|Gas|Hydrogen"] 
        + gas_usage.filter(like="gas for industry").sum()
    )

    assert isclose(
        var["Primary Energy|Gas"],
        gas_usage.sum(),
    )
    # ! There are CC sub-categories that could be used

    coal_usage = n.statistics.withdrawal(
        bus_carrier=["lignite", "coal"], 
        **kwargs,
    ).filter(
        like=region
    ).groupby(
        "carrier"
    ).sum().multiply(MWh2PJ)

    var["Primary Energy|Coal|Hard Coal"] = \
        coal_usage.get("coal", 0)

    var["Primary Energy|Coal|Lignite"] = \
        coal_usage.get("lignite", 0)
    
    var["Primary Energy|Coal|Electricity"] = \
        var["Primary Energy|Coal|Hard Coal"] + \
        var["Primary Energy|Coal|Lignite"]
    
    var["Primary Energy|Coal"] = (
        var["Primary Energy|Coal|Electricity"] 
        + coal_usage.get("coal for industry", 0)
    )
    
    assert isclose(var["Primary Energy|Coal"], coal_usage.sum())

    var["Primary Energy|Fossil"] = (
        var["Primary Energy|Coal"]
        + var["Primary Energy|Gas"]
        + var["Primary Energy|Oil"]
    )

    biomass_usage = n.statistics.withdrawal(
        bus_carrier=["solid biomass", "biogas"], 
        **kwargs,
    ).filter(
        like=region
    ).groupby(
        "carrier"
    ).sum().multiply(MWh2PJ)

    
    var["Primary Energy|Biomass|w/ CCS"] = \
        biomass_usage[biomass_usage.index.str.contains("CC")].sum()
    
    var["Primary Energy|Biomass|w/o CCS"] = \
        biomass_usage[~biomass_usage.index.str.contains("CC")].sum()
    
    var["Primary Energy|Biomass|Electricity"] = \
        biomass_usage.filter(like="CHP").sum()
    # !!! ADDING CHP ONLY TO ELECTRICITY INSTEAD OF SPLITTING, CORRECT?
    var["Primary Energy|Biomass|Heat"] = \
        biomass_usage.filter(like="boiler").sum()
    
    # var["Primary Energy|Biomass|Gases"] = \
    # Gases are only E-Fueld in AriadneDB
    # Not possibly in an easy way because biogas to gas goes to the
    # gas bus, where it mixes with fossil imports
    
    var["Primary Energy|Biomass"] = (
        var["Primary Energy|Biomass|Electricity"]
        + var["Primary Energy|Biomass|Heat"]
        + biomass_usage.filter(like="solid biomass for industry").sum()
        + biomass_usage.filter(like="biogas to gas").sum()
    )
    
        
    assert isclose(
        var["Primary Energy|Biomass"],
        biomass_usage.sum(),
    )

    var["Primary Energy|Nuclear"] = \
        n.statistics.withdrawal(
            bus_carrier=["uranium"], 
            **kwargs,
        ).filter(
            like=region
        ).groupby(
            "carrier"
        ).sum().multiply(MWh2PJ).get("nuclear", 0)


    # ! This should basically be equivalent to secondary energy
    renewable_electricity = n.statistics.supply(
        bus_carrier=["AC", "low voltage"],
        **kwargs,
    ).drop([
        # Assuming renewables are only generators and StorageUnits 
        "Link", "Line"
    ]).filter(like=region).groupby("carrier").sum().multiply(MWh2PJ)

    
    solar_thermal_heat = n.statistics.supply(
        bus_carrier=[
            "urban decentral heat", 
            "urban central heat", 
            "rural heat",
        ],
        **kwargs,
    ).filter(
        like=region
    ).groupby("carrier").sum().filter(
        like="solar thermal"
    ).multiply(MWh2PJ).sum()

    var["Primary Energy|Hydro"] = \
        renewable_electricity.get([
            "ror", "PHS", "hydro",
        ]).sum()
    
    var["Primary Energy|Solar"] = \
        renewable_electricity.filter(like="solar").sum() + \
        solar_thermal_heat

        
    var["Primary Energy|Wind"] = \
        renewable_electricity.filter(like="wind").sum()

    assert isclose(
        renewable_electricity.sum(),
        (
            var["Primary Energy|Hydro"] 
            + var["Primary Energy|Solar"] 
            + var["Primary Energy|Wind"]
        )
    )
    # Primary Energy|Other
    # Not implemented

    return var


def get_secondary_energy(n, region):
    var = pd.Series()

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

def get_final_energy(n, region, industry_demand):
    var = {}

    # industry
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



# convert EURXXXX to EUR2020
def get_prices(n, region):
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


def get_emissions(n, region):
    var = pd.Series()

        
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


    return var 