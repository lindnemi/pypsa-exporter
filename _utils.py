MW2GW = 1e-3
t2Mt = 1e-6
MWh2PJ = 3.6e-6

import numpy as np
import pandas as pd
import math
# %% more helpers





# %% More abstractions

def get_CHP_elec_fraction(n, carrier, region):
    assert "CHP" in carrier

    elec_output = sum_link_output(n, carrier, region)
    heat_output = sum_link_output(n, carrier, region, port="p2")

    return elec_output / (elec_output + heat_output)

def _get_t_sum(df, df_t, carrier, region, snapshot_weightings, port):
    if type(carrier) == list:
        return sum(
            [
                _get_t_sum(
                    df, df_t, car, region, snapshot_weightings, port
                ) for car in carrier
            ]            
        )
    idx = df[df.carrier == carrier].filter(like=region, axis=0).index

    return df_t[port][idx].multiply(
        snapshot_weightings,
        axis=0,
    ).values.sum()


def sum_link_input(n, carrier, region, port="p0"):
    return MWh2PJ * _get_t_sum(
        n.links,
        n.links_t,
        carrier,
        region,
        n.snapshot_weightings.generators,
        port,
    )

def sum_link_output(n, carrier, region, port="p1"):
    return -1 * sum_link_input(
        n,
        carrier,
        region,
        port=port,
    )

def sum_load(n, carrier, region):
    return MWh2PJ * _get_t_sum(
        n.loads,
        n.loads_t,
        carrier,
        region,
        n.snapshot_weightings.generators,
        "p",
    )   

def sum_generator_output(n, carrier, region):
    return -1 * MWh2PJ * _get_t_sum(
        n.generators,
        n.generators_t,
        carrier,
        region,
        n.snapshot_weightings.generators,
        "p",
    )

def sum_storage_unit_output(n, carrier, region):
    return -1 * MWh2PJ * _get_t_sum(
        n.storage_units,
        n.storage_units_t,
        carrier,
        region,
        n.snapshot_weightings.generators,
        "p",
    )

def sum_co2(n, carrier, region):
    if type(carrier) == list:
        return sum([sum_co2(n, car, region) for car in carrier])
    try:
        port = n.links.groupby(
            "carrier"
        ).first().loc[
            carrier
        ].filter(
            like="bus"
        ).tolist().index("co2 atmosphere")
    except KeyError:
        print(
            "Warning: carrier `", carrier, "` not found in network.links.carrier!",
            sep="")
        return 0

    return -1 * t2Mt * _get_t_sum(
        n.links,
        n.links_t,
        carrier,
        region,
        n.snapshot_weightings.generators,
        f"p{port}",
    )


#%% CO2
#n50.statistics.supply(bus_carrier="co2 stored", groupby=groupby).filter(like="DE").groupby("carrier").sum()
# n50.statistics.withdrawal(bus_carrier="co2", groupby=groupby).filter(like="DE").groupby("carrier").sum()
def get_co2(n, carrier, region):
    # including international bunker fuels and negative emissions
    if type(carrier) == list:
        return sum([get_co2(n, car, region) for car in carrier])
    
    df = n.links[n.links.carrier == carrier].filter(like=region, axis=0)

    co2 = 0
    for port in [col[3:] for col in df if col.startswith("bus")]:
        links = df.index[df[f"bus{port}"] == "co2 atmosphere"]
        co2 -= n.links_t[f"p{port}"][links].multiply(
            n.snapshot_weightings.generators,
            axis=0,
        ).values.sum()
    return t2Mt * co2



def get_link_production(n, carrier, region):
    if type(carrier) == list:
        return sum(
            [get_link_production(n, car, region) for car in carrier]
        )
    
    df = n.links[n.links.carrier == carrier].filter(
        like=region, 
        axis=0,
    )

    return -1 * MWh2PJ * n.links_t.p1[df.index].multiply(
        n.snapshot_weightings.generators,
        axis=0,
    ).values.sum()


def get_load_consumption(n, carrier, region):
    if type(carrier) == list:
        return sum(
            [get_load_consumption(n, car, region) for car in carrier]
        )
    
    df = n.loads[n.loads.carrier == carrier].filter(
        like=region, 
        axis=0,
    )

    return MWh2PJ * n.loads_t.p[df.index].multiply(
        n.snapshot_weightings.generators,
        axis=0,
    ).values.sum()



def get_link_consumption(n, carrier, region):
    if type(carrier) == list:
        return sum(
            [get_link_consumption(n, car, region) for car in carrier]
        )
    
    df = n.links[n.links.carrier == carrier].filter(
        like=region, 
        axis=0,
    )

    return MWh2PJ * n.links_t.p0[df.index].multiply(
        n.snapshot_weightings.generators,
        axis=0,
    ).values.sum()





def get_all_emitters(n):
    df = n.links
    carriers = []
    for port in [col[3:] for col in df if col.startswith("bus")]:
        links = df.index[df[f"bus{port}"] == "co2 atmosphere"]
        carriers.append(df.loc[links].carrier.unique())
    return carriers

def get_all_carriers_to_bus(n, bus):
    df = n.links
    carriers = []
    for port in [col[3:] for col in df if col.startswith("bus")]:
        links = df.index[df[f"bus{port}"] == bus]
        carriers.append(df.loc[links].carrier.unique())
    return carriers


def get_total_co2(n, region):
    # including international bunker fuels and negative emissions 
    df = n.links.filter(like=region, axis=0)
    co2 = 0
    for port in [col[3:] for col in df if col.startswith("bus")]:
        links = df.index[df[f"bus{port}"] == "co2 atmosphere"]
        co2 -= n.links_t[f"p{port}"][links].multiply(
            n.snapshot_weightings.generators,
            axis=0,
        ).values.sum()
    return t2Mt * co2


def get_cols(df, carrier, like="bus"):
    return df[
        df.carrier.str.contains(carrier)
    ].filter(like=like)

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
    
# groupby=n.statistics.groupers.get_name_bus_and_carrier
# n.statistics.optimal_capacity( storage=True).loc["Store"]
    
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

# groupby=n.statistics.groupers.get_name_bus_and_carrier
# n.statistics.optimal_capacity(bus_carrier="urban central heat", groupby=groupby).filter(like="DE").groupby("carrier").sum()

def get_nodal_flows(n, bus_carrier, region, query='index == index or index != index'):
    groupby = n.statistics.groupers.get_name_bus_and_carrier

    result = n.statistics.withdrawal(
        bus_carrier=bus_carrier, 
        groupby=groupby,
        aggregate_time=False,
        ).filter(
            like=region,
            axis=0,
        ).query(query).groupby("bus").sum().T 
    
    return result

def price_load(n, load_carrier, region):

    load = n.loads[(n.loads.carrier == load_carrier) & (n.loads.bus.str.contains(region))]
    if n.loads_t.p[load.index].sum().sum() == 0:
        return np.nan, 0
    result = (n.loads_t.p[load.index] * n.buses_t.marginal_price[load.bus].values).sum().sum()
    result /= n.loads_t.p[load.index].sum().sum()
    return result, n.loads_t.p[load.index].sum().sum()

def costs_gen_links(n, region, carrier, gen_bus="p1"):

    # CAPEX
    links = n.links[(n.links.carrier == carrier) & (n.links.index.str.contains(region))]
    capacity_expansion = links.p_nom_opt - links.p_nom
    capex = (capacity_expansion * n.links.capital_cost[capacity_expansion.index]).sum()

    # OPEX
    gen = abs(n.links_t[gen_bus][links.index].multiply(n.snapshot_weightings.generators, axis="index")).sum()
    if gen.empty or gen.sum() == 0:
        return np.nan, 0
    opex = (gen * n.links.marginal_cost[gen.index]).sum()

    # input costs and output revenues other than main generation @ gen_bus
    sum = 0
    for i in range(0,4):
        if f"p{i}" == gen_bus:
            continue
        elif links.empty:
            break
        elif n.links.loc[links.index][f"bus{i}"].iloc[0] == "":
            break
        else:
            if links[f"bus{i}"].str.contains("co2").iloc[0]:
                sum -= (n.links_t[f"p{i}"][links.index] * n.buses_t.marginal_price[links[f"bus{i}"]].values
                    ).multiply(n.snapshot_weightings.generators, axis="index"
                               ).sum().sum()
            else:
                sum += (n.links_t[f"p{i}"][links.index] * n.buses_t.marginal_price[links[f"bus{i}"]].values
                    ).multiply(n.snapshot_weightings.generators, axis="index"
                               ).sum().sum()
              
    result = (capex + opex + sum) / gen.sum()
    return result, gen.sum()

def get_weighted_costs_links(carriers, n, region):
    numerator = 0
    denominator = 0
    
    for c in carriers:    
        if not math.isnan(costs_gen_links(n, region, c)[0]):
            numerator += costs_gen_links(n, region, c)[0] * costs_gen_links(n, region, c)[1]
            denominator += costs_gen_links(n, region, c)[1]
        
    if denominator == 0:
        return np.nan
    result = numerator / denominator 
    return result

def costs_gen_generators(n, region, carrier):

    # CAPEX
    gens = n.generators[(n.generators.carrier == carrier) & (n.generators.bus.str.contains(region))]
    capacity_expansion = gens.p_nom_opt - gens.p_nom
    capex = (capacity_expansion * n.generators.capital_cost[capacity_expansion.index]).sum()

    # OPEX
    gen = n.generators_t.p[gens.index].multiply(n.snapshot_weightings.generators, axis="index").sum()
    if gen.empty or gen.sum() == 0:
        return np.nan, 0
    opex = (gen * n.generators.marginal_cost[gen.index]).sum()
              
    result = (capex + opex) / gen.sum()
    return result, gen.sum()

import pandas as pd
import numpy as np
import math

def get_weighted_costs(costs, flows):

    cleaned_costs = []
    cleaned_flows = []

    for cost, flow in zip(costs, flows):
        if not math.isnan(cost) and not math.isnan(flow) and flow != 0:
            cleaned_costs.append(cost)
            cleaned_flows.append(flow)
    
    if not cleaned_costs or not cleaned_flows:
        return np.nan
    
    df_cleaned = pd.DataFrame({'costs': cleaned_costs, 'flows': cleaned_flows})
    result = (df_cleaned["costs"] * df_cleaned["flows"]).sum() / df_cleaned["flows"].sum()
    return result
