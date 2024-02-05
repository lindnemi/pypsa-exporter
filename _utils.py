MW2GW = 1e-3
t2Mt = 1e-6
MWh2PJ=3.6e-6 

#%% CO2

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

    