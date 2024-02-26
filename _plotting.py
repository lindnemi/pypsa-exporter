# %%
import pandas as pd
import matplotlib.pyplot as plt
import pyam
import os

df = pd.read_csv(
    "/home/micha/git/pypsa-exporter/pypsa_output.csv",
    index_col=["Model", "Scenario", "Region", "Variable", "Unit"]
).groupby(["Variable","Unit"]).sum()
df.columns = pd.to_numeric(df.columns)
# Set USERNAME and PASSWORD for the Ariadne DB
pyam.iiasa.set_config(
    os.environ["IIASA_USERNAME"], 
    os.environ["IIASA_PASSWORD"],
)

model_df= pyam.read_iiasa(
    "ariadne_intern",
    model="Hybrid",
    scenario="8Gt_Bal_v3",
).timeseries()

hybrid_df = model_df.loc[
    "Hybrid", "8Gt_Bal_v3", "Deutschland"
][pd.to_numeric(df.keys())]


# %%


idx_intersected = hybrid_df.index.intersection(df.index)

dfh = hybrid_df.loc[idx_intersected]
# %%
def ariadne_plot(df, title, select_regex="", drop_regex=""):
    df = df.T.copy()
    if select_regex:
        df = df.filter(
            regex=select_regex,
        )
    if drop_regex:
        df = df.filter(
            regex=drop_regex,
        )
    ax = df.plot.area()
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(
        reversed(handles), 
        reversed(labels), 
        bbox_to_anchor=(1.0,1.0)
    )
    ax.set_title(title)
    fig = ax.get_figure()
    fig.savefig(f"{title}.png", bbox_inches="tight")
    return ax 


# %%
def plot_all_ariadne(df):
    ariadne_plot(
        df,
        "Primary Energy in PJ_yr",
        select_regex="Primary Energy\|[^|]*$",
        drop_regex="^(?!.*(Fossil)).+"
    )
    ariadne_plot(
        df,
        "Secondary Energy in PJ_yr",
        select_regex="Secondary Energy\|[^|]*$",
    )

    ariadne_plot(
        df,
        "Detailed Secondary Energy in PJ_yr",
        # Secondary Energy|Something|Something (exactly two pipes)
        select_regex="Secondary Energy\|[^|]*\|[^|]*$",
        # Not ending in Fossil or Renewables (i.e., categories)
        drop_regex="^(?!.*(Fossil|Renewables)).+"
    )

    # Sectoral
    ariadne_plot(
        df,
        "Final Energy in PJ_yr",
        select_regex="Final Energy\|[^|]*$",
        drop_regex="^(?!.*(Electricity)).+"
    )


    ariadne_plot(
        df,
        "Capacity in GW",
        select_regex="Capacity\|[^|]*$",
    )


    ariadne_plot(
        df,
        "Detailed Capacity in GW",
        select_regex="Capacity\|[^|]*\|[^|]*$",
            drop_regex="^(?!.*(Reservoir|Converter)).+"
    )
# %%
plot_all_ariadne(df)
plot_all_ariadne(dfh)
# %%
plot_all_ariadne(df.loc[df.index.intersection(dfh.index)].subtract(dfh))
# %%

df.loc[df.index.intersection(dfh.index)].subtract(dfh).T.plot().legend(bbox_to_anchor=)(1,1)
# %%

df.loc[df.index.intersection(dfh.index)].subtract(
    dfh
).T.plot().legend(bbox_to_anchor=(1,1))


# %%
plot_all_ariadne(
    abs(df.loc[df.index.intersection(dfh.index)].subtract(dfh))

)

# %%
