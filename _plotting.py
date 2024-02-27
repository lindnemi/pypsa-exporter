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

dfhybrid = model_df.loc[
    "Hybrid", "8Gt_Bal_v3", "Deutschland"
][pd.to_numeric(df.keys())]
dfhybrid.index.names = df.index.names

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
    # Check that all values have the same Unit
    assert df.columns.unique(level="Unit").size == 1
    # Simplify variable names
    # df.columns = pd.MultiIndex.from_tuples(
    #     map(
    #         lambda x: (x[0][(x[0].find("|") + 1):], x[1]), 
    #         df.columns,
    #     ),
    #     names=df.columns.names,
    # )
    # Simplify variable names even further
    df.columns = pd.Index(
        map(
            lambda x: x[0][(x[0].find("|") + 1):], 
            df.columns,
        ),
        name=df.columns.names[0],
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
    fig.savefig(f"{title}_single.png", bbox_inches="tight")
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
        "Detailed Primary Energy in PJ_yr",
        select_regex="Primary Energy\|[^|]*\|[^|]*$",
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
        "Detailed Final Energy in PJ_yr",
        select_regex="Final Energy\|[^|]*\|[^|]*$",
        #drop_regex="^(?!.*(Electricity)).+"
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
#plot_all_ariadne(dfhybrid)


# %%



def ariadne_subplot(df, ax, title, select_regex="", drop_regex=""):
    df = df.T.copy()
    if select_regex:
        df = df.filter(
            regex=select_regex,
        )
    if drop_regex:
        df = df.filter(
            regex=drop_regex,
        )
    # Check that all values have the same Unit
    assert df.columns.unique(level="Unit").size == 1

    # Simplify variable names
    df.columns = pd.Index(
        map(
            lambda x: x[0][(x[0].find("|") + 1):], 
            df.columns,
        ),
        name=df.columns.names[0],
    )

    return df.plot.area(ax=ax, title=title, legend=False)



def side_by_side_plot(df, dfhybrid, title, rshift=1.25, **kwargs):
    idx = df.index.intersection(dfhybrid.index)
    df = df.loc[idx]
    dfhybrid = dfhybrid.loc[idx]

    fig, axes = plt.subplots(ncols=2, sharey=True)
    ax = ariadne_subplot(df, axes[0], "PyPSA-Eur", **kwargs)
    ax2 = ariadne_subplot(dfhybrid, axes[1], "Hybrid", **kwargs)
    
    handles, labels = ax.get_legend_handles_labels()
    labels2 = ax2.get_legend_handles_labels()[1]
    assert labels == labels2

    fig.legend(
        reversed(handles), 
        reversed(labels), 
        bbox_to_anchor=(rshift,0.9)
    )
    fig.suptitle(title)
    title = title.replace(" ", "_")
    # fig = ax.get_figure()
    fig.savefig(f"{title}_comparison.png", bbox_inches="tight")
    return fig 

# %%

def plot_all_side_by_side(df, dfhybrid):
    side_by_side_plot(
        df,
        dfhybrid,
        "Primary Energy in PJ_yr",
        select_regex="Primary Energy\|[^|]*$",
        drop_regex="^(?!.*(Fossil)).+"
    )

    side_by_side_plot(
        df,
        dfhybrid,
        "Detailed Primary Energy in PJ_yr",
        select_regex="Primary Energy\|[^|]*\|[^|]*$",
        drop_regex="^(?!.*(CCS)).+"
    )

    side_by_side_plot(
        df,
        dfhybrid,
        "Secondary Energy in PJ_yr",
        select_regex="Secondary Energy\|[^|]*$",
    )

    side_by_side_plot(
        df,
        dfhybrid,
        "Detailed Secondary Energy in PJ_yr",
        # Secondary Energy|Something|Something (exactly two pipes)
        select_regex="Secondary Energy\|[^|]*\|[^|]*$",
        # Not ending in Fossil or Renewables (i.e., categories)
        drop_regex="^(?!.*(Fossil|Renewables|Losses)).+"
    )

    # Sectoral
    side_by_side_plot(
        df,
        dfhybrid,
        "Final Energy in PJ_yr",
        select_regex="Final Energy\|[^|]*$",
        drop_regex="^(?!.*(Electricity)).+"
    )

    side_by_side_plot(
        df,
        dfhybrid,
        "Detailed Final Energy in PJ_yr",
        select_regex="Final Energy\|[^|]*\|[^|]*$",
        rshift = 1.45,
        #drop_regex="^(?!.*(Electricity)).+"
    )



    side_by_side_plot(
        df,
        dfhybrid,
        "Capacity in GW",
        select_regex="Capacity\|[^|]*$",
    )


    side_by_side_plot(
        df,
        dfhybrid,
        "Detailed Capacity in GW",
        select_regex="Capacity\|[^|]*\|[^|]*$",
        drop_regex="^(?!.*(Reservoir|Converter)).+"
    )

#%%
side_by_side_plot(
    df,
    dfhybrid,
    "Detailed Capacity in GW",
    select_regex="Capacity\|[^|]*\|[^|]*$",
    drop_regex="^(?!.*(Reservoir|Converter)).+"
)
#%%
plot_all_side_by_side(df, dfhybrid)
# %%
