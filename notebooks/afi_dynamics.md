---
jupytext:
  formats: ipynb,md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.14.0
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
---

# LBD

```{code-cell} ipython3
:tags: []

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from rurec.reseng.nbd import Nbd
nbd = Nbd('rurec')
```

## Aggregate industry size

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: []
---
df = pd.read_excel(nbd.root / 'data/rdc/2019.xlsx', 'emp_pay_wage_agg_by_year', 
                   index_col=0, header=[0, 1])

fig, ax = plt.subplots()
fig.set_dpi(150)
lines = []

lines += ax.plot(df[('emp_ann', 'sum')] / 1000, ls='--', c='k', label='non-ag employment (left)')
ax.set_ylim(0, 100)
ax.set_yticks(range(0, 100, 20))
ax.set_ylabel('Number of employees, thousands')

ax2 = ax.twinx()
lines += ax2.plot(df[('r_p941', 'sum')] / 1e6, c='k', label='non-ag payroll (right)')
lines += ax2.plot(df[('r_p943', 'sum')] / 1e6, c='gray', label='ag payroll (right)')
ax2.set_ylim(0, 5)
ax2.set_ylabel('Payroll, billions $')
ax.set_xticks(range(1992, 2018, 5))

ax.legend(lines, [l.get_label() for l in lines], loc='upper left')
ax.grid()
ax.set_title('Aggregate industry size');
```

## Average establishment size

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: []
---
df = pd.read_excel(nbd.root / 'data/rdc/2019.xlsx', 'emp_pay_wage_agg_by_year', 
                   index_col=0, header=[0, 1])

fig, ax = plt.subplots()
fig.set_dpi(150)
lines = []

lines += ax.plot(df[('emp_ann', 'mean')], ls='--', c='k', label='non-ag employment (left)')
ax.set_ylim(0, 25)
ax.set_ylabel('Number of employees')

ax2 = ax.twinx()
lines += ax2.plot(df[('r_p941', 'mean')], c='k', label='non-ag payroll (right)')
lines += ax2.plot(df[('r_p943', 'mean')], c='gray', label='ag payroll (right)')
ax2.set_ylim(0, 1000)
ax2.set_ylabel('Payroll, thousands $')
ax.set_xticks(range(1992, 2018, 5))


ax.legend(lines, [l.get_label() for l in lines], loc='upper left')
ax.grid()
ax.set_title('Average establishment size');
```

## Nonag wage

```{code-cell} ipython3
:tags: []

df = pd.read_excel(nbd.root / 'data/rdc/2019.xlsx', 'emp_pay_wage_agg_by_year', 
                   index_col=0, header=[0, 1])

fig, ax = plt.subplots()
fig.set_dpi(150)

ax.plot(df[('r_wage_q1', 'mean')] / 1000, c='k')
ax.set_xticks(range(1992, 2018, 5))
ax.set_ylim(0, 40)
ax.set_yticks(range(0, 40, 10))
ax.set_ylabel('Thousands $')

ax.grid()
ax.set_title('Average real wage of non-ag employees in 1151');
```

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: []
---

fig, ax = plt.subplots()
fig.set_dpi(150)
lines = []

lines += ax.plot(df[('emp_ann', 'mean')], ls='--', c='k', label='non-ag employment (left)')
ax.set_ylim(0, 25)
ax.set_ylabel('Number of employees')

ax2 = ax.twinx()
lines += ax2.plot(df[('r_p941', 'mean')], c='k', label='non-ag payroll (right)')
lines += ax2.plot(df[('r_p943', 'mean')], c='gray', label='ag payroll (right)')
ax2.set_ylim(0, 1000)
ax2.set_ylabel('Payroll, thousands $')
ax.set_xticks(range(1992, 2018, 5))


ax.legend(lines, [l.get_label() for l in lines], loc='upper left')
ax.grid()
ax.set_title('Average establishment size');
```

## Growth in 1151 and farm labor

```{code-cell} ipython3
:tags: []

def get_deflator():
    d = pd.read_csv(nbd.root / 'data/A191RD3A086NBEA.csv').rename(columns={'DATE': 'year', 'A191RD3A086NBEA': 'deflator'})
    d['year'] = d['year'].str[:4].astype('int16')
    d = d.sort_values('year', ignore_index=True)
    # normalize latest year = 1
    d['deflator'] /= d['deflator'].iloc[-1]
    return d
get_deflator().tail()
```

Farm labor expenses in thousands of nominal dollars, manually copied from national Ag Census tables. [2017](https://www.nass.usda.gov/Publications/AgCensus/2017/Full_Report/Volume_1,_Chapter_1_US/) and [before](https://agcensus.library.cornell.edu/).

```{code-cell} ipython3
:tags: []

import io
df = pd.read_csv(io.StringIO('''
year	hired_labor	contract_labor
1992	12961639	2323904
1997	15457896	2959005
2002	18568446	3451190
2007	21877661	4514166
2012	26986669	6473989
2017	31635981	7594815
'''), sep='\t')

df = df.merge(get_deflator(), 'left', 'year')

df['hired_labor'] /= df['deflator']
df['contract_labor'] /= df['deflator']

df_farm = df
df
```

```{code-cell} ipython3
:tags: []

df = pd.read_excel(nbd.root / 'data/rdc/2019.xlsx', 'emp_pay_wage_agg_by_year', 
                   index_col=0, header=[0, 1])
df.columns = df.columns.to_flat_index()
df = df.reset_index().rename(columns={'yr': 'year', ('r_p941', 'sum'): '1151_nonag_pay', ('r_p943', 'sum'): '1151_ag_pay'}).merge(df_farm, 'outer', 'year')
df = df.set_index('year')
df = df[['hired_labor', 'contract_labor', '1151_nonag_pay', '1151_ag_pay']]
df = df.apply(lambda c: c / c[1992], 0)
# df.tail(6)

fig, ax = plt.subplots()
fig.set_dpi(150)

ax.plot(df['hired_labor'].dropna(), c='k', ls='-', label='farm hired labor')
ax.plot(df['contract_labor'].dropna(), c='gray', ls='-', label='farm contract labor')
ax.plot(df['1151_nonag_pay'], c='k', ls='--', label='services nonag pay')
ax.plot(df['1151_ag_pay'], c='gray', ls='--', label='services ag pay')

ax.set_xticks(range(1992, 2018, 5))
ax.set_yticks(np.arange(1, 3, .5))

ax.legend(loc='upper left')
ax.grid()
ax.set_title('Growth of labor expenses (1992 = 1)');
```

+++ {"tags": []}

# AFI business dynamics

From CBP.

```{code-cell} ipython3
:tags: []

import pandas as pd
import matplotlib.pyplot as plt

from rurec.pubdata import naics, cbp
from rurec.reseng.nbd import Nbd
nbd = Nbd('rurec')

PATH = {
    'data': nbd.root / 'data',
    'slides': nbd.root / 'tmp/slides_20220613'
}
```

```{code-cell} ipython3
:tags: []

afi_naics = pd.read_csv(PATH['data'] / 'afi_naics_afbd_20220607.csv', dtype={'NAICS': str})
```

# AFI NAICS codes missing from CBP

Primarily NAICS subsectors "111" and "112".

```{code-cell} ipython3
---
jupyter:
  source_hidden: true
tags: []
---
d = cbp.get_df('us', 2017).query('lfo == "-" and ind_digits == 6')[['industry']]
d = d.merge(afi_naics.query('YEAR == 2017'), 'outer', left_on='industry', right_on='NAICS', indicator=True)
d['_merge'] = d['_merge'].replace({'left_only': 'CBP only', 'right_only': 'AFI only'})
pd.crosstab(d['_merge'], [d['AFI'], d['SUPPLY_CHAIN']], margins=True)
```

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: []
---
n = naics.get_df(2017, 'code')[['CODE', 'TITLE']].rename(columns={'CODE': 'NAICS'})
t = d.query('_merge == "AFI only"').merge(n)
t[['NAICS', 'TITLE', 'AFI', 'SUPPLY_CHAIN']]
```

+++ {"tags": []}

# Build main df

```{code-cell} ipython3
:tags: []

df = cbp.get_parquet('us', cols=['year', 'industry', 'ind_digits', 'est', 'emp', 'ap'], filters=[('lfo', '=', '-'), ('year', '>=', 1998)])

# NAICS revisions in CBP:
# https://www.census.gov/programs-surveys/cbp/technical-documentation/reference/naics-descriptions.html

naics_revision_breaks = [2003, 2008, 2012, 2017]

def _naics_year(year):
    if 1998 <= year <= 2002:
        return 1997
    if 2003 <= year <= 2007:
        return 2002
    if 2008 <= year <= 2011:
        return 2007
    if 2012 <= year <= 2016:
        return 2012
    if 2017 <= year <= 2020:
        return 2017
    
df['NAICS_YEAR'] = df['year'].map(_naics_year)
df = df.merge(afi_naics, 'left', left_on=['NAICS_YEAR', 'industry'], right_on=['YEAR', 'NAICS'])
del df['YEAR']
```

# Total AFI size

```{code-cell} ipython3
---
jupyter:
  source_hidden: true
tags: []
---
c = plt.rcParams['axes.prop_cycle'].by_key()['color']

t = df.groupby(['year', 'AFI'])[['est', 'emp', 'ap']].sum()
i = pd.IndexSlice
t_ = t.loc[i[:, 'Yes'], :].droplevel('AFI').add_suffix(' AFI')
ax = t_.plot(subplots=True, layout=(1, 3), figsize=(18, 6), ls='-', color=c[0])

t_ = t.loc[i[:, 'Mixed'], :].droplevel('AFI').add_suffix(' Mixed')
t_.plot(subplots=True, ax=ax, ls='--', color=c[0], grid=True)

for a in ax[0]:
    u = a.get_ylim()[1]
    a.set_ylim(0, u * 1.2)
    for x in naics_revision_breaks:
        a.axvline(x, ls=':', c=c[2])
```

# AFI share in the economy

```{code-cell} ipython3
---
jupyter:
  source_hidden: true
tags: []
---
c = plt.rcParams['axes.prop_cycle'].by_key()['color']
i = pd.IndexSlice

t_all = df.query('industry == "-"').groupby('year')[['est', 'emp', 'ap']].sum()

t = df.groupby(['year', 'AFI'])[['est', 'emp', 'ap']].sum()

t_ = t.loc[i[:, 'Yes'], :].droplevel('AFI')
t_ = (t_ / t_all).add_suffix(' AFI')
ax = t_.plot(subplots=True, layout=(1, 3), figsize=(18, 6), ls='-', color=c[0])

t_ = t.loc[i[:, 'Mixed'], :].droplevel('AFI')
t_ = (t_ / t_all).add_suffix(' Mixed')
t_.plot(subplots=True, ax=ax, ls='--', color=c[0], grid=True)

for a in ax[0]:
    u = a.get_ylim()[1]
    a.set_ylim(0, u * 1.2)
    for x in naics_revision_breaks:
        a.axvline(x, ls=':', c=c[2])
```

# Supply chain

Excluding "Both" (only one industry) and "Farm" (only "113" and "114").

```{code-cell} ipython3
---
jupyter:
  source_hidden: true
tags: []
---
c = plt.rcParams['axes.prop_cycle'].by_key()['color']
i = pd.IndexSlice

t = df.query('SUPPLY_CHAIN.isin(["Up", "Down"])').groupby(['year', 'AFI', 'SUPPLY_CHAIN'])[['est', 'emp', 'ap']].sum()
t.columns.name = 'measure'
t = t.stack().unstack(['AFI', 'measure'])
t.columns = t.columns.to_flat_index()

t_ = t.loc[i[:, 'Up'], :].droplevel('SUPPLY_CHAIN').add_suffix(' upstream')
ax = t_.plot(subplots=True, layout=(2, 3), figsize=(18, 10), color=c[0], ls='-')

t_ = t.loc[i[:, 'Down'], :].droplevel('SUPPLY_CHAIN').add_suffix(' downstream')
t_.plot(subplots=True, ax=ax, grid=True, color=c[0], ls='--')

for a in ax.flatten():
    u = a.get_ylim()[1]
    a.set_ylim(0, u * 1.2)
```
