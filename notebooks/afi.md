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

# Agricultural and food industries

```{code-cell} ipython3
:tags: []

import json
import pickle

import pandas as pd

from rurec.pubdata import naics
from rurec.reseng.nbd import Nbd
nbd = Nbd('rurec')

PATH = {
    'data': nbd.root / 'data',
    'slides': nbd.root / 'tmp/slides_20220613'
}
PATH['slides'].mkdir(parents=True, exist_ok=True)
```

```{code-cell} ipython3
:tags: []

pd.options.display.max_colwidth = None
```

# Which NAICS revision was used to generate old lists?

+++

The list of 168 6-digit codes in `fai.json` was almost certainly generated from the 2002 NAICS revision, the only revision where all codes can be found.

```{code-cell} ipython3
:tags: []

codes = json.load((PATH['data'] / 'fai.json').open())
codes = pd.DataFrame(codes.items(), columns=['CODE', 'TITLE'])
for y in range(1997, 2020, 5):
    # using concordance files, since 1997 code file is incomplete
    df = naics.get_concordance_df('naics', y, 'naics', y + 5)
    codes['EXISTS'] = codes['CODE'].isin(df[f'NAICS_{y}'])
    print(y, *codes['EXISTS'].value_counts())
```

All 100 6-digit codes from `fai_subsectors.json` can be found in both 2002 and 2007 revisions.

```{code-cell} ipython3
:tags: []

codes = json.load((PATH['data'] / 'fai_subsectors.json').open())
codes = {k: v for sub in codes.values() for k, v in sub.items()}
codes = pd.DataFrame(codes.items(), columns=['CODE', 'TITLE'])
for y in range(1997, 2020, 5):
    # using concordance files, since 1997 code file is incomplete
    df = naics.get_concordance_df('naics', y, 'naics', y + 5)
    codes['EXISTS'] = codes['CODE'].isin(df[f'NAICS_{y}'])
    print(y, *codes['EXISTS'].value_counts())
```

# Combine codes from manual reviews

```{code-cell} ipython3
:tags: []

t = {}
df = []
d = pd.read_excel(PATH['data'] / 'afi_naics_for_review_v2_AK.xlsx', usecols=['afi', 'supply_chain', 'industry_code'], dtype='str')
d = d.dropna(subset='industry_code')
df.append(d)
t['Score = 1'] = d['afi'].value_counts()

d = pd.read_excel(PATH['data'] / 'afi_naics_for_review_v3_AK.xlsx', usecols=['afi', 'supply_chain', 'industry_code'], dtype='str')
d = d.dropna(subset='industry_code')
df.append(d)
t['Score >= 0.91'] = d['afi'].value_counts()

d = pd.read_excel(PATH['data'] / 'afi_naics_for_review_v4_AK.xlsx', usecols=['afi', 'supply_chain', 'industry_code'], dtype='str')
d = d.dropna(subset='industry_code')
df.append(d)
t['Sector "11"'] = d['afi'].value_counts()

df = pd.concat(df, ignore_index=True)
df.to_csv(PATH['data'] / 'afi_naics_2017_2022-05-06.csv', index=False)
```

```{code-cell} ipython3
:tags: []

t = pd.concat(t).unstack().fillna(0)
t['ALL'] = t.sum(1)
t.loc['ALL'] = t.sum()
t = t.astype(int)
t.to_pickle(PATH['slides'] / 'review_stats_by_score.pkl')
t
```

## Starting set of 2017 codes

```{code-cell} ipython3
:tags: []

df = pd.read_csv(PATH['data']/'afi_naics_2017_2022-05-06.csv', dtype=str)\
    .rename(columns={'afi': 'AFI', 'supply_chain': 'SUPPLY_CHAIN', 'industry_code': 'NAICS'})\
    .add_suffix('_2017')

df['AFI_2017'] = df['AFI_2017'].str.capitalize()
df['SUPPLY_CHAIN_2017'] = df['SUPPLY_CHAIN_2017'].str.capitalize()
df = df.query('AFI_2017 != "No"')

# industry omitted in auto search
df.loc[len(df)] = ['Yes', 'Farm', '112130']

t = pd.crosstab(df['AFI_2017'], df['SUPPLY_CHAIN_2017'], margins=True)
t = t.loc[['Yes', 'Mixed', 'Maybe', 'Mixed maybe', 'All'], ['Farm', 'Up', 'Down', 'Both', 'All']]
t.to_pickle(PATH['slides'] / 'review_summary_raw.pkl')
t
```

```{code-cell} ipython3
:tags: []

# manual edits
df['AFI_2017'] = df['AFI_2017'].replace({
    'Maybe': 'Mixed',
    'Mixed maybe': 'Mixed'
})

t = pd.crosstab(df['AFI_2017'], df['SUPPLY_CHAIN_2017'], margins=True)
t = t.loc[['Yes', 'Mixed', 'All'], ['Farm', 'Up', 'Down', 'Both', 'All']]
t.to_pickle(PATH['slides'] / 'review_summary.pkl')
t
```

```{code-cell} ipython3
:tags: []

afi_codes = {}
afi_codes[2017] = df[['NAICS_2017', 'AFI_2017', 'SUPPLY_CHAIN_2017']]
```

## 2017 -> 2012

```{code-cell} ipython3
:tags: []

y0, y1 = 2017, 2012
n0, n1 = f'NAICS_{y0}', f'NAICS_{y1}'
a0, a1 = f'AFI_{y0}', f'AFI_{y1}'
s0, s1 = f'SUPPLY_CHAIN_{y0}', f'SUPPLY_CHAIN_{y1}'

df = naics.get_concordance_df('naics', y0, 'naics', y1)
d = afi_codes[y0]
assert d[n0].isin(df[n0]).all()
assert not d[n0].duplicated().any()
df = df.merge(d, 'left')
df[a0] = df[a0].fillna('No')
df[s0] = df[s0].fillna('NA')
```

```{code-cell} ipython3
:tags: []

d = df.drop_duplicates(n0)
pd.crosstab(d[a0], d[s0], margins=True)
```

```{code-cell} ipython3
:tags: []

d = df.drop_duplicates([n1, a0, s0]).sort_values([n1, a0, s0])
d = d.groupby(n1)[[a0, s0]]\
    .sum()\
    .rename(columns={a0: a1, s0: s1})\
    .reset_index()
assert set(d[n1]) == set(df[n1])
df = df.merge(d, on=n1)
```

```{code-cell} ipython3
:tags: []

d = df.drop_duplicates(n1)
pd.crosstab(d[a1], d[s1], margins=True)
```

```{code-cell} ipython3
:tags: []

df.query(f'{a1} == "Yes"')[f'FLAG_{y0}_TO_{y1}'].value_counts()
```

```{code-cell} ipython3
:tags: []

# splits
df.query(f'{a1} == "Yes" and FLAG_{y0}_TO_{y1}.str.contains("split")')
```

```{code-cell} ipython3
:tags: []

# ambiguous
d = df[~df[a1].isin(['Yes', 'No', 'Mixed']) | ~df[s1].isin(['Farm', 'Up', 'Down', 'Both', 'NA'])]
d = d.sort_values(n1)
c = naics.get_df(y1, 'code')[['CODE', 'TITLE']].rename(columns={'CODE': n1})
d[[n1, a1, s1]].drop_duplicates().merge(c, 'left')
```

```{code-cell} ipython3
x = iter(d.groupby(n1))
```

```{code-cell} ipython3
:tags: []

next(x)[1]
```

```{code-cell} ipython3
:tags: []

# manual edits
df.loc[df[n1] == '541711', [a1, s1]] = ['Mixed', 'Up'] # Research and Development in Biotechnology
df.loc[df[n1] == '541712', [a1, s1]] = ['Mixed', 'Up'] # Research and Development in the Physical, Engineering, and Life Sciences (except Biotechnology)
```

```{code-cell} ipython3
:tags: []

d = df.drop_duplicates(n1)
pd.crosstab(d[a1], d[s1], margins=True)
```

```{code-cell} ipython3
:tags: []

d = afi_codes[y1] = df.loc[df[a1] != 'No', [n1, a1, s1]].drop_duplicates()
pd.crosstab(d[a1], d[s1], margins=True)
```

## 2012 -> 2007

```{code-cell} ipython3
:tags: []

y0, y1 = 2012, 2007
n0, n1 = f'NAICS_{y0}', f'NAICS_{y1}'
a0, a1 = f'AFI_{y0}', f'AFI_{y1}'
s0, s1 = f'SUPPLY_CHAIN_{y0}', f'SUPPLY_CHAIN_{y1}'

df = naics.get_concordance_df('naics', y0, 'naics', y1)
d = afi_codes[y0]
assert d[n0].isin(df[n0]).all()
assert not d[n0].duplicated().any()
df = df.merge(d, 'left')
df[a0] = df[a0].fillna('No')
df[s0] = df[s0].fillna('NA')
```

```{code-cell} ipython3
:tags: []

d = df.drop_duplicates(n0)
pd.crosstab(d[a0], d[s0], margins=True)
```

```{code-cell} ipython3
:tags: []

d = df.drop_duplicates([n1, a0, s0]).sort_values([n1, a0, s0])
d = d.groupby(n1)[[a0, s0]]\
    .sum()\
    .rename(columns={a0: a1, s0: s1})\
    .reset_index()
assert set(d[n1]) == set(df[n1])
df = df.merge(d, on=n1)
```

```{code-cell} ipython3
:tags: []

d = df.drop_duplicates(n1)
pd.crosstab(d[a1], d[s1], margins=True)
```

```{code-cell} ipython3
:tags: []

df.query(f'{a1} == "Yes"')[f'FLAG_{y0}_TO_{y1}'].value_counts()
```

```{code-cell} ipython3
:tags: []

# splits
df.query(f'{a1} == "Yes" and FLAG_{y0}_TO_{y1}.str.contains("split")')
```

```{code-cell} ipython3
:tags: []

# example
t = df.loc[df['NAICS_2012'] == '311314', ['NAICS_2012', 'TITLE_2012', 'NAICS_2007', 'TITLE_2007']].reset_index(drop=True)
t.to_pickle(PATH['slides'] / 'concord_clean_split.pkl')
t
```

```{code-cell} ipython3
:tags: []

# ambiguous
d = df[~df[a1].isin(['Yes', 'No', 'Mixed']) | ~df[s1].isin(['Farm', 'Up', 'Down', 'Both', 'NA'])]
d = d.sort_values(n1)
c = naics.get_df(y1, 'code')[['CODE', 'TITLE']].rename(columns={'CODE': n1})
d[[n1, a1, s1]].drop_duplicates().merge(c, 'left')
```

```{code-cell} ipython3
:tags: []

x = iter(d.groupby(n1))
```

```{code-cell} ipython3
:tags: []

next(x)[1]
```

```{code-cell} ipython3
:tags: []

# manual edits
# none
```

```{code-cell} ipython3
:tags: []

d = df.drop_duplicates(n1)
pd.crosstab(d[a1], d[s1], margins=True)
```

```{code-cell} ipython3
:tags: []

d = afi_codes[y1] = df.loc[df[a1] != 'No', [n1, a1, s1]].drop_duplicates()
pd.crosstab(d[a1], d[s1], margins=True)
```

## 2007 -> 2002

```{code-cell} ipython3
:tags: []

y0, y1 = 2007, 2002
n0, n1 = f'NAICS_{y0}', f'NAICS_{y1}'
a0, a1 = f'AFI_{y0}', f'AFI_{y1}'
s0, s1 = f'SUPPLY_CHAIN_{y0}', f'SUPPLY_CHAIN_{y1}'

df = naics.get_concordance_df('naics', y0, 'naics', y1)
d = afi_codes[y0]
assert d[n0].isin(df[n0]).all()
assert not d[n0].duplicated().any()
df = df.merge(d, 'left')
df[a0] = df[a0].fillna('No')
df[s0] = df[s0].fillna('NA')
```

```{code-cell} ipython3
:tags: []

d = df.drop_duplicates(n0)
pd.crosstab(d[a0], d[s0], margins=True)
```

```{code-cell} ipython3
:tags: []

d = df.drop_duplicates([n1, a0, s0]).sort_values([n1, a0, s0])
d = d.groupby(n1)[[a0, s0]]\
    .sum()\
    .rename(columns={a0: a1, s0: s1})\
    .reset_index()
assert set(d[n1]) == set(df[n1])
df = df.merge(d, on=n1)
```

```{code-cell} ipython3
:tags: []

d = df.drop_duplicates(n1)
pd.crosstab(d[a1], d[s1], margins=True)
```

```{code-cell} ipython3
:tags: []

df.query(f'{a1} == "Yes"')[f'FLAG_{y0}_TO_{y1}'].value_counts()
```

```{code-cell} ipython3
:tags: []

# splits
# no change needed
df.query(f'{a1} == "Yes" and (not FLAG_{y0}_TO_{y1}.str.contains("1-to-1"))')
```

```{code-cell} ipython3
:tags: []

# example
t = df.loc[df['NAICS_2007'].isin(['111211', '111219']), ['NAICS_2007', 'TITLE_2007', 'NAICS_2002', 'TITLE_2002']].reset_index(drop=True)
t.to_pickle(PATH['slides'] / 'concord_messy_split.pkl')
t
```

```{code-cell} ipython3
:tags: []

# ambiguous
d = df[~df[a1].isin(['Yes', 'No', 'Mixed']) | ~df[s1].isin(['Farm', 'Up', 'Down', 'Both', 'NA'])]
d = d.sort_values(n1)
c = naics.get_df(y1, 'code')[['CODE', 'TITLE']].rename(columns={'CODE': n1})
d[[n1, a1, s1]].drop_duplicates().merge(c, 'left')
```

```{code-cell} ipython3
:tags: []

x = iter(d.groupby(n1))
```

```{code-cell} ipython3
:tags: []

next(x)[1]
```

```{code-cell} ipython3
:tags: []

# save example
d = naics.get_concordance_df('naics', 2007, 'naics', 2002)
d = naics.find_concordance_group(d, 'NAICS_2007', 'NAICS_2002', ['531110'])
fig = naics.viz_concordance(d[['NAICS_2007', 'NAICS_2002', 'FLAG_2007_TO_2002']].values, 2007, 2002)
pickle.dump(fig, (PATH['slides'] / 'concord_531190_diagram.pkl').open('wb'))
t = d.iloc[:, :4].reset_index(drop=True)
t.to_pickle(PATH['slides'] / 'concord_531190_table.pkl')
display(fig)
display(t)
```

```{code-cell} ipython3
:tags: []

# manual edits
df.loc[df[n1] == '315211', [a1, s1]] = ['Mixed', 'Down'] # Men's and Boys' Cut and Sew Apparel Contractors
df.loc[df[n1] == '315212', [a1, s1]] = ['Mixed', 'Down'] # Women's, Girls', and Infants' Cut and Sew Apparel Contractors
df.loc[df[n1] == '525930', [a1, s1]] = ['Mixed', 'Up'] # Real Estate Investment Trusts
```

```{code-cell} ipython3
:tags: []

d = df.drop_duplicates(n1)
pd.crosstab(d[a1], d[s1], margins=True)
```

```{code-cell} ipython3
:tags: []

d = afi_codes[y1] = df.loc[df[a1] != 'No', [n1, a1, s1]].drop_duplicates()
pd.crosstab(d[a1], d[s1], margins=True)
```

## 2002 -> 1997

```{code-cell} ipython3
:tags: []

y0, y1 = 2002, 1997
n0, n1 = f'NAICS_{y0}', f'NAICS_{y1}'
a0, a1 = f'AFI_{y0}', f'AFI_{y1}'
s0, s1 = f'SUPPLY_CHAIN_{y0}', f'SUPPLY_CHAIN_{y1}'

df = naics.get_concordance_df('naics', y0, 'naics', y1)
d = afi_codes[y0]
assert d[n0].isin(df[n0]).all()
assert not d[n0].duplicated().any()
df = df.merge(d, 'left')
df[a0] = df[a0].fillna('No')
df[s0] = df[s0].fillna('NA')
```

```{code-cell} ipython3
:tags: []

d = df.drop_duplicates(n0)
pd.crosstab(d[a0], d[s0], margins=True)
```

```{code-cell} ipython3
:tags: []

d = df.drop_duplicates([n1, a0, s0]).sort_values([n1, a0, s0])
d = d.groupby(n1)[[a0, s0]]\
    .sum()\
    .rename(columns={a0: a1, s0: s1})\
    .reset_index()
assert set(d[n1]) == set(df[n1])
df = df.merge(d, on=n1)
```

```{code-cell} ipython3
:tags: []

d = df.drop_duplicates(n1)
pd.crosstab(d[a1], d[s1], margins=True)
```

```{code-cell} ipython3
:tags: []

df.query(f'{a1} == "Yes"')[f'FLAG_{y0}_TO_{y1}'].value_counts()
```

```{code-cell} ipython3
:tags: []

# splits
# no change needed
df.query(f'{a1} == "Yes" and (not FLAG_{y0}_TO_{y1}.str.contains("1-to-1"))')
```

```{code-cell} ipython3
:tags: []

# ambiguous
d = df[~df[a1].isin(['Yes', 'No', 'Mixed']) | ~df[s1].isin(['Farm', 'Up', 'Down', 'Both', 'NA'])]
d = d.sort_values(n1)
c = naics.get_df(y1, 'code')[['CODE', 'TITLE']].rename(columns={'CODE': n1})
d[[n1, a1, s1]].drop_duplicates().merge(c, 'left')
```

```{code-cell} ipython3
:tags: []

x = iter(d.groupby(n1))
```

```{code-cell} ipython3
:tags: []

next(x)[1]
```

```{code-cell} ipython3
:tags: []

# manual edits
df.loc[df[n1] == '233220', [a1, s1]] = ['No', 'NA'] # Multifamily housing construction
df.loc[df[n1] == '233310', [a1, s1]] = ['Mixed', 'Up'] # Manufacturing and Industrial Building Construction
df.loc[df[n1] == '234120', [a1, s1]] = ['No', 'NA'] # Bridge and Tunnel Construction
df.loc[df[n1] == '234910', [a1, s1]] = ['Mixed', 'Up'] # Water, Sewer, and Pipeline Construction
df.loc[df[n1] == '234990', [a1, s1]] = ['Mixed', 'Up'] # All Other Heavy Construction
df.loc[df[n1] == '235990', [a1, s1]] = ['Mixed', 'Up'] # All Other Special Trade Contractors
df.loc[df[n1] == '421310', [a1, s1]] = ['Yes', 'Down'] # Lumber, Plywood, Millwork, and Wood Panel Wholesalers
df.loc[df[n1] == '421820', [a1, s1]] = ['Yes', 'Up'] # Farm and Garden Machinery and Equipment Wholesalers
df.loc[df[n1] == '421830', [a1, s1]] = ['Mixed', 'Up'] # Industrial Machinery and Equipment Wholesalers
df.loc[df[n1] == '422130', [a1, s1]] = ['Mixed', 'Both'] # Industrial and Personal Service Paper Wholesalers
df.loc[df[n1] == '422420', [a1, s1]] = ['Mixed', 'Down'] # Packaged Frozen Food Wholesalers
df.loc[df[n1] == '422430', [a1, s1]] = ['Yes', 'Down'] # Dairy Product (except Dried or Canned) Wholesalers
df.loc[df[n1] == '422440', [a1, s1]] = ['Yes', 'Down'] # Poultry and Poultry Product Wholesalers
df.loc[df[n1] == '422450', [a1, s1]] = ['Mixed', 'Down'] # Confectionery Wholesalers
df.loc[df[n1] == '422460', [a1, s1]] = ['Yes', 'Down'] # Fish and Seafood Wholesalers
df.loc[df[n1] == '422470', [a1, s1]] = ['Yes', 'Down'] # Meat and Meat Product Wholesalers
df.loc[df[n1] == '422480', [a1, s1]] = ['Yes', 'Down'] # Fresh Fruit and Vegetable Wholesalers 	
df.loc[df[n1] == '422490', [a1, s1]] = ['Yes', 'Down'] # Other Grocery and Related Products Wholesalers
df.loc[df[n1] == '422510', [a1, s1]] = ['Yes', 'Down'] # Grain and Field Bean Wholesalers
df.loc[df[n1] == '422520', [a1, s1]] = ['Yes', 'Down'] # Livestock Wholesalers
df.loc[df[n1] == '422590', [a1, s1]] = ['Yes', 'Down'] # Other Farm Product Raw Material Wholesalers
df.loc[df[n1] == '422810', [a1, s1]] = ['Mixed', 'Down'] # Beer and Ale Wholesalers
df.loc[df[n1] == '422910', [a1, s1]] = ['Yes', 'Up'] # Farm Supplies Wholesalers
df.loc[df[n1] == '422930', [a1, s1]] = ['Mixed', 'Down'] # Flower, Nursery Stock, and Florists' Supplies Wholesalers
df.loc[df[n1] == '422990', [a1, s1]] = ['Mixed', 'Down'] # Other Miscellaneous Nondurable Goods Wholesalers
df.loc[df[n1] == '511110', [a1, s1]] = ['Mixed', 'Up'] # Newspaper Publishers
df.loc[df[n1] == '511130', [a1, s1]] = ['Mixed', 'Up'] # Book Publishers
df.loc[df[n1] == '511140', [a1, s1]] = ['Mixed', 'Up'] # Database and Directory Publishers
df.loc[df[n1] == '511191', [a1, s1]] = ['Mixed', 'Up'] # Greeting Card Publishers
df.loc[df[n1] == '511199', [a1, s1]] = ['Mixed', 'Up'] # All Other Publishers
df.loc[df[n1] == '514199', [a1, s1]] = ['Mixed', 'Up'] # All Other Information Services
```

```{code-cell} ipython3
:tags: []

d = df.drop_duplicates(n1)
pd.crosstab(d[a1], d[s1], margins=True)
```

```{code-cell} ipython3
:tags: []

d = afi_codes[y1] = df.loc[df[a1] != 'No', [n1, a1, s1]].drop_duplicates()
pd.crosstab(d[a1], d[s1], margins=True)
```

## Save all years

```{code-cell} ipython3
:tags: []

df = pd.concat({y: d.rename(columns=lambda c: c[:-5]) for y, d in afi_codes.items()}, axis=1)
df = df.stack(0).droplevel(0).reset_index().rename(columns={'index': 'YEAR'})\
    .sort_values(['YEAR', 'NAICS']).reset_index(drop=True)
df = df[['YEAR', 'NAICS', 'AFI', 'SUPPLY_CHAIN']]
df.to_csv(PATH['data'] / 'afi_naics_afbd_20220607.csv', index=False)
df.sample(5)
```

Save to spreadsheet by year.

```{code-cell} ipython3
:tags: []

df = pd.read_csv(PATH['data'] / 'afi_naics_afbd_20220607.csv', dtype={'NAICS': str})
with pd.ExcelWriter(PATH['data'] / 'afi_naics_afbd_20220607.xlsx') as w:
    for y, d in df.groupby('YEAR'):
        n = naics.get_df(y, 'code')[['CODE', 'TITLE']].rename(columns={'CODE': 'NAICS'})
        d = d.merge(n, 'left')
        d.to_excel(w, str(y), index=False)
```

# Summary counts by year

```{code-cell} ipython3
:tags: []

df = pd.read_csv(PATH['data'] / 'afi_naics_afbd_20220607.csv', dtype={'NAICS': str})
```

```{code-cell} ipython3
:tags: []

t = pd.crosstab(df['YEAR'], [df['AFI'], df['SUPPLY_CHAIN']], margins=True)
t = t.iloc[:-1]
t = t.loc[:, [('Mixed',   'Up'), ('Mixed', 'Down'), ('Mixed', 'Both'), 
              (  'Yes', 'Farm'), (  'Yes',   'Up'), (  'Yes', 'Down'), 
              (  'All',     '')]]
t.to_pickle(PATH['slides'] / 'concord_summary_cats.pkl')
t
```

```{code-cell} ipython3
:tags: []

t = {}
for y0 in range(2017, 2000, -5):
    y1 = y0 - 5
    d = naics.get_concordance_df('naics', y0, 'naics', y1)
    d = df.query('YEAR == @y0').merge(d, 'left', left_on='NAICS', right_on=f'NAICS_{y0}')
    t[f'{y0}->{y1}'] = d.drop_duplicates('NAICS')[f'FLAG_{y0}_TO_{y1}'].str.startswith('1-to-1').value_counts()

t = pd.concat(t).unstack().rename(columns={True: '1-to-1', False: 'other'})
t.to_pickle(PATH['slides'] / 'concord_summary_1to1.pkl')
t
```

+++ {"tags": []}

# Compare methods

```{code-cell} ipython3
---
jupyter:
  source_hidden: true
tags: []
---
df = pd.read_csv(PATH['data'] / 'andrews_results.csv')
df = df[['NAICS17']]
df.columns = ['NAICS']
df = df.query('NAICS != "******"')
df = df.drop_duplicates().reset_index(drop=True)
df_andrew = df

df_brent = pd.DataFrame(json.load((PATH['data'] / 'fai.json').open()).keys(), columns=['NAICS'])

df_aajr = pd.read_csv(PATH['data'] / 'afi_naics_afbd_20220607.csv', dtype={'NAICS': str})
```

## AAJR vs Andrew

```{code-cell} ipython3
---
jupyter:
  source_hidden: true
tags: []
---
n = naics.get_df(2017, 'code')

d0 = df_andrew
d1 = df_aajr
d1 = d1.loc[d1['YEAR'] == 2017, ['NAICS']]
df = d0.merge(d1, 'outer', indicator='METHOD')
df['METHOD'] = df['METHOD'].replace({'left_only': 'Andrew only', 'right_only': 'AAJR only'})
df = df.merge(n[['CODE', 'TITLE', 'CODE_2']], 'left', left_on='NAICS', right_on='CODE').drop(columns='CODE')

t = pd.crosstab(df['CODE_2'], df['METHOD'], margins=True)
t = t.reset_index()
t.columns.name = None
t = t.merge(n.query('DIGITS == 2')[['CODE_2', 'TITLE']], 'left')
t = t.fillna('')
t = t[['TITLE', 'CODE_2', 'both', 'Andrew only', 'AAJR only', 'All']]
t
```

### AAJR only

```{code-cell} ipython3
:tags: []

df.query('METHOD == "AAJR only"')
```

### Andrew only

```{code-cell} ipython3
:tags: []

with pd.option_context('display.max_rows', None):
    display(df.query('METHOD == "Andrew only"').sort_values('NAICS'))
```

## AAJR vs Brent

```{code-cell} ipython3
---
jupyter:
  source_hidden: true
tags: []
---
n = naics.get_df(2002, 'code')

d0 = df_brent
d1 = df_aajr
d1 = d1.loc[d1['YEAR'] == 2002, ['NAICS']]
df = d0.merge(d1, 'outer', indicator='METHOD')
df['METHOD'] = df['METHOD'].replace({'left_only': 'Brent only', 'right_only': 'AAJR only'})
df = df.merge(n[['CODE', 'TITLE', 'CODE_2']], 'left', left_on='NAICS', right_on='CODE').drop(columns='CODE')

t = pd.crosstab(df['CODE_2'], df['METHOD'], margins=True)
t = t.reset_index()
t.columns.name = None
t = t.merge(n.query('DIGITS == 2')[['CODE_2', 'TITLE']], 'left')
t = t.fillna('')
t = t[['TITLE', 'CODE_2', 'both', 'Brent only', 'AAJR only', 'All']]
t
```

### AAJR only

```{code-cell} ipython3
:tags: []

df.query('METHOD == "AAJR only"')
```

### Brent only

```{code-cell} ipython3
:tags: []

df.query('METHOD == "Brent only"').sort_values('NAICS')
```
