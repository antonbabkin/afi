---
jupytext:
  formats: ipynb,md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.13.7
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
---

# Economic activity by state and rurality

Interactive dashboard.

```{code-cell} ipython3
import json
from pathlib import Path

import ipywidgets as widgets
import ipyleaflet as leaflet
import branca.colormap
import numpy as np
import pandas as pd
import joblib
import geopandas
import shapely
import matplotlib.pyplot as plt

from rurec import rurality
from rurec import geography as geo
from rurec import ers_codes
from rurec.reseng.config import Paths
from rurec.reseng.caching import simplecache

PATH = Paths(
    data='cache/dashboard_data.pkl'
)

OPTS = {
    'years': list(range(1997, 2018, 10)), # "None" for all years
    'states': ['WI', 'CT'], # "None" for all states
    'force_recalc': True
}
```

```{code-cell} ipython3
memory = joblib.Memory(PATH.root/'cache')
```

# Prepare data

```{code-cell} ipython3
@memory.cache
def download_cbsa_boundaries():
    return geopandas.read_file('https://www2.census.gov/geo/tiger/GENZ2019/shp/cb_2019_us_cbsa_20m.zip')

@memory.cache
def download_county_boundaries():
    return geopandas.read_file('https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_county_20m.zip')

@memory.cache
def download_tract_boundaries():
    return geopandas.read_file('https://www2.census.gov/geo/tiger/GENZ2019/shp/cb_2019_us_tract_500k.zip')
```

```{code-cell} ipython3
def prep_cbsa():
    df = download_cbsa_boundaries()[['CBSAFP', 'NAME', 'LSAD', 'geometry']]
    df = df.rename(columns={'CBSAFP': 'CBSA_CODE', 'NAME': 'CBSA_NAME', 'LSAD': 'CBSA_LEVEL'})
    df['CBSA_LABEL'] = df['CBSA_LEVEL'].map({'M1': 'Metropolitan', 'M2': 'Micropolitan'})
    df['CBSA_COLORCODE'] = df['CBSA_LEVEL'].map({'M1': 1, 'M2': 0.5})
    df['RURAL_CBSA'] = df['CBSA_LEVEL'].map(data['CBSA']['rural levels']).fillna(True)
    return df


def prep_county():
    df = download_county_boundaries()
    df = df.drop(columns=['COUNTYNS', 'AFFGEOID', 'LSAD', 'ALAND', 'AWATER'])
    df = df.rename(columns={'STATEFP': 'STATE_CODE', 'COUNTYFP': 'COUNTY_CODE', 'GEOID': 'STATECOUNTY_CODE', 'NAME': 'COUNTY_NAME'})
    df['STATE_ABBR'] = df['STATE_CODE'].map(geo.get_mapping('STATE_CODE', 'STATE_ABBR'))
    df['STATE_NAME'] = df['STATE_CODE'].map(geo.get_mapping('STATE_CODE', 'STATE_NAME'))
    df0 = df
    
    df = ers_codes.get_ui_df()
    df = df.rename(columns={'FIPS': 'STATECOUNTY_CODE', 'UI_CODE': 'UI_LEVEL', 'UI_CODE_DESCRIPTION': 'UI_LABEL'})
    df = df[df['UI_YEAR'] == 2013]
    df = df[['STATECOUNTY_CODE', 'UI_LEVEL', 'UI_LABEL']]
    df['UI_COLORCODE'] = (df['UI_LEVEL'].astype(int) - 1) / 11
    df['RURAL_UI'] = df['UI_LEVEL'].map(data['UI']['rural levels'])
    df0 = df0.merge(df, 'left', 'STATECOUNTY_CODE')
    
    df = ers_codes.get_ruc_df()
    df = df.rename(columns={'FIPS': 'STATECOUNTY_CODE', 'RUC_CODE': 'RUC_LEVEL', 'RUC_CODE_DESCRIPTION': 'RUC_LABEL'})
    df = df[df['RUC_YEAR'] == 2013]
    # this was not a problem before... what happened? 
    # did I break ers_codes module in last commit? did ERS source data change?
    df = df[df['RUC_LEVEL'].notna()]
    df = df[['STATECOUNTY_CODE', 'RUC_LEVEL', 'RUC_LABEL']]
    df['RUC_COLORCODE'] = df['RUC_LEVEL'].astype(int) / 9
    df['RURAL_RUC'] = df['RUC_LEVEL'].map(data['RUC']['rural levels'])
    df0 = df0.merge(df, 'left', 'STATECOUNTY_CODE')
    
    return df0

def prep_tract():
    df = download_tract_boundaries()[['STATEFP', 'COUNTYFP', 'TRACTCE', 'geometry']]
    df = df.rename(columns={'STATEFP': 'STATE_CODE', 'COUNTYFP': 'COUNTY_CODE', 'TRACTCE': 'TRACT_CODE'})
    df['STATE_ABBR'] = df['STATE_CODE'].map(geo.get_mapping('STATE_CODE', 'STATE_ABBR'))
    df['STATE_NAME'] = df['STATE_CODE'].map(geo.get_mapping('STATE_CODE', 'STATE_NAME'))
    df0 = df
    
    df = ers_codes.get_ruca_df()
    df = df[df['YEAR'] == 2010]
    df['STATE_CODE'] = df['FIPS'].str[:2]
    df['COUNTY_CODE'] = df['FIPS'].str[2:5]
    df['TRACT_CODE'] = df['FIPS'].str[5:]
    df = df[['STATE_CODE', 'COUNTY_CODE', 'TRACT_CODE', 'RUCA_CODE']]
    primary = df['RUCA_CODE'].astype('float32').round().replace({99: np.nan})
    df['RUCA_COLORCODE'] = ((primary - 1) / 9).fillna(1)
    df['RURAL_RUCA'] = primary.map(data['RUCA']['rural levels'])
    df0 = df0.merge(df, 'left', ['STATE_CODE', 'COUNTY_CODE', 'TRACT_CODE'])
    
    return df0
```

```{code-cell} ipython3
def prep_county_fai():
    df = download_county_boundaries()[['geometry', 'GEOID']]
    df = df.rename(columns={'GEOID': 'STATECOUNTY_CODE'})

    d = data['county FAI share']
    d = d.to_frame('FAI_EMP_SHARE').reset_index().dropna(subset=['FAI_EMP_SHARE'])
    d = d[d['YEAR'] == d['YEAR'].max()]
    df = df.merge(d, 'right', 'STATECOUNTY_CODE')
    return df
```

```{code-cell} ipython3
def prep_county_info():
    df = download_county_boundaries()[['STATEFP', 'GEOID', 'NAME', 'ALAND']].rename(columns={'GEOID': 'STATECOUNTY_CODE'})
    d = data['county FAI share']
    d = d.to_frame('FAI_EMP_SHARE').reset_index().dropna(subset=['FAI_EMP_SHARE'])
    d = d[d['YEAR'] == d['YEAR'].max()]
    df = df.merge(d, 'left')
    d = data['RUC']['rural areas'][['STATECOUNTY_CODE', 'UI_LEVEL', 'RUC_LEVEL']]
    df = df.merge(d, 'left') 

    t = dict()
    for r in df.itertuples(False):
        t[r.STATECOUNTY_CODE] = '''{cty} County, {st}
Area: {a:,.0f} sq. km
Population: TODO
Employment: TODO
FAI employment share: {fai:.2%}
CBSA: {cbsa}
RUC code: {ruc}
UI code: {ui}'''.format(cty=r.NAME, st=r.STATE_ABBR, a=r.ALAND/1e6,
                       fai=r.FAI_EMP_SHARE, cbsa='TODO', ruc=r.RUC_LEVEL, ui=r.UI_LEVEL)
    return t
```

```{code-cell} ipython3
def prep_rurality_tables():
    df = pd.DataFrame({'DESCRIPTION': ['Metropolitan', 'Micropolitan'], 'RURAL': [False, False]}, index=['M1', 'M2'])
    df.index.name = 'CBSA_CODE'
    data['CBSA']['rurality table'] = df
    
    df = ers_codes.get_ui_df()
    df = df.loc[df['UI_YEAR'] == 2013, ['UI_CODE', 'UI_CODE_DESCRIPTION']]
    df = df.drop_duplicates().rename(columns={'UI_CODE_DESCRIPTION': 'DESCRIPTION'})
    df['RURAL'] = df['UI_CODE'].map(data['UI']['rural levels'])
    df = df.set_index('UI_CODE').sort_index()
    data['UI']['rurality table'] = df
    
    df = ers_codes.get_ruc_df()
    df = df.loc[df['RUC_YEAR'] == 2013, ['RUC_CODE', 'RUC_CODE_DESCRIPTION']]
    df = df.drop_duplicates().rename(columns={'RUC_CODE_DESCRIPTION': 'DESCRIPTION'})
    df['RURAL'] = df['RUC_CODE'].map(data['RUC']['rural levels'])
    df = df.set_index('RUC_CODE').sort_index()
    data['RUC']['rurality table'] = df
    
    df = pd.DataFrame({'DESCRIPTION': [
        'Metropolitan area core: primary flow within an urbanized area (UA)',
        'Metropolitan area high commuting: primary flow 30% or more to a UA',
        'Metropolitan area low commuting: primary flow 10% to 30% to a UA',
        'Micropolitan area core: primary flow within an urban cluster of 10,000 to 49,999 (large UC)',
        'Micropolitan high commuting: primary flow 30% or more to a large UC',
        'Micropolitan low commuting: primary flow 10% to 30% to a large UC',
        'Small town core: primary flow within an urban cluster of 2,500 to 9,999 (small UC)',
        'Small town high commuting: primary flow 30% or more to a small UC',
        'Small town low commuting: primary flow 10% to 30% to a small UC',
        'Rural areas: primary flow to a tract outside a UA or UC'],
                       'RUCA_CODE': list(range(1, 11))})
    df['RURAL'] = df['RUCA_CODE'].map(data['RUCA']['rural levels'])
    df = df.set_index('RUCA_CODE').sort_index()
    data['RUCA']['rurality table'] = df
```

```{code-cell} ipython3
# economics by year, geography, FAI and rurality

def compute_econ(years=None, states=None):
    cols = ['YEAR', 'STATE', 'FIPS_CODE', 'EMPLOYEES', 'NAICS', 'UI_CODE', 'RUC_CODE', 'RUCA_CODE', 'CBSA_LEVEL']
    df = rurality.get_df(years=years, cols=cols, states=states)
    df['STATE'] = df['STATE'].cat.remove_unused_categories()
    df = df.rename(columns={'STATE': 'STATE_ABBR', 
                            'FIPS_CODE': 'STATECOUNTY_CODE'})
    df['RURAL_CBSA'] = df['CBSA_LEVEL'].isna()
    df['RURAL_UI'] = df['UI_CODE'].map(data['UI']['rural levels'])
    df['RURAL_RUC'] = df['RUC_CODE'].map(data['RUC']['rural levels'])
    df['RURAL_RUCA'] = df['RUCA_CODE'].astype(float).round().map(data['RUCA']['rural levels'])

    fai = json.load(open('../data/fai_subsectors.json'))
    fai = [c for subsector in fai.values() for c in subsector.keys()]
    fai = pd.DataFrame({'NAICS': fai})
    fai['FAI'] = True
    df['NAICS'] = df['NAICS'].str[:6]
    df = df.merge(fai, 'left', 'NAICS')
    df['FAI'] = df['FAI'].fillna(False)

    for rural_classification in ['CBSA', 'UI', 'RUC', 'RUCA']:
        rural_col = f'RURAL_{rural_classification}'
        d = df.groupby(['YEAR', 'STATE_ABBR', 'FAI', rural_col])['EMPLOYEES'].agg(['size', 'sum'])
        d.columns.name = 'SIZE_MEASURE'
        d = d.rename(columns={'size': 'establishments', 'sum': 'employees'})
        d = d.stack('SIZE_MEASURE').unstack([rural_col, 'FAI']).sort_index(1)
        d = d.apply(lambda c: c / d.sum(1))
        data[rural_classification]['econ shares'] = d

    d = df.groupby(['YEAR', 'STATE_ABBR', 'STATECOUNTY_CODE', 'FAI'])['EMPLOYEES'].sum()
    d = d.unstack('FAI')
    d = d[True] / d.sum(1)
    data['county FAI share'] = d
```

```{code-cell} ipython3
@simplecache(PATH.data)
def prepare_all_data():
    compute_econ(years=OPTS['years'], states=OPTS['states'])
    data['CBSA']['rural areas'] = prep_cbsa()
    data['UI']['rural areas'] = data['RUC']['rural areas'] = prep_county()
    data['RUCA']['rural areas'] = prep_tract()
    data['county FAI share geo'] = prep_county_fai()
    prep_rurality_tables()
    return data
```

```{code-cell} ipython3
data = {
    'CBSA': {},
    'UI': {},
    'RUC': {},
    'RUCA': {}
}

data['CBSA']['rural levels'] = {'M1': False, 'M2': False}
data['UI']['rural levels'] = {'1': False, '2': False, '3': False, '4': False, '5': False,
                              '6': True, '7': True, '8': True, '9': True, '10': True, '11': True, '12': True}
data['RUC']['rural levels'] = {'0': False, '1': False, '2': False, '3': False, '4': False, '6': False, '8': True,
                               '5': True, '7': True, '9': True}
data['RUCA']['rural levels'] = {x: x == 10 for x in range(1, 11)}

if OPTS['force_recalc']:
    PATH.data.unlink(missing_ok=True)
data = prepare_all_data()
data['county info'] = prep_county_info()
```

# Prepare widgets

```{code-cell} ipython3
pd.options.display.max_colwidth = 200
```

```{code-cell} ipython3
class TableRurality:
    def __init__(self):
        self.widget = widgets.Output()
    
    def update(self, rural_classification):
        df = data[rural_classification]['rurality table'][['RURAL', 'DESCRIPTION']]
        with self.widget:
            self.widget.clear_output(True)
            display(df)
            
w_rurality = TableRurality()
```

```{code-cell} ipython3
class TableShares:
    def __init__(self):
        self.widget = widgets.Output()
    
    def update(self, state, rural_classification):
        idx = pd.IndexSlice
        df = data[rural_classification]['econ shares'].loc[idx[:, state, 'employees'], :].droplevel(['STATE_ABBR', 'SIZE_MEASURE'])
        with self.widget:
            self.widget.clear_output(True)
            print('Share of employment')
            display(df.T.style.format('{:.2%}'))
            
w_shares = TableShares()
```

```{code-cell} ipython3
class Map:
    def __init__(self):
        self.widget = m = leaflet.Map(center=(38, -95), zoom=4)
        m.layout.height = '500px'
        m.layout.width = '800px'
        
        self.rural_classification = 'CBSA'
        self.rural_layer = l = leaflet.GeoJSON()
        m.add_layer(l)
        
        self.fai_layer = leaflet.GeoJSON()
        m.add_layer(self.fai_layer)
        
        self.county_info = widgets.Output()
        m.add_control(leaflet.WidgetControl(widget=self.county_info, position='bottomright'))
        
    def rural_style(self, area):
        rural = area['properties'][f'RURAL_{self.rural_classification}']
        fc = 'yellow' if rural else 'gray'
        return dict(fillColor=fc)
    
    def replace_rural_layer(self, state, rural_classification):
        self.rural_classification = rural_classification
        df = data[rural_classification]['rural areas']
        if rural_classification == 'CBSA':
            df = df[df['CBSA_NAME'].str.contains(state)]
        else:
            df = df[df['STATE_ABBR'] == state]
            
        self.widget.remove_layer(self.rural_layer)
        self.rural_layer = l = leaflet.GeoJSON(name='Rurality', data=json.loads(df.to_json()), 
                                               style={'stroke': False, 'fillOpacity': 0.5}, style_callback=self.rural_style)
        self.widget.add_layer(l)
        self.widget.fit_bounds(self.poly_bounds(df.geometry))
        
    def fai_style(self, area):
        sh = area['properties']['FAI_EMP_SHARE']
        if sh < 0.05:
            return dict(color='black', weight=1, dashArray='9')
        else:
            c = 'red' if sh > 0.1 else 'orange'
            return dict(color=c, weight=3, dashArray='0')
    
    def show_county_info(self, feature, **kwargs):
        code = feature['properties']['STATECOUNTY_CODE']
        with self.county_info:
            self.county_info.clear_output(True)
            print(data['county info'][code])
    
    def replace_fai_layer(self, state):
        df = data['county FAI share geo']
        df = df[df['STATE_ABBR'] == state]
        self.widget.remove_layer(self.fai_layer)
        self.fai_layer = leaflet.GeoJSON(name='FAI', data=json.loads(df.to_json()), 
                                         style={'opacity': 0.7, 'fillOpacity': 0},
                                         style_callback=self.fai_style,
                                         hover_style={'dashArray': '0', 'opacity': 1})
        self.fai_layer.on_hover(self.show_county_info)
        self.widget.add_layer(self.fai_layer)
        
    def update(self, state, rural_classification):
        self.replace_rural_layer(state, rural_classification)
        self.replace_fai_layer(state)
        
    
    @staticmethod
    def poly_bounds(polygons):
        polygons = list(polygons)
        """Bounding box for `ipyleaflet.Map.fit_bounds()`."""
        xmin, ymin, xmax, ymax = shapely.geometry.MultiPolygon(polygons).bounds
        return [[ymin, xmax], [ymax, xmin]]

w_map = Map()
```

```{code-cell} ipython3
data_states = data['CBSA']['econ shares'].index.get_level_values('STATE_ABBR').categories.tolist()
w_state = widgets.Dropdown(description='State', options=sorted(data_states))
w_rural_scheme = widgets.RadioButtons(description='Rurality', options=[('Core Based Statistical Area', 'CBSA'), 
                                                                       ('Rural-Urban Continuum', 'RUC'), 
                                                                       ('Urban Influence', 'UI'),
                                                                       ('Rural-Urban Commuting Area', 'RUCA')])

def update(change):
    w_rurality.update(w_rural_scheme.value)
    w_shares.update(w_state.value, w_rural_scheme.value)
    w_map.update(w_state.value, w_rural_scheme.value)
    

w_state.observe(update, names='value')
w_rural_scheme.observe(update, names='value')
```

# Dashboard

```{code-cell} ipython3
widgets.HBox([w_state, w_rural_scheme])
```

```{code-cell} ipython3
w_rurality.widget
```

```{code-cell} ipython3
print('Shading: rural = yellow, non-rural = gray.\nCounty border: FAI employment share between 5% and 10% (orange) or above 10% (red).')
w_map.widget
```

```{code-cell} ipython3
w_shares.widget
```
