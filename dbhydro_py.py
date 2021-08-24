import pandas as pd


def get_river(period='1week', dbkey='91686/91687/91401/91399/91656/AL760/WH036',):
    '''
    period: options are  'year', 'month', '1week', '3day', 'today', 
                        'uspec&v_start_date=YYYYMMDD&v_end_date=YYYYMMDD'

    dbkey: dbhydro code for var/station. to get other codes see link on dbkeys
    ________________________________________________________
                        
    documentation : pages 87-89 https://www.sfwmd.gov/sites/default/files/dbhydro_browser_user_documentation.pdf
    
    dbkeys: https://my.sfwmd.gov/dbhydroplsql/show_dbkey_info.show_dbkeys_matched?v_station=L001&v_js_flag=N
    ________________________________________________________
    '''
    
    link = [
        "http://my.sfwmd.gov/dbhydroplsql/web_io.report_process?"+
        "v_period=%s&" % period+
        "v_report_type=format6&"+
        "v_target_code=file_csv&"+
        "v_run_mode=onLine&v_js_flag=Y&"+
        "v_dbkey=%s" % dbkey
    ]
    print(link)

    skip = len(dbkey.split('/'))+2  # the number of blabla lines will be the number of variables + 2

    df = pd.read_csv(link[0], skiprows=skip, parse_dates=['Daily Date'])

    df.to_csv('raw.csv')
    # [ 'S65EX1_S', 'S65E_S', 'S84_S', 'S84XS', 'S154C_C', 'S154_C', 'G34_C', ]
    var_key = {
            '91686': 'Mean flow [cfs] S84X_S',
            '91687': 'Mean flow [cfs] S84_S',
            '91401': 'Mean flow [cfs] S154_C',
            '91399': 'Mean flow [cfs] S154C_C',
            '91656': 'Mean flow [cfs] S65E_S',
            'AL760': 'Mean flow [cfs] S65EX1_S',
            'WH036': 'Mean flow [cfs] FISHCR',
            'AL761': 'Flow [cfs] S65EX1_S',

            }
    
    frames = []
    for ID in df['DBKEY'].unique():
        sub = df.loc[df['DBKEY']==ID].set_index('Daily Date')
        cname = sub['Station']
        flow = sub['Data Value'].rename(cname[0])
        # sub = sub.loc[~sub.index.duplicated(keep='first')]
        frames.append(flow)
    
    return pd.concat(frames, axis=1)


def get_ts(period='1week', station='L001', dvar=['Date', 'Val'], dbkey='IX837/IX845/IX847'):
    '''
    period: options are  'year', 'month', '1week', '3day', 'today', 
                        'uspec&v_start_date=YYYYMMDD&v_end_date=YYYYMMDD'

    dbkey: (Optional, is station is not given) dbhydro code for var/station. to get other codes see link on dbkeys

    station: Optional. to get a pre selected subset of data from a station.
            Optios are: L001, L005
    ________________________________________________________
                        
    documentation : pages 87-89 https://www.sfwmd.gov/sites/default/files/dbhydro_browser_user_documentation.pdf
    
    dbkeys: https://my.sfwmd.gov/dbhydroplsql/show_dbkey_info.show_dbkeys_matched?v_station=L001&v_js_flag=N
    ________________________________________________________
    '''

    if station is 'L001':
        dbkey = 'IX837/IX845/IX847/UT737/KV264/KV247'
    elif station is 'L005':
        dbkey = 'IX857/IX864/IX866/UT739/KV266/KV249/'
    else:
        if not dbkey:
            print('Only L001 and L005 stations preset. Provide station or dbkey.')

    link = [
        "http://my.sfwmd.gov/dbhydroplsql/web_io.report_process?"+
        "v_period=%s&" % period+
        "v_report_type=format6&"+
        "v_target_code=file_csv&"+
        "v_run_mode=onLine&v_js_flag=Y&"+
        "v_dbkey=%s" % dbkey
    ]
    print(link)

    skip = len(dbkey.split('/'))+2  # the number of blabla lines will be the number of variables + 2

    df = pd.read_csv(link[0], skiprows=skip, 
                     usecols=[0,2,3,],
                     names = [dvar[0], 'DBKEY', dvar[1]],
                     parse_dates=[dvar[0]]
            )

    var_key = {
    # L001
            'IX837': 'Air Temp [ºC]',
            'IX845': 'Rain [in]',
            'IX847': 'Wind speed [mph]',
            'UT737': 'Wind direction [degrees clockwise from North]',
            'KV264': 'VECTOR WIND DIRECTION',
            'KV247': 'VECTOR WIND SPEED',
    #L005
            'IX857': 'Air Temp [ºC]',
            'IX864': 'Rain [in]',
            'IX866': 'Wind speed [mph]',
            'UT739': 'Wind direction [degrees clockwise from North]',
            'KV266': 'VECTOR WIND DIRECTION',
            'KV249': 'VECTOR WIND SPEED',
            # '12515': 'Rain [in/day]'
            }
    df.replace({'DBKEY': var_key}, inplace=True)
    
    frames = []
    for name in df['DBKEY'].unique():
        sub = df.loc[df['DBKEY']==name].rename(columns={dvar[1]: name}).set_index(dvar[0])
        sub = sub.loc[~sub.index.duplicated(keep='first')]
        frames.append(sub.drop(columns='DBKEY'))
    
    return pd.concat(frames, axis=1)