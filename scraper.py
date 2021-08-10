import pandas as pd


def get_dbhydro(period='1week', dbkey='IX837/IX845/IX847', dvar=['Date', 'Val'], station=None):
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
        'Only L001 and L005 preset. Provide dbkey.'

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
                     # usecols = [dvar[0], 'DBKEY', dvar[1]], 
                     usecols=[0,2,3,],
                     names = [dvar[0], 'DBKEY', dvar[1]],
                     parse_dates=[dvar[0]]
            )
    # df = pd.read_csv(link[0], skiprows=skip, 
    #              # names = [dvar, 'DBKEY', 'Val'], 
    #              # usecols=[0,2,3,],
    #              parse_dates=[dvar]
    #         )
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


if __name__ == "__main__":

    # dbkey = '12515'
    # dbkey = 'IX864'
    # dbkey = 'IY029'
    dbkey = 'IX837/IX845/IX847/UT737/KV264/KV247'
    period = 'uspec&v_start_date=20210630'
    # dvar = ['Daily Date', 'Data Value']
    # dvar = ['Date', 'Val']
    output_path = './'
    df = get_dbhydro(period=period, station='L005')
    df.to_csv(output_path+'L005_atmo.csv')


   
