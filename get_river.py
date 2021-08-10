import pandas as pd


def get_dbhydro(period='1week', dbkey='IX837/IX845/IX847',):
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

    df = pd.read_csv(link[0], skiprows=skip, 
                     # names = ['Date', 'DBKEY', 'Val'], 
                     # names = ['Site', 'Date', 'Val'],
                     # usecols=[0,2,3,],
            )

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
        # vname =var_key[ID]
        sub = df.loc[df['DBKEY']==ID].set_index('Daily Date')
        cname = sub['Station']
        flow = sub['Data Value'].rename(cname[0])
        # sub = sub.loc[~sub.index.duplicated(keep='first')]
        frames.append(flow)
    
    return pd.concat(frames, axis=1)


if __name__ == "__main__":

    # dbkey = 'IX837/IX845/IX847'
    dbkey = '91686/91687/91401/91399/91656/AL760/WH036'

    period = 'uspec&v_start_date=20210630'
    # period = 'uspec&v_start_date=20210603&v_end_date=YYYYMMDD'
    output_path = './'
    df = get_dbhydro(period=period, dbkey=dbkey)
    # df.to_csv(output_path+'dbhydro.csv')
    df.to_csv(output_path+'dbhydro_river.csv')