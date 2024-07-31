import pandas as pd
import numpy as np
from statsmodels.robust import mad
import math
import datetime
import json
import warnings
import re
warnings.filterwarnings('ignore')

# Visits Data - Product (Target Variable) - US All Product (Web-App) Visits
def visits_product(file_path):
    df_v1 = pd.read_excel(file_path+"US All Product (Web-App) Visits 2021-10.17.23 - REVISED 1.4.24.xlsx")
    df_v2 = pd.read_excel(file_path+"US All Product (Web-App) Visits 10.1.23-12.31.23.xlsx")
    df_v1['Date'] = pd.to_datetime(df_v1['Date'])
    df_v2['Date'] = pd.to_datetime(df_v2['Date'])
    df_v1 = df_v1[df_v1['Date']<'2023-10-01']
    df_ = pd.concat([df_v1, df_v2])
    df_['% of Goal']=df_['% of Goal'].replace('-', pd.NA)
    df_['US Weather Impact Rating']=df_['US Weather Impact Rating'].replace('-',pd.NA)
    return df_

def visits_product_aggregate(file_path):
    df_ = visits_product(file_path)
    df_grp = df_.groupby('Date').agg({'Value':'sum', 'Goal':'sum', '% of Goal':'mean'}).reset_index()
    df_grp = pd.merge(df_grp, df_[['Date','US Weather Impact Rating']].drop_duplicates().sort_values('Date').reset_index(drop=True), on='Date')
    df_grp['% of Goal'] = df_grp['Value']/df_grp['Goal']
    df_grp['% of Goal'] = df_grp['% of Goal'].replace([np.inf, -np.inf], 0)
    return df_grp

def title_case(var):
    return ''.join(x for x in var.title() if not x.isspace())

def get_model_product_visits(file_path):
    df_ = visits_product(file_path)
    df_['Date'] = pd.to_datetime(df_['Date'])
    df_result = pd.DataFrame(columns=['Date'])
    for ven in df_['Product'].unique():
        temp = df_[df_['Product']==ven]
        var = title_case(temp['Platform'].values[0])+"_"+ven
        df_grp = temp.groupby(['Date', 'Platform', 'Product']).sum().reset_index()
        df_grp = df_grp.drop(['Platform', 'Product'], axis=1)
        df_grp = df_grp.rename({'Value':var+"_Visits", 'Goal':var+"_Goal"}, axis=1)
        df_result = pd.merge(df_result, df_grp, on='Date', how='outer')
    df_grp = visits_product_aggregate(file_path)
    df_grp = df_grp.drop('% of Goal', axis=1)
    df_grp = df_grp.rename({'Value':"Overall_Product_Visits", 'Goal':"Overall_Product_Goal"}, axis=1)
    df_result = pd.merge(df_result, df_grp, on='Date', how='outer')
    df_result['US Weather Impact Rating'] = df_result['US Weather Impact Rating'].astype('float')
    return df_result

# Visits - Partner - Desktop Web and Mobile Web (Will not be used for MMM as more robust visits data at Platform/Product is available)
def visits_pre_processing(df_21, df_22, df_23):
    df_21.columns = df_21.columns.str.strip()
    df_21['par_consolidated'] = df_21['par_consolidated'].str.strip()
    df_22.columns = df_22.columns.str.strip()
    df_22['par_consolidated'] = df_22['par_consolidated'].str.strip()
    df_23.columns = df_23.columns.str.strip()
    df_23['par_consolidated'] = df_23['par_consolidated'].str.strip()
    df_ = pd.merge(df_21, df_22, on='par_consolidated', how='outer')
    df_ = pd.merge(df_, df_23, on='par_consolidated', how='outer')
    df_columns = df_.columns
    df_columns_to_include = df_columns[df_columns>'2021-08-01']
    df_ = df_[df_columns_to_include]
    df_.set_index('par_consolidated', inplace=True)
    df_ = df_.T
    df_=df_.reset_index()
    df_=df_.rename({'index':'Date'}, axis=1)
    return df_

def visits_get_partner_columns(df_):
    df_['All Partner'] = 0
    df_['Partner + No Partner'] = 0
    col = list(set(df_.columns)-set(['index', 'No Partner']))
    df_['All Partner'] = df_[col].fillna(0).sum(axis = 1)
    col = list(set(df_.columns)-set(['index', 'All Partner']))
    df_['Partner + No Partner'] = df_[col].fillna(0).sum(axis = 1)
    return df_

def visits_desktop_partner(file_path, agg):
    df_21 = pd.read_excel(file_path+"US Desktop Web Visits - Partner - 2021.xlsx", skiprows=6)
    df_22 = pd.read_excel(file_path+"US Desktop Web Visits - Partner - 2022.xlsx", skiprows=6)
    df_23 = pd.read_excel(file_path+"US Desktop Web Visits - Partner - 2023 1.1-10.15.xlsx", skiprows=6)
    df_ = visits_pre_processing(df_21, df_22, df_23)
    if agg == 1:
        df_ = visits_get_partner_columns(df_)
    return df_

def visits_mobile_partner(file_path, agg):
    df_21 = pd.read_excel(file_path+"US Mobile Web Visits - Partner - 2021.xlsx", skiprows=5)
    df_22 = pd.read_excel(file_path+"US Mobile Web Visits - Partner - 2022.xlsx", skiprows=5)
    df_23 = pd.read_excel(file_path+"US Mobile Web Visits - Partner - 2023 1.1-10.15.xlsx", skiprows=5)
    df_ = visits_pre_processing(df_21, df_22, df_23)
    if agg == 1:
        df_ = visits_get_partner_columns(df_)
    return df_

def get_desktop_mobile_combined(file_path, agg):
    df_web = visits_desktop_partner(file_path, agg)
    df_mobile = visits_mobile_partner(file_path, agg)
    df_ = pd.concat([df_web, df_mobile], ignore_index=True)
    df_ = df_.groupby('Date').sum().reset_index()
    df_ = df_[df_['Date']!='2023-10-16']
    return df_

def get_model_partner_visits(file_path):
    df_ = get_desktop_mobile_combined(file_path, agg = 1)
    df_ = df_[['Date', 'Partner + No Partner']]
    df_ = df_.rename({'Partner + No Partner':'Partner_Visits_Overall_(Desktop&Mobile)'}, axis=1)
    return df_

# Organic Search - Impressions/Clicks: Google Platform
def get_organic_search_google(file_path):
    df_v1 = pd.read_excel(file_path+"Search Impressions - Google Only - 6.18.22-10.17.23.xlsx", sheet_name='Dates')
    # df_v2 = pd.read_excel(file_path+"Search Impressions - Google Only - 10.18.23-12.31.23.xlsx", sheet_name='Dates')
    df_v2 = pd.read_excel(file_path+"Search Impressions - Google Only - 10.18.23-12.31.23 FIXED.xlsx", sheet_name='Dates')
    df_ = pd.concat([df_v1, df_v2])
    df_['Date'] = pd.to_datetime(df_['Date'])
    return df_

# SEO Clicks - For Organic Search Clicks Imputation for missing data
def seo_pre_processing(df_21, df_22, df_23):
    df_21.columns = df_21.columns.str.strip()
    df_21['Segment'] = df_21['Segment'].str.strip()
    df_22.columns = df_22.columns.str.strip()
    df_22['Segment'] = df_22['Segment'].str.strip()
    df_23.columns = df_23.columns.str.strip()
    df_23['Segment'] = df_23['Segment'].str.strip()
    df_ = pd.merge(df_21, df_22, on='Segment', how='outer')
    df_ = pd.merge(df_, df_23, on='Segment', how='outer')
    df_columns = df_.columns
    df_columns_to_include = df_columns[df_columns>'2021-08-01']
    df_ = df_[df_columns_to_include]
    df_.set_index('Segment', inplace=True)
    df_ = df_.T
    df_=df_.reset_index()
    df_=df_.rename({'index':'Date'}, axis=1)
    return df_

def seo_get_combined_column(df_):
    df_['Desktop & Mobile SEO Clicks (Combined)'] = df_['Organic Search; United States; desktop'] + df_['Organic Search; United States; mobile web']
    df_ = df_[['Date', 'Desktop & Mobile SEO Clicks (Combined)']]
    return df_

def seo_clicks(file_path):
    df_21 = pd.read_excel(file_path+"US Web SEO Visits - 2021.xlsx", skiprows=6)
    df_22 = pd.read_excel(file_path+"US Web SEO Visits - 2022.xlsx", skiprows=6)
    df_23 = pd.read_excel(file_path+"US Web SEO Visits - 2023 1.1-10.16.xlsx", skiprows=6)
    df_ = seo_pre_processing(df_21, df_22, df_23)
    df_ = seo_get_combined_column(df_)
    return df_

# Pricing/App Installs (Android and iOS)
def pricing_android(file_path):
    df_v1 = pd.read_excel(file_path+"9.1.21_9.30.23 _ Android Overview Dash Table Exported.xlsx")
    df_v1['Date'] = pd.to_datetime(df_v1['Date']).dt.strftime('%Y-%m-%d')
    df_v1['Installs'] = df_v1['Installs'].astype(float)
    df_v2 = pd.read_csv(file_path+"TWC _ Android Q4 2023 Installs.csv")
    df_v2['Date'] = pd.to_datetime(df_v2['Date'], format='%m/%d/%y').dt.strftime('%Y-%m-%d')
    df_v2['Installs'] = df_v2['Installs'].str.replace(',', '').astype('float')
    df_v2 = df_v2[['Media_source', 'Date', 'Installs']]
    df_ = pd.concat([df_v1, df_v2]).reset_index(drop=True)
    df_['Date'] = pd.to_datetime(df_['Date'])
    return df_

def pricing_iOS(file_path):
    df_v1 = pd.read_excel(file_path+"9.1.21_9.30.23 _ iOS Overview Dash Table Exported.xlsx")
    df_v1['Date'] = pd.to_datetime(df_v1['Date']).dt.strftime('%Y-%m-%d')
    df_v1['Installs'] = df_v1['Installs'].astype(float)
    df_v2 = pd.read_csv(file_path+"TWC _ iOS Q4 2023 Installs.csv")
    df_v2['Date'] = pd.to_datetime(df_v2['Date'], format='%m/%d/%y').dt.strftime('%Y-%m-%d')
    df_v2['Installs'] = df_v2['Installs'].str.replace(',', '').astype('float')
    df_v2 = df_v2[['Media_source', 'Date', 'Installs']]
    df_ = pd.concat([df_v1, df_v2]).reset_index(drop=True)
    df_['Date'] = pd.to_datetime(df_['Date'])
    return df_

def pricing_aggregate(file_path):
    df_android = pricing_android(file_path)
    df_iOS = pricing_iOS(file_path)
    df_android = df_android.groupby('Date').sum()
    df_iOS = df_iOS.groupby('Date').sum()
    df_android = df_android.rename({'Installs':'Android_Installs'}, axis=1)
    df_iOS = df_iOS.rename({'Installs':'iOS_Installs'}, axis=1)
    df_ = pd.merge(df_android, df_iOS, on='Date', how='outer').reset_index()
    df_['Total_Installs'] = df_['Android_Installs'] + df_['iOS_Installs']
    return df_

# User Acquisition - Media Spend, Impression and Clicks
def mkt_media_spend_pre_processing(df_, paid_media_sheet):
    df_columns = list(df_.columns)
    if df_['Spend'].dtypes == 'object':
        df_['Spend'] = df_['Spend'].str.replace(',', '').str.replace('$', '').astype('float')
    if 'clicks' in df_.columns:
            df_=df_.rename({'clicks':'Clicks'},axis=1)
    if 'Clicks' in df_.columns and df_['Clicks'].dtypes == 'object':
        df_['Clicks'] = df_['Clicks'].replace({'\$': ''}, regex=True).replace({'\,': ''}, regex=True)
        df_['Clicks'] = df_['Clicks'].astype('float')
    if paid_media_sheet == 'IronSource Sonic':
        df_=df_.rename({'OS':'event_date', 'Day':'OS'}, axis=1)
    if paid_media_sheet == 'Persona.ly':
        df_=df_[~df_['Platform'].isna()]
    if 'event_date' in list(df_.columns):
        df_=df_.rename({'event_date':'Day'}, axis=1)
    if paid_media_sheet == 'Persona.ly':
        df_['new_date'] = df_['Day']
        df_['format'] = 1
        df_.loc[df_.Day.str.contains('/')==True, 'format'] = 2
        df_.loc[df_.format == 2, 'new_date'] = pd.to_datetime(df_.loc[df_.format == 2, 'Day'], format = '%m/%d/%y').dt.strftime('%Y-%m-%d')
        df_.loc[df_.Date_Issue_flag == 1, 'new_date'] = pd.to_datetime(df_.loc[df_.Date_Issue_flag == 1, 'Day'], format = '%Y-%d-%m %H:%M:%S').dt.strftime('%Y-%m-%d')
        df_.loc[df_.format == 1, 'new_date'] = pd.to_datetime(df_.loc[df_.format == 1, 'Day'], format = '%Y-%m-%d %H:%M:%S').dt.strftime('%Y-%m-%d')
        df_.loc[df_.Date_Issue_flag == 1, 'new_date'] = pd.to_datetime(df_.loc[df_.Date_Issue_flag == 1, 'Day'], format = '%Y-%d-%m %H:%M:%S').dt.strftime('%Y-%m-%d')
        df_ = df_.drop('Date_Issue_flag', axis=1)
    else:
        df_['format'] = 1
        df_.loc[df_.Day.str.contains('/')==True, 'format'] = 2
        df_.loc[df_.format == 1, 'new_date'] = pd.to_datetime(df_.loc[df_.format == 1, 'Day'], format = '%Y-%d-%m %H:%M:%S').dt.strftime('%Y-%m-%d')
        if paid_media_sheet == 'Bidease':
            pattern = r'\b\d{2}/\d{2}/\d{2}\b'
            df_.loc[((df_.format == 2) & (df_.Day.str.contains(pattern, regex=True))), 'format'] = 3
            df_.loc[df_.format == 2, 'new_date'] = pd.to_datetime(df_.loc[df_.format == 2, 'Day'], format = '%m/%d/%Y').dt.strftime('%Y-%m-%d')
            df_.loc[df_.format == 3, 'new_date'] = pd.to_datetime(df_.loc[df_.format == 3, 'Day'], format = '%m/%d/%y').dt.strftime('%Y-%m-%d')
        else:
            df_.loc[df_.format == 2, 'new_date'] = pd.to_datetime(df_.loc[df_.format == 2, 'Day'], format = '%m/%d/%y').dt.strftime('%Y-%m-%d')
    if 'event_date' in df_columns:
        df_=df_.rename({'Day':'event_date'}, axis=1)
        date_var = 'event_date'
    else:
        date_var = 'Day'
    df_[date_var] = df_['new_date']
    df_ = df_.drop(['format', 'new_date'], axis=1)
    return df_, date_var
    
def mkt_media_spend_analyis(file_path, paid_media, data_source_dic):
    df_result = pd.DataFrame(columns=['Date'])
    for paid_media_sheet in paid_media:
        if paid_media_sheet == 'Persona.ly':
            df_ = pd.read_excel(file_path+"BL_TWC _ Marketing Spend Data_v4_revised.xlsx", sheet_name='Persona.ly')
        else:
            df_ = pd.read_excel(file_path+"BL_TWC _ Marketing Spend Data_v3.xlsx", sheet_name=paid_media_sheet)
        df_, date_var = mkt_media_spend_pre_processing(df_, paid_media_sheet)
        df_ = df_.rename({'Day':'Date', 'event_date':'Date'}, axis=1)
        # platform = list(df_['Platform'].unique())
        # os_type = df_['OS'].unique()
        # temp_os_var = ''
        # if len(os_type)==1:
        #     temp_os_var = '_Overall'
        # for os in os_type:
        #     df_grp = df_[df_['OS']==os].reset_index(drop=True)
        #     df_grp['Date']=pd.to_datetime(df_grp['Date'])
        #     df_grp = df_grp.groupby(['Date', 'Platform', 'OS']).sum().reset_index()
        #     df_grp = df_grp.drop(['Platform', 'OS'], axis=1)
        #     df_grp = df_grp.rename(columns={col: data_source_dic[paid_media_sheet]+ "_"+ paid_media_sheet +"_" + os + temp_os_var + "_" + col if col != 'Date' else col for col in df_grp.columns})
        #     df_grp.columns = df_grp.columns.str.replace(' ', '_')
        #     df_grp['Date']=pd.to_datetime(df_grp['Date'])
        #     df_result = pd.merge(df_result, df_grp, on='Date', how='outer')
        # if len(os_type)>1:
        #     df_grp = df_.groupby('Date').sum().reset_index()
        #     df_grp['Date']=pd.to_datetime(df_grp['Date'])
        #     df_grp = df_grp.rename(columns={col: data_source_dic[paid_media_sheet]+ "_"+ paid_media_sheet + "_Overall" + "_" + col if col != 'Date' else col for col in df_grp.columns})
        #     df_grp.columns = df_grp.columns.str.replace(' ', '_')
        #     df_result = pd.merge(df_result, df_grp, on='Date', how='outer')
        df_grp = df_.groupby('Date').sum().reset_index()
        df_grp['Date']=pd.to_datetime(df_grp['Date'])
        df_grp = df_grp.rename(columns={col: data_source_dic[paid_media_sheet]+ "_"+ paid_media_sheet + "_" + col if col != 'Date' else col for col in df_grp.columns})
        df_grp.columns = df_grp.columns.str.replace(' ', '_')
        df_result = pd.merge(df_result, df_grp, on='Date', how='outer')
    return df_result

def get_model_user_acquisition_mkt_media_spend(file_path):
    paid_media = ['Google', 'IronSource Aura', 'Bidease', 'Digital Turbine', 'Tapjoy', 'Apple Search Ads', 'LiftOff', 'IronSource Sonic', 'Twitter', 'TikTok', 'Vibe', 'Persona.ly']
    data_source_dic = {'Google':'SEM',
                       'IronSource Aura':'Preload',
                       'Bidease':'Programmatic',
                       'Digital Turbine':'Preload',
                       'Tapjoy':'Programmatic',
                       'Apple Search Ads':'SEM',
                       'LiftOff':'Programmatic',
                       'IronSource Sonic':'Programmatic',
                       'Twitter':'PaidSocial',
                       'TikTok':'Programmatic',
                       'Vibe':'PaidSocial',
                       'Persona.ly':'Programmatic'
                      }
    df_ = mkt_media_spend_analyis(file_path, paid_media, data_source_dic)
    return df_

# Brand - Media Spend, Impression (US Brand Basis)
def US_brand_media_spend_impression(file_path):
    df_v1 = pd.read_excel(file_path+"US Brand - Basis - 2021-2023.xlsx")
    df_v2 = pd.read_excel(file_path+"US Brand - Basis - 10.16.23-12.31.23.xlsx")
    df_ = pd.concat([df_v1, df_v2])
    df_['Date'] = pd.to_datetime(df_['Date'])
    return df_
    
def get_model_brand_media_spend_impression(file_path):
    df_ = US_brand_media_spend_impression(file_path)
    df_result= pd.DataFrame(columns=['Date'])
    # for ven in df_['Vendor'].unique():
    #     temp = df_[df_['Vendor']==ven]
    #     var = temp['Advertising Type'].values[0]+"_"+ven
    #     df_grp = temp.groupby(['Date', 'Vendor', 'Advertising Type']).sum().reset_index()
    #     df_grp = df_grp.drop(['Vendor', 'Advertising Type'], axis = 1)
    #     df_grp = df_grp.rename({'Impressions':var+"_Impressions", 'Spend':var+"_Spend"}, axis=1)
    #     df_result = pd.merge(df_result, df_grp, on='Date', how='outer')
    # for ven in df_['Advertising Type'].unique():
    #     temp = df_[df_['Advertising Type']==ven]
    #     var = temp['Advertising Type'].values[0]
    #     df_grp = temp.groupby(['Date', 'Advertising Type']).sum().reset_index()
    #     df_grp = df_grp.drop(['Advertising Type'], axis = 1)
    #     df_grp = df_grp.rename({'Impressions':var+"_Impressions", 'Spend':var+"_Spend"}, axis=1)
    #     df_result = pd.merge(df_result, df_grp, on='Date', how='outer')
    df_grp = df_.groupby(['Date']).sum().reset_index()
    df_grp = df_grp.rename({'Impressions':"Brand_Impressions", 'Spend':"Brand_Spend"}, axis=1)
    df_result = pd.merge(df_result, df_grp, on='Date', how='outer')
    return df_result

# Brand Health Measures
def get_brand_health_measures(file_path):
    df_ = pd.read_csv(file_path+"Brand health measures 2021 - 2023.csv")
    df_.set_index('Measure', inplace=True)
    df_ = df_.T
    df_=df_.reset_index()
    df_ = df_.rename({'index':'Date'}, axis=1)
    return df_

def get_year_qtr(df):
    pattern = re.compile(r'(\d+)-Q(\d+)')
    df[['Year', 'Quarter']] = df['Date'].apply(lambda x: pd.Series(pattern.match(x).groups()))
    df['Quarter'] = df['Quarter'].astype(int)
    df['Year'] = df['Year'].astype(int)
    return df

def get_model_brand_health_measures(file_path):
    df_qtr = get_brand_health_measures(file_path)
    df_qtr = df_qtr.append({'Date':'2021-Q2'}, ignore_index=True)
    df_qtr = df_qtr.append({'Date':'2023-Q4'}, ignore_index=True)
    df_qtr = df_qtr.append({'Date':'2024-Q1'}, ignore_index=True)
    df_qtr =  get_year_qtr(df_qtr)
    df_qtr = df_qtr.sort_values(['Year', 'Quarter'])
    df_qtr['Date_New'] = pd.to_datetime(df_qtr['Year'].astype(str) + 'Q' + df_qtr['Quarter'].astype(str))
    df_qtr.set_index('Date_New', inplace=True)
    df_ = df_qtr.resample('D').ffill()
    df_.reset_index(inplace=True)
    df_ = df_.drop('Date', axis=1)
    df_ = df_.rename({'Date_New':'Date'}, axis=1)
    df_['Date'] = pd.to_datetime(df_['Date'])
    return df_

# Social Engagement
def social_engagemnet_pre_processing_khoros(file_path):
    df_ = pd.read_excel(file_path+"Social Engagements-2020-2023_Revised.xlsx", sheet_name='2020 - 2023 Oct 25th')
    df_=df_[df_['Outbound Post']!='Outbound Post']
    df_=df_[df_['Impressions']!='Awareness']
    df_=df_[df_['Total Engagements (SUM)']!='Consideration']
    df_=df_.drop(['Unnamed: 52'], axis=1)
    df_ = df_[~df_['Platform'].isna()]
    df_['Platform'] = np.where(df_['Platform']=='Tiktok', 'TikTok', df_['Platform'])
    df_['Platform'] = np.where(df_['Platform']=='Linkedin', 'LinkedIn', df_['Platform'])
    df_['Platform'] = np.where(df_['Platform']=='Youtube', 'YouTube', df_['Platform'])
    df_['Estimated Clicks (SUM)']=df_['Estimated Clicks (SUM)'].astype('float')
    df_['Impressions']=df_['Impressions'].astype('float')
    df_['Total Engagements (SUM)']=df_['Total Engagements (SUM)'].astype('float')
    df_['Date'] = pd.to_datetime(df_['Date'])
    df_ = df_[df_['Source']=='Khoros']
    df_[['Date_Copy', 'Time_Copy']] = df_[['Date', 'Time']]
    df_[['Date', 'Time']] = df_['Date'].astype(str).str.split(' ', expand=True)
    df_['Date'] = pd.to_datetime(df_['Date'])
    return df_

def social_engagemnet_pre_processing_sprinklr(file_path):
    df_ = pd.read_excel(file_path+"spr_web_analyst_with_Impressions_available_after_January_2022.xlsx")
    df_=df_[df_['Outbound Post']!='Outbound Post']
    df_=df_[df_['Impressions']!='Awareness']
    df_=df_[df_['Total Engagements (SUM)']!='Consideration']
    df_ = df_[~df_['Platform'].isna()]
    df_['Platform'] = np.where(df_['Platform']=='Tiktok', 'TikTok', df_['Platform'])
    df_['Platform'] = np.where(df_['Platform']=='Linkedin', 'LinkedIn', df_['Platform'])
    df_['Platform'] = np.where(df_['Platform']=='Youtube', 'YouTube', df_['Platform'])
    df_['Estimated Clicks (SUM)']=df_['Estimated Clicks (SUM)'].astype('float')
    df_['Impressions']=df_['Impressions'].astype('float')
    df_['Total Engagements (SUM)']=df_['Total Engagements (SUM)'].astype('float')
    df_['Date'] = pd.to_datetime(df_['Date'])
    df_ = df_[df_['Platform'].isin(['Facebook', 'LinkedIn', 'YouTube', 'Instagram', 'Twitter', 'TikTok'])]
    df_['Source']='Sprinklr'
    return df_

def get_model_social_engagemnet(file_path):
    df_khrs = social_engagemnet_pre_processing_khoros(file_path)
    df_spr = social_engagemnet_pre_processing_sprinklr(file_path)
    df_ = pd.concat([df_spr, df_khrs], axis=0)
    df_result = pd.DataFrame(columns=['Date'])
    df_['Platform'] = np.where(df_['Platform']=='Tiktok', 'TikTok', df_['Platform'])
    df_['Platform'] = np.where(df_['Platform']=='Linkedin', 'LinkedIn', df_['Platform'])
    df_['Platform'] = np.where(df_['Platform']=='Youtube', 'YouTube', df_['Platform'])
    for plt in df_['Platform'].unique():
        temp = df_[df_['Platform']==plt]
        df_grp = temp.groupby(['Date', 'Platform'])['Impressions',
                                                    'Total Engagements (SUM)',
                                                    'Estimated Clicks (SUM)'].sum().reset_index()
        df_grp = df_grp.drop(['Platform'], axis = 1)
        df_grp = df_grp.rename(columns={col: "SocialEng_" + plt + "_" + col if col != 'Date' else col for col in df_grp.columns})
        df_grp.columns = df_grp.columns.str.rstrip('(SUM)').str.strip()
        df_grp.columns = df_grp.columns.str.replace(' ', '_')
        df_result = pd.merge(df_result, df_grp, on='Date', how='outer')
    return df_result

# Marketing Events
def get_model_mkt_events(file_path, ads_date_range):
    df_ = pd.read_csv(file_path + "CampaignVendor_Events.csv")
    
    df_['Start_Date'] = pd.to_datetime(df_['Start_Date'], format="%d-%m-%Y")
    df_['End_Date'] = pd.to_datetime(df_['End_Date'], format="%d-%m-%Y")
    
    df_campaign = pd.DataFrame(columns=['Date','Campaign_Flag', 'Campaign/Vendor(s)'])
    
    for idx, row in df_.iterrows():
        campaign_name = row['Campaign/Vendor(s)']
        start_date = row['Start_Date']
        end_date = row['End_Date']
        
        df_date_range = pd.DataFrame(pd.date_range(start=start_date, end=end_date, freq='D'), columns=['Date'])
        df_date_range['Campaign_Flag']=1
        df_date_range['Campaign/Vendor(s)'] = campaign_name
        # Merge date range DataFrame with the main DataFrame
        df_campaign = pd.concat([df_campaign, df_date_range], ignore_index=True)
   
        df_campaign = df_campaign.fillna(0)
    df_campaign = df_campaign[(df_campaign['Date']>=ads_date_range[0]) & (df_campaign['Date']<=ads_date_range[1])]
    df_campaign = df_campaign.groupby('Date').agg({'Campaign_Flag': 'max', 'Campaign/Vendor(s)': lambda x: ' ; '.join(x)}).reset_index()
    
    all_dates = pd.DataFrame(pd.date_range(start=ads_date_range[0], end=ads_date_range[1], freq='D'), columns=['Date'])
    
    # Merge with the events DataFrame to include missing dates
    df_campaign = pd.merge(all_dates, df_campaign, on='Date', how='left')
    
    # Fill missing values with 0 and "NA"
    df_campaign['Campaign_Flag'] = df_campaign['Campaign_Flag'].fillna(0).astype(int)
    df_campaign['Campaign/Vendor(s)'] = df_campaign['Campaign/Vendor(s)'].fillna(pd.NA)

    return df_campaign

# Critical Events
def get_model_critical_events(file_path, ads_date_range):
    df_ = pd.read_csv(file_path+"Critical_Events.csv")
    
    df_['Start_Date'] = pd.to_datetime(df_['Start_Date'], format="%d-%m-%Y")
    df_['End_Date'] = pd.to_datetime(df_['End_Date'], format="%d-%m-%Y")

    df_event = pd.DataFrame(columns=['Date', 'Critical_Event_Flag', 'Event_Name'])
    
    for idx, row in df_.iterrows():
        event_name = row['Critical_Event'].split(': ')[1]  # Extract event name after ": "
        start_date = row['Start_Date']
        end_date = row['End_Date']
        
        df_date_range = pd.DataFrame(pd.date_range(start=start_date, end=end_date, freq='D'), columns=['Date'])
        df_date_range['Critical_Event_Flag']=1
        df_date_range['Event_Name'] = event_name
        
        # Merge date range DataFrame with the main DataFrame
        df_event = pd.concat([df_event, df_date_range], ignore_index=True)

        df_event = df_event.fillna(0)
    df_event = df_event[(df_event['Date']>=ads_date_range[0]) & (df_event['Date']<=ads_date_range[1])]
    df_event = df_event.groupby('Date').agg({'Critical_Event_Flag': 'max', 'Event_Name': lambda x: ' ; '.join(x)}).reset_index()
    
    all_dates = pd.DataFrame(pd.date_range(start=ads_date_range[0], end=ads_date_range[1], freq='D'), columns=['Date'])
    
    # Merge with the events DataFrame to include missing dates
    df_event = pd.merge(all_dates, df_event, on='Date', how='left')
    
    # Fill missing values with 0 and "NA"
    df_event['Critical_Event_Flag'] = df_event['Critical_Event_Flag'].fillna(0).astype(int)
    df_event['Event_Name'] = df_event['Event_Name'].fillna(pd.NA)
    
    return df_event

def get_model_data(file_path):

    # Visits Data - Product (Target Variable) - US All Product (Web-App) Visits
    df_model_product_visits = get_model_product_visits(file_path)
    df_model_product_visits['Date'] = pd.to_datetime(df_model_product_visits['Date'])
    print("Product Visits", df_model_product_visits.shape, df_model_product_visits['Date'].min().strftime("%d-%m-%Y"), df_model_product_visits['Date'].max().strftime("%d-%m-%Y"), df_model_product_visits['Date'].max() - df_model_product_visits['Date'].min())
    
    # Visits - Partner - Desktop Web and Mobile Web (Will not be used for MMM as more robust visits data at Platform/Product is available)
    df_model_partner_visits = get_model_partner_visits(file_path)
    df_model_partner_visits['Date'] = pd.to_datetime(df_model_partner_visits['Date'])
    print("Partner Visits", df_model_partner_visits.shape, df_model_partner_visits['Date'].min().strftime("%d-%m-%Y"), df_model_partner_visits['Date'].max().strftime("%d-%m-%Y"), df_model_partner_visits['Date'].max() - df_model_partner_visits['Date'].min())
    
    df_ads = pd.merge(df_model_product_visits,
                      df_model_partner_visits,
                      on = 'Date',
                      how = 'outer')
    print("ADS", df_ads.shape, df_ads['Date'].min().strftime("%d-%m-%Y"), df_ads['Date'].max().strftime("%d-%m-%Y"), df_ads['Date'].max() - df_ads['Date'].min())
    
    # Organic Search - Impressions/Clicks: Google Platform
    df_model_organic_search = get_organic_search_google(file_path)
    df_model_organic_search = df_model_organic_search.rename({'Clicks':'OrganicSearch_Google_Clicks',
                                                              'Impressions':'OrganicSearch_Google_Impressions',
                                                              'CTR':'OrganicSearch_Google_CTR',
                                                              'Position':'OrganicSearch_Google_Position'}, axis=1)
    print("Organic Search", df_model_organic_search.shape, df_model_organic_search['Date'].min().strftime("%d-%m-%Y"), df_model_organic_search['Date'].max().strftime("%d-%m-%Y"), df_model_organic_search['Date'].max() - df_model_organic_search['Date'].min())
    df_ads = pd.merge(df_ads,
                      df_model_organic_search,
                      on = 'Date',
                      how = 'outer')
    print("ADS", df_ads.shape, df_ads['Date'].min().strftime("%d-%m-%Y"), df_ads['Date'].max().strftime("%d-%m-%Y"), df_ads['Date'].max() - df_ads['Date'].min())
    
    # SEO Clicks - For Organic Search Clicks Imputation for missing data
    df_model_seo_clicks = seo_clicks(file_path)
    df_model_seo_clicks = df_model_seo_clicks.rename({'Organic Search; United States; desktop':'SEO_Clicks_OrganicSearch_Desktop',
                                                      'Organic Search; United States; mobile web':'SEO_Clicks_OrganicSearch_MobileWeb',
                                                      'Desktop & Mobile SEO Clicks (Combined)':'SEO_Clicks_OrganicSearch_Desktop_MobileWeb(Combined)'}, axis=1)
    df_model_seo_clicks['Date'] = pd.to_datetime(df_model_seo_clicks['Date'])
    print("SEO Clicks (Organic)", df_model_seo_clicks.shape, df_model_seo_clicks['Date'].min().strftime("%d-%m-%Y"), df_model_seo_clicks['Date'].max().strftime("%d-%m-%Y"), df_model_seo_clicks['Date'].max() - df_model_seo_clicks['Date'].min())
    df_ads = pd.merge(df_ads,
                      df_model_seo_clicks,
                      on = 'Date',
                      how = 'outer')
    print("ADS", df_ads.shape, df_ads['Date'].min().strftime("%d-%m-%Y"), df_ads['Date'].max().strftime("%d-%m-%Y"), df_ads['Date'].max() - df_ads['Date'].min())
    
    # Pricing/App Installs (Android and iOS)
    df_model_pricing_installs = pricing_aggregate(file_path)
    df_model_pricing_installs = df_model_pricing_installs.rename(columns={col: 'Pricing_' + col if col != 'Date' else col for col in df_model_pricing_installs.columns})
    print("Pricing", df_model_pricing_installs.shape, df_model_pricing_installs['Date'].min().strftime("%d-%m-%Y"), df_model_pricing_installs['Date'].max().strftime("%d-%m-%Y"), df_model_pricing_installs['Date'].max() - df_model_pricing_installs['Date'].min())
    df_ads = pd.merge(df_ads,
                      df_model_pricing_installs,
                      on = 'Date',
                      how = 'outer')
    print("ADS", df_ads.shape, df_ads['Date'].min().strftime("%d-%m-%Y"), df_ads['Date'].max().strftime("%d-%m-%Y"), df_ads['Date'].max() - df_ads['Date'].min())

    # User Acquisition - Media Spend, Impression and Clicks
    df_model_mkt_media_spend = get_model_user_acquisition_mkt_media_spend(file_path)
    df_model_mkt_media_spend['Date'] = pd.to_datetime(df_model_mkt_media_spend['Date'])
    print("Media Spend Impression", df_model_mkt_media_spend.shape, df_model_mkt_media_spend['Date'].min().strftime("%d-%m-%Y"), df_model_mkt_media_spend['Date'].max().strftime("%d-%m-%Y"), df_model_mkt_media_spend['Date'].max() - df_model_mkt_media_spend['Date'].min())
    df_ads = pd.merge(df_ads,
                      df_model_mkt_media_spend,
                      on = 'Date',
                      how = 'outer')
    print("ADS", df_ads.shape, df_ads['Date'].min().strftime("%d-%m-%Y"), df_ads['Date'].max().strftime("%d-%m-%Y"), df_ads['Date'].max() - df_ads['Date'].min())

    # Brand - Media Spend, Impression (US Brand Basis)
    df_model_brand_media_spend_impression = get_model_brand_media_spend_impression(file_path)
    print("Brand Media Spend Impression (US Brand Basis)", df_model_brand_media_spend_impression.shape, df_model_brand_media_spend_impression['Date'].min().strftime("%d-%m-%Y"), df_model_brand_media_spend_impression['Date'].max().strftime("%d-%m-%Y"), df_model_brand_media_spend_impression['Date'].max() - df_model_brand_media_spend_impression['Date'].min())
    df_ads = pd.merge(df_ads,
                      df_model_brand_media_spend_impression,
                      on = 'Date',
                      how = 'outer')
    print("ADS", df_ads.shape, df_ads['Date'].min().strftime("%d-%m-%Y"), df_ads['Date'].max().strftime("%d-%m-%Y"), df_ads['Date'].max() - df_ads['Date'].min())
    
    # Brand Health Measures
    df_model_brand_health_measures = get_model_brand_health_measures(file_path)
    df_model_brand_health_measures = df_model_brand_health_measures[df_model_brand_health_measures['Date']<=df_ads['Date'].max()]
    print("Brand Measure", df_model_brand_health_measures.shape, df_model_brand_health_measures['Date'].min().strftime("%d-%m-%Y"), df_model_brand_health_measures['Date'].max().strftime("%d-%m-%Y"), df_model_brand_health_measures['Date'].max() - df_model_brand_health_measures['Date'].min())
    df_ads = pd.merge(df_ads,
                      df_model_brand_health_measures,
                      on = 'Date',
                      how = 'outer')
    print("ADS", df_ads.shape, df_ads['Date'].min().strftime("%d-%m-%Y"), df_ads['Date'].max().strftime("%d-%m-%Y"), df_ads['Date'].max() - df_ads['Date'].min())

    # Social Engagement
    df_model_social_engagemnet = get_model_social_engagemnet(file_path)
    print("Social Engagement", df_model_social_engagemnet.shape, df_model_social_engagemnet['Date'].min().strftime("%d-%m-%Y"), df_model_social_engagemnet['Date'].max().strftime("%d-%m-%Y"), df_model_social_engagemnet['Date'].max() - df_model_social_engagemnet['Date'].min())
    df_ads = pd.merge(df_ads,
                      df_model_social_engagemnet,
                      on = 'Date',
                      how = 'outer')
    print("ADS", df_ads.shape, df_ads['Date'].min().strftime("%d-%m-%Y"), df_ads['Date'].max().strftime("%d-%m-%Y"), df_ads['Date'].max() - df_ads['Date'].min())
        
    # Marketing Events
    df_model_mkt_events = get_model_mkt_events(file_path, [df_ads['Date'].min(), df_ads['Date'].max()])
    df_model_mkt_events = df_model_mkt_events.rename(columns={col: 'EventsCamp/Vend_' + col if col != 'Date' else col for col in df_model_mkt_events.columns})
    print("Marketing Events", df_model_mkt_events.shape, df_model_mkt_events['Date'].min().strftime("%d-%m-%Y"), df_model_mkt_events['Date'].max().strftime("%d-%m-%Y"), df_model_mkt_events['Date'].max() - df_ads['Date'].min())
    df_ads = pd.merge(df_ads,
                      df_model_mkt_events,
                      on = 'Date',
                      how = 'left')
    print("ADS", df_ads.shape, df_ads['Date'].min().strftime("%d-%m-%Y"), df_ads['Date'].max().strftime("%d-%m-%Y"), df_ads['Date'].max() - df_ads['Date'].min())
    
    # Critical Events
    df_model_critical_events = get_model_critical_events(file_path, [df_ads['Date'].min(), df_ads['Date'].max()])
    
    print("Critical Events", df_model_critical_events.shape, df_model_critical_events['Date'].min().strftime("%d-%m-%Y"), df_model_critical_events['Date'].max().strftime("%d-%m-%Y"), df_model_critical_events['Date'].max() - df_model_critical_events['Date'].min())
    df_ads = pd.merge(df_ads,
                      df_model_critical_events,
                      on = 'Date',
                      how = 'left')
    print("ADS", df_ads.shape, df_ads['Date'].min().strftime("%d-%m-%Y"), df_ads['Date'].max().strftime("%d-%m-%Y"), df_ads['Date'].max() - df_ads['Date'].min())

    df_ads = df_ads[(df_ads['Date']>='2021-09-01') & (df_ads['Date']<='2023-12-31')]
    print("ADS", df_ads.shape, df_ads['Date'].min().strftime("%d-%m-%Y"), df_ads['Date'].max().strftime("%d-%m-%Y"), df_ads['Date'].max() - df_ads['Date'].min())

    return df_ads