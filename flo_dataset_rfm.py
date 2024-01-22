#FLO DATASET RFM DOSYASI

#-----KÜTÜPHANE VE AYARLAR-------#
import datetime as dt
import pandas as pd
pd.set_option("display.max_columns",None)
pd.set_option("display.max_rows",500)
pd.set_option("display.float_format",lambda x:"%.3f"%x)


#-----VERİ HAZIRLAMA-------#
flo_df=pd.read_csv("flo_data_20k.csv")
df_flo=flo_df.copy()

df_flo.head(10)
df_flo.columns
df_flo.shape
df_flo.describe().T
df_flo.isnull().sum() #boş değer yok(not null)
df_flo.dtypes #sütunların tipleri hakkında bilgi
df_flo.info() #tarih bilgileri object olarak kullanılmış

df_flo["order_channel"].value_counts()
df_flo["last_order_channel"].value_counts()

df_flo["order_num_total"]=df_flo["order_num_total_ever_online"]+df_flo["order_num_total_ever_offline"]
df_flo["order_value_total"]=df_flo["customer_value_total_ever_offline"]+df_flo["customer_value_total_ever_online"]

date_columns = df_flo.columns[df_flo.columns.str.contains("date")] #içinde date geçen değişkenleri seçtik
df_flo[date_columns] = df_flo[date_columns].apply(pd.to_datetime) #bu değişkenleri apply fonksiyonu ile datetime formatına çevirdik

df_flo.groupby("order_channel").agg({"master_id":"count", #farklı kanallardaki müşteri sayısı
                                     "order_num_total":"sum", #farklı kanallardaki toplam sipariş sayısı
                                     "order_value_total":"sum"}) #farklı kanallardaki toplam sipariş tutarı

df_flo["order_value_total"].sort_values(ascending=False).head(10)
df_flo["order_num_total"].sort_values(ascending=False).head(10)


#-----VERİ ÖNHAZIRLAMAYI FONKSİYONLAŞTIRMA-------#
def prepare_data(dataframe):
    dataframe["order_num_total"]=dataframe["order_num_total_ever_online"]+dataframe["order_num_total_ever_offline"]
    dataframe["order_value_total"] = dataframe["customer_value_total_ever_offline"] + dataframe["customer_value_total_ever_online"]
    date_columns = dataframe.columns[dataframe.columns.str.contains("date")]
    dataframe[date_columns] = dataframe[date_columns].apply(pd.to_datetime)
    return dataframe


#-----RFM METRİKLERİ BELİRLEME-------#
df_flo["last_order_date"].max() #2021-05-30
analysis_date = dt.datetime(2021,6,1) #yıl,ay,gün şeklinde, son sipariş tarihinin 2 gün sonrası

rfm=pd.DataFrame()
rfm["customer_id"] = df_flo["master_id"]
rfm["recency"] = (analysis_date - df_flo["last_order_date"]).astype('timedelta64[D]')
rfm["frequency"] = df_flo["order_num_total"]
rfm["monetary"] = df_flo["order_value_total"]

rfm=rfm[(rfm["monetary"]>0)]
rfm.head()


#-----RFM METRİKLERİNİ SKORLAŞTIRMA-------#
rfm["recency_score"]=pd.qcut(rfm["recency"],5,labels=["5","4","3","2","1"])
rfm["frequency_score"]=pd.qcut(rfm["frequency"].rank(method="first"),5,labels=["5","4","3","2","1"])
rfm["monetary_score"]=pd.qcut(rfm["monetary"],5,labels=["5","4","3","2","1"])

rfm["rf_score"]=rfm["recency_score"].astype(str)+rfm["frequency_score"].astype(str)
rfm["rfm_score"]=rfm["recency_score"].astype(str)+rfm["frequency_score"].astype(str)+rfm["monetary_score"].astype(str)


#-----RFM METRİKLERİNİ SEGMENTLEŞTİRME-------#
seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}
rfm['segment'] = rfm['rf_score'].replace(seg_map, regex=True)


#-----RFM METRİKLERİNDEN AKSİYON ALMAK-------#
rfm[["segment", "recency", "frequency", "monetary"]].groupby("segment").agg(["mean", "count"])

target_segments_customer_ids = rfm[rfm["segment"].isin(["champions","loyal_customers"])]["customer_id"]
cust_ids = df[(df["master_id"].isin(target_segments_customer_ids)) &(df["interested_in_categories_12"].str.contains("KADIN"))]["master_id"]
cust_ids.to_csv("yeni_marka_hedef_müşteri_id.csv", index=False)
cust_ids.shape

target_segments_customer_ids = rfm[rfm["segment"].isin(["cant_loose","hibernating","new_customers"])]["customer_id"]
cust_ids = df[(df["master_id"].isin(target_segments_customer_ids)) & ((df["interested_in_categories_12"].str.contains("ERKEK"))|(df["interested_in_categories_12"].str.contains("COCUK")))]["master_id"]
cust_ids.to_csv("indirim_hedef_müşteri_ids.csv", index=False)