#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd

url = "http://transport.productsup.io/e9ba40e8e3597b1588a0/channel/188050/vidaXL_ch_de_dropshipping.csv"
data = pd.read_csv(url)

df = pd.DataFrame({
    "gtin": data["EAN"],
    "title": data["Product_title"],
    "brand": data["Brand"],
    "main_image_url": data["Image 1"],
    "description": data["HTML_description"],
    "category": data["Category"],
    "country": "CH",
    "condition": "new",
    "price": data["B2B price"] * 1.3 - 2.95,
    "tax": 8.1,
    "currency": "CHF",
    "delivery_time_days": data["estimated_total_delivery_time"],
    "stock": data["Stock"],
    "return_days": 40,
    "size": data["Size"],
    "image_2_url": data.get("Image 2", ""),
    "image_3_url": data.get("Image 3", ""),
    "image_4_url": data.get("Image 4", ""),
    "image_5_url": data.get("Image 5", "")
})

df.drop_duplicates(subset='gtin', keep='first', inplace=True)
df['gtin'] = pd.to_numeric(df['gtin'], errors='coerce').dropna().astype(int)

df = df[
    df[['gtin', 'title', 'country', 'condition', 'price', 'currency']].notnull().all(axis=1) &
    df[['gtin', 'title', 'country', 'condition', 'price', 'currency']].astype(str).ne('0').all(axis=1) &
    df[['title', 'country', 'condition', 'currency']].ne('').all(axis=1)
]

df.loc[df['stock'] == 0, 'delivery_time_days'] = 0
df['stock'] = pd.to_numeric(df['stock'], errors='coerce').fillna(0).astype(int)
df['delivery_time_days'] = pd.to_numeric(df['delivery_time_days'], errors='coerce').dropna().astype(int)

df_old = pd.read_csv("seller10.csv")
df_difference = df_old[~df_old['gtin'].isin(df['gtin'])].copy()
df_difference['stock'] = 0
df_difference['delivery_time_days'] = 0
df = pd.concat([df, df_difference], ignore_index=True)

mapping_file_path = "vidaxl_category_mapping.xlsx"        
mapping_df = pd.read_excel(mapping_file_path, dtype={"ZF": str, "Nalda": str})
category_mapping = dict(zip(mapping_df["ZF"], mapping_df["Nalda"]))
df["category"] = df["category"].map(category_mapping).astype("string")

output_csv = "seller10.csv"
output_xlsx = "seller10.xlsx"
df.to_csv(output_csv, index=False)
df.head(1000000).to_excel(output_xlsx, index=False)


# In[4]:


import paramiko

def upload_file(hostname, port, username, password, local_file, remote_file):
    with paramiko.Transport((hostname, port)) as transport:
        transport.connect(username=username, password=password)
        with paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.put(local_file, remote_file)

port = 2022
username = "nalda-seller-10"
local_file = remote_file = "seller10.csv"

upload_file("sftp.nalda.com", port, username, "bmvPAetiB6gy%v4w", local_file, remote_file)
upload_file("sftp-staging.nalda.com", port, username, "nalda-seller-10", local_file, remote_file)

