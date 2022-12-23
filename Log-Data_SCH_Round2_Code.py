import pandas as pd
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta
from calendar import monthrange
import xlrd
import requests
import scipy.stats as st


URL = "https://github.com/schackfest/data/blob/main/DATA_ROUND2_SCHACKFEST2022.xlsx?raw=true"
response = requests.get(URL)
open("Raw.xlsx", "wb").write(response.content)

#Import data
df_R_BOM = pd.read_excel("Raw.xlsx",sheet_name = "2_BOM", usecols = "A,B,C,D")
df_R_RTM = pd.read_excel("Raw.xlsx",sheet_name="5_ITEM MASTER - RAW", usecols = "A,B,C,D,E,F,G")
df_R_FTM = pd.read_excel("Raw.xlsx",sheet_name="6_ITEM MASTER - FG", usecols = "A,B,C,D")

df_I_FDM = pd.read_excel("Raw.xlsx",sheet_name="1_DEMAND", usecols = "A,B,C,D,E,F,G")

df_T_SOG = pd.read_excel("Raw.xlsx",sheet_name="4_SUPPLY-ONGOING", usecols = "A,B,C,D,E,F")
df_T_SOH = pd.read_excel("Raw.xlsx",sheet_name="3_SUPPLY-SOH", usecols = "A,B,C,D")

df_O_ORD = pd.DataFrame({"ITEM":df_R_RTM["ITEM"],"SUPPLIER":df_R_RTM["NHÀ CUNG CẤP"]})


#SOH: 31/12/22

#Process data

M_dict = {
    "JAN_" : "01-01-",
    "FEB_" : "01-02-",
    "MAR_" : "01-03-",
    "APR_" : "01-04-",
    "MAY_" : "01-05-",
    "JUN_" : "01-06-",
    "JUL_" : "01-07-",
    "AUG_" : "01-08-",
    "SEP_" : "01-09-",
    "OCT_" : "01-10-",
    "NOV_" : "01-11-",
    "DEC_" : "01-12-"
    } 

df_R_BOM["SỐ LƯỢNG NGUYÊN LIỆU \n(TRÊN 1 TẤN THÀNH PHẨM)"] = df_R_BOM["SỐ LƯỢNG NGUYÊN LIỆU \n(TRÊN 1 TẤN THÀNH PHẨM)"]/1000 #tấn --> kg
df_R_BOM.rename(columns = {"THÀNH PHẨM":"SKU", "NGUYÊN LIỆU":"ITEM", "SỐ LƯỢNG NGUYÊN LIỆU \n(TRÊN 1 TẤN THÀNH PHẨM)":"BOM_QUANT", "ĐƠN VỊ CỦA NGUYÊN LIỆU":"BOM_UNIT"}, inplace = True)

df_R_RTM.rename(columns = {"LOẠI":"TYPE", "NHÀ CUNG CẤP":"SUPPLIER", "LEADTIME ĐẶT HÀNG (NGÀY)":"LEADTIME", "Đơn vị":"UNIT", "SHELF LIFE ĐƯỢC ƯỚC TÍNH KỂ TỪ NGÀY NHẬP KHO (NGÀY)":"SHELF_LIFE"}, inplace = True)

df_R_FTM.rename(columns = {"SKU":"ITEM", "LOẠI ":"TYPE", "LEADTIME SẢN XUẤT \n(NGÀY)":"LEADTIME", "SHELF LIFE KỂ TỪ NGÀY SẢN XUẤT\n(NGÀY)":"SHELF_LIFE"}, inplace = True)

df_I_FDM = df_I_FDM.melt("ITEM").rename(columns={"variable":"FDM_MONTH","value":"FDM_QUANT"})
df_I_FDM.rename(columns = {"ITEM":"SKU"}    , inplace = True)

df_T_SOH.rename(columns = {"TỒN KHO CUỐI NGÀY 31/12/2022":"SOH_QUANT", "ĐƠN VỊ":"SOH_UNIT", "HẠN SỬ DỤNG":"EXP"}, inplace = True)

df_T_SOG.rename(columns = {"NGUYÊN LIỆU":"ITEM", "MÃ ĐƠN ĐẶT HÀNG":"PO", "NGÀY VỀ":"ARRIVAL_DATE", "SỐ LƯỢNG":"SOH_QUANT", "ĐƠN VỊ":"SOH_UNIT", "HẠN SỬ DỤNG":"EXP"}, inplace = True)

df_R_FTM["SUPPLIER"] = "FESTORY"
df_R_FTM = df_R_FTM.merge(df_R_BOM, how = "left", left_on = "ITEM", right_on = "SKU", suffixes= ("","_BOM"))
df_R_FTM = df_R_FTM[df_R_FTM["ITEM_BOM"] == "BAO_BÌ_ĐÓNG_GÓI"]
df_R_FTM["MOQ"] = 1/df_R_FTM["BOM_QUANT"]

df_R_FTM["UNIT"] = "kg"
df_R_FTM = df_R_FTM[["ITEM", "TYPE", "SUPPLIER", "LEADTIME", "MOQ", "UNIT", "SHELF_LIFE"]]

df_R_ITM = pd.concat([df_R_RTM, df_R_FTM], ignore_index = True)
df_R_ITM.loc[df_R_ITM["SHELF_LIFE"].isnull(), "SHELF_LIFE"] = 0

del df_R_FTM, df_R_RTM

df_I_FDM = df_I_FDM.replace({"FDM_MONTH":M_dict}, regex = True)
df_I_FDM["FDM_MONTH"] = pd.to_datetime(df_I_FDM["FDM_MONTH"], format="%d-%m-%Y")
df_I_FDM["FDM_QUANT"] = df_I_FDM["FDM_QUANT"]*1000 #kg

df_T_SOG["EXP"] = pd.to_datetime(df_T_SOG["EXP"], format="%d-%m-%Y")
df_T_SOG["ARRIVAL_DATE"]=pd.to_datetime(df_T_SOG["ARRIVAL_DATE"].apply(lambda x: xlrd.xldate.xldate_as_datetime(x,0)))
df_T_SOG.sort_values(by=["EXP","ARRIVAL_DATE"], inplace = True, ascending = True)
df_T_SOG[["SUPPLIER", "COMPLETE"]] = np.nan
df_T_SOG = df_T_SOG.merge(df_R_ITM, how = "inner", on = "ITEM", suffixes = ("", "_ITM"))
df_T_SOG = df_T_SOG[["PO", "ITEM", "TYPE", "SUPPLIER", "SOH_QUANT", "SOH_UNIT", "ARRIVAL_DATE", "EXP", "COMPLETE"]]

df_T_SOH["EXP"] = pd.to_datetime(df_T_SOH["EXP"],format="%d-%m-%Y")
df_T_SOH.sort_values(by=["EXP","ITEM"], inplace = True, ascending = True)

S_month = df_I_FDM["FDM_MONTH"].max()
S_month = datetime.datetime(S_month.year, S_month.month, monthrange(S_month.year, S_month.month)[1])
R_month = datetime.datetime(2022, 12, 31)+relativedelta(days=1)

#*******
df_T_SOH = df_T_SOH.merge(df_R_ITM, how = "left", on = "ITEM", suffixes = ("_SOH", "_ITM"))
df_T_SOH["FDM_QUANT_1"] = np.nan
df_T_SOH = df_T_SOH[["ITEM", "TYPE", "SUPPLIER", "SOH_QUANT", "SOH_UNIT", "EXP", "FDM_QUANT_1"]]

#*******
za =  abs(st.norm.ppf(1-1)) #assume customer satisfaction = 100%
for i in df_I_FDM["SKU"].unique():
    rop_quant = df_I_FDM.loc[df_I_FDM["SKU"] == i, "FDM_QUANT"].mean()
    rop_std = df_I_FDM.loc[df_I_FDM["SKU"] == i, "FDM_QUANT"].std()
    df_I_FDM.loc[df_I_FDM["SKU"] == i, "ROP_QUANT"] = rop_quant
    df_R_ITM.loc[df_R_ITM["ITEM"] == i, "ROP_QUANT"] = rop_quant/30
    df_R_BOM.loc[df_R_BOM["SKU"] == i, "ROP_QUANT"] = rop_quant * df_R_BOM["BOM_QUANT"]
    df_I_FDM.loc[df_I_FDM["SKU"] == i, "ROP_STD"] = rop_std
    df_R_ITM.loc[df_R_ITM["ITEM"] == i, "ROP_STD"] = rop_std/30
    df_R_BOM.loc[df_R_BOM["SKU"] == i, "ROP_STD"] = rop_std * df_R_BOM["BOM_QUANT"]
df_R_BOM_R = df_R_BOM.groupby(["ITEM"])["ROP_QUANT", "ROP_STD"].sum()

for i in df_R_BOM_R.index:
    df_R_ITM.loc[df_R_ITM["ITEM"] == i, "ROP_QUANT"] = df_R_BOM_R.loc[i, "ROP_QUANT"]/30
    df_R_ITM.loc[df_R_ITM["ITEM"] == i, "ROP_STD"] = df_R_BOM_R.loc[i, "ROP_STD"]/30

df_R_ITM["ROP"] = df_R_ITM["ROP_QUANT"] * df_R_ITM["LEADTIME"] + np.sqrt(df_R_ITM["LEADTIME"] * df_R_ITM["ROP_STD"]**2 + df_R_ITM["ROP_QUANT"]**2)

df_O_ORD_AHP = pd.DataFrame({"ITEM":df_R_ITM["ITEM"],"SUPPLIER":df_R_ITM["SUPPLIER"]})
df_O_ORD_AHP_B = pd.DataFrame({"ITEM":df_R_ITM["ITEM"],"SUPPLIER":df_R_ITM["SUPPLIER"]})
df_O_ORD_AHP_E = pd.DataFrame({"ITEM":df_R_ITM["ITEM"],"SUPPLIER":df_R_ITM["SUPPLIER"]})
df_O_ORD_AHP_P = pd.DataFrame({"ITEM":df_R_ITM["ITEM"],"SUPPLIER":df_R_ITM["SUPPLIER"]})

#******
#1 lần sản xuất = 1 cái
POcount = 0
PRcount = 1
for i in df_I_FDM["FDM_MONTH"].unique():
    i = pd.to_datetime(i)
    df_I_FDM.loc[df_I_FDM["FDM_MONTH"] == i, "FDM_QUANT_1"] = df_I_FDM["FDM_QUANT"]/monthrange(i.year, i.month)[1]

def refill (R_month, df_T_SOG, df_T_SOH):
    df_T_SOG_D = df_T_SOG[(df_T_SOG["ARRIVAL_DATE"] == R_month) & (df_T_SOG["COMPLETE"].isnull())] #move cuối làm điều kiện regression
    for i in df_T_SOG_D["PO"]:
        df_T_SOG.loc[df_T_SOG["PO"] == i, "COMPLETE"] = "X"
    df_T_SOH = pd.concat([df_T_SOH, df_T_SOG_D[["ITEM", "TYPE", "SUPPLIER", "SOH_QUANT", "SOH_UNIT", "EXP"]]], ignore_index = True)
    return df_T_SOH, df_T_SOG

def order_check (df_T_SOH, df_T_SOG, df_R_ITM):
    df_T_SOH_D = df_T_SOH.groupby(["ITEM"])["SOH_QUANT"].sum()   #Lượng hàng có trong SOH
    df_T_SOG_D = df_T_SOG[df_T_SOG["COMPLETE"].isnull()].groupby(["ITEM"])["SOH_QUANT"].sum() #Lương hàng đang SOG
    for i in df_R_BOM_R.index:
        try:
            df_R_ITM.loc[df_R_ITM["ITEM"] == i, "SOH_QUANT"] = df_T_SOH_D.loc[i]#lỗi nếu hơn 2 NCC
        except:
            df_R_ITM.loc[df_R_ITM["ITEM"] == i, "SOH_QUANT"] = 0
        try:
            df_R_ITM.loc[df_R_ITM["ITEM"] == i, "SOG_QUANT"] = df_T_SOG_D.loc[i]
        except:
            df_R_ITM.loc[df_R_ITM["ITEM"] == i, "SOG_QUANT"] = 0
    for i in df_R_ITM.loc[df_R_ITM["TYPE"] == "Thành phẩm", "ITEM"]:
        try:
            df_R_ITM.loc[df_R_ITM["ITEM"] == i, "SOH_QUANT"] = df_T_SOH_D.loc[i]
        except:
            df_R_ITM.loc[df_R_ITM["ITEM"] == i, "SOH_QUANT"] = 0
        try:
            df_R_ITM.loc[df_R_ITM["ITEM"] == i, "SOG_QUANT"] = df_T_SOG_D.loc[i]
        except:
            df_R_ITM.loc[df_R_ITM["ITEM"] == i, "SOG_QUANT"] = 0
    return df_T_SOH, df_T_SOG, df_R_ITM

def order_conduct (R_month, POcount, PRcount, df_T_SOH, df_T_SOG, df_R_ITM):
    df_T_SOH, df_T_SOG, df_R_ITM = order_check(df_T_SOH, df_T_SOG, df_R_ITM)
    df_O_ORD_D = df_R_ITM[(df_R_ITM["SOH_QUANT"] + df_R_ITM["SOG_QUANT"]) <= df_R_ITM["ROP"]]
    for i in df_O_ORD_D.index:
        if df_O_ORD_D.loc[i, "ROP"] < df_O_ORD_D.loc[i, "MOQ"]:
            ordquant = df_O_ORD_D.loc[i, "MOQ"]
        else:
            ordquant = df_O_ORD_D.loc[i, "ROP"]
        ordunit = df_O_ORD_D.loc[i, "UNIT"]
        ordleadtime = int(df_O_ORD_D.loc[i, "LEADTIME"])
        ordshelflife = int(df_O_ORD_D.loc[i, "SHELF_LIFE"])
        ordarrival = R_month + relativedelta(days = ordleadtime)
        ordexp = ordarrival + relativedelta(days = ordshelflife)
        if df_O_ORD_D.loc[i, "TYPE"] == "Thành phẩm": #extreme chỉ có thành phẩm và bán thành phẩm only --> thành phẩm k làm bán thành phẩm cho thành phẩm khác
            for m in df_R_BOM.loc[df_R_BOM["SKU"] == df_O_ORD_D.loc[i, "ITEM"]].index:
                if df_T_SOH.loc[df_T_SOH.loc[df_T_SOH["ITEM"] == df_R_BOM.loc[m, "ITEM"]].iloc[0:1].index, "FDM_QUANT_1"].isnull:
                    df_T_SOH.loc[df_T_SOH.loc[df_T_SOH["ITEM"] == df_R_BOM.loc[m, "ITEM"]].iloc[0:1].index, "FDM_QUANT_1"] = df_R_BOM.loc[m, "BOM_QUANT"] * ordquant
                else:
                    df_T_SOH.loc[df_T_SOH.loc[df_T_SOH["ITEM"] == df_R_BOM.loc[m, "ITEM"]].iloc[0:1].index, "FDM_QUANT_1"] += df_R_BOM.loc[m, "BOM_QUANT"] * ordquant
                # print(df_T_SOH.loc[df_T_SOH.loc[df_T_SOH["ITEM"] == df_R_BOM.loc[m, "ITEM"]].iloc[0:1].index])
            # df_T_SOH = transaction (R_month, POcount, PRcount, df_T_SOH, df_T_SOG, df_R_ITM)
            df_T_SOG_D_2 = pd.DataFrame(np.array([["#PR_F_"+str(PRcount), df_O_ORD_D.loc[i, "ITEM"], "Thành phẩm", "FESTORY", ordquant, ordunit, ordarrival, ordexp, np.nan]]), columns = df_T_SOG.columns)
            df_T_SOG = pd.concat([df_T_SOG, df_T_SOG_D_2], ignore_index = True)
            df_T_SOH, df_T_SOG = refill (R_month, df_T_SOG, df_T_SOH)
            PRcount += 1
        else:
            df_T_SOG_D_1 = pd.DataFrame(np.array([["#PO_F_"+str(POcount), df_O_ORD_D.loc[i, "ITEM"], "Nguyên liệu", df_O_ORD_D.loc[i, "SUPPLIER"], ordquant, ordunit, ordarrival, ordexp, np.nan]]), columns = df_T_SOG.columns)
            try:
                df_O_ORD.loc[df_O_ORD["ITEM"] == df_O_ORD_D.loc[i, "ITEM"], R_month] += ordquant
            except KeyError:
                df_O_ORD.loc[df_O_ORD["ITEM"] == df_O_ORD_D.loc[i, "ITEM"], R_month] = ordquant
            df_T_SOG = pd.concat([df_T_SOG, df_T_SOG_D_1], ignore_index = True)
            df_T_SOH, df_T_SOG = refill (R_month, df_T_SOG, df_T_SOH)
            POcount += 1
    return R_month, POcount, PRcount, df_T_SOH, df_T_SOG, df_R_ITM

def transaction (R_month, POcount, PRcount, df_T_SOH, df_T_SOG, df_R_ITM):
    #repeat ****
    df_T_SOH.loc[df_T_SOH["FDM_QUANT_1"].notnull(), "SOH_QUANT"] = df_T_SOH["SOH_QUANT"] - df_T_SOH["FDM_QUANT_1"]
    df_T_SOH["FDM_QUANT_1"] = np.nan
    for i in df_T_SOH[df_T_SOH["SOH_QUANT"] <= 0].index:
        df_T_SOH_ITEM = df_T_SOH.loc[i, "ITEM"] #tên hàng thiếu
        df_T_SOH_ITEM_1 = df_T_SOH[(df_T_SOH["ITEM"] == df_T_SOH_ITEM) & (df_T_SOH["SOH_QUANT"] > 0)].iloc[0:1] #list hàng kế
        # print(df_T_SOH.to_string)
        try:
            df_T_SOH.loc[df_T_SOH_ITEM_1.index.to_list(), "SOH_QUANT"] += df_T_SOH.loc[i, "FDM_QUANT_1"]
            df_T_SOH = df_T_SOH.drop([i])
        except KeyError:
            R_month, POcount, PRcount, df_T_SOH, df_T_SOG, df_R_ITM = order_conduct (R_month, POcount, PRcount, df_T_SOH, df_T_SOG, df_R_ITM)
            R_month, POcount, PRcount, df_T_SOH, df_T_SOG, df_R_ITM = transaction (R_month, POcount, PRcount, df_T_SOH, df_T_SOG, df_R_ITM)
    if df_T_SOH[df_T_SOH["SOH_QUANT"] < 0].size != 0:
        R_month, POcount, PRcount, df_T_SOH, df_T_SOG, df_R_ITM = transaction (R_month, POcount, PRcount, df_T_SOH, df_T_SOG, df_R_ITM)
    return R_month, POcount, PRcount, df_T_SOH, df_T_SOG, df_R_ITM

while R_month <= S_month:
    print(R_month, "-", S_month)
    #EXP
    df_T_SOH = df_T_SOH.loc[(df_T_SOH["EXP"] > R_month) | (df_T_SOH["EXP"].isnull())]
    #Refill --> assumption vì là điều kiện forecast nên exp không thể hết hạn trước/vào ngày về
    df_T_SOH, df_T_SOG = refill (R_month, df_T_SOG, df_T_SOH)
    #recording AHP
    if R_month == datetime.datetime(2023, 1, 1):
        df_O_ORD_AHP_B_1 = df_T_SOH.groupby(["ITEM"])["SOH_QUANT"].sum()
    #Demand
    df_I_FDM_D = df_I_FDM[df_I_FDM["FDM_MONTH"] == datetime.datetime(R_month.year, R_month.month, 1)]
    for i in df_I_FDM_D["SKU"]:
        dquant = df_I_FDM_D.loc[df_I_FDM_D["SKU"] == i, "FDM_QUANT_1"].values[0]
        df_T_SOH.loc[df_T_SOH[df_T_SOH["ITEM"] == i].iloc[0:1].index, "FDM_QUANT_1"] = dquant
    # print("TEST", df_T_SOH.to_string)
    R_month, POcount, PRcount, df_T_SOH, df_T_SOG, df_R_ITM = transaction(R_month, POcount, PRcount, df_T_SOH, df_T_SOG, df_R_ITM)
    #Demand of Date 
    df_T_SOH.loc[df_T_SOH["FDM_QUANT_1"].notnull(), "FDM_QUANT_1"] = np.nan #gộp lại thành trừ thẳng k thêm cột
    R_month, POcount, PRcount, df_T_SOH, df_T_SOG, df_R_ITM = order_conduct (R_month, POcount, PRcount, df_T_SOH, df_T_SOG, df_R_ITM)
    R_month, POcount, PRcount, df_T_SOH, df_T_SOG, df_R_ITM = transaction (R_month, POcount, PRcount, df_T_SOH, df_T_SOG, df_R_ITM)
    if R_month == S_month:
        df_O_ORD_AHP_E_1 = df_T_SOH.groupby(["ITEM"])["SOH_QUANT"].sum()
        df_O_AHP_P_2 = df_T_SOG[df_T_SOG["ARRIVAL_DATE"].between(datetime.datetime(2023 , 1, 1), R_month)]
        df_O_ORD_AHP_P_1 = df_O_AHP_P_2.groupby(["ITEM"])["SOH_QUANT"].sum()
    R_month += pd.Timedelta(days=1) #repeat point

#AHP
# print(df_O_ORD_AHP_P_1.to_string)
# for i in df_O_ORD_AHP.index:
#     avginv = (df_O_ORD_AHP_B_1. loc[df_O_ORD_AHP.loc[i, "ITEM"]] + df_O_ORD_AHP_E_1.loc[df_O_ORD_AHP.loc[i, "ITEM"]])/2
#     cogs = df_O_ORD_AHP_B_1.loc[df_O_ORD_AHP.loc[i, "ITEM"]] + df_O_ORD_AHP_P_1.loc[df_O_ORD_AHP.loc[i, "ITEM"]] - df_O_ORD_AHP_E_1.loc[df_O_ORD_AHP.loc[i, "ITEM"]]
#     df_O_ORD_AHP.loc[i, "AHP"] = avginv/cogs * (R_month - datetime.datetime(2023, 1, 1)).days

#*******
df_R_BOM.to_excel("input.xlsx", sheet_name= "R_BOM", index= False)
with pd.ExcelWriter("input.xlsx", engine= 'openpyxl', mode= 'a') as df:
    df_R_ITM.to_excel(df, "R_ITM", index= False)
    df_I_FDM.to_excel(df, "I_FDM", index= False)
    df_T_SOG.to_excel(df, "T_SOG", index= False)
    df_T_SOH.to_excel(df, "T_SOH", index= False)
# #Execution #ctrl-k-c ; ctrl-k-u


df_O_ORD.to_excel("output.xlsx", sheet_name= "Order_Schedule", index= False)
# with pd.ExcelWriter("output.xlsx", engine= "openpyxl", mode= "a") as df:
#     df_O_ORD_AHP.to_excel(df, "Average_Holding_Period", index= False)
