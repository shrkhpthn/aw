# -*- coding: utf-8 -*-
"""
Created on Tue Jan 28 18:17:15 2025

@author: Data Implementation Team
"""

import pandas as pd
import datetime
from pandas.tseries.offsets import BDay
import numpy as np
import os
import re

day1= BDay(29)
day2= BDay(30)

reported_date1 = (datetime.datetime.today()- day1).strftime('%d/%m/%Y')
reported_date2 = (datetime.datetime.today()- day2).strftime('%d/%m/%Y')
#reported_dateT1 = (datetime.datetime.today()- day1).strftime('%#m/%d/%Y')
#reported_dateT2 = (datetime.datetime.today()- day2).strftime('%#m/%d/%Y')
print(reported_date1)
print(reported_date2)


SummaTotal = pd.read_excel("C:/Users/"+ os.environ["USERNAME"] + "/ACA Group/PyProjectsHub - Documents/Reconciliations/Summary/Summary_%s.xlsx" % (datetime.datetime.today()- day1).strftime('%Y%m%d'))

# Positions1R = pd.read_csv("C:/Users/"+ os.environ["USERNAME"] +"/ACA Group/Risk MFS - Documents/Current/Cisu Capital/6. Daily Reports/Positions/Cisu_Positions_%s.csv" % (datetime.datetime.today()- day1).strftime('%Y%m%d'),thousands=',')
try:
    Positions1R = pd.read_excel("C:/Users/"+ os.environ["USERNAME"] +"/ACA Group/Risk MFS - Documents/Current/MPP&E Capital/6. Daily Reports/Positions/MPPE_POS_%s.xlsx" % (datetime.datetime.today()- day1).strftime('%Y%m%d'))
except:
    Positions1R = pd.read_csv("C:/Users/"+ os.environ["USERNAME"] +"/ACA Group/Risk MFS - Documents/Current/MPP&E Capital/6. Daily Reports/Positions/MPPE_POS_%s.csv" % (datetime.datetime.today()- day1).strftime('%Y%m%d'),thousands=',')


# Positions2R = pd.read_csv("C:/Users/"+ os.environ["USERNAME"] +"/ACA Group/Risk MFS - Documents/Current/Cisu Capital/6. Daily Reports/Positions/Cisu_Positions_%s.csv" % (datetime.datetime.today()- day2).strftime('%Y%m%d'),thousands=',')
try:
    Positions2R = pd.read_excel("C:/Users/"+ os.environ["USERNAME"] +"/ACA Group/Risk MFS - Documents/Current/MPP&E Capital/6. Daily Reports/Positions/MPPE_POS_%s.xlsx" % (datetime.datetime.today()- day2).strftime('%Y%m%d'))
except:
    Positions2R = pd.read_csv("C:/Users/"+ os.environ["USERNAME"] +"/ACA Group/Risk MFS - Documents/Current/MPP&E Capital/6. Daily Reports/Positions/MPPE_POS_%s.csv" % (datetime.datetime.today()- day2).strftime('%Y%m%d'),thousands=',')



Positions1R["Concat_Inst"] = Positions1R["Instrument Type"] + "_" + Positions1R["Instrument Subtype"] 
#Check_table_1 = pd.read_excel("C:/Users/"+ os.environ["USERNAME"] + "/ACA Group/PyProjectsHub - Documents/Reconciliations/CheckTables/Aquatic_Checktable.xlsx", sheet_name="AQTC")
Check_table_1 = pd.read_excel("C:/Users/"+ os.environ["USERNAME"] + "/ACA Group/PyProjectsHub - Documents/Reconciliations/CheckTables/MPPECapital_Checktable.xlsx", sheet_name="MPP10")


Check_table_1["Concat_Inst"] = Check_table_1["InstrumentType"] + "_" +  Check_table_1["InstrumentSubtype"] 
Check_table_1["Concat_PM_TR"] = Check_table_1["PMs"].apply(str) + "_+_" +  Check_table_1["Traders"].apply(str) 
Position_columns = ["Security ID","Quantity"]
Trade_columns = ["Security ID", "Quantity","Trade Side","Security Description"] #Need to ask

try:
    # Trades1Rt = pd.read_csv("C:/Users/"+ os.environ["USERNAME"] +"/ACA Group/Risk MFS - Documents/Current/Cisu Capital/6. Daily Reports/Trades/Cisu_Trades_%s.csv" % (datetime.datetime.today()- day1).strftime('%Y%m%d'),thousands=',')
    try:
        #C:\Users\Bravo_PyProjects\ACA Group\Risk MFS - Documents\Current\EBIT\7. Daily Reports\Trades\EBIT_Trades_20250124
        Trades1Rt = pd.read_csv("C:/Users/"+ os.environ["USERNAME"] +"/ACA Group/Risk MFS - Documents/Current/MPP&E Capital/6. Daily Reports/Trades/MPPE_TRD_%s.csv" % (datetime.datetime.today()- day1).strftime('%Y%m%d'))
        #Trades1Rt = pd.read_excel("C:/Users/"+ os.environ["USERNAME"] +"/ACA Group/Risk MFS - Documents/Current/ALPHATERRA/7. Daily Reports/Trades/EBIT_Trades_%s.xlsx" % (datetime.datetime.today()- day1).strftime('%Y%m%d'))
        #Trades1Rt = pd.read_excel("C:/Users/"+ os.environ["USERNAME"] +"/ACA Group/Risk MFS - Documents/Current/ALPHATERRA/7. Daily Reports/Trades/EBIT_Trades_%s.xlsx" % (datetime.datetime.today()- day1).strftime('%Y%m%d'),thousands=',')
        #print("Try")
    except:
        Trades1Rt = pd.read_excel("C:/Users/"+ os.environ["USERNAME"] +"/ACA Group/Risk MFS - Documents/Current/MPP&E Capital/6. Daily Reports/Trades/MPPE_TRD_%s.xlsx" % (datetime.datetime.today()- day1).strftime('%Y%m%d'))
        #Trades1Rt = pd.read_csv("C:/Users/"+ os.environ["USERNAME"] +"/ACA Group/Risk MFS - Documents/Current/ALPHATERRA/7. Daily Reports/Trades/EBIT_Trades_%s.csv" % (datetime.datetime.today()- day1).strftime('%Y%m%d'),thousands=',')
        #print("Except")
    #print("Outer try")
    
    
    Trades1R = Trades1Rt.copy()
    #print(Trades1R)
    Trades1R["Concat_Inst"] = Trades1R["Instrument Type"].fillna('')  + "_" + Trades1R["InstrumentSubtype"].fillna('') 
    Trades1R["Concat_PM_TR"] = Trades1R["RiskTaker"].apply(str)   + "_+_" +  Trades1R["Trader"].apply(str) 
    Trades1R = Trades1R.loc[Trades1R['Status'] == "New"]
    Trades1R = Trades1R.reset_index(drop=True)
    Trade_imported = "Yes"

      
except:
    Trades1R = pd.DataFrame([['','','','']], columns = Trade_columns )
    Trade_imported = "No"
    print(Trades1R)
    #print("Outer Except")


## PASS THIS TO several FUNCTIONs TO REDUCE MAIN SCRIPT Variable to send trades 1R 
#Instruments CHECK
    
#TRADERS CHECK
if Trade_imported == "Yes":
    if (Trades1R.loc[~Trades1R["Concat_Inst"].isin(Check_table_1["Concat_Inst"])]).shape[0] == 0:
        Mtra_INST = "OK"
    else:
        Trader1CM = Trades1R.loc[~Trades1R["Concat_Inst"].isin(Check_table_1["Concat_Inst"])]
        Mtra_INST = Trader1CM['Concat_Inst'].drop_duplicates().to_list()
        
    print(Mtra_INST)   
        
    if (Trades1R.loc[~Trades1R["Concat_PM_TR"].isin(Check_table_1["Concat_PM_TR"])]).shape[0] == 0:
        Mtraders = "OK"
    else:
        Trader1CM = Trades1R.loc[~Trades1R["Concat_PM_TR"].isin(Check_table_1["Concat_PM_TR"])]
        Mtraders = Trader1CM['Concat_PM_TR'].drop_duplicates().to_list()
        
    print(Mtraders)
    
    #PMs CHECK
    
    # if (Trades1R.loc[~Trades1R["Concat_PM_TR"].isin(Check_table_1["Concat_PM_TR"])]).shape[0] == 0:
    #     Mpms = "OK"
    # else:
    #     Trader1CCM = Trades1R.loc[~Trades1R["Concat_PM_TR"].isin(Check_table_1["Concat_PM_TR"])]
    #     Mpms = Trader1CCM['Concat_PM_TR'].drop_duplicates().to_list()    

    
    #CCs CHECK
    if (Trades1R.loc[~Trades1R["ClearingCounterparty"].isin(Check_table_1["CCs"])]).shape[0] == 0:
        Mccs = "OK"
    else:
        Trader1CCM = Trades1R.loc[~Trades1R["ClearingCounterparty"].isin(Check_table_1["CCs"])]                         ################################## Clearing counterparty is missing
        Mccs = Trader1CCM['ClearingCounterparty'].drop_duplicates().to_list()
        
    print(Mccs)
    
    #PBs CHECK
    if (Trades1R.loc[~Trades1R["Counterparty"].isin(Check_table_1["EBs"])]).shape[0] == 0:                ######################Need to chcek
        Mpbs = "OK"
    else:
        Trader1CCM = Trades1R.loc[~Trades1R["Counterparty"].isin(Check_table_1["EBs"])]
        Mpbs = Trader1CCM['Counterparty'].drop_duplicates().to_list()
        
    print(Mpbs)
 
else:
    Mtraders = "Trade File Missing"
    Mccs     = "Trade File Missing"
    Mpms     = "Trade File Missing"
    Mpbs     = "Trade File Missing"    
    Mtra_INST= "Trade File Missing"   


     
## RECONCILIATION EXERCISE
    
if Trades1R.shape[0] > 0:       
    for i in range(len(Trades1R)):           
        if (Trades1R.loc[i, "Trade Side"] == "Buy"): #or (Trades1R.loc[i, "TradeSide"] == "Buy Cover") or (Trades1R.loc[i, "TradeSide"] == "Buy"):                   #  Nedd to check
            Trades1R.loc[i, "Quantity"] = Trades1R.loc[i, "Quantity"]
        else:
            Trades1R.loc[i, "Quantity"] = Trades1R.loc[i, "Quantity"] * -1

# else:
#     Trades1R["14/N QuantityA"]= 0

# if Trades1R.shape[0] > 0:       
#     for i in range(len(Trades1R)):           
#         if (Trades1R.loc[i, "B/SIndicator"] == "B") or (Trades1R.loc[i, "B/SIndicator"] == "BC"):                   #  Nedd to check
#             Trades1R.loc[i, "TradedQuantity"] = Trades1R.loc[i, "TradedQuantity"]
#         else:
#             Trades1R.loc[i, "TradedQuantity"] = Trades1R.loc[i, "TradedQuantity"] * -1

else:
    Trades1R["Quantity"]= 0
    
#Trades1R['Name'].replace('', np.nan, inplace=True)
#Trades1R.dropna(subset=['Name'], inplace=True) 

Positions1R["Date"] = reported_date1
Positions2R["Date"] = reported_date2
Trades1R["Date"] = reported_date1


Positions1=Positions1R[["Date","Security Description","Quantity"]].rename(columns={'Security Description': 'Name'})

Positions2RM = Positions2R.loc[~(Positions2R['Maturity'].isin([(datetime.datetime.today()- day2).strftime('%#m/%d/%Y')]))]
Positions2RM = Positions2RM.reset_index(drop=True) 

Positions2=Positions2RM[["Date","Security Description","Quantity"]].rename(columns={'Security Description': 'Name'})



if Trades1R.shape[0] > 0:
    Trades1 = Trades1R[["Security Description", "Quantity"]].rename(columns={'Security Description': 'Name'})
    Trades1M = Trades1[["Name", "Quantity"]].copy()
    
else:
    Trades1M = Trades1R[["Security Description", "Quantity"]].rename(columns={'Security Description': 'Name'})


#Split positions by type Fund or management
position1Mcon = Positions1.groupby(["Name"], as_index=False, sort=False).sum()
position1Mcon = position1Mcon.rename(columns={'Quantity': reported_date1})
position2Mcon = Positions2.groupby(["Name"],as_index=False, sort=False).sum()
position2Mcon = position2Mcon.rename(columns={'Quantity': reported_date2})


# position1Mcon = position1Mcon.drop(columns=['Date'])
# position2Mcon = position2Mcon.drop(columns=['Date'])

trades1Mcon = (Trades1M.groupby([Trades1M["Name"]]).sum().reset_index())

position3Mcon = position1Mcon.merge(position2Mcon, how='outer')
position3Mcon = position3Mcon.reset_index(drop=True).fillna(0)
try:
    position3Mcon["Diff"] = position3Mcon[reported_date1] - position3Mcon[reported_date2]
except:
    if reported_date1 in position3Mcon  and reported_date2 not in position3Mcon:
        position3Mcon[reported_date2] = 0
        position3Mcon["Diff"] = position3Mcon[reported_date1] - position3Mcon[reported_date2]
        
    elif reported_date1 not in position3Mcon  and reported_date2 in position3Mcon:
        position3Mcon[reported_date1] = 0
        position3Mcon["Diff"] = position3Mcon[reported_date1] - position3Mcon[reported_date2]
        
    elif reported_date1 not in position3Mcon  and reported_date2 not in position3Mcon:
        position3Mcon[reported_date1] = 0
        position3Mcon[reported_date2] = 0
        position3Mcon["Diff"] = 0
    print("Position file is missing")



trades1Mcon['Name'] = trades1Mcon['Name'].replace('', np.nan)
trades1Mcon['Name'] = trades1Mcon['Name'].astype(str)

position3Mcon['Name'] = position3Mcon['Name'].astype(str)



position4Mcon = position3Mcon.merge(trades1Mcon, how='outer')
position4Mcon = position4Mcon.reset_index(drop=True).fillna(0)

#print(position4Mcon.head())



if trades1Mcon.shape[0] > 0:
    try:
        position4Mcon["Recon"] = position4Mcon["Diff"] - position4Mcon["Quantity"]
    except:
        position4Mcon["Recon"] = 0
else:
    position4Mcon["Quantity"] = 0
    position4Mcon["Recon"] = position4Mcon["Diff"]
#position3Mcon = position3Mcon.loc[position3Mcon['Diff'] != 0]


position4Mcon['Quantity'] = pd.to_numeric(position4Mcon['Quantity'])
position4Mcon['Quantity'] = position4Mcon['Quantity'].fillna(0)
 


 

    

MSrtrades = position4Mcon['Diff'].sum()
if trades1Mcon.shape[0] >= 1:
    MStrades = position4Mcon['Quantity'].sum()
else:
    MStrades = 0
    
MDiff = position4Mcon['Recon'].sum()

Mtraders = 'NA' 
Mccs = 'NA'
data1 = [["Trades Check:", ""], ["Recon Trades",MSrtrades],["Trades:",MStrades],["Diff:", MDiff],["",""],["Other Checks", ""],["New PM + Traders Check:",Mtraders],["New Prime Brokers check:", Mpbs],["New Clearing Counterparty check:", Mccs],["New instrument Check", Mtra_INST]]
df1 = pd.DataFrame(data1)

column1 = ["ReportDate", "ClientCode", "Client", "Fund", "Reconciliation Result", "New instrument Check", "New PM + Traders Check", "Clearing Counterparties Check" ,"Executing Brokers Check" ]
lista = []
lista.append([(datetime.datetime.today()- day1).strftime('%d/%m/%Y'),"MPP10","MPPE", "MPPE Principia Fund", MDiff,Mtra_INST, Mtraders, Mccs, Mpbs])
summa = pd.DataFrame(lista, columns= column1) 
SummaTotal = pd.concat((SummaTotal,summa), axis=0)

# print("Trades1R shape:", Trades1R.shape)
# print("Trades1R columns:", Trades1R.columns.tolist())
# print("First rows of Trades1R:\n", Trades1R.head())

# with pd.ExcelWriter('C://Users//'+ os.environ["USERNAME"] +'//ACA Group//Risk MFS - Documents//Current//Cisu Capital/6. Daily Reports/Reconciliation/Cisu_POS_RECON_%s.xlsx' % (datetime.datetime.today()- day1).strftime('%Y%m%d') ) as writer:     
with pd.ExcelWriter('C://Users//'+ os.environ["USERNAME"] +'//ACA Group//Risk MFS - Documents/Current/MPP&E Capital/6. Daily Reports/Reconciliations/MPPECapital_POS_RECON_%s.xlsx' % (datetime.datetime.today()- day1).strftime('%Y%m%d') ) as writer:     

    Positions2R.to_excel(writer, sheet_name="Positions T-1",index=False)
    Positions1R.to_excel(writer, sheet_name="Positions T",index=False)
    try:
        Trades1R.to_excel(writer, sheet_name="Trades",index=False)
    except:
        Trades1R.to_excel(writer, sheet_name="Trades",index=False)
        
    position4Mcon.to_excel(writer, sheet_name="Recon_MPP10",index=False)
    df1.to_excel(writer,sheet_name="Recon_MPP10", startcol=8,startrow=1, header=None, index=False)


# with pd.ExcelWriter("C:/Users/"+ os.environ["USERNAME"] + "/ACA Group/PyProjectsHub - Documents/Reconciliations/Summary/Summary_%s.xlsx" % (datetime.datetime.today()- day1).strftime('%Y%m%d'))as writer1:
with pd.ExcelWriter("C:/Users/"+ os.environ["USERNAME"] + "/ACA Group/PyProjectsHub - Documents/Reconciliations/Summary/Summary_%s.xlsx" % (datetime.datetime.today()- day1).strftime('%Y%m%d'))as writer1:

    SummaTotal.to_excel(writer1, index=False)
print("Done. File created and Saved succesfully!")
