NANS_FILLVALUE = {
    "Spend__c": 0.0,
    "Trx__c": 0.0,
    "Avg_Ticket__c": 0.0,
    "Avg_Ticket_Rank__c": 0.0,
    "Customer__c": '',
    "Product__c": '',
    "Indexed_Number_of_cards__c": 0.0,
    "Indexed_Spend__c": 0.0,
    "Indexed_Trx__c": 0.0,
    "Industry__c": '',
    "Month__c": '',
    "Year__c": '',
    "Municipality__c": '',
    "Province__c": '',
    "Region__c": '',
    "Nationality__c": '',
    "Num_cards_Rank__c": 0.0,
    "Spend_per_card__c": 0.0,
    "Spend_per_card_Rank__c": 0.0,
    "Spend_Rank__c": 0.0,
    "Trx_Rank__c": 0.0,
    "Var_MoM_Avg_Ticket__c": 0.0,
    "Var_MoM_Num_Cards__c": 0.0,
    "Var_MoM_Spend__c": 0.0,
    "Var_MoM_Spend_per_Card__c": 0.0,
    "Var_MoM_Trx__c": 0.0,
    "Var_Yo2Y_Avg_Ticket__c": 0.0,
    "Var_Yo2Y_Num_Cards__c": 0.0,
    "Var_Yo2Y_Spend__c": 0.0,
    "Var_Yo2Y_Spend_per_Card__c": 0.0,
    "Var_Yo2Y_Trx__c": 0.0,
    "Var_Yo3Y_Avg_Ticket__c": 0.0,
    "Var_Yo3Y_Num_Cards__c": 0.0,
    "Var_Yo3Y_Spend__c": 0.0,
    "Var_Yo3Y_Spend_per_Card__c": 0.0,
    "Var_Yo3Y_Trx__c": 0.0,
    "Var_YoY_Avg_Ticket__c": 0.0,
    "Var_YoY_Num_Cards__c": 0.0,
    "Var_YoY_Spend__c": 0.0,
    "Var_YoY_Spend_per_Card__c": 0.0,
    "Var_YoY_Trx__c": 0.0,
    "View__c": '',
    "Week_Start_Date__c": '2021-12-01',
    "Year_Month_Date__c": '',
    "Zip_Code__c": ''
}

def fill(df):
    df = df.fillna(value=NANS_FILLVALUE)
    df = df.replace('nan','')
    df = df.replace('None','')
    return df