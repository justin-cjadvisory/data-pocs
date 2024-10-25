# config.py

# Define the base URL for API requests
base_url = "https://data.odatalink.com/CJ-Adv-672986/ryan_developmen_4670/Rya-Dev/"

# Define a dictionary to map endpoint types to their corresponding fields
endpoint_config = {
    "Accounts": [
        "AccountID", "Code", "Name", "Status", "Type", "TaxType", "Description", 
        "Class", "SystemAccount", "EnablePaymentsToAccount", "ShowInExpenseClaims", 
        "BankAccountNumber", "BankAccountType", "CurrencyCode", "ReportingCode", 
        "ReportingCodeName", "HasAttachments", "UpdatedDateUTC", "AddToWatchlist", 
        "DataFileID", "DataFileName", "DataFileCode"
    ],
    "AccountsClass": [
        "Class", "IsProfitLoss", "IsBalanceSheet", "IsDebit", "IsCredit", 
        "DrCrSign", "ProfitLossSign", "VarianceImpactSign"
    ],
    "AccountsTable": [
        "DataFileID", "DataFileName", "DataFileCode", "AccountID", "Code",
        "Name", "Status", "Type", "TaxType", "Description", "Class", "SystemAccount",
        "EnablePaymentsToAccount", "ShowInExpenseClaims", "BankAccountNumber",
        "BankAccountType", "CurrencyCode", "ReportingCode", "ReportingCodeName",
        "HasAttachments", "UpdatedDateUTC", "AddToWatchlist"
    ],
    "AccountsTotals": [
        "AccountTotal", "AccountTotalName", "IsProfitLoss", 
        "IsBalanceSheet", "IsAccountClassification", "Order", 
        "VarianceImpactSign", "AccountsTypes"  
    ],
    "AccountsTypes": [
        "AccountType", "AccountTypeName", "Class", "IsProfitLoss",
        "IsBalanceSheet", "IsDebit", "IsCredit", "DrCrSign",
        "VarianceImpactSign"
    ],
    "BalanceSheetAdvanced": [
        "DataFileID", "DataFileName", "DataFileCode", "Date", "PaymentsOnly", "StandardLayout", 
        "TimeFrame", "Periods", "TrackingOptionID", "TrackingOptionID2", "LineType", "AccountID", 
        "AccountName", "AccountLineType", "LineDescription", "Amount"
    ],
    "BalanceSheetByMonth": [
        "DataFileID", "DataFileName", "DataFileCode", "FinancialYear", "PaymentsOnly", 
        "StandardLayout", "UpdatedDateUTC", "Lines"
    ],
    "BalanceSheetByTrackingOption": [
        "DataFileID", "DataFileName", "DataFileCode", "Date", 
        "PaymentsOnly", "StandardLayout", "TrackingOptionID", 
        "TrackingOptionID2", "LineType", "AccountID", 
        "AccountName", "AccountLineType", "Amount"
    ],
    "BalanceSheetMultiPeriodTable": [
        "DataFileID", "DataFileName", "DataFileCode", "Date", 
        "Period", "PaymentsOnly", "StandardLayout", "LineType", 
        "AccountID", "AccountName", "AccountLineType", "Amount"
    ],
    "BalanceSheetTable": [
        "DataFileID", "DataFileName", "DataFileCode", "Date", "PaymentsOnly", 
        "StandardLayout", "LineType", "AccountID", "AccountName", 
        "AccountLineType", "Amount"
    ],
    "BankSummary": [
        "DataFileID", "DataFileName", "DataFileCode", 
        "FromDate", "ToDate", "UpdatedDateUTC", "Lines", 
        "LineType", "AccountID", "AccountName", "OpeningBalanceAmount", 
        "CashReceivedAmount", "CashSpentAmount", 
        "FXGainLossAmount", "ClosingBalanceAmount"
    ],
    "BankTransactions": [
        "DataFileID", "DataFileName", "DataFileCode", 
        "BankTransactionID", "AccountID", "Code", "Name", 
        "BatchPayment", "Type", "Reference", "Url", "IsReconciled", 
        "PrepaymentID", "OverpaymentID", "HasAttachments", "ContactID", 
        "ContactName", "Date", "Status", "SubTotal", 
        "TotalTax", "Total", "UpdatedDateUTC", "CurrencyCode", "CurrencyRate"
    ],
    "BankTransactionsExpanded": [
        "DataFileID", "DataFileName", "DataFileCode", "BankTransactionID", 
        "AccountID", "Code", "Name", "BatchPayment", "Type", "Reference", 
        "Url", "IsReconciled", "PrepaymentID", "OverpaymentID", 
        "HasAttachments", "ContactID", "ContactName", "Date", "Status", 
        "LineAmountTypes", "ItemCode", "Description", "UnitAmount", "TaxType", 
        "TaxAmount", "LineAmount", "AccountCode", "TrackingName", "TrackingOption", 
        "TrackingCategoryID", "Quantity", "LineItemID", "AccountID", "SubTotal", 
        "TotalTax", "Total", "UpdatedDateUTC", "CurrencyCode", "CurrencyRate"
    ],
    "BankTransfers": [
        "DataFileID", "DataFileName", "DataFileCode", "BankTransferID", 
        "CreatedDateUTC", "Date", "FromAccountID", "FromCode", 
        "FromName", "ToAccountID", "ToCode", "ToName", "Amount", 
        "FromBankTransactionID", "ToBankTransactionID", "FromIsReconciled", 
        "ToIsReconciled", "CurrencyRate", "Reference", "HasAttachments"
    ],
    "BatchPayments": [
        "DataFileID", "DataFileName", "DataFileCode", "AccountID", 
        "BatchPaymentID", "Reference", "Particulars", "Narrative", 
        "Date", "InvoiceID", "InvoiceAddresses", "InvoicePaymentServices", 
        "PaymentID", "Amount", "Type", "Status", "TotalAmount", "UpdatedDateUTC", "IsReconciled"
    ]
}
