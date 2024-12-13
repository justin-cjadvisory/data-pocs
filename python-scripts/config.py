# config.py

# Define the base URL for API requests
base_url = "https://data.odatalink.com/CJ-Adv-672986/ryan_developmen_4670/Rya-Dev/"
bucket_name = 'cj-data-bucket'
output_folder = 'jsonoutput/newline-delimited'
project_name = "cj-data-platform"
dataset_id = 'cj_dataset_xero_exports'

# Define a dictionary to map endpoint types to their corresponding fields
endpoint_config = {
    "Accounts": [
    "DataFile",
    "DataFileID",
    "DataFileName",
    "DataFileCode",
    "AccountID",
    "Code",
    "Name",
    "Status",
    "Type",
    "TaxType",
    "Description",
    "Class",
    "SystemAccount",
    "EnablePaymentsToAccount",
    "ShowInExpenseClaims",
    "BankAccountNumber",
    "BankAccountType",
    "CurrencyCode",
    "ReportingCode",
    "ReportingCodeName",
    "HasAttachments",
    "UpdatedDateUTC",
    "AddToWatchlist"
    ],
    "AccountsClass": [
    "Class",
    "IsProfitLoss",
    "IsBalanceSheet",
    "IsDebit",
    "IsCredit",
    "DrCrSign",
    "ProfitLossSign",
    "VarianceImpactSign"
    ],
    "AccountsTable":[
    "DataFileID",
    "DataFileName",
    "DataFileCode",
    "AccountID",
    "Code",
    "Name",
    "Status",
    "Type",
    "TaxType",
    "Description",
    "Class",
    "SystemAccount",
    "EnablePaymentsToAccount",
    "ShowInExpenseClaims",
    "BankAccountNumber",
    "BankAccountType",
    "CurrencyCode",
    "ReportingCode",
    "ReportingCodeName",
    "HasAttachments",
    "UpdatedDateUTC",
    "AddToWatchlist"
    ],
    "AccountsTotals":[
    "AccountTotal",
    "AccountTotalName",
    "IsProfitLoss",
    "IsBalanceSheet",
    "IsAccountClassification",
    "Order",
    "VarianceImpactSign",
    "AccountsTypes"
    ],
    "AccountsTypes":[
    "AccountType",
    "AccountTypeName",
    "Class",
    "IsProfitLoss",
    "IsBalanceSheet",
    "IsDebit",
    "IsCredit",
    "DrCrSign",
    "VarianceImpactSign"
    ],
    "BalanceSheet":[
    "DataFile",
    "Date",
    "PaymentsOnly",
    "StandardLayout",
    "UpdatedDateUTC",
    "Lines"
    ],
    "BalanceSheetAdvanced":[
    "DataFileID",
    "DataFileName",
    "DataFileCode",
    "Date",
    "PaymentsOnly",
    "StandardLayout",
    "TimeFrame",
    "Periods",
    "TrackingOptionID",
    "TrackingOptionID2",
    "LineType",
    "AccountID",
    "AccountName",
    "AccountLineType",
    "LineDescription",
    "Amount"
    ],
    "BalanceSheetByMonth":[
    "DataFile",
    "FinancialYear",
    "PaymentsOnly",
    "StandardLayout",
    "UpdatedDateUTC",
    "Lines"
    ],
    "BalanceSheetByTrackingOption":[
    "DataFileID",
    "DataFileName",
    "DataFileCode",
    "Date",
    "PaymentsOnly",
    "StandardLayout",
    "TrackingOptionID",
    "TrackingOptionID2",
    "LineType",
    "AccountID",
    "AccountName",
    "AccountLineType",
    "Amount"
    ],
    "BalanceSheetMultiPeriodTable":[
    "DataFileID",
    "DataFileName",
    "DataFileCode",
    "Date",
    "Period",
    "PaymentsOnly",
    "StandardLayout",
    "LineType",
    "AccountID",
    "AccountName",
    "AccountLineType",
    "Amount"
    ],
    "BalanceSheetTable":[
    "DataFileID",
    "DataFileName",
    "DataFileCode",
    "Date",
    "PaymentsOnly",
    "StandardLayout",
    "LineType",
    "AccountID",
    "AccountName",
    "AccountLineType",
    "Amount"
    ],
    "BankSummary":[
    "DataFile",
    "FromDate",
    "ToDate",
    "UpdatedDateUTC",
    "Lines"
    ],
    "BankTransactions":[
    "DataFile",
    "BankTransactionID",
    "BankAccount",
    "BatchPayment",
    "Type",
    "Reference",
    "Url",
    "IsReconciled",
    "PrepaymentID",
    "OverpaymentID",
    "HasAttachments",
    "Contact",
    "Date",
    "Status",
    "SubTotal",
    "TotalTax",
    "Total",
    "UpdatedDateUTC",
    "CurrencyCode",
    "CurrencyRate"
    ],
    "BankTransactionsExpanded":[
    "DataFile",
    "BankTransactionID",
    "BankAccount",
    "BatchPayment",
    "Type",
    "Reference",
    "Url",
    "IsReconciled",
    "PrepaymentID",
    "OverpaymentID",
    "HasAttachments",
    "Contact",
    "Date",
    "Status",
    "LineAmountTypes",
    "LineItems"
    ],
    "BankTransfers":[
    "DataFile",
    "BankTransferID",
    "CreatedDateUTC",
    "Date",
    "FromBankAccount",
    "ToBankAccount",
    "Amount",
    "FromBankTransactionID",
    "ToBankTransactionID",
    "FromIsReconciled",
    "ToIsReconciled",
    "CurrencyRate",
    "Reference",
    "HasAttachments"
    ],
    "BatchPayments":[
    "DataFile",
    "Account",
    "BatchPaymentID",
    "Reference",
    "Particulars",
    "Narrative",
    "Date",
    "Payments",
    "Type",
    "Status",
    "TotalAmount",
    "UpdatedDateUTC",
    "IsReconciled"
    ],
    "Contacts":[
    "DataFile",
    "ContactID",
    "CompanyNumber",
    "ContactNumber",
    "AccountNumber",
    "ContactStatus",
    "Name",
    "FirstName",
    "LastName",
    "EmailAddress",
    "SkypeUserName",
    "BankAccountDetails",
    "TaxNumber",
    "AccountsReceivableTaxType",
    "AccountsPayableTaxType",
    "Addresses",
    "Phones",
    "UpdatedDateUTC",
    "IsSupplier",
    "IsCustomer",
    "DefaultCurrency",
    "Balances",
    "Website",
    "HasAttachments"
    ],
    "ContactsExpanded":[
    "DataFile",
    "ContactID",
    "CompanyNumber",
    "ContactNumber",
    "AccountNumber",
    "ContactStatus",
    "Name",
    "FirstName",
    "LastName",
    "EmailAddress",
    "SkypeUserName",
    "BankAccountDetails",
    "TaxNumber",
    "AccountsReceivableTaxType",
    "AccountsPayableTaxType",
    "Addresses",
    "Phones",
    "UpdatedDateUTC",
    "ContactGroups",
    "IsSupplier",
    "IsCustomer",
    "Website",
    "DefaultCurrency",
    "Discount",
    "BrandingTheme",
    "PurchasesDefaultAccountCode",
    "SalesDefaultAccountCode",
    "SalesTrackingCategories",
    "PurchasesTrackingCategories",
    "SalesDefaultLineAmountType",
    "PurchasesDefaultLineAmountType",
    "BatchPayments",
    "PaymentTerms",
    "Balances",
    "ContactPersons",
    "XeroNetworkKey",
    "HasAttachments"
    ],
    "CreditNotes":[
    "DataFile",
    "CreditNoteID",
    "CreditNoteNumber",
    "InvoiceAddresses",
    "CurrencyRate",
    "BrandingThemeID",
    "Type",
    "Reference",
    "RemainingCredit",
    "Allocations",
    "HasAttachments",
    "Contact",
    "SentToContact",
    "Date",
    "DueDate",
    "Status",
    "SubTotal",
    "TotalTax",
    "Total",
    "UpdatedDateUTC",
    "CurrencyCode",
    "FullyPaidOnDate"
    ],
    "CreditNotesExpanded":[
    "DataFile",
    "CreditNoteID",
    "CreditNoteNumber",
    "InvoiceAddresses",
    "CurrencyRate",
    "BrandingThemeID",
    "Type",
    "Reference",
    "RemainingCredit",
    "Allocations",
    "HasAttachments",
    "Contact",
    "SentToContact",
    "Date",
    "DueDate",
    "Status",
    "LineAmountTypes",
    "LineItems",
    "SubTotal",
    "TotalTax",
    "Total",
    "UpdatedDateUTC",
    "CurrencyCode",
    "FullyPaidOnDate"
    ],
    "ExecutiveSummary":[
    "DataFile",
    "Date",
    "UpdatedDateUTC",
    "Lines"
    ],
    "Invoices":[
    "DataFile",
    "Type",
    "InvoiceID",
    "InvoiceNumber",
    "Reference",
    "Payments",
    "CreditNotes",
    "Prepayments",
    "Overpayments",
    "AmountDue",
    "AmountPaid",
    "AmountCredited",
    "SentToContact",
    "ExpectedPaymentDate",
    "PlannedPaymentDate",
    "CurrencyRate",
    "HasAttachments",
    "InvoiceAddresses",
    "InvoicePaymentServices",
    "Contact",
    "RepeatingInvoiceID",
    "Date",
    "DueDate",
    "BrandingThemeID",
    "Url",
    "Status",
    "SubTotal",
    "TotalTax",
    "Total",
    "UpdatedDateUTC",
    "CurrencyCode",
    "FullyPaidOnDate",
    "CISDeduction"
    ],
    "InvoicesExpanded":[
    "DataFile",
    "Type",
    "InvoiceID",
    "InvoiceNumber",
    "Reference",
    "Payments",
    "CreditNotes",
    "Prepayments",
    "Overpayments",
    "AmountDue",
    "AmountPaid",
    "AmountCredited",
    "SentToContact",
    "ExpectedPaymentDate",
    "PlannedPaymentDate",
    "CurrencyRate",
    "HasAttachments",
    "InvoiceAddresses",
    "InvoicePaymentServices",
    "Contact",
    "RepeatingInvoiceID",
    "Date",
    "DueDate",
    "BrandingThemeID",
    "Url",
    "Status",
    "LineAmountTypes",
    "LineItems",
    "SubTotal",
    "TotalTax",
    "Total",
    "UpdatedDateUTC",
    "CurrencyCode",
    "FullyPaidOnDate",
    "CISDeduction"
    ],
    "InvoicesExpandedTable":[
    "DataFileID",
    "DataFileName",
    "DataFileCode",
    "InvoiceID",
    "Type",
    "InvoiceNumber",
    "Reference",
    "Status",
    "Date",
    "DueDate",
    "ExpectedPaymentDate",
    "PlannedPaymentDate",
    "FullyPaidOnDate",
    "ContactID",
    "ContactName",
    "LineAmountTypes",
    "LineID",
    "LineDescription",
    "LineQuantity",
    "LineUnitAmount",
    "LineAmount",
    "LineTaxAmount",
    "LineDiscountRate",
    "LineAccountID",
    "LineAccountCode",
    "LineItemID",
    "LineItemCode",
    "LineItemName",
    "LineTaxType",
    "TrackingCategory1ID",
    "TrackingCategory1Name",
    "TrackingOption1Name",
    "TrackingCategory2ID",
    "TrackingCategory2Name",
    "TrackingOption2Name",
    "SubTotal",
    "TotalTax",
    "Total",
    "AmountDue",
    "AmountPaid",
    "AmountCredited",
    "CurrencyCode",
    "CurrencyRate",
    "HasAttachments",
    "SentToContact",
    "CISDeduction",
    "RepeatingInvoiceID",
    "BrandingThemeID",
    "Url",
    "UpdatedDateUTC"
    ],
    "Items":[
    "DataFile",
    "ItemID",
    "Code",
    "Name",
    "Description",
    "PurchaseDescription",
    "UpdatedDateUTC",
    "PurchaseDetails",
    "SalesDetails",
    "IsTrackedAsInventory",
    "InventoryAssetAccountCode",
    "TotalCostPool",
    "QuantityOnHand",
    "IsSold",
    "IsPurchased"
    ],
    "ItemsTable":[
    "DataFileID",
    "DataFileName",
    "DataFileCode",
    "ItemID",
    "Code",
    "Name",
    "IsSold",
    "IsPurchased",
    "Description",
    "PurchaseDescription",
    "PurchaseUnitPrice",
    "PurchaseAccountCode",
    "COGSAccountCode",
    "PurchaseTaxType",
    "SalesUnitPrice",
    "SalesAccountCode",
    "SalesTaxType",
    "IsTrackedAsInventory",
    "InventoryAssetAccountCode",
    "TotalCostPool",
    "QuantityOnHand",
    "UpdatedDateUTC"
    ],
    "JournalsAccrual":[
    "DataFile",
    "JournalID",
    "JournalDate",
    "JournalNumber",
    "CreatedDateUTC",
    "Reference",
    "SourceID",
    "SourceType",
    "JournalLines"
    ],
    "JournalsAccrualTable":[
    "DataFileID",
    "DataFileName",
    "DataFileCode",
    "JournalID",
    "JournalDate",
    "JournalNumber",
    "Reference",
    "SourceID",
    "SourceType",
    "JournalLineID",
    "AccountID",
    "AccountCode",
    "AccountType",
    "AccountName",
    "Description",
    "NetAmount",
    "GrossAmount",
    "TaxAmount",
    "TaxType",
    "TaxName",
    "TrackingCategory1ID",
    "TrackingCategory1Name",
    "TrackingOption1ID",
    "TrackingOption1Name",
    "TrackingCategory2ID",
    "TrackingCategory2Name",
    "TrackingOption2ID",
    "TrackingOption2Name",
    "CreatedDateUTC"
    ],
    "JournalsCash":[
    "DataFile",
    "JournalID",
    "JournalDate",
    "JournalNumber",
    "CreatedDateUTC",
    "Reference",
    "SourceID",
    "SourceType",
    "JournalLines"
    ],
    "ManualJournals":[
    "DataFile",
    "Date",
    "Status",
    "UpdatedDateUTC",
    "ManualJournalID",
    "Narration",
    "ShowOnCashBasisReports",
    "HasAttachments",
    "Url"
    ],
    "ManualJournalsExpanded":[
    "DataFile",
    "Date",
    "Status",
    "LineAmountTypes",
    "UpdatedDateUTC",
    "ManualJournalID",
    "Narration",
    "JournalLines",
    "ShowOnCashBasisReports",
    "Url",
    "HasAttachments"
    ],
    "Organisation":[
    "DataFile",
    "APIKey",
    "Name",
    "LegalName",
    "PaysTax",
    "Version",
    "OrganisationType",
    "BaseCurrency",
    "CountryCode",
    "IsDemoCompany",
    "OrganisationStatus",
    "RegistrationNumber",
    "EmployerIdentificationNumber",
    "TaxNumber",
    "FinancialYearEndDay",
    "FinancialYearEndMonth",
    "SalesTaxBasis",
    "SalesTaxPeriod",
    "DefaultSalesTax",
    "DefaultPurchasesTax",
    "PeriodLockDate",
    "EndOfYearLockDate",
    "CreatedDateUTC",
    "OrganisationEntityType",
    "Timezone",
    "ShortCode",
    "OrganisationID",
    "Edition",
    "Class",
    "LineOfBusiness",
    "Addresses",
    "Phones",
    "ExternalLinks",
    "PaymentTerms"
    ],
    "Overpayments":[
    "DataFile",
    "OverpaymentID",
    "Type",
    "Reference",
    "RemainingCredit",
    "Allocations",
    "Payments",
    "HasAttachments",
    "Contact",
    "Date",
    "Status",
    "SubTotal",
    "TotalTax",
    "Total",
    "UpdatedDateUTC",
    "CurrencyCode",
    "CurrencyRate"
    ],
    "OverpaymentsExpanded":[
    "DataFile",
    "OverpaymentID",
    "Type",
    "Reference",
    "RemainingCredit",
    "Allocations",
    "Payments",
    "HasAttachments",
    "Contact",
    "Date",
    "Status",
    "LineAmountTypes",
    "LineItems",
    "SubTotal",
    "TotalTax",
    "Total",
    "UpdatedDateUTC",
    "CurrencyCode",
    "CurrencyRate"
    ],
    "Payments":[
    "DataFile",
    "PaymentID",
    "BatchPayment",
    "Date",
    "Amount",
    "Reference",
    "CurrencyRate",
    "PaymentType",
    "Status",
    "UpdatedDateUTC",
    "IsReconciled",
    "Account",
    "Invoice"
    ],
    "PaymentsTable":[
    "DataFileID",
    "DataFileName",
    "DataFileCode",
    "PaymentID",
    "PaymentType",
    "Reference",
    "Date",
    "Status",
    "AccountID",
    "AccountCode",
    "InvoiceID",
    "InvoiceNumber",
    "InvoiceType",
    "ContactID",
    "ContactName",
    "Amount",
    "CurrencyCode",
    "CurrencyRate",
    "BatchPaymentID",
    "BatchPaymentDate",
    "BatchPaymentType",
    "BatchPaymentStatus",
    "BatchPaymentTotalAmount",
    "IsReconciled",
    "UpdatedDateUTC"
    ],
    "ProfitAndLoss":[
    "DataFile",
    "FromDate",
    "ToDate",
    "PaymentsOnly",
    "StandardLayout",
    "UpdatedDateUTC",
    "Lines"
    ],
    "ProfitAndLossAdvanced":[
    "DataFile",
    "FromDate",
    "ToDate",
    "PaymentsOnly",
    "StandardLayout",
    "Periods",
    "TimeFrame",
    "TrackingCategoryID",
    "TrackingOptionID",
    "TrackingCategoryID2",
    "TrackingOptionID2",
    "UpdatedDateUTC",
    "Lines"
    ],
    "ProfitAndLossByMonth":[
    "DataFile",
    "FinancialYear",
    "PaymentsOnly",
    "StandardLayout",
    "UpdatedDateUTC",
    "Lines"
    ],
    "ProfitAndLossByTrackingCategory":[
    "DataFile",
    "FromDate",
    "ToDate",
    "PaymentsOnly",
    "StandardLayout",
    "TrackingCategoryID",
    "TrackingCategoryID2",
    "UpdatedDateUTC",
    "Lines"
    ],
    "ProfitAndLossMultiPeriodTable":[
    "DataFileID",
    "DataFileName",
    "DataFileCode",
    "FromDate",
    "ToDate",
    "Period",
    "PaymentsOnly",
    "StandardLayout",
    "LineType",
    "AccountID",
    "AccountName",
    "AccountLineType",
    "Amount"
    ],
    "ProfitAndLossTable":[
    "DataFileID",
    "DataFileName",
    "DataFileCode",
    "FromDate",
    "ToDate",
    "PaymentsOnly",
    "StandardLayout",
    "LineType",
    "AccountID",
    "AccountName",
    "AccountLineType",
    "Amount"
    ],
    "PurchaseOrders":[
    "DataFile",
    "PurchaseOrderID",
    "PurchaseOrderNumber",
    "Date",
    "DeliveryDate",
    "ExpectedArrivalDate",
    "DeliveryAddress",
    "AttentionTo",
    "Telephone",
    "DeliveryInstructions",
    "SentToContact",
    "Reference",
    "Type",
    "CurrencyRate",
    "CurrencyCode",
    "Contact",
    "BrandingThemeID",
    "Status",
    "LineAmountTypes",
    "LineItems",
    "SubTotal",
    "TotalTax",
    "Total",
    "UpdatedDateUTC",
    "HasAttachments"
    ],
    "Quotes":[
    "DataFile",
    "QuoteID",
    "QuoteNumber",
    "Reference",
    "Terms",
    "Contact",
    "LineItems"
    ],
    "TaxRates":[
    "DataFile",
    "Name",
    "TaxType",
    "ReportTaxType",
    "CanApplyToAssets",
    "CanApplyToEquity",
    "CanApplyToExpenses",
    "CanApplyToLiabilities",
    "CanApplyToRevenue",
    "DisplayTaxRate",
    "EffectiveRate",
    "Status",
    "TaxComponents"
    ],
    "TaxRatesTable":[
    "DataFileID",
    "DataFileName",
    "DataFileCode",
    "Name",
    "TaxType",
    "Status",
    "ReportTaxType",
    "CanApplyToAssets",
    "CanApplyToEquity",
    "CanApplyToExpenses",
    "CanApplyToLiabilities",
    "CanApplyToRevenue",
    "DisplayTaxRate",
    "EffectiveRate",
    "TaxComponentName",
    "TaxComponentRate",
    "TaxComponentIsCompound",
    "TaxComponentIsNonRecoverable"
    ],
    "TrackingCategories":[
    "DataFile",
    "Name",
    "Status",
    "TrackingCategoryID",
    "Options"
    ],
    "TrackingCategoriesTable":[
    "DataFileID",
    "DataFileName",
    "DataFileCode",
    "TrackingCategoryID",
    "TrackingCategoryName",
    "TrackingCategoryStatus",
    "TrackingOptionID",
    "TrackingOptionName",
    "TrackingOptionStatus"
    ],
    "TrialBalance":[
    "DataFile",
    "Date",
    "PaymentsOnly",
    "UpdatedDateUTC",
    "Lines"
    ],
    "TrialBalanceMultiPeriodTable":[
    "DataFileID",
    "DataFileName",
    "DataFileCode",
    "Date",
    "Period",
    "PaymentsOnly",
    "LineType",
    "AccountID",
    "AccountName",
    "AccountLineType",
    "DebitAmount",
    "CreditAmount",
    "DebitAmountYTD",
    "CreditAmountYTD"
    ],
    "Users":[
    "DataFile",
    "GlobalUserID",
    "UserID",
    "EmailAddress",
    "FirstName",
    "LastName",
    "UpdatedDateUTC",
    "IsSubscriber",
    "OrganisationRole"
    ]



























































}
