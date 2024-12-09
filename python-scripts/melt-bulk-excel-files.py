import os
import pandas as pd

# Define the folder path
folder_path = 'bulk-excel-files'
output_csv = os.path.join(folder_path, "consolidated_data.csv")

# Check if the folder exists
if not os.path.exists(folder_path):
    raise FileNotFoundError(f"The folder '{folder_path}' does not exist. Please check the path.")

# Initialize an empty list to store all transformed data
all_data = []

# Process each Excel file in the folder
for file_name in os.listdir(folder_path):
    if file_name.endswith('.xlsx'):  # Check if the file is an Excel file
        file_path = os.path.join(folder_path, file_name)
        
        # Read metadata to extract dates
        metadata = pd.read_excel(file_path, sheet_name='Profit and Loss', nrows=5)
        cleaned_data = pd.read_excel(file_path, sheet_name='Profit and Loss', skiprows=5)
        
        # Extract and calculate dynamic dates
        to_date_string = metadata.iloc[1, 0]  # 2nd row, 1st column
        to_date = pd.to_datetime(to_date_string.split("ended ")[1], format='%d %B %Y')
        from_date = pd.to_datetime(to_date) - pd.offsets.MonthBegin(1)
        
        # Reshape the data into long format
        reshaped_data = pd.melt(
            cleaned_data,
            id_vars=['Account'],
            var_name='Category',
            value_name='Amount'
        )
        
        # Drop rows with NaN in the Amount column
        reshaped_data = reshaped_data.dropna(subset=['Amount']).reset_index(drop=True)
        
        # Add 'from_date' and 'to_date' columns dynamically
        reshaped_data['from_date'] = from_date.strftime('%Y-%m-%d')
        reshaped_data['to_date'] = to_date.strftime('%Y-%m-%d')
        
        # Define account line type mapping function
        def determine_account_line_type(account):
            if "Trading Income" in account or "Sales" in account:
                return "Income"
            elif "Cost of Sales" in account or any(term in account for term in ["Contractors", "Cost of Goods Sold", "Hire of Equipment/Tools"]):
                return "Cost of Sales"
            elif "Operating Expenses" in account:
                return "Operating Expenses"
            return "Other"

        # Apply the mapping to create the 'account_line_type' column
        reshaped_data['account_line_type'] = reshaped_data['Account'].apply(determine_account_line_type)
        
        # Append the transformed data to the list
        all_data.append(reshaped_data)

# Concatenate all transformed data
if all_data:
    consolidated_data = pd.concat(all_data, ignore_index=True)

    # Save the consolidated data as a UTF-8 encoded CSV file
    consolidated_data.to_csv(output_csv, index=False, encoding='utf-8')

    print(f"Consolidated data saved to {output_csv}")
else:
    print(f"No Excel files found in the folder '{folder_path}'.")
