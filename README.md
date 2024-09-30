
# IRG Distributors Inventory

This python application is used for the retrieval, processing, formatting and uploading of IRG's distributor's inventory count.

## Preview: 
![](https://github.com/calebrosen/distributors-inventory/blob/main/distributors_inventory_preview.gif)

## Installation

Install python first, then open CMD and run

```bash
    pip install PyQt6 selenium smtplib openpyxl pandas
    pip install mysql-connector-python mysql requests python-dotenv

```
    
## Environment Variables

To run this, your .env file must have the following variables:

Zoho API: `ZOHO_CLIENT_ID`, `ZOHO_CLIENT_SECRET`, `ZOHO_REFRESH_TOKEN`

Zoho Mail ID's: `ZOHO_MAIL_ACCOUNT_ID`, `ZOHO_MAIL_FOLDER_ID`

Zoho Login: `ZOHO_USERNAME_EMAIL`, `ZOHO_PASSWORD`

Mail Info for Error Catches: `SMTP_SERVER`, `SMTP_PORT`, `GMAIL_USER`, `GMAIL_PASSWORD`, `IT_EMAIL`

MySQL Connection: `MYSQL_HOST`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DB`

Distributor Logins: `PCS_USERNAME`, `PCS_PASSWORD`, `PIN_USERNAME`, `PIN_PASSWORD`

Other: `CSV_FOLDER_PATH`
# Adding another Distributor


#### For this example, I am using "NEW", as the new distributor's name.

###

Add the distributor's abbreviation.
```python
# Old
checkbox_labels = ["AES", "AZF", "FOR", "IRG", "RMI", "RUT", "TSD", "PIN", "PCS"]

# Added "NEW" Distributor
checkbox_labels = ["AES", "AZF", "FOR", "IRG", "RMI", "RUT", "TSD", "PIN", "PCS", "NEW"]
```

Create a function to get the spreadsheet (must be exactly like this):
```python
def get_new_spreadsheet:
    # code here
```

### If using Zoho Mail API:
Copy a different function (not AES) using Zoho Mail API and swap out the 3 letter abbreviations.

This should be around 50 times. You can also do a find and replace (while being careful to not mess any others up)


#### Depending on how Zoho Mail parses their spreadsheet, you may have to modify how the CSV is created. This was the case with AES.


### If using Selenium:

Copy a different selenium-based function (like PCS or PIN) and adjust the relevant code.

When finding elements, if ID's or classes are available, use those. If not, use ```CSS_SELECTOR```. If ```CSS_SELECTOR``` isn't an option (meaning the element has no distinguishable attributes), use ```XPATH```.


### After the spreadsheet is acquired

Find this code:

```python
    switch = {
        'aes': process_aes,
        'azf': process_azf,
        'for': process_for,
        'pcs': process_pcs,
        'pin': process_pin,
        'rmi': process_rmi,
        'rut': process_rut,
        'tsd': process_tsd,
        'irg': process_irg
    }
```
and add on to it.

```python
    switch = {
        'aes': process_aes,
        'azf': process_azf,
        'for': process_for,
        'pcs': process_pcs,
        'pin': process_pin,
        'rmi': process_rmi,
        'rut': process_rut,
        'tsd': process_tsd,
        'irg': process_irg,
        'new': process_new
    }
```

Now create the process_new function:
```python
    def process_new:
        # code here
```
You will have to format and re-order the columns into ```Distributor, Model, Warehouse, Quantity, Supplier```. Use a different distributor's 'process' function to get an idea of what to do.

### Example process_new function:

```python

def process_new(df):

    # In this example, the distributor provides a spreadsheet with columns:
    # [Part Number, Product Description, Supplier, Quantity, MSRP]


    # Deleting columns 'Product Description' and 'MSRP'
    df.drop(['Product Description', 'MSRP'], axis=1, inplace=True)

    # Renaming column 'Part Number' to 'Model'
    df.rename(columns={'Part Number': 'Model'}, inplace=True)
    
    # Inserting the distributor name as the first column
    df.insert(0, 'Distributor', 'NEW')

    # Inserting Warehouse as an empty string
    df.insert(2, 'Warehouse', '')

    # Reordering Columns
    columns = ['Distributor', 'Model', 'Warehouse', 'Quantity', 'Supplier']
    df = df[columns]

    # Formatting some models for consolidation
    df['Model'] = df['Model'].apply(replace_values)

    append_log_messages("- Formatted New.", 0)

    df.to_csv(f"{download_dir}/new_formatted.csv", index=False)

    return df
```
#### After all of this is done, you should be able to run the program and process this new distributor.

## 🔗 API & Dependency Documentation

- [Zoho Mail API Documentation](https://www.zoho.com/mail/help/api/)
- [Selenium Documentation](https://www.selenium.dev/documentation/)
