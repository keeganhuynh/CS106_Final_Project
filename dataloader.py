import pandas as pd

class OurDatasetCollector:
    def __init__(self):
        pass

    def check_dataset(self, df):
        duplicates = df.duplicated().sum()
        print(f"Number of duplicated rows: {duplicates}")

        missing_values = df.isnull().sum().sum()
        print(f"Number of missing values: {missing_values}")

        min_max_dates = df.groupby('tic')['date'].agg(['min', 'max'])

        largest_min_date = min_max_dates['min'].max()
        smallest_max_date = min_max_dates['max'].min()

        print(f"Largest minimum date across all tics: {largest_min_date}")
        print(f"Smallest maximum date across all tics: {smallest_max_date}")

        df = df[(df['date'] >= largest_min_date) & (df['date'] <= smallest_max_date)]
        
        day_counts = df['day'].value_counts().sort_index()
        
        print("Day of the week counts:")
        for day, count in day_counts.items():
            print(f"{day}: {count}")

        return df

    def csv_to_yf(self, csv_path, stock_ticker):

        df = pd.read_csv(csv_path)
        
        if df.columns[0] == 'Date':
            df['date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y')
            df['open'] = df['Open'].str.replace(',', '').astype(float)
            df['high'] = df['High'].str.replace(',', '').astype(float)
            df['low'] = df['Low'].str.replace(',', '').astype(float)
            df['close'] = df['Price'].str.replace(',', '').astype(float)  
            if df['Vol.'].isnull().any():
                df['Vol.'].fillna(0)  
            df['volume'] = df['Vol.'].apply(lambda x: float(x.replace('M', '').replace('K', '')) * (1e6 if 'M' in x else 1e3 if 'K' in x else 1))
            df['tic'] = stock_ticker
            df['day'] = df['date'].dt.dayofweek
            df['date'] = df['date'].astype(str)
            
            drop_col = ['Date',	'Price', 'Open', 'High', 'Low',	'Vol.','Change %']
            for col in drop_col:
                df.drop(columns=col, inplace=True)

        else:
            # Ngày,Lần cuối,Mở,Cao,Thấp,KL,% Thay đổi
            df['date'] = pd.to_datetime(df['Ngày'], format='%d/%m/%Y')
            df['open'] = df['Mở'].str.replace(',', '').astype(float)
            df['high'] = df['Cao'].str.replace(',', '').astype(float)
            df['low'] = df['Thấp'].str.replace(',', '').astype(float)
            df['close'] = df['Lần cuối'].str.replace(',', '').astype(float)    
            
            if df['KL'].isnull().any():
                df['KL'].fillna(0)

            df['volume'] = df['KL'].apply(lambda x: float(x.replace('M', '').replace('K', '')) * (1e6 if 'M' in x else 1e3 if 'K' in x else 1))
            df['tic'] = stock_ticker
            df['day'] = df['date'].dt.dayofweek
            df['date'] = df['date'].astype(str)
            
            drop_col = ['Ngày','Lần cuối','Mở','Cao','Thấp','KL','% Thay đổi']
            for col in drop_col:
                df.drop(columns=col, inplace=True)

        return df
    
    def Importer(self, csv_path, tik):
        df = self.csv_to_yf(csv_path, tik)
        self.check_dataset(df)
        return df
    
    def MultiStock(self, csv_path, stock_ticker_list):
        df = pd.DataFrame()
        for i, csv_path in enumerate(csv_path):
            print(f'read {csv_path}')
            df_temp = self.csv_to_yf(csv_path, stock_ticker_list[i])
            df = pd.concat([df, df_temp])
        df = self.check_dataset(df)
        return df
    
    def read_csv_files(self, folder_path):
        import os
        csv_path_list = []
        stock_ticker_list = []

        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if file.endswith('.csv'):
                    file_path = os.path.join(root, file)
                    csv_path_list.append(file_path)
                    stock_ticker_list.append(file[:-4])

        return csv_path_list, stock_ticker_list
    
    def ReadFromFolder(self, folder_path):
        csv_path_list, stock_ticker_list = self.read_csv_files(folder_path)
        return self.MultiStock(csv_path_list, stock_ticker_list)