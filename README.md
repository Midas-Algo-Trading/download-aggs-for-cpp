# download-aggs-for-cpp
### Downloads and compiles stock data for our C++ projects

## Usage
### Download stocks' minute aggregates
`main.py <start date> <end date> <path to write aggregates to> <Polygon.IO API key>`  
`main.py 2022-01-01 2023-01-01 Desktop/StockData/StockAggs HD818hFHdh1f9hj919jf1`
### Compile market snapshots
`market_snapshot_maker.py <start date> <end date> <path to write snapshots to>`  
`market_snapshot_maker.py 2022-01-01 2023-01-01 Desktop/StockData/MarketSnapshots`


## Note
- This code asyncronously sends GET HTTPS requests to Polygon.IO to download most stocks' minute aggregates and writes the responses to files
  - Downloads all stocks whos symbol does not contain any non-letters (1,^,3,., etc.) 
  - Aggregates are saved to a .csv file
    - Max file size of 50,000 aggregates
    - Would save to a .feather file for faster reading and smaller file sizes, but support for .feather files has not
      been added to the C++ project yet.
  - Expect ~10GB of files per year of downloaded data
- Can further compile stock market minute snapshots from the downloaded stock aggregates
