# WikiStat Downloader

In this project I am trying to download the WikiStat dataset from wikimedia's own data dump, which can be found here: https://dumps.wikimedia.org/other/pageviews/

This is sort of a client request to find out if different database engines had different performance, and I spent a whole afternoon to build it using lots of prompting.

## How to use:

1. Clone the repo
2. Install dependencies "pip install -r requirements.txt"
3. Modify env using cat/vim and dont forget to rename using "mv env .env"
4. run main.py script "python main.py"
5. Adjust .env to meet your VM specs and optimize run time

## Questions:

1. Why not just download the files one by one?
There are about 85,000 files each containing up to 40 million rows (about 50mb) - this would take 3 days according to ClickHouse

2. Why are you using mysql?
Its not exactly mysql, I am uploading the dataset to an enhanced version of mysql developed by Alibaba Cloud known as AnalyticDB for mysql - it is designed as a lakehouse with similar performance to ClickHouse (that means you can use a single database for your datawarehouse architecture)

# Acknowledgements:
1. ClickHouse for making their own tutorial (https://clickhouse.com/docs/en/getting-started/example-datasets/wikistat)
2. Claude + Gemini (mostly Claude)