# pubmed-parser

## Set up env
1. `docker-compose build`でビルドします．

2. `docker-compose up`でコンテナを立ち上げます．

http://0.0.0.0:8081 に飛ぶとMongo Expressが開けます．

## How to use
#### 1. Download pubmed XML files
dataset/download_dataset.shを実行すると，
dataset下にbaselineとupdatesをそれぞれダウンロードします．

#### 2. Exec pubmed_iter_parser.py
`python -m script.pubmed_iter_parser`で実行できます．
` nohup python -m script.pubmed_iter_parser > log/nohup.out &
`でバックグラウンドで実行します．

MongoDB内に逐次保存していきます．
途中で中断してもOKです．同じかつ古いバージョンのレコードは保存しません．


## XMLの仕様
PMIDが同じレコードが存在しますが，Version違いです．
最新のレコードを保存するようにしています．
