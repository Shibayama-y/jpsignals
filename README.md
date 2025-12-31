# jpsignals

## 日本株データ取得サンプル (yfinance)

前提:
- Python 3.9+
- `pip install pandas yfinance requests xlrd openpyxl`

### 銘柄コード一覧を取得

JPXサイトのページから最新のリストURLを自動検出してダウンロードし、コード・名称・yfinance用ティッカー（.T付き）のCSVを出力します。

```bash
python scripts/fetch_jp_list.py > jpx_list.csv
```
- 保存先を指定: `python scripts/fetch_jp_list.py -o data/jpx_list.csv`
- JPXページのURLが変わった場合は `--page <URL>` を複数指定して上書きできます。

### 指定日の株価を取得（複数銘柄対応）

```bash
python scripts/fetch_jp_stock.py 7203.T 9984.T -d 2024-01-05
```
- 複数銘柄に対応（行で銘柄別に表示）。1銘柄だけでも可。
- 調整後株価が必要なら `--auto-adjust` を付ける:

```bash
python scripts/fetch_jp_stock.py 7203.T 9984.T -d 2024-01-05 --auto-adjust
```

補足:
- 東証銘柄は末尾に `.T` を付けて指定してください。
- 取得日は取引所ローカル時間 (Asia/Tokyo) の YYYY-MM-DD で入力します。
- 非営業日や誤ったティッカーは標準エラーに警告を出し、すべて取得できなければ終了します。
