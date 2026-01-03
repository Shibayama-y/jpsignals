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

### JPX-Nikkei 400 / Nikkei 225 の構成銘柄を取得

Nikkei公式の構成銘柄CSVをダウンロードし、インデックス名付きで CSV 出力します。

```bash
python scripts/fetch_index_constituents.py               # 両指数を標準出力へ
python scripts/fetch_index_constituents.py --index nikkei225 -o data/nikkei225.csv
python scripts/fetch_index_constituents.py --output-dir data  # data/nikkei225.csv と data/jpx_nikkei400.csv を書き込み
# universe 形式のコード一覧を出力（Index列なし）
python scripts/fetch_index_constituents.py --universe-output data/universe.txt
```
- サイトから 403 が返った場合は `--cookie cf_clearance=<値>` のようにブラウザで取得した Cookie を渡せます（複数指定可）。
- ブラウザから CSV/PDF を手動ダウンロードした場合は `--nikkei225-file <path>` や `--jpx-nikkei400-file <path>` でローカルファイルを読み込めます（PDF は銘柄コード/名称を抽出して処理）。
- 出力 CSV は `# Index,Code,Name,Ticker` ヘッダー行の後にデータを流し込みます。企業名は JPX 銘柄リストから日本語で補完します（取得に失敗した場合はダウンロード元の名称を使用）。

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

### デイリーパイプライン（ポジション更新 + レポート生成）

`scripts/daily_technical.py` が出力する `out/daily.jsonl` を入力に、保有状態を更新しメール本文を作成します。

1. ポジション/レジャー更新とイベント要約（冪等、同日再実行はスキップ）
```bash
python scripts/update_positions.py \
  --asof 2026-01-04 \
  --signals out/daily.jsonl \
  --positions data/state/positions.json \
  --ledger-dir data/state/ledger \
  --events-out data/runs/daily/2026-01-04.events.json \
  --default-qty 200 \
  --run-url "https://github.com/owner/repo/actions/runs/123"
```

2. 企業名解決＋Markdownメール生成（signalsのコピーとcompany cache更新を伴う）
```bash
python scripts/build_daily_report.py \
  --asof 2026-01-04 \
  --signals out/daily.jsonl \
  --events data/runs/daily/2026-01-04.events.json \
  --positions data/state/positions.json \
  --company-cache data/master/company_names.json \
  --report-out data/reports/daily/2026-01-04.md \
  --run-out data/runs/daily/2026-01-04.jsonl \
  --status success \
  --run-url "https://github.com/owner/repo/actions/runs/123"
```

3. 古い daily の削除（任意）
```bash
python scripts/prune_data.py --keep-days 90
```
