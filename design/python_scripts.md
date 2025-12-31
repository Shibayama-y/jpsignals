# Python スクリプト設計書

本リポジトリの Python スクリプト群（`scripts/` 配下）について、目的・入出力・依存・エラー処理などをまとめる。

## 対象スクリプト
- `scripts/fetch_jp_list.py`：JPX 公開の銘柄リスト（Excel）を取得し、コード・銘柄名・yfinance 用ティッカー（.T）を CSV で出力する。
- `scripts/fetch_jp_stock.py`：yfinance を用いて指定日の東証銘柄データを取得し、複数銘柄を行単位で表示する。

## 共通前提
- Python 3.9+。
- 依存パッケージ（例）：`pandas`、`requests`、`yfinance`、`xlrd`、`openpyxl`。
- 実行例はリポジトリルートで `python scripts/....py`。

## fetch_jp_list.py
- 目的：JPX 統計ページを探索し、最新の銘柄リスト Excel をダウンロード。ETF・ETN を除外して出力。
- 処理フロー:
  1. 既知の JPX ページから `data_j.xls/xlsx` へのリンクを探索。見つからない場合は既知の直接リンクを順に試行。
  2. Microsoft Office Viewer 経由リンクの場合は `src` パラメータから元 URL を抽出。
  3. Excel をダウンロードし、先頭行付近からヘッダ（「コード」「銘柄名」）を確定。
  4. 「市場・商品区分」が `ETF・ETN` の行を除外。
  5. コードは数値抽出し 4 桁ゼロ埋め、`Ticker` 列に `.T` を付与。
  6. `Code, Name, Ticker` の CSV を標準出力または `--output` で保存。
- 主要引数:
  - `--output/-o`: 出力 CSV パス（省略時は stdout）。
  - `--page`: JPX ページ URL を上書き（複数可）。
- 主な例外/エラーハンドリング:
  - JPX ページでリンク未発見 → `SystemExit`。
  - ダウンロード/パース失敗 → `SystemExit` でエラーメッセージ出力。
  - 必須列（コード・銘柄名）が無い場合 → `SystemExit`。

## fetch_jp_stock.py
- 目的：指定日付（取引所ローカル日付）における東証銘柄の株価指標を取得。
- 処理フロー:
  1. 引数で複数ティッカー（末尾 `.T`）と `-d/--date` を受け取る。
  2. `yfinance.download` で 1 日幅を取得し、Open/High/Low/Close/Volume を抽出。
  3. データが空の銘柄は標準エラーに警告を出し、全銘柄空なら終了。
  4. 行インデックスにティッカーを置き、列で指標を表示。
- 主要引数:
  - 位置引数 `tickers...`: 1 件以上のティッカー（例: `7203.T 9984.T`）。
  - `-d/--date`: 取得日（YYYY-MM-DD, Asia/Tokyo）。
  - `--auto-adjust`: 配当・分割調整後価格を取得。
- 主な例外/エラーハンドリング:
  - 全銘柄でデータなし → `SystemExit`。
  - 個別銘柄のデータなし → 標準エラーに警告。

## 今後の拡張候補
- リトライやタイムアウトの調整をオプション化。
- ログ出力（ファイル/JSONL）サポート。
- JPX リストのキャッシュ保存と更新日時の表示。
- 株価取得でのリトライやプロキシ設定のオプション追加。
