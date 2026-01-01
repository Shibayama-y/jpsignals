# 日本株スイングトレード向けファンダメンタル・スクリーナー設計書（yfinance / 無料データ）

- 対象：日本株（Yahoo Finance ティッカー例：`7203.T`）
- 目的：指定銘柄群のファンダメンタル（定量）を **yfinance の無料取得データ**で自動判定し、合否・スコア・理由を出力する
- 監視銘柄の受け渡し：**引数（CLI）のみ**
- 注意：yfinance は非公式データ取得であり、銘柄・項目により欠損/揺れが起こり得る（設計で吸収する）

---

## 1. 目的・スコープ

### 1.1 目的
- 銘柄リスト（引数で渡す）について、以下を自動実行する
  1. yfinance から財務データ等を取得
  2. 一般的なファンダ指標を算出（ROE、営業利益率、自己資本比率、CFO、FCF、PER、PBR など）
  3. 初期設定の **一般的な判定ルール**で合否を判定
  4. スコア（0–100）と、合否理由（どの条件を満たした/満たさない/欠損）を出力

### 1.2 非スコープ（将来拡張）
- TDnet/EDINET 等の一次開示連携
- PDF/XBRL 解析
- 定性評価（競争優位、経営者、ガバナンス等）
- コンセンサス予想の網羅（yfinance のアナリスト系は銘柄依存）

---

## 2. 実行インターフェース（引数のみ）

### 2.1 CLI 仕様（argparse）
- 位置引数：`TICKERS...`（1個以上、空はエラー）
- 実行例：
  - `python jf_screener.py 7203.T 9984.T 6758.T`

### 2.2 オプション引数（初期実装）
- `--format {text,json}`：出力形式（既定：`text`）
- `--min-score N`：合格下限（既定：`60`）
- `--strict`：欠損が一定数以上ある場合に不合格寄り（既定：OFF）
- `--timeout SEC`：HTTP タイムアウト（既定：`10`）
- `--log-level {ERROR,WARNING,INFO,DEBUG}`（既定：`INFO`）

### 2.3 入力バリデーション
- ティッカーは **Yahoo Finance 形式**（例：`7203.T`）を推奨
- 実装上の任意仕様：`7203` のような 4 桁のみが入力された場合、`.T` を補完して `7203.T` として扱う（ON/OFFは将来オプション化してもよい）

---

## 3. データソースと取得方針（yfinance）

### 3.1 yfinance で取得する情報（銘柄単位）
- 財務三表（TTM 優先、無ければ年次）
  - 損益：`ttm_income_stmt` → 無ければ `income_stmt`
  - 貸借：`balance_sheet`
  - CF：`ttm_cashflow` → 無ければ `cashflow`
- 付帯情報（取れる範囲で使用）
  - `info`：`trailingPE`、`priceToBook`、`dividendYield`、`sharesOutstanding`、`currentPrice` 等
  - `calendar`：決算日等（参考として保持、判定には未使用）

### 3.2 取得戦略（欠損を前提）
- 各データ取得は例外処理で囲み、銘柄単位で失敗しても全体は継続する
- 指標算出は **可能な項目だけで**行い、欠損は `NaN` として扱う
- `--strict` で欠損の扱いを厳格化する（詳細は 6.3）

---

## 4. 算出指標（一般的な定量ファンダ）

### 4.1 収益性・効率
- **ROE**：`NetIncome / Equity`
- **営業利益率（Operating Margin）**：`OperatingIncome / Revenue`

### 4.2 財務健全性
- **自己資本比率（Equity Ratio）**：`Equity / TotalAssets`
- **D/E（負債資本倍率）**：`TotalLiabilities / Equity`（業種差が大きいので参考扱い）

### 4.3 キャッシュフロー
- **営業CF（CFO）**：`OperatingCashFlow`
- **FCF（簡易）**：`CFO - Capex`（Capex が取れる場合のみ）

### 4.4 バリュエーション
- **PER**：
  - `info["trailingPE"]` があれば利用
  - 欠損時：`price / (net_income / shares)` を試みる（`price` と `shares` が取れる場合）
- **PBR**：
  - `info["priceToBook"]` があれば利用
  - 欠損時：`price / (equity / shares)` を試みる（`price` と `shares` が取れる場合）

---

## 5. 初期のファンダ判定ルール（世間的に一般的な目安）

> 注意：閾値は「一般的な目安」を初期値として採用し、業種差や成長局面はテクニカルと併用して最終判断する前提。

### 5.1 ハードフィルタ（最低条件）
取得できた項目のみで判定し、欠損は原則「判定不能」として即不合格にしない（`--strict` を除く）。

1. ROE ≥ **8%**
2. 営業利益率 ≥ **5%**
3. 自己資本比率 ≥ **30%**
4. CFO > **0**
5. PER ≤ **20**（割高域の回避）
6. PBR ≤ **1.5**（1倍目安を踏まえた許容上限）

### 5.2 スコアリング（100点満点・初期案）
欠損は 0 点（非評価）。合計点で優先順位付け。

- **収益性（40点）**
  - ROE：8%未満 0 / 8–10% 15 / 10%以上 25
  - 営業利益率：0–5% 5 / 5–10% 10 / 10%以上 15
- **健全性（25点）**
  - 自己資本比率：30%未満 0 / 30–50% 10 / 50%以上 15
  - D/E：極端に高い場合のみ減点（初期は `D/E > 5` で -5 点等の軽い扱い）
- **キャッシュフロー（20点）**
  - CFO > 0：10
  - FCF > 0：10（Capex 欠損で FCF が作れない場合は 0 点）
- **バリュエーション（15点）**
  - PBR：<=1.0 10 / 1.0–1.5 5 / >1.5 0
  - PER：<=15 5 / 15–20 3 / >20 0

### 5.3 合格判定（初期）
- `score >= --min-score`（既定 60）
- かつ、ハードフィルタの「判定可能な項目」のうち **過半**が合格  
  - 分母：欠損（判定不能）を除外
  - 分子：合格した項目数

---

## 6. 欠損・例外処理方針

### 6.1 欠損（NaN）の基本ルール
- 指標の算出に必要な要素が揃わない場合：指標は `NaN`
- ハードフィルタ：`NaN` は “判定不能” として扱い、即不合格にしない（既定）

### 6.2 例外（ネットワーク、yfinance 例外）
- 銘柄単位で try/except し、失敗銘柄は `errors` に理由を保持
- 失敗銘柄は `pass=false` とするが、実行全体は継続する

### 6.3 `--strict` の仕様
- 主要 6 項目（ROE, OpMargin, EquityRatio, CFO, PER, PBR）のうち、
  - `NaN` が **3 個以上**なら `pass=false`（スコアが高くても不合格）
- 目的：データ欠損が多い銘柄を自動売買候補から排除し、誤判定を減らす

---

## 7. 出力仕様

### 7.1 text（既定）
- 1 銘柄 1 レコード（複数行でも可）
- 出力項目（例）：
  - `ticker, score, pass/fail, metrics_summary, passed_rules, failed_rules, missing_rules, errors`

#### 表示例（イメージ）
- `7203.T  score=72  PASS  ROE=0.11 OpM=0.08 EqR=0.45 PER=12.3 PBR=1.1 CFO=+ FCF=+  pass=[ROE,OpM,EqR,CFO,PER,PBR] fail=[] missing=[]`

### 7.2 json
- JSONL（1行1銘柄）を推奨（パイプ処理・集計が容易）
- フィールド：
  - `ticker: str`
  - `score: int`
  - `pass: bool`
  - `metrics: {roe, op_margin, equity_ratio, de_ratio, cfo, fcf, per, pbr, dividend_yield}`
  - `passed_rules: [str]`
  - `failed_rules: [str]`
  - `missing_rules: [str]`
  - `errors: [str]`

---

## 8. 実装メモ（CODEX向け）

### 8.1 必須依存
- Python 3.10+（推奨 3.11+）
- `yfinance`, `pandas`, `numpy`
- `argparse`（標準ライブラリ）

### 8.2 財務表の行名揺れ対策
yfinance の DataFrame index は銘柄/会計基準で揺れる可能性があるため、行名の候補リストを用意し最初に取れたものを採用する。

- Revenue：`["Total Revenue", "TotalRevenue"]`
- Operating Income：`["Operating Income", "OperatingIncome"]`
- Net Income：`["Net Income", "NetIncome"]`
- Equity：`["Total Stockholder Equity", "Stockholders Equity", "Total Equity Gross Minority Interest"]`
- Total Assets：`["Total Assets", "TotalAssets"]`
- Total Liabilities：`["Total Liab", "TotalLiabilitiesNetMinorityInterest", "Total Liabilities"]`
- CFO：`["Total Cash From Operating Activities", "Operating Cash Flow"]`
- Capex：`["Capital Expenditures", "Capital Expenditure"]`

### 8.3 最新カラムの扱い
- yfinance の財務 DataFrame は「列＝期」「行＝科目」の形式が多い
- 原則：先頭列（`df.columns[0]`）を最新として扱う。ただし、将来の仕様変更に備え、列をソート/解釈する関数を 1 箇所に集約する。

### 8.4 価格・発行株式数の扱い
- `price` は `info["currentPrice"]` を優先（欠損時は別キーをフォールバック）
- `shares` は `info["sharesOutstanding"]` を使用
- どちらか欠損時は自前 PER/PBR の算出をスキップし `NaN`

---

## 9. ロギング・観測性

- 進捗：銘柄数、成功数、失敗数
- 失敗：例外種別、メッセージ（ただし秘密情報は出さない）
- `--log-level DEBUG` で、取得した主要キーの欠損状況（どの行名が当たったか）も出力可能にする

---

## 10. テスト計画（最小）

### 10.1 正常系
- 大型株（例：`7203.T` など）を数銘柄渡し、指標・スコア・合否が出ること

### 10.2 欠損系
- 財務表が欠損しやすい銘柄を混ぜ、欠損許容ロジックで落ちないこと
- `--strict` で欠損銘柄が落ちること

### 10.3 異常系
- 無効ティッカー（例：`XXXX.T`）で errors が出ること
- ネットワーク障害時に処理が継続すること

---

## 11. 拡張ポイント（将来）

- セクター別閾値（金融、商社、REIT 等で別ルール）
- 価格（テクニカル）条件との AND 結合（Fundamental OK AND Technical Trigger）
- キャッシュフローの複数年推移（単年でなく 3 年平均など）
- 企業イベント（決算日近接の回避など、`calendar` 活用）
