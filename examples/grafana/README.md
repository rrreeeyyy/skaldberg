# Grafana 連携サンプル

Skaldberg が公開する Prometheus HTTP API subset (`/api/v1/{query,query_range,labels,label/<n>/values,series}`) を、Grafana 組み込みの **Prometheus datasource** からそのまま叩く構成。

## 用意するもの

- Skaldberg サーバ (host で `cargo run --release -- --catalog memory ...` 等)
- Docker (Grafana を立ち上げる)

## 立ち上げ手順

1. Skaldberg を host で起動

    ```sh
    ./target/release/skaldberg-server \
      --catalog memory \
      --warehouse-uri memory:///wh \
      --wal-dir /tmp/skaldberg-wal \
      --bind 127.0.0.1:8080 \
      --flush-interval-secs 10
    ```

2. データを少し入れる

    ```sh
    ts=$(python3 -c 'import time; print(int(time.time()*1000))')
    curl -X POST http://127.0.0.1:8080/api/v1/ingest \
      -H 'content-type: application/json' \
      -d "{\"samples\":[
        {\"metric\":\"cpu\",\"labels\":{\"host\":\"a\"},\"ts\":$ts,\"value\":1.0},
        {\"metric\":\"cpu\",\"labels\":{\"host\":\"b\"},\"ts\":$ts,\"value\":2.0}
      ]}"
    ```

3. Grafana を docker で起動

    ```sh
    cd examples/grafana
    docker compose up -d
    ```

4. ブラウザで `http://localhost:3000` を開き、`admin` / `admin` でログイン

5. Explore で datasource `Skaldberg` (auto-provisioned) を選び、metric `cpu` 等を入れて Run query すると時系列が出る

## datasource の設定

`provisioning/datasources/skaldberg.yaml` で auto-provision:

- 名前: `Skaldberg`
- Type: `prometheus` (Grafana 組み込み、追加 plugin 不要)
- URL: `http://host.docker.internal:8080`

URL を変えたい (Skaldberg を別ホストで動かす等) は yaml 直編集 or UI 上で override。

### auth 付きの場合

サーバを `--api-token <T>` (または `SKALDBERG_API_TOKEN=T`) 付きで起動した場合は datasource 設定で次のようにヘッダを追加:

- Custom HTTP Headers
    - Header: `Authorization`
    - Value: `Bearer <T>`

provisioning yaml の `jsonData.httpHeaderName1` / `secureJsonData.httpHeaderValue1` でも指定できる (例はコメントで記載)。

## サポートしている PromQL 機能 (step 1)

| 構文 | 動作 |
|------|------|
| `metric_name` | ✅ |
| `metric_name{label="v"}` | ✅ |
| `metric_name{label!="v"}` | ✅ |
| `metric_name{label=~"v1\|v2"}` | ✅ (regex) |
| `metric_name{label!~"..."}` | ✅ |
| `metric_name[5m]` (range vector) | ✅ (range は parse して ignore、raw 点を返す) |
| `metric_name offset 1h` | parse のみ (offset まだ未適用) |
| `rate(...)`, `sum(...)`, `histogram_quantile(...)` 等の関数 | parse OK、内側の selector を実行して **raw 点を返す** (数値は本来の関数結果と異なる、step 2 で対応) |
| `__name__` matcher | `__name__="x"` のみ (= で metric 名を絞る) |

## エンドポイント

| Endpoint | 用途 |
|----------|------|
| `GET/POST /api/v1/query` | instant query |
| `GET/POST /api/v1/query_range` | range query (step は parse して ignore、raw 点) |
| `GET /api/v1/labels` | 全 label 名一覧 |
| `GET /api/v1/label/{name}/values` | 指定 label の値一覧 |
| `GET /api/v1/series` | `match[]=...` で matching series 一覧 |

## 既知の制約 (step 2 以降で対応予定)

- `rate / increase / sum() by / histogram_quantile / topk` などの関数を本来のセマンティクスで実行する (今は parse → unwrap → raw 値)
- `step` パラメータに沿った resampling (今は raw 点を返す、Grafana 側で表示時に処理)
- alerting / recording rules
- federation / exemplars / metadata API
