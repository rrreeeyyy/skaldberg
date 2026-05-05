# Grafana 連携サンプル

Skaldberg を Grafana から読む最小構成のサンプル。SimPod JSON Datasource plugin (`simpod-json-datasource`) 経由で `/api/v1/grafana/{,search,query}` を叩く。

## 用意するもの

- Skaldberg サーバ (host で `cargo run --release -- --catalog memory ...` 等)
- Docker (Grafana を立ち上げる)

## 立ち上げ手順

1. Skaldberg を host で起動

    ```sh
    # 例: in-memory catalog で 8080 で listen
    ./target/release/skaldberg-server \
      --catalog memory \
      --warehouse-uri memory:///wh \
      --wal-dir /tmp/skaldberg-wal \
      --bind 127.0.0.1:8080 \
      --flush-interval-secs 10
    ```

2. データを少し入れる (Grafana で見るための seed)

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

    plugin install 込みで初回 30 秒程度で立ち上がる

4. ブラウザで `http://localhost:3000` を開き、`admin` / `admin` でログイン

5. 左サイドバーの **Explore** で datasource `Skaldberg` を選び、metric 名 (`cpu` 等) を入れて Run query すると時系列が表示される

## datasource の設定

`provisioning/datasources/skaldberg.yaml` で auto-provision している:

- 名前: `Skaldberg`
- Type: `simpod-json-datasource`
- URL: `http://host.docker.internal:8080/api/v1/grafana`

URL を変えたい場合 (e.g. Skaldberg を別ホストで動かす) は `provisioning/datasources/skaldberg.yaml` を直接編集するか、Grafana UI から override する。

### auth 付きの場合

サーバを `--api-token <T>` (または `SKALDBERG_API_TOKEN=T`) 付きで起動した場合、Grafana の datasource 設定で次のようにヘッダを追加する:

- Custom HTTP Headers
    - Header: `Authorization`
    - Value: `Bearer <T>`

provisioning から渡したいなら `provisioning/datasources/skaldberg.yaml` の `jsonData.httpHeaderName1` / `secureJsonData.httpHeaderValue1` に書く (コメントで例示済)。

## endpoint 仕様 (Skaldberg 側)

SimPod JSON Datasource の HTTP contract に従う:

| エンドポイント | 用途 |
|----------------|------|
| `POST /api/v1/grafana/` | connection test (200 OK) |
| `POST /api/v1/grafana/search` | metric 名一覧 (`{"target": "<substring>"}` で絞り込み) |
| `POST /api/v1/grafana/query` | 時系列データ (`{range, targets, maxDataPoints}` 受け、`[{target, datapoints}]` 返す) |

target 文字列は `metric_name` (label 無し) または `metric_name{k1=v1,k2=v2}` 形式 (label sorted) で series を区別する。

## 既知の制約

- annotation / ad-hoc filter / variable は未対応 (必要になり次第拡張)
- downsampling は無し: 範囲内の生 sample をそのまま返す。`maxDataPoints` は LIMIT として効く
