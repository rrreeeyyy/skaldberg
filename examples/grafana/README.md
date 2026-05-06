# Grafana 連携サンプル (dogfood)

Skaldberg が公開する Prometheus HTTP API subset (`/api/v1/query`,
`/api/v1/query_range`, `/api/v1/labels`, `/api/v1/label/<n>/values`,
`/api/v1/series`) を Grafana 組み込みの **Prometheus datasource** から
そのまま叩く構成。datasource とダッシュボードは provisioning で
auto-loaded、起動するだけで動くサンプルダッシュボードが見える。

## 用意するもの

- Skaldberg サーバ (host で `cargo run --release ...` 等)
- Docker (Grafana を立ち上げる)
- Python 3 (synthetic emitter `scripts/emit_demo_metrics.py` 用)

## 立ち上げ手順

1. **Skaldberg を host で起動** (in-memory dev catalog)

   ```sh
   ./target/release/skaldberg-server \
     --wal-dir /tmp/skaldberg-wal \
     --bind 127.0.0.1:8080 \
     --flush-interval-secs 5
   ```

   実 S3 Tables に向ける場合はトップ README の "Build & run" 参照。

2. **Synthetic metric emitter を別端末で回す**

   ```sh
   ./scripts/emit_demo_metrics.py
   ```

   5 秒おきに 3 jobs × 数 routes 分の counter / histogram / gauge を
   `/api/v1/ingest` に投げる (`SKALDBERG_API_TOKEN=...` で bearer 付与可)。

3. **Grafana を docker で起動**

   ```sh
   cd examples/grafana
   docker compose up -d
   ```

4. ブラウザで `http://localhost:3000` (`admin` / `admin`) を開くと
   datasource `Skaldberg` と dashboard "Skaldberg demo" が auto-provision
   済み。Dashboard を開けば 4 パネル (リクエストレート / エラーレート /
   p95 レイテンシ / inflight) が描画される。

5. emitter を止める時は Ctrl-C。WAL は `/tmp/skaldberg-wal` に残るので
   サーバ再起動でもデータは引き継がれる。

## 描画されるパネル

| パネル                          | PromQL                                                       | Phase 8 のどの pushdown を使うか                |
|---------------------------------|--------------------------------------------------------------|------------------------------------------------|
| Request rate by job             | `sum(rate(app_requests_total[1m])) by (job)`                 | `<agg>(rate(...))` 二段                         |
| Error rate by route (status=500)| `sum(rate(app_requests_total{status="500"}[1m])) by (route)` | label `=` pushdown + `<agg>(rate(...))`         |
| p95 latency by (job, route)     | `histogram_quantile(0.95, rate(app_latency_seconds_bucket[5m]))` | `histogram_quantile(q, rate(bucket[r]))`     |
| Inflight by job                 | `app_inflight`                                                | bare selector                                   |

panel の生 query は dashboard JSON 直編集 or UI 上で書き換え可。

## datasource の設定

`provisioning/datasources/skaldberg.yaml` で auto-provision:

- 名前: `Skaldberg`
- uid: `skaldberg` (dashboard panel から参照される)
- Type: `prometheus` (Grafana 組み込み、追加 plugin 不要)
- URL: `http://host.docker.internal:8080`

URL を変えたい (Skaldberg を別ホストで動かす等) は yaml 直編集 or UI 上で
override。

### auth 付きの場合

サーバを `--api-token <T>` (または `SKALDBERG_API_TOKEN=T`) で起動した場合
は datasource 側に Authorization ヘッダを追加:

- Custom HTTP Headers
    - Header: `Authorization`
    - Value: `Bearer <T>`

provisioning yaml の `jsonData.httpHeaderName1` / `secureJsonData.httpHeaderValue1`
でも指定できる。

## サポートしている PromQL 機能

ダッシュボードが踏むパネルは全て Phase 8 で SQL pushdown 済み。詳細な
カバレッジはトップ README の "PromQL pushdown" セクション参照。

## 既知の制約

- `on (...)` / `ignoring (...)` / `group_left` / `group_right` の
  ラベルマッチ修飾子は Rust fallback のまま (panel で使うと動くが SQL
  push されない、性能上の差が出るかは規模次第)
- 規模感: emitter は ~5 sps × 数十 series 程度の合成データ。実本番ボリュームは
  Phase 9 で実 Prometheus 接続に切り替えてからの観察対象
