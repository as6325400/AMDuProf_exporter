# AMD uProf Memory-Bandwidth Exporter

A Prometheus exporter that wraps AMD uProf's `AMDuProfPcm` tool and exposes its
memory-bandwidth metrics on `/metrics` so they can be scraped into Grafana.

Built to mirror [`nvml_exporter`](https://github.com/as6325400/nvml_exporter):
same project layout, same systemd / CI / Grafana conventions — but the data
source is AMD's CPU performance-counter monitor instead of NVML.

目標:把公司要求的這條指令的 metric 放上 Grafana —

```
/opt/AMDuProf_5.3-518/bin/AMDuProfPcm -m memory --msr --verbose -a -t 1000
```

## How it works

`AMDuProfPcm` 和 NVML 不一樣 — 它不是「每次 query 回一次」,而是**持續 streaming CSV**:
啟動後每隔一個 sample interval 就往 stdout 印一列數值。所以 exporter 的架構是:

1. 把那條 `AMDuProfPcm` 指令當成**長駐 child process** 啟動一次
2. 一個 reader thread 一行一行讀它的 stdout,丟給 `MemoryReportParser`
3. 每讀到一列新的 sample 就更新「最新 snapshot」
4. Prometheus 來 scrape `/metrics` 時,直接回最新 snapshot(不會每次 scrape 都重跑工具)
5. child process 掛掉會自動 backoff 重啟;太久沒新資料時 `amd_uprof_up` 變 0


- **CSV(逗號分隔)**,不是空白對齊的表格
- **欄位 = metric**(`Total Mem Bw`、`Local DRAM Read` …),**每一列 = 一個時間取樣**
- `-a` 只會給一個 `System (Aggregated)` 的彙總(整機),所以 scope 都是 `system`。
  若改用 `-A package`,parser 會把每個 `Package N` 當成不同 scope。
- header 只印一次,之後 data 列**連續 streaming、中間沒有空行**
- 雖然下了 `-t 1000`,工具可能自己把 sample interval 拉到 ~4000ms(輸出會講)

`read` / `write` / `total` 三個總頻寬會額外被收斂成一個乾淨的 canonical metric
`amd_uprof_mem_bandwidth_gbps{scope,kind}`,Grafana dashboard 用這個。

## Metrics

### Canonical 記憶體頻寬(dashboard 用)— labels: `scope`, `kind`
| Metric | 來源欄位 |
| --- | --- |
| `amd_uprof_mem_bandwidth_gbps{kind="total"}` | `Total Mem Bw (GB/s)` |
| `amd_uprof_mem_bandwidth_gbps{kind="read"}`  | `Total Mem RdBw (GB/s)` |
| `amd_uprof_mem_bandwidth_gbps{kind="write"}` | `Total Mem WrBw (GB/s)` |

### 通用 metric — label: `scope`
每個 CSV 欄位都會變成一個 `amd_uprof_<sanitized>[_<unit>]`:

| Metric | 來源欄位 |
| --- | --- |
| `amd_uprof_total_mem_bw_gbps` | Total Mem Bw (GB/s) |
| `amd_uprof_total_mem_rdbw_gbps` | Total Mem RdBw (GB/s) |
| `amd_uprof_total_mem_wrbw_gbps` | Total Mem WrBw (GB/s) |
| `amd_uprof_local_dram_read_data_bytes_gbps` | Local DRAM Read Data Bytes (GB/s) |
| `amd_uprof_local_dram_write_data_bytes_gbps` | Local DRAM Write Data Bytes (GB/s) |
| `amd_uprof_remote_dram_read_data_bytes_gbps` | Remote DRAM Read Data Bytes (GB/s) |
| `amd_uprof_remote_dram_write_data_bytes_gbps` | Remote DRAM Write Data Bytes (GB/s) |

> 預設只輸出記憶體 / DataFabric 相關欄位(名稱含 `mem`/`bw`/`bandwidth`/`dram`/`df`/`umc`)。
> 想把**所有**欄位都輸出,加 `--all-metrics`。

### Health
| Metric | Description |
| --- | --- |
| `amd_uprof_up` | 1 = 工具在跑**且**資料新鮮,否則 0 |
| `amd_uprof_process_running` | child process 現在還活著 = 1 |
| `amd_uprof_snapshot_age_seconds` | 距離上次成功解析 sample 的秒數 |
| `amd_uprof_process_restarts_total` | child process 重啟次數 |
| `amd_uprof_parse_errors_total` | 解析失敗的行數 |

## Install

需要:Python 3.10+、已安裝的 AMD uProf(提供 `AMDuProfPcm`)。`--msr` 需要 root +
`/dev/cpu/*/msr`(msr kernel module 通常會自動載入)。

```bash
sudo git clone https://github.com/as6325400/AMDuProf_exporter.git /opt/AMDuProf_exporter
cd /opt/AMDuProf_exporter
sudo python3 -m venv .venv
sudo .venv/bin/pip install -r requirements.txt
sudo cp systemd/amduprof-exporter.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now amduprof-exporter
```

驗證:

```bash
sudo systemctl status amduprof-exporter --no-pager
curl -s localhost:9836/metrics | grep amd_uprof_mem_bandwidth_gbps
```

### 先在前景測一下(不裝 systemd)

```bash
sudo .venv/bin/python /opt/AMDuProf_exporter/amduprof_exporter.py --port 9836
# 另一個 shell
curl -s localhost:9836/metrics | grep amd_uprof_
```

CLI flags:
`--port` (預設 9836)、`--addr` (預設 0.0.0.0)、`--amduprof-bin` (預設
`/opt/AMDuProf_5.3-518/bin/AMDuProfPcm`,也可用 `AMDUPROF_BIN` 環境變數)、
`--metric` (預設 memory)、`--interval`(`-t` 毫秒,預設 1000)、`--no-msr`、
`--no-verbose-tool`、`--extra-args "..."`、`--all-metrics`、`--staleness`(預設 15 秒)、
`--log-level`。

## 抓 per-socket(每顆 CPU)數字

公司那條指令用 `-a`,只給整機彙總(`scope="system"`)。想看每個 socket:把
exporter 的 AMDuProfPcm 參數換成 `-A package`(用 `--extra-args` 或改 systemd unit),
parser 會自動把 `Package 0` / `Package 1` 變成 `scope="package0"` / `scope="package1"`。

## Re-tuning the parser(換機器 / 換版本時)

解析器已對 EPYC 9374F + uProf 5.3.518 校準好,但換 Zen 世代 / uProf 版本格式可能小變。
驗證方式:

```bash
# 抓真實輸出(跑幾秒後 Ctrl-C)
sudo /opt/AMDuProf_exporter/.venv/bin/python \
  /opt/AMDuProf_exporter/amduprof_exporter.py --dump-raw > /tmp/real.txt

# 看 parser 抓到什麼
/opt/AMDuProf_exporter/.venv/bin/python \
  /opt/AMDuProf_exporter/amduprof_exporter.py --parse-file /tmp/real.txt
```

`python3 test_parser.py` 是對真實樣本的自我測試,CI 也會跑。`MemoryReportParser`
是獨立的一個 class,要調整就改它。

## Prometheus scrape config

工具大約每 4 秒才出一筆,scrape interval 設 5s 即可:

```yaml
scrape_configs:
  - job_name: amduprof
    scrape_interval: 5s
    static_configs:
      - targets: ['amd-host-1:9836']
```

實用 PromQL:
```promql
# 整台機器的總記憶體頻寬
sum(amd_uprof_mem_bandwidth_gbps{kind="total"})

# 讀 / 寫拆開
amd_uprof_mem_bandwidth_gbps{kind=~"read|write"}

# 跨 NUMA 的 remote DRAM 流量(高 = placement 不理想)
amd_uprof_remote_dram_read_data_bytes_gbps + amd_uprof_remote_dram_write_data_bytes_gbps

# 哪台機器記憶體頻寬最高
topk(5, sum by (instance) (amd_uprof_mem_bandwidth_gbps{kind="total"}))
```

## Grafana

兩份 dashboard,搭配 drill-down 一起用(多台機器用):

- **`grafana/cluster.json`** — cluster overview。一張表每台 host 一列
  (Total BW / Read BW / Write BW / Remote DRAM / Up),**點 Host 那欄就跳到該機器
  的詳細 dashboard**;上方還有 cluster 級的 stat 與「by host」時序圖。
- **`grafana/host.json`** — 單一 host 的詳細頁(drill-down 的目標)。有**二級下拉**:
  `Host`(第一級)→ `Scope`(第二級,跟著 Host 變,可多選 / All)。用 `-a` 時
  Scope 只有 `system`;用 `-A package` 時會有 `package0` / `package1` 可切。內容:
  上方 stat、總頻寬與 read/write 時序、Local vs Remote DRAM 堆疊圖、exporter 健康。

匯入順序:**先匯 `host.json`,再匯 `cluster.json`**(cluster 的 Host 連結指到
host dashboard 的 uid `amduprof-host-memory`,先存在連結才有效)。
Grafana UI → Dashboards → New → Import → 選檔案 → 指到你的 Prometheus datasource。

## Scope / 不做什麼

- **Per-process / per-core 歸戶** — `-m memory` 是 DataFabric/UMC 層級的頻寬,
  本來就沒有 per-process 資訊。要 per-process 請改用 uProf 的 profiling 模式。
- **其他 metric group**(`ipc` / `l3` / `xgmi` / `pcie` …)— parser 是通用的,
  `--metric xgmi --all-metrics` 理論上也能出東西,但 dashboard 只針對 memory 調過。
- **TLS / auth** — 內網 scrape 不需要,要的話前面擺 reverse proxy。
- **WSL / 非 AMD 機器** — `AMDuProfPcm` 只在實體 AMD CPU + 對應 driver 上有效。
