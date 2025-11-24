# 纯行业事件驱动回测骨架

该骨架实现了文稿中的“行情集中度→行业内部分化→滞涨补涨”流程，完全基于 TuShare 数据接口，可直接用于
二次开发或替换为其他数据源。

## 目录结构

```
src/backtest_industry/
    __init__.py
    cli.py              # Typer 命令行入口（含单跑 & 网格敏感性）
    config.py           # 参数与默认值
    concentration.py    # 集中度与收益计算
    data.py             # TuShare 数据访问与缓存
    engine.py           # 事件循环与持仓收益评估
    selectors.py        # 行业内滞涨股筛选
requirements.txt
README.md
```

## 快速开始

1. 创建并激活 Python 3.10+ 虚拟环境。
2. 安装依赖：

   ```powershell
   pip install -r requirements.txt
   ```

3. 配置 TuShare Token（可任选其一）：
   - 设置环境变量 `TUSHARE_TOKEN`；
   - 或者在运行 CLI 时通过 `--token` 传入；
   - 若都不设置，将默认使用仓库内置的 token：`98b2900883e70c8b1e141fdb33e4a5a1123dc999d217fcd2c0ce4c89`。

4. 运行回测（首次会自动缓存行情与申万二级行业成分。行情抓取默认以 3 个月为窗口批量请求 `pro.daily`/`pro.adj_factor`；如单次响应逼近 6000 行，程序会自动查询 `trade_cal` 并退回到“逐交易日”方式继续拼接，从而覆盖完整区间。行业成分使用 TuShare 文档 [doc_id=181](https://tushare.pro/document/2?doc_id=181) 所述的「先 `index_classify(level='L2')`，再根据每个 `index_code` 逐个调用 `index_member_all(l2_code=...)`」流程，确保所有行业都确实获取到成分股）：

   ```powershell
   python -m backtest_industry.cli run --start-date 2020-01-01 --end-date 2025-08-31 --trigger-threshold 0.3
   ```

   输出示例：

   ```json
   {
     "events": 18,
     "avg_return": 0.042,
     "win_rate": 0.55
   }
   ```

5. 复现表 3 / 表 4 的参数敏感性网格：

   ```powershell
   python -m backtest_industry.cli grid --triggers 0 0.1 0.3 0.5 --laggards 0.3 0.5 0.7 --output-prefix outputs/grid
   ```

   - 控制台会打印 60 日平均收益和胜率的透视表；
   - 若传入 `--output-prefix`，会额外生成 `_returns.csv`、`_winrates.csv`、`_raw.csv` 便于复刻文稿表格。

### 数据完整性验证

若只想验证缓存是否覆盖指定区间，可执行：

```powershell
python -m backtest_industry.cli verify-data --start-date 2020-01-01 --end-date 2025-08-31
```

该命令会触发同样的分片下载逻辑（默认以 3 个月为一个 TuShare 请求窗口，可通过 `StrategyConfig.daily_chunk_months` 调整；若单窗行数 ≥ `StrategyConfig.daily_row_limit`≈5500，则自动切回逐交易日抓取），然后打印：

- 行情行数、股票数以及最早/最晚交易日；
- 申万行业映射的记录条数与行业数量；
- 实际写入的 `data_cache` 目录。

命令结束后，可用任何方式查看 `data_cache/*.parquet`（例如 README 顶部的 PowerShell 脚本）来确认缓存细节，再运行 `run`/`grid` 指令即可复用这些数据。

### 申万二级行业抓取流程

行业缓存位于 `data_cache/sw_level2_mapping.parquet`，如需单独更新，可直接运行以下伪代码所描述的两步：

1. `sw_meta = pro.index_classify(level='L2', src='SW')` —— 获取所有二级行业代码与名称；
2. `for code in sw_meta.index_code: pro.index_member_all(l2_code=code)` —— 逐行业抓取成分股，若返回字段为 `con_code` 会自动重命名为 `ts_code`，并仅保留 `is_new == 'Y'` 的记录；
3. 合并得到 `ts_code / industry_name / in_date / out_date` 四列后写入缓存。

CLI 会默认执行上述流程，无需手动干预，除非 TuShare 缓存损坏或想强制刷新。

## 模块职责

- `concentration.py`
  - `build_daily_returns`：基于复权价生成日收益；
  - `resample_weekly_returns`：从日收益平滑到周；
  - `compute_market_concentration`：市场级别的“前 30% - 中位数”集中度及其 delta；
  - `compute_industry_concentration`：行业内集中度曲线；
  - `rank_industries`：每周挑选集中度最高的若干行业。
- `selectors.py`
  - `pick_laggards`：对候选行业取收益位于后 N% 的滞涨股集合。
- `engine.py`
  - 将上面所有步骤串起来：
    1. 全市场集中度突破阈值时触发事件；
    2. 选择集中度最高的行业；
    3. 行业内挑选滞涨股并持有 60 个交易日；
    4. 输出每个事件的收益路径、涉及行业、单票收益等细项，便于制图与报告。
- `cli.py`
  - Typer 命令行：提供日期、阈值、滞涨比例等参数，支持单次回测 / 参数网格敏感性两种模式。
- `grid.py`
  - `run_parameter_grid`：以 `trigger_threshold × laggard_pct` 网格跑回测，返回收益、胜率透视表。

## 二次开发建议

- **参数网格**：可在 `engine.BacktestEngine` 外层编写简单的 for-loop，改变 `StrategyConfig` 中的
  `trigger_threshold` 和 `laggard_pct` 来复现文稿的敏感性表格。
- **输出扩展**：在 `EventResult` 中增加更详细的字段（如行业名单、单票收益等），可直接在
  `_iterate_events` 中写入 CSV 或数据库。
- **图表**：利用 `market_conc` 与 `industry_conc` DataFrame，可快速复刻图 3 / 图 7 / 图 9 等曲线。

## 测试

仓库包含使用假数据的回测单元测试（`tests/test_engine.py`）。运行：

```powershell
pytest
```

> 如果只想验证核心逻辑是否可运行，可在未安装 tushare 的环境下执行测试，它会自动使用 `MockDataProvider`。
