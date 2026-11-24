# Growatt Classic 会话修复

用于个人 Home Assistant 的 Growatt 自定义覆盖组件。基于 Home Assistant Core **2026.11.2**，依赖固定为 `growattServer==2.2.0`，组件版本为 `2026.11.2+classic.2`。

## 功能

- 同一服务器和账户复用认证会话，登录与设备请求通过共享锁串行执行。
- 在明确的会话失效响应后重新认证，每次数据读取最多重试一次。识别 HTTP 401、已知登录重定向、登录表单以及 `/error.do` 的明确 `dumpLogin` 页面。
- 业务错误 507 后保存 **24 小时 UTC 冷却截止时间**，跨 HA 重启保留。存储只含散列账户标识与截止时间，不包含密码。这个窗口是客户端停止登录请求的冷却，不是云端账户重置接口。
- Classic 自动轮询和定时设备扫描在**日出前 30 分钟至日落后 30 分钟**运行，按 HA 配置的位置及当地时区计算。无法计算太阳时间时回退到 **06:30–20:30**。
- 夜间保留最后读数与可用状态。HA 启动时的首次采集和手动操作沿用原路径，并遵守已有冷却。Token API 不使用 Classic 会话和夜间暂停逻辑。
- 保持原配置条目、实体身份和统计定义。

## 安装

仅按此版本在 HA Core 2026.11.2 上使用。升级 HA 前应重新比较官方 Growatt 组件及依赖。

1. 备份 HA，尤其是 Recorder 数据库、配置条目、实体注册表和 Energy 配置。
2. 如果已有 `/config/custom_components/growatt_server`，先保留其副本。
3. 将本仓库的 `custom_components/growatt_server` 完整复制到 HA 的 `/config/custom_components/growatt_server`，包括翻译目录。
4. 重启 HA，确认加载 `2026.11.2+classic.2`。不要删除并重建原 Growatt 配置条目。
5. 核对原实体与 Energy 来源，确认原统计继续写入。若出现 507，遵守日志中的冷却截止时间。

回滚时移走新增覆盖目录并重启。内置组件不会读取此版本的冷却文件，仍处于冷却时应先停止其轮询。保留原配置和 Recorder 数据。

## 测试

使用 Python 3.14：

```sh
uv venv --python 3.14
uv pip install -r requirements-test.txt
.venv/bin/python -m pytest -q
.venv/bin/ruff check --isolated --target-version py314 --select E4,E7,E9,F custom_components/growatt_server tests
```

35 项测试覆盖会话复用、并发认证恢复、持久化冷却、取消请求后的冷却写入、真实 Requests 重定向与响应钩子、实体统计身份和太阳轮询窗口。测试中的账户、设备、位置和读数均为示例；测试默认拒绝真实云端请求。

实际运行已观察到一次 HA 连续运行期间的跨夜自动采集恢复与统计续写。这只是一轮运行验证，未直接记录实时登录次数，也不保证以后不会再出现 507。

## 使用限制

- 夜间暂停会使电池、电网功率等实时数值停在上一笔读数。需要持续观察储能或夜间功率时，应调整这一策略。
- 仅对明确的失效页面恢复认证；其他非 JSON 响应、HTTP 403 和网络错误不会主动再次登录。
- 冷却依赖准确的系统时间及 HA 存储写入成功。
- 同域名覆盖组件会替换内置版本，HA 升级后需重新验证兼容性。

此仓库只包含通用代码、模拟测试与使用说明。生产配置、凭据、设备标识、日志、备份及私人核查记录不在仓库内。

## 许可证

代码使用 Apache License 2.0。原始组件来源及修改说明见 [NOTICE.md](NOTICE.md)，许可全文见 [LICENSE.md](LICENSE.md)。
