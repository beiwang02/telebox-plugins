# TeleBox 插件

个人使用的 TeleBox Classic 插件，适用于 Classic 0.2.9，非 Next 版。

## 插件

| 名称 | 功能 |
| --- | --- |
| `aix` | AI 对话、搜索、图片与视频生成 |
| `s` | 消息总结、定时任务 |
| `r` | 复读消息 |
| `save_sticker` | 保存贴纸，处理超长视频贴纸 |
| `track` | 按用户和关键词自动回复 |
| `yl` | 生成语录贴纸 |
| `hb` | 绑定指定 Bot，自动领取回调红包 |
| `hb` | 绑定指定 Bot，自动领取回调红包 |
| `hb` | 绑定指定 Bot，自动领取回调红包 |
| `hb` | 绑定指定 Bot，自动领取回调红包 |
| `hb` | 绑定指定 Bot，自动领取回调红包 |
| `hb` | 绑定指定 Bot，自动领取回调红包 |
| `hb` | 绑定指定 Bot，自动领取回调红包 |
| `hb` | 绑定指定 Bot，自动领取回调红包 |

## 订阅安装

在 Telegram 中使用运行 TeleBox 的账号发送：

```text
.tpm source add https://github.com/beiwang02/telebox-plugins
```

按需安装，将 `track` 换成其他插件名即可：

```text
.tpm i track
```

安装本仓库全部插件：

```text
.tpm i aix s r save_sticker track yl hb hb hb hb hb hb hb hb
```

手动更新已安装的远程插件：

```text
.tpm update
```

> 默认命令前缀为 `.`。添加源会替换已有自定义源；安装会替换同名插件。不要用 `i all`，它还会安装官方源插件。`update` 会检查所有已安装的远程插件，不限本仓库。

## 说明

- 需要 TeleBox Classic 环境，相关 API、依赖和配置需自行准备；其他版本不保证兼容。
- 仅收录插件源码，不含账号配置和运行数据，也不会自动同步 VPS 修改。
- 代码包含个人修改及上游代码，保留原有声明，使用与分发请遵循对应许可。上游：[TeleBoxOrg](https://github.com/TeleBoxOrg)。

## 定向抢红包 hb

安装/更新：`.tpm i hb`。群聊、私聊、收藏夹均可管理，全局生效。

| 指令 | 作用 |
| --- | --- |
| `.hb bind @bot` | 添加机器人并立即开启；不删除其它绑定 |
| `.hb list` | 查看编号、开关及最近回调 |
| `.hb off [编号]` | 暂停指定绑定，保留配置 |
| `.hb on [编号]` | 恢复指定绑定 |
| `.hb del [编号]` | 删除指定绑定 |
| `.hb status` | 查看状态及最近回调耗时，不是完整历史 |

编号以最新 list 为准；on/off/del 不带编号操作全部。更新保留现有开关，不会擅自恢复已暂停绑定。

平时监听新建/编辑推送，/red或绑定Bot准备提示触发同群8秒加速窗口，账号共享500ms查询预算，限流暂停。支持可信准备提示后15秒内空正文图片的唯一普通回调，不依赖祝福语。非图片、多按钮、无上下文不走此路径；不执行URL/WebApp/需密码按钮或inline代发。

数据在 assets/hb/state.json，单进程使用。领取不发群消息，Bot可能公布名单。最近回调只保存在内存，重载重置；RPC返回不等于到账，也不保证秒抢。
