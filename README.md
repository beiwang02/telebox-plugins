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
.tpm i aix s r save_sticker track yl hb
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

安装：`.tpm i hb`。仅支持 TeleBox Classic；默认关闭，不自动绑定任何 Bot。

1. 在目标群中，回复指定 Bot 直接发出的一条**未抢完、带领取按钮**的红包，发送 `.hb bind`。
2. 检查返回的 Bot ID 和按钮，然后发送 `.hb on`，仅在当前会话启用。
3. `.hb status` 查看状态，`.hb off` 关闭，`.hb list` 查看绑定，`.hb unbind` 删除当前绑定。
4. 自动识别失败时，回复样本发送 `.hb inspect`，再执行 `.hb bind 按钮完整文字`。检查不会点击按钮，也不会输出回调数据。

收到匹配更新后无固定等待，直接使用事件中的按钮，不额外刷新消息；新建和编辑更新均支持。提交前持久化去重，Telegram 限流时暂停，超时或未知结果不自动重试。只接受启用后发送且不超过 120 秒的消息，不补扫历史。

**限制：**仅支持 Bot 直接发出的 callback 红包；URL、WebApp、支付、需密码按钮、转发、inline 代发均不自动执行。绑定范围是整个当前会话，不按论坛话题隔离。身份、关键词和按钮文字无法保证 Bot 的业务安全，只绑定可信 Bot，遵守群及 Bot 规则。网络和服务端影响速度，不保证秒到或领取成功；回调返回不等于抢到。

配置及去重数据位于 `assets/hb/state.json`，勿随意清除；仅支持单进程。源码含中文设计注释。已用 VPS 实际 teleproto 依赖运行离线回归测试，**未进行真实红包领取验收**。
