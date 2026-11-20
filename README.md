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
.tpm i aix s r save_sticker track yl hb hb
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

安装或覆盖更新：`.tpm i hb`。命令可以在群聊、私聊或收藏夹发送，**全局生效，不按命令所在会话限制**。

1. 发送 `.hb bind @机器人用户名`，无需回复红包或提供消息链接。
2. 发送 `.hb on` 开启，等待绑定 Bot 发新红包。
3. `.hb status` 查看、`.hb off` 全局停止、`.hb unbind` 解绑。

默认自动识别唯一的领取回调按钮；可用 `.hb bind @机器人用户名 按钮完整文字` 指定。`.hb inspect` 仅用于回复消息检查按钮，不点击。只锁定一个 Bot 的真实数字 ID，在账号能收到它直接消息的所有会话生效。管理反馈显示在命令发送位置，领取过程不发群消息，但 Bot 可能公布名单。

无固定延迟，不额外刷新消息；新建与编辑均监听。只处理开启后发送且不超过120秒的消息，不补扫历史。去重先持久化，未知结果不重试，限流暂停。只支持直接 Bot callback；URL/WebApp/支付/需密码按钮、转发和 inline 代发不支持。关键词和按钮文字不能保证 Bot 业务安全，只绑定可信 Bot。

旧版会话绑定升级后会清空绑定并关闭，保留去重和限流；请重新 bind、on。数据：`assets/hb/state.json`，仅支持单进程。离线回归已通过，未实测真实领取；不保证秒到或抢到。
