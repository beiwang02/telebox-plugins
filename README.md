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
.tpm i aix s r save_sticker track yl
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
