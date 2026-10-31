# TeleBox 本地插件

从个人 CCS VPS 的 TeleBox Classic 现用目录导出的本地插件源码快照。

## 插件清单

| 文件 | 简介 |
| --- | --- |
| `plugins/aix.ts` | AI 对话、搜索及图片/视频生成 |
| `plugins/s.ts` | 消息总结及定时任务 |
| `plugins/r.ts` | 复读消息 |
| `plugins/save_sticker.ts` | 保存贴纸，含超长视频贴纸处理 |
| `plugins/track.ts` | 按目标用户及关键词自动回复，支持媒体及冷却 |
| `plugins/yl.ts` | 生成文字语录贴纸 |

## TPM 订阅（推荐）

在运行 TeleBox 的 Telegram 账号中发送（以默认 `.` 前缀为例）：

```text
.tpm source add https://github.com/beiwang02/telebox-plugins
```

查看自定义源与插件列表：

```text
.tpm source
.tpm ls
```

按需安装，或安装本仓库这六个插件：

```text
.tpm i track
.tpm i aix s r save_sticker track yl
```

安装会替换同名插件文件，请先确认目标环境兼容。不要为了只安装本仓库而用 `tpm i all`：TPM 会合并官方源与自定义源，`all` 包含官方源。

后续手动更新已安装的远程插件：

```text
.tpm update
```

`update` 涉及所有已安装远程插件，不限本仓库。不要随意加 `-f`，以免覆盖本地修改。

移除自定义源（不会卸载已安装插件）：

```text
.tpm source remove
```

添加源不是自动更新定时任务；VPS 修改也不会自动上传 GitHub。当前框架仅支持一个自定义源，添加本仓库会替换已有自定义源。同名插件优先使用自定义源。

## 使用说明

- 适用来源环境：TeleBox Classic 0.2.9（teleproto），不是 TeleBox Next。
- 这是插件仓库，不是独立应用；需要已有可用的 TeleBox Classic 环境和相应依赖。
- 下载所需 `.ts` 文件，放入 TeleBox 的 `plugins/` 目录，按框架的插件加载方式加载。
- 插件引用框架的 `@utils/*` 工具；其他 Classic 版本可能缺少本地环境里的工具函数，不能保证直接兼容。
- AI 服务、语录后端、ffmpeg 等外部依赖和配置仍需在自己的运行环境准备。
- 本次仅导出源码，没有更改源码，也没有重新执行功能测试。

## 收录范围与隐私

本次收录的是未在当前 TPM 远程插件记录中登记的 6 个顶层 `.ts` 插件；21 个已登记远程插件不重复收录。未收录禁用插件、备份及未被这些插件引用的旧 `quote/` 目录。

不包含 `config.json`、`.env`、Telegram 登录会话、API 密钥、`assets/` 运行数据、数据库、缓存或日志。发布前做了常见凭据模式扫描，未发现命中；这不构成完整安全审计。

`SHA256SUMS` 记录插件源码校验值，导出时间为 2026-10-31。本仓库是一次性快照，不会自动跟随 VPS 更新。

## 来源与许可

插件可能包含基于 [TeleBoxOrg](https://github.com/TeleBoxOrg) 及第三方项目的代码和个人修改。保留源码原有声明；本仓库不将所有代码声明为原创，也不擅自统一更改第三方许可。再分发或商用前请自行核对对应上游许可。
