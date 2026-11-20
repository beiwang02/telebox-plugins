import { Api } from "teleproto";
import { Plugin, PluginRuntimeContext } from "@utils/pluginBase";
import { getPrefixes } from "@utils/pluginManager";
import { getGlobalClient } from "@utils/runtimeManager";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { randomUUID } from "node:crypto";

/**
 * hb — TeleBox Classic 0.2.9 定向抢红包插件。
 *
 * 使用：在任意聊天执行 `.hb bind @bot`，再执行 `.hb on`。
 * 按真实 Bot ID 全局匹配，不受管理命令发送位置影响；数据在 assets/hb/state.json。
 *
 * 安全边界：只提交明确选定的 callback，不打开链接、WebApp、支付或授权页面。
 * Bot 身份、红包关键词及按钮文字只是触发条件，不能证明 Bot 的业务行为安全。
 * 不承诺抢到，也不将 RPC 返回当成领取成功；结果不确定时宁可漏领，不重复提交。
 * 运行前应遵守所在群及 Bot 的使用规则。仅支持单进程运行，不支持共享状态多实例。
 */
type Rule = { bot: string; label: string; buttons: string[]; enabled: boolean; since: number; owner: string };
type State = { version: 1 | 2; rules: Record<string, Rule>; seen: Record<string, number>; pausedUntil: number };
type Button = { text: string; kind: string; data?: Buffer };
const root = path.resolve("assets/hb");
const esc = (v: string) => v.replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]!));
const packet = (s: string) => /红包|紅包/.test(s);
const ended = (s: string) => /已.{0,8}(抢光|搶光|领完|領完|领取完|領取完|过期|過期)|红包.{0,8}(结束|結束|失效)/.test(s);
const claimLabel = (s: string) => /领取|領取|抢红包|搶紅包|拆红包|拆紅包|打开红包|打開紅包|开红包|開紅包|^\s*[🧧🎁\s]*(抢|搶|拆|开|開)[🧧🎁\s]*$/.test(s);
/**
 * 统一新旧 TL 按钮结构：layer 229 将类型与 data 放在 button.type，旧层直接放在按钮上。
 * 必须先核对类型白名单，再读取原始字节；有 data 字段不代表它就是回调按钮。
 * 需要密码的回调也拒绝，避免自动进入授权流程。返回值只在内存使用，不打印 data。
 */
export function buttonsOf(msg: Api.Message): Button[] {
  const rows = (msg.replyMarkup as any)?.rows;
  if (!Array.isArray(rows)) return [];
  const result: Button[] = [];
  for (const row of rows) for (const b of row.buttons || []) {
    const t = b.type || b;
    const kind = t.className || t.constructor?.name || "unknown";
    const callback = kind === "InlineButtonTypeCallback" || kind === "KeyboardButtonCallback";
    result.push({ text: String(b.text || ""), kind, data: callback && (Buffer.isBuffer(t.data) || t.data instanceof Uint8Array) && !t.requiresPassword ? Buffer.from(t.data) : undefined });
  }
  return result;
}
const help = () => {
  const c = esc(`${getPrefixes()[0] || "."}hb`);
  return `🧧 <b>全局定向抢红包</b>\n\n<code>${c} bind @bot</code> — 绑定 Bot，无需回复红包\n<code>${c} bind @bot 按钮完整文字</code> — 可选指定按钮\n<code>${c} on</code> / <code>${c} off</code> — 全局开关\n<code>${c} status</code> / <code>${c} list</code> — 状态\n<code>${c} unbind</code> — 解绑\n<code>${c} inspect</code> — 回复消息检查按钮（不点击）\n\n群聊、私聊和收藏夹都可管理。只按真实 Bot ID 匹配所有会话，不按命令位置限定。默认关闭；只领取正文含红包且只有一个明确领取回调的消息，不执行 URL/WebApp、转发、inline 代发。开启后等新红包，不补扫历史。领取过程不发群消息，Bot 可能公布名单。`;
};

export class HbPlugin extends Plugin {
  name = "hb";
  description = help;
  listenMessageHandlerIgnoreEdited = false;
  private state: State = { version: 2, rules: {}, seen: {}, pausedUntil: 0 };
  private tail: Promise<unknown> = Promise.resolve();
  private signal?: AbortSignal;
  private active = false;
  private queued = 0;
  // 不设人为延迟；Telegram 返回限流后才按服务端要求暂停。
  private recent = "尚未提交回调";
  private owner = "";
  private epoch = 0;
  private live() { return this.active && !this.signal?.aborted; }
  /** 所有状态变更与领取串行执行，避免新消息和编辑事件同时通过去重检查。 */
  private serial<T>(fn: () => Promise<T>): Promise<T> {
    const result = this.tail.then(fn); this.tail = result.catch(() => undefined); return result;
  }
  /** 热重载时恢复配置；文件损坏必须报错停用，不能悄悄清空去重记录。 */
  async setup(ctx: PluginRuntimeContext) {
    this.signal = ctx.signal;
    const client = await getGlobalClient();
    if (!client) throw Error("hb 无法获取当前账号");
    this.owner = (await client.getMe()).id.toString();
    await fs.mkdir(root, { recursive: true, mode: 0o700 });
    try {
      const s: State = JSON.parse(await fs.readFile(path.join(root, "state.json"), "utf8"));
      if (![1, 2].includes(s.version) || !s.rules || Array.isArray(s.rules) || !s.seen || Array.isArray(s.seen) || !Number.isFinite(s.pausedUntil)) throw Error("invalid state");
      for (const [id, r] of Object.entries(s.rules)) if (!/^-?\d+$/.test(id) || !r || !/^\d+$/.test(r.bot) || typeof r.label !== "string" || typeof r.enabled !== "boolean" || !Number.isFinite(r.since) || !/^\d+$/.test(r.owner) || !Array.isArray(r.buttons) || r.buttons.some(x => typeof x !== "string" || !x || x.length > 256)) throw Error("invalid rule");
      for (const [k, v] of Object.entries(s.seen)) if (!/^-?\d+:\d+$/.test(k) || !Number.isFinite(v)) throw Error("invalid dedup record");
      // 旧规则仅授权单个会话，不得升级后自动扩展到所有会话；清除绑定但保留去重与限流。
      if (s.version === 1) { s.version = 2; s.rules = {}; await this.save(s); }
      else this.state = s;
    } catch (e) { if ((e as NodeJS.ErrnoException).code !== "ENOENT") throw Error("hb 状态文件无效，拒绝覆盖"); }
    this.active = true;
  }
  async cleanup() { this.active = false; await this.tail; }
  /** 同目录临时文件 + rename 原子替换；写入失败时绝不继续提交领取回调。 */
  private async save(next: State) {
    const tmp = path.join(root, `.${randomUUID()}.tmp`);
    try { await fs.writeFile(tmp, JSON.stringify(next, null, 2), { mode: 0o600, flag: "wx" }); await fs.rename(tmp, path.join(root, "state.json")); this.state = next; }
    finally { await fs.unlink(tmp).catch(() => undefined); }
  }
  private copy(): State { return JSON.parse(JSON.stringify(this.state)); }
  /**
   * 共同入口条件：当前账号、绑定的真实 Bot 发送者、启用时间、消息年龄及正文。
   * viaBotId 是 inline 内容提供者，不是真实发送者，不能用它替代 senderId 校验。
   * 转发及 inline 代发一律跳过；年龄按原始发送时间计算，不因编辑而重新变年轻。
   */
  private matches(msg: Api.Message): Rule | undefined {
    if (!this.live() || msg.out || msg.fwdFrom || msg.viaBotId || !msg.chatId || !msg.senderId || !(msg.fromId instanceof Api.PeerUser)) return;
    const r = this.state.rules[msg.senderId.toString()];
    const age = Date.now() / 1000 - Number(msg.date);
    if (!r?.enabled || !this.owner || r.owner !== this.owner || Number(msg.date) < r.since || r.bot !== msg.senderId.toString() || !Number.isFinite(age) || age < -10 || age > 120 || !packet(msg.message || "") || ended(msg.message || "")) return;
    return r;
  }
  /** 新建与编辑走相同流水线；队列最多保留 20 个任务，积压时丢弃而非突发补抢。 */
  listenMessageHandler = async (msg: Api.Message) => {
    if (!this.matches(msg) || this.queued >= 20) return;
    this.queued++;
    const epoch = this.epoch;
    try { await this.serial(async () => { if (epoch === this.epoch) await this.receive(msg); }); }
    catch { this.recent = "本地存储或处理异常，未自动重试"; console.error("[hb] processing-failed"); }
    finally { this.queued--; }
  };
  private async receive(msg: Api.Message) {
    const epoch = this.epoch;
    const key = `${msg.chatId}:${msg.id}`;
    if (!this.matches(msg) || this.state.seen[key] || Date.now() < this.state.pausedUntil || !msg.client) return;
    // 快速路径：新建/编辑事件已有完整身份和键盘，直接使用，不额外 getMessages。
    // 取舍：红包可能在事件到达后已结束，交给 Bot 回调处理；不为刷新再付一次网络 RTT。
    // 缺失身份或键盘的残缺更新直接跳过，不根据猜测提交。
    const fresh = msg;
    const r = this.matches(fresh);
    if (!r || this.state.seen[key]) return;
    const choices = buttonsOf(fresh).filter(b => b.data?.length && (r.buttons.length ? r.buttons.includes(b.text) : claimLabel(b.text)));
    if (choices.length !== 1 || !this.live() || epoch !== this.epoch) return;
    const next = this.copy(); const now = Date.now();
    next.seen = Object.fromEntries(Object.entries(next.seen).filter(([, ts]) => now - ts < 7 * 86400000));
    // 不为腾空间驱逐近期记录；达到上限则停止提交，避免同一红包重新触发。
    if (Object.keys(next.seen).length >= 10000) { this.recent = "去重记录已达上限，暂停提交"; return; }
    next.seen[key] = now;
    // 去重键不含按钮 data 或编辑版本，防止 Bot 改按钮后重复领取。
    // 先持久化占位再发 RPC：崩溃可能造成漏领，但不会因重启主动重放未知结果。
    await this.save(next);
    if (!this.live() || !this.matches(fresh) || epoch !== this.epoch) return;
    const submittedAt = Date.now();
    try {
      await msg.client.invoke(new Api.messages.GetBotCallbackAnswer({ peer: fresh.peerId, msgId: fresh.id, data: choices[0].data! }));
      this.recent = `回调已返回，RPC 耗时 ${Date.now() - submittedAt}ms（不代表红包领取成功）`;
    } catch (e) {
      const err = e as { seconds?: number; errorMessage?: string; message?: string };
      const label = String(err.errorMessage || err.message || "");
      const wait = Number(err.seconds) || Number(label.match(/(?:FLOOD|SLOWMODE)_WAIT[_ ](\d+)/)?.[1]);
      if (wait > 0 && Number.isFinite(wait)) {
        const paused = this.copy(); paused.pausedUntil = Date.now() + Math.min(wait + 1, 365 * 86400) * 1000;
        // 限流作用于整个账号的本插件；即使落盘失败，当前进程仍保留暂停状态。
        this.state.pausedUntil = paused.pausedUntil;
        await this.save(paused);
        this.recent = "Telegram 限流，已暂停；该红包不重试";
      } else this.recent = "回调失败或结果未知，已去重，不自动重试";
    }
  }
  /** 管理命令在群聊、私聊、收藏夹均可执行；只有账号本人有权修改全局绑定。 */
  cmdHandlers = { hb: async (msg: Api.Message) => {
    const me = await msg.client?.getMe();
    if (!me || !msg.out || msg.senderId?.toString() !== me.id.toString() || !this.live()) return;
    const args = (msg.message || "").trim().split(/\s+/).slice(1);
    const command = args.shift() || "help";
    if (["bind", "off", "unbind"].includes(command)) this.epoch++;
    await this.serial(async () => {
      const respond = (text: string) => msg.edit({ text, parseMode: "html" });
      if (command === "bind") {
        const username = args.shift();
        if (!username || !/^@?[a-zA-Z][a-zA-Z0-9_]{3,31}$/.test(username)) { await respond("用法：<code>hb bind @机器人用户名</code>，无需回复红包；群聊、私聊和收藏夹均可操作。"); return; }
        let bot;
        try { bot = await msg.client!.getEntity(username.startsWith("@") ? username : `@${username}`); }
        catch { await respond("无法解析这个用户名，原绑定未改变。请确认 Bot 用户名有效。"); return; }
        if (!(bot instanceof Api.User) || !bot.bot) { await respond("目标不是 Bot，原绑定未改变。"); return; }
        const next = this.copy();
        // 单 Bot 模式：重绑不继承旧的开启状态，也不清除历史去重。
        next.rules = { [bot.id.toString()]: { bot: bot.id.toString(), label: bot.username ? `@${bot.username}` : (bot.firstName || "Bot"), buttons: args.length ? [args.join(" ")] : [], enabled: false, since: 0, owner: me.id.toString() } };
        await this.save(next);
        await respond(`已绑定 <b>${esc(next.rules[bot.id.toString()].label)}</b>\nBot ID：<code>${bot.id}</code>\n范围：账号能收到该 Bot 直接消息的所有会话\n状态：关闭。发送 <code>${esc(getPrefixes()[0] || ".")}hb on</code> 开启。\n领取时不发送群消息；Bot 可能公布领取名单。`); return;
      }
      if (command === "inspect") {
        const target = await msg.getReplyMessage();
        if (!(target instanceof Api.Message)) { await respond("inspect 需要回复一条消息；bind 不需要回复，直接 hb bind @bot。"); return; }
        await respond(buttonsOf(target).slice(0, 20).map(b => `${esc(b.text.slice(0, 100))} — ${esc(b.kind)}（${b.data ? "回调" : "不执行"}）`).join("\n") || "没有 inline 按钮；未执行任何点击。"); return;
      }
      const entries = Object.entries(this.state.rules).filter(([, r]) => r.owner === me.id.toString());
      if (["status", "list"].includes(command)) {
        await respond(`🧧 <b>全局 Bot 绑定</b>\n${entries.map(([id, r]) => `${esc(r.label)} · <code>${id}</code> · ${r.enabled ? "开启" : "关闭"}`).join("\n") || "未绑定；发送 hb bind @bot"}\n范围：所有能收到该 Bot 消息的会话\n限流暂停：${Math.max(0, Math.ceil((this.state.pausedUntil - Date.now()) / 1000))} 秒\n最近：${esc(this.recent)}`); return;
      }
      if (!["on", "off", "unbind"].includes(command)) { await respond(help()); return; }
      if (!entries.length) { await respond("尚未绑定 Bot，请发送 hb bind @机器人用户名。"); return; }
      const next = this.copy();
      for (const [id] of entries) {
        if (command === "unbind") delete next.rules[id];
        else { next.rules[id].enabled = command === "on"; if (command === "on") next.rules[id].since = Math.floor(Date.now() / 1000) + 1; }
      }
      await this.save(next);
      await respond(command === "unbind" ? "已删除全局 Bot 绑定，保留防重复记录。" : command === "on" ? "已全局开启：锁定绑定 Bot，自动识别新红包的领取回调；不会在群里发送领取提示，不补扫历史消息。" : "已全局关闭；已发出的请求无法撤回。");
    });
  } };
}
export default new HbPlugin();
