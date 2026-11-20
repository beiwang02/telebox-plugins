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
 * 使用：回复 Bot 的未结束红包执行 `.hb bind`，确认按钮后执行 `.hb on`。
 * 每个会话独立绑定一个 Bot；配置及去重记录保存在 assets/hb/state.json。
 *
 * 安全边界：只提交明确选定的 callback，不打开链接、WebApp、支付或授权页面。
 * Bot 身份、红包关键词及按钮文字只是触发条件，不能证明 Bot 的业务行为安全。
 * 不承诺抢到，也不将 RPC 返回当成领取成功；结果不确定时宁可漏领，不重复提交。
 * 运行前应遵守所在群及 Bot 的使用规则。仅支持单进程运行，不支持共享状态多实例。
 */
type Rule = { bot: string; label: string; buttons: string[]; enabled: boolean; since: number; owner: string };
type State = { version: 1; rules: Record<string, Rule>; seen: Record<string, number>; pausedUntil: number };
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
  return `🧧 <b>定向抢红包</b>\n\n回复目标 Bot 的一条未抢完红包：\n<code>${c} bind</code> — 绑定当前会话与 Bot，自动识别领取回调按钮\n<code>${c} bind 按钮完整文字</code> — 指定样本中的领取按钮\n<code>${c} inspect</code> — 只读检查回复消息的按钮类型（不显示回调数据）\n\n<code>${c} on</code> — 开启当前会话\n<code>${c} off</code> — 关闭当前会话\n<code>${c} status</code> — 状态与最近动作\n<code>${c} list</code> — 列出绑定\n<code>${c} unbind</code> — 删除当前绑定\n\n默认关闭；只匹配当前会话、绑定 Bot、红包文字与精确按钮文字。只支持直接由 Bot 发出的回调红包，不访问 URL/WebApp，不处理转发或 inline 代发。超时不重试；API 回调返回不等于领取成功。`;
};

export class HbPlugin extends Plugin {
  name = "hb";
  description = help;
  listenMessageHandlerIgnoreEdited = false;
  private state: State = { version: 1, rules: {}, seen: {}, pausedUntil: 0 };
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
      if (s.version !== 1 || !s.rules || Array.isArray(s.rules) || !s.seen || Array.isArray(s.seen) || !Number.isFinite(s.pausedUntil)) throw Error("invalid state");
      for (const [id, r] of Object.entries(s.rules)) if (!/^-?\d+$/.test(id) || !r || !/^\d+$/.test(r.bot) || typeof r.label !== "string" || typeof r.enabled !== "boolean" || !Number.isFinite(r.since) || !/^\d+$/.test(r.owner) || !Array.isArray(r.buttons) || !r.buttons.length || r.buttons.some(x => typeof x !== "string" || !x || x.length > 256)) throw Error("invalid rule");
      for (const [k, v] of Object.entries(s.seen)) if (!/^-?\d+:\d+$/.test(k) || !Number.isFinite(v)) throw Error("invalid dedup record");
      this.state = s;
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
   * 共同入口条件：当前账号、当前会话、真实 Bot 发送者、启用时间、消息年龄及正文。
   * viaBotId 是 inline 内容提供者，不是真实发送者，不能用它替代 senderId 校验。
   * 转发及 inline 代发一律跳过；年龄按原始发送时间计算，不因编辑而重新变年轻。
   */
  private matches(msg: Api.Message): Rule | undefined {
    if (!this.live() || msg.out || msg.fwdFrom || msg.viaBotId || !msg.chatId || !msg.senderId || !(msg.fromId instanceof Api.PeerUser)) return;
    const r = this.state.rules[msg.chatId.toString()];
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
    const choices = buttonsOf(fresh).filter(b => b.data?.length && r.buttons.includes(b.text));
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
  cmdHandlers = { hb: async (msg: Api.Message) => {
    // 只允许账号本人管理自动动作，不将 TeleBox sudo 命令权限扩大成自动领取权限。
    const me = await msg.client?.getMe();
    if (!me || msg.senderId?.toString() !== me.id.toString() || !msg.out) return;
    this.owner = me.id.toString();
    const earlyCommand = (msg.message || "").trim().split(/\s+/)[1];
    // 在进入串行队列前使旧任务失效，关闭/重绑无需等待积压队列全部执行。
    // 已经发往 Telegram 的 RPC 无法撤销，因此不能承诺关闭瞬间撤回在途请求。
    if (["off", "unbind", "bind"].includes(earlyCommand)) this.epoch++;
    if (!this.live()) return;
    await this.serial(async () => {
      const chat = msg.chatId?.toString(); if (!chat) return;
      const args = (msg.message || "").trim().split(/\s+/).slice(1); const command = args.shift() || "help";
      const respond = (text: string) => msg.edit({ text, parseMode: "html" });
      if (command === "help") { await respond(help()); return; }
      if (["bind", "inspect"].includes(command)) {
        const target = await msg.getReplyMessage();
        if (!(target instanceof Api.Message) || target.chatId?.toString() !== chat) { await respond("请回复当前会话中 Bot 发出的红包消息再执行。"); return; }
        const bs = buttonsOf(target);
        if (command === "inspect") {
          await respond(`🧧 <b>按钮检查（未点击）</b>\n${target.fwdFrom ? "转发消息；不支持绑定\n" : ""}${target.viaBotId ? "Inline 代发；不支持绑定\n" : ""}${bs.length ? bs.slice(0, 20).map((b, i) => `${i + 1}. ${esc(b.text.slice(0, 100))} — ${esc(b.kind)}${b.data ? "（回调）" : "（不执行）"}`).join("\n") : "没有 inline 按钮"}`); return;
        }
        if (target.fwdFrom || target.viaBotId || !(target.fromId instanceof Api.PeerUser)) { await respond("只支持直接由 Bot 发出的红包，转发、频道代发和 inline 代发均跳过。"); return; }
        const bot = await target.getSender();
        if (!(bot instanceof Api.User) || !bot.bot || !packet(target.message || "") || ended(target.message || "")) { await respond("请回复 Bot 直接发出的一条未抢完、正文含“红包”的消息。"); return; }
        const exact = args.join(" ");
        const candidates = bs.filter(b => b.data?.length && (exact ? b.text === exact : claimLabel(b.text)));
        if (candidates.length !== 1) { await respond("无法唯一确定领取回调按钮。先用 hb inspect 检查，再用 hb bind 按钮完整文字绑定。URL/WebApp/需密码按钮不支持。"); return; }
        const next = this.copy(); next.rules[chat] = { bot: bot.id.toString(), label: bot.username ? `@${bot.username}` : (bot.firstName || "Bot"), buttons: [candidates[0].text], enabled: false, since: 0, owner: me.id.toString() };
        await this.save(next);
        await respond(`已绑定 <b>${esc(next.rules[chat].label)}</b>\nBot ID：<code>${bot.id}</code>\n按钮：${esc(candidates[0].text)}\n范围：当前会话\n状态：关闭。确认后发送 <code>${esc(getPrefixes()[0] || ".")}hb on</code>；不会补抢绑定样本或历史红包。`); return;
      }
      if (command === "list") {
        await respond(Object.entries(this.state.rules).map(([id, r]) => `<code>${esc(id)}</code> · ${esc(r.label)} · ${r.enabled ? "开启" : "关闭"}`).join("\n") || "尚未绑定 Bot。"); return;
      }
      const rule = this.state.rules[chat];
      if (command === "status") { await respond(`当前会话：${rule ? `${esc(rule.label)} / ${rule.enabled ? "开启" : "关闭"}` : "未绑定"}\n限流暂停：${this.state.pausedUntil > Date.now() ? `${Math.ceil((this.state.pausedUntil - Date.now()) / 1000)} 秒` : "无"}\n最近：${esc(this.recent)}`); return; }
      if (!["on", "off", "unbind"].includes(command)) { await respond(help()); return; }
      if (!rule) { await respond("当前会话尚未绑定，请回复 Bot 红包执行 hb bind。"); return; }
      const next = this.copy();
      if (command === "unbind") delete next.rules[chat]; else { next.rules[chat].enabled = command === "on"; if (command === "on") { next.rules[chat].since = Math.floor(Date.now() / 1000) + 1; next.rules[chat].owner = me.id.toString(); } }
      await this.save(next);
      await respond(command === "unbind" ? "已删除当前绑定（保留防重复记录）。" : command === "on" ? "已开启当前会话的定向抢红包。只处理最近 120 秒内的实时更新；不扫描历史消息，不保证抢到。" : "已关闭当前会话的定向抢红包。");
    });
  } };
}
export default new HbPlugin();
