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
 * 使用：在任意聊天执行 `.hb @bot`，绑定后直接开启。
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
// 红包初始正文可为空，按钮为可变祝福语；不能据此推断稳定的 callback 协议。
// 空正文的图片仅在可信 Bot 刚发送准备提示之后，且只有一个普通回调时作为候选。
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
  return `<b>🧧 抢红包助手</b>\n\n<b>🤖 机器人绑定:</b>\n• <code>${c} &lt;@机器人用户名&gt;</code> - 添加并立即开启\n\n<b>🛠 绑定管理:</b>\n• <code>${c} list</code> - 查看绑定编号与状态\n• <code>${c} on [编号]</code> - 开启绑定\n• <code>${c} off [编号]</code> - 暂停绑定\n• <code>${c} del [编号]</code> - 删除绑定\n未填编号时操作全部，编号以最新列表为准。\n\n<b>📋 回调查看:</b>\n• <code>${c} status</code> - 查看最近回调结果与耗时\n最近回调不是完整历史，也不代表已到账。\n\n群聊、私聊、收藏夹均可管理，绑定全局生效。`;
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
  private preparations = new Map<string, { bot: string; at: number; id: number }>();
  private contextualClaim(msg: Api.Message) {
    const p = this.preparations.get(msg.chatId?.toString() || "");
    if (!p || p.bot !== msg.senderId?.toString() || Date.now() - p.at > 15000 || msg.id <= p.id || msg.message || !msg.photo) return false;
    const bs = buttonsOf(msg);
    return bs.length === 1 && !!bs[0].data?.length;
  }
  private boosts = new Map<string, { until: number; started: number; epoch: number; minId: number; attempts: number; timer?: ReturnType<typeof setTimeout> }>();
  private lastPoll = 0;
  private polling = false;
  private queryTimes: number[] = [];
  private floodUntil = 0;
  private isPaused() { return Date.now() < Math.max(this.floodUntil, this.state.pausedUntil); }
  private arrivals = new WeakMap<Api.Message, number>();
  private pollOrigins = new WeakMap<Api.Message, { until: number; epoch: number }>();
  private stopBoost(chat: string) { const w = this.boosts.get(chat); if (w?.timer) clearTimeout(w.timer); this.boosts.delete(chat); }
  private stopAllBoosts() { for (const chat of this.boosts.keys()) this.stopBoost(chat); }
  /** 普通消息只走推送；/red 或可信 Bot 的准备提示才开启有界查询，重复触发不延长。 */
  private triggerBoost(msg: Api.Message) {
    if (!this.live() || !msg.isGroup || !msg.chatId || msg.fwdFrom || msg.viaBotId || this.isPaused()) return;
    const age = Date.now() / 1000 - Number(msg.date);
    if (!Number.isFinite(age) || age < -10 || age > 15) return;
    const rules = Object.values(this.state.rules).filter(r => r.enabled && r.owner === this.owner && Number(msg.date) >= r.since);
    if (!rules.length) return;
    const text = msg.message || "";
    const command = text.match(/^\/red(?:@([a-zA-Z0-9_]+))?(?:\s|$)/i);
    const red = command && (!command[1] || rules.some(r => r.label.toLowerCase() === `@${command[1].toLowerCase()}`));
    const preparing = msg.fromId instanceof Api.PeerUser && rules.some(r => r.bot === msg.senderId?.toString()) && /正在准备红包|正在準備紅包/.test(text);
    if (preparing) {
      const key = msg.chatId.toString();
      if (this.preparations.size >= 100) for (const [k, v] of this.preparations) if (Date.now() - v.at > 15000) this.preparations.delete(k);
      if (this.preparations.size < 100 || this.preparations.has(key)) this.preparations.set(key, { bot: msg.senderId!.toString(), at: Date.now(), id: msg.id });
    }
    if (!red && !preparing) return;
    const chat = msg.chatId.toString();
    if (this.boosts.has(chat) || this.boosts.size >= 4) return;
    const window = { until: Date.now() + 8000, started: Date.now(), epoch: this.epoch, minId: msg.id - 1, attempts: 0 };
    this.boosts.set(chat, window);
    void this.pollBoost(msg, window);
  }
  /** 备用查询全局间隔至少 2 秒，每分钟最多 6 次，每窗口最多 2 次。 */
  private async pollBoost(seed: Api.Message, w: { until: number; started: number; epoch: number; minId: number; attempts: number; timer?: ReturnType<typeof setTimeout> }) {
    const chat = seed.chatId!.toString();
    const valid = () => this.live() && this.boosts.get(chat) === w && w.epoch === this.epoch && Date.now() < w.until && !this.isPaused();
    if (!valid()) { if (this.boosts.get(chat) === w) this.stopBoost(chat); return; }
    const now = Date.now();
    this.queryTimes = this.queryTimes.filter(t => now - t < 60_000);
    if (w.attempts >= 2 || this.queryTimes.length >= 6) { this.stopBoost(chat); return; }
    const delay = Math.max(this.polling ? 200 : 0, 2000 - (now - this.lastPoll));
    if (delay > 0) {
      if (now + delay < w.until) w.timer = setTimeout(() => { void this.pollBoost(seed, w); }, delay);
      else this.stopBoost(chat);
      return;
    }
    this.polling = true;
    this.lastPoll = now;
    this.queryTimes.push(now);
    w.attempts++;
    try {
      // 查询最新10条兼容“准备消息原地编辑”和“另发红包”两种路径，不扫描历史。
      const messages = await seed.client!.getMessages(seed.peerId, { limit: 10 });
      if (!valid()) return;
      for (const m of messages) {
        if (!valid()) break;
        if (m instanceof Api.Message && m.id > w.minId && this.matches(m)) {
          this.arrivals.set(m, Date.now());
          this.pollOrigins.set(m, w);
          await this.listenMessageHandler(m);
        }
      }
    } catch (e) {
      const err = e as { seconds?: number; errorMessage?: string; message?: string };
      const wait = Number(err.seconds) || Number(String(err.errorMessage || err.message || "").match(/(?:FLOOD|SLOWMODE)_WAIT[_ ](\d+)/)?.[1]);
      if (wait > 0 && Number.isFinite(wait)) {
        this.floodUntil = Math.max(this.floodUntil, Date.now() + (wait + 1) * 1000);
        this.stopAllBoosts();
        await this.persistFlood();
        this.recent = "加速查询触发限流，已停止全部窗口并暂停";
      } else { this.stopBoost(chat); this.recent = "加速查询失败，结束本轮；仍保留实时推送监听"; }
    } finally {
      this.polling = false;
      if (valid() && w.attempts < 2) w.timer = setTimeout(() => { void this.pollBoost(seed, w); }, 2000);
      else if (this.boosts.get(chat) === w) this.stopBoost(chat);
    }
  }
  private live() { return this.active && !this.signal?.aborted; }
  /** 状态变更与去重占位串行执行，网络回调在队列外等待。 */
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
    this.signal?.addEventListener("abort", () => this.stopAllBoosts(), { once: true });
  }
  async cleanup() { this.active = false; this.stopAllBoosts(); await this.tail; }
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
    if (!r?.enabled || !this.owner || r.owner !== this.owner || Number(msg.date) < r.since || r.bot !== msg.senderId.toString() || !Number.isFinite(age) || age < -10 || age > 120 || (!packet(msg.message || "") && !this.contextualClaim(msg)) || ended(msg.message || "")) return;
    return r;
  }
  /** 新建与编辑走相同流水线；队列最多保留 20 个任务，积压时丢弃而非突发补抢。 */
  listenMessageHandler = async (msg: Api.Message) => {
    if (!this.arrivals.has(msg)) this.arrivals.set(msg, Date.now());
    this.triggerBoost(msg);
    if (!this.matches(msg) || this.queued >= 20) return;
    this.queued++;
    try { await this.receive(msg); }
    catch { this.recent = "本地存储或处理异常，未自动重试"; console.error("[hb] processing-failed"); }
    finally { this.queued--; }
  };
  private async receive(msg: Api.Message) {
    const epoch = this.epoch;
    const origin = this.pollOrigins.get(msg);
    const currentOrigin = () => !origin || (this.boosts.get(msg.chatId!.toString()) === origin && origin.epoch === this.epoch && Date.now() < origin.until);
    const key = `${msg.chatId}:${msg.id}`;
    const client = msg.client;
    if (!client) return;
    const data = await this.serial(async () => {
      const rule = this.matches(msg);
      if (!rule || epoch !== this.epoch || this.state.seen[key] || this.isPaused() || !currentOrigin()) return;
      const choices = buttonsOf(msg).filter(b => b.data?.length && (rule.buttons.length ? rule.buttons.includes(b.text) : claimLabel(b.text) || this.contextualClaim(msg)));
      if (choices.length !== 1) return;
      const next = this.copy();
      const now = Date.now();
      next.seen = Object.fromEntries(Object.entries(next.seen).filter(([, ts]) => now - ts < 7 * 86400000));
      if (Object.keys(next.seen).length >= 10000) { this.recent = "去重记录已达上限，暂停提交"; return; }
      // 同一消息先持久化占位，重启时也不会重放结果未知的回调。
      next.seen[key] = now;
      await this.save(next);
      return choices[0].data;
    });
    if (!data || !this.matches(msg) || epoch !== this.epoch || this.isPaused() || !currentOrigin()) return;
    const submittedAt = Date.now();
    const window = this.boosts.get(msg.chatId!.toString());
    const localMs = submittedAt - (this.arrivals.get(msg) || submittedAt);
    const windowMs = window ? submittedAt - window.started : undefined;
    this.stopBoost(msg.chatId!.toString());
    try {
      await client.invoke(new Api.messages.GetBotCallbackAnswer({ peer: msg.peerId, msgId: msg.id, data }));
      this.recent = `回调已返回，本地提交 ${localMs}ms${windowMs === undefined ? "" : `，触发至提交 ${windowMs}ms`}，RPC ${Date.now() - submittedAt}ms（不代表红包领取成功）`;
    } catch (e) {
      const err = e as { seconds?: number; errorMessage?: string; message?: string };
      const label = String(err.errorMessage || err.message || "");
      const wait = Number(err.seconds) || Number(label.match(/(?:FLOOD|SLOWMODE)_WAIT[_ ](\d+)/)?.[1]);
      if (wait > 0 && Number.isFinite(wait)) {
        this.floodUntil = Math.max(this.floodUntil, Date.now() + (wait + 1) * 1000);
        this.stopAllBoosts();
        await this.persistFlood();
        this.recent = "Telegram 限流，已暂停；该红包不重试";
      } else this.recent = "回调失败或结果未知，已去重，不自动重试";
    }
  }
  private async persistFlood() {
    await this.serial(async () => {
      const next = this.copy();
      next.pausedUntil = Math.max(next.pausedUntil, this.floodUntil);
      await this.save(next);
    }).catch(() => console.error("[hb] flood-state-save-failed"));
  }
  /** 管理命令在群聊、私聊、收藏夹均可执行；只有账号本人有权修改全局绑定。 */
  cmdHandlers = { hb: async (msg: Api.Message) => {
    const me = await msg.client?.getMe();
    if (!me || !msg.out || msg.senderId?.toString() !== me.id.toString() || !this.live()) return;
    const args = (msg.message || "").trim().split(/\s+/).slice(1);
    const c = esc(`${getPrefixes()[0] || "."}hb`);
    const prompt = `请输入机器人用户名：<code>${c} &lt;@机器人用户名&gt;</code>`;
    const first = args.shift();
    if (!first) {
      await msg.edit({ text: prompt, parseMode: "html" });
      return;
    }
    const aliases = new Map([["pause", "off"], ["resume", "on"], ["del", "unbind"]]);
    const sub = aliases.get(first) ?? first;
    const command = ["bind", "off", "on", "unbind", "list", "status", "inspect"].includes(sub) ? sub : "bind";
    // 普通参数直接视作 Bot 用户名；保留旧 bind 写法兼容已有用法。
    const username = command === "bind" ? (first === "bind" ? args.shift() : first) : undefined;
    if (command === "bind" && (!username || !/^@?[a-zA-Z][a-zA-Z0-9_]{3,31}$/.test(username))) {
      await msg.edit({ text: prompt, parseMode: "html" });
      return;
    }
    if (["bind", "off", "unbind"].includes(command)) { this.epoch++; this.stopAllBoosts(); this.preparations.clear(); }
    await this.serial(async () => {
      const respond = (text: string) => msg.edit({ text, parseMode: "html" });
      if (command === "bind" && username) {
        let bot;
        try { bot = await msg.client!.getEntity(username.startsWith("@") ? username : `@${username}`); }
        catch { await respond("无法解析这个用户名，原绑定未改变。请确认 Bot 用户名有效。"); return; }
        if (!(bot instanceof Api.User) || !bot.bot) { await respond("目标不是 Bot，原绑定未改变。"); return; }
        const next = this.copy();
        // 添加/重绑立即开启，只更新这个 Bot，不覆盖其它绑定，也不清除防重复记录。
        next.rules[bot.id.toString()] = { bot: bot.id.toString(), label: bot.username ? `@${bot.username}` : (bot.firstName || "Bot"), buttons: args.length ? [args.join(" ")] : [], enabled: true, since: Math.floor(Date.now() / 1000), owner: me.id.toString() };
        await this.save(next);
        await respond(`✅ 已绑定并开启 <b>${esc(next.rules[bot.id.toString()].label)}</b>\n发送 <code>${esc(getPrefixes()[0] || ".")}hb list</code> 查看和管理。`); return;
      }
      if (command === "inspect") {
        const target = await msg.getReplyMessage();
        if (!(target instanceof Api.Message)) { await respond("inspect 需要回复一条消息；绑定不需要回复。"); return; }
        await respond(buttonsOf(target).slice(0, 20).map(b => `${esc(b.text.slice(0, 100))} — ${esc(b.kind)}（${b.data ? "回调" : "不执行"}）`).join("\n") || "没有 inline 按钮；未执行任何点击。"); return;
      }
      const entries = Object.entries(this.state.rules).filter(([, r]) => r.owner === me.id.toString()).sort(([a], [b]) => a.localeCompare(b));
      if (["status", "list"].includes(command)) {
        const rows = entries.map(([, r], i) => `${i + 1}. ${esc(r.label)} · ${r.enabled ? "🟢 开启" : "⏸ 已暂停"}`).join("\n");
        await respond(`🧧 <b>红包管理</b>\n${rows || "暂无绑定"}\n\n<code>${c} on [编号]</code> - 开启绑定\n<code>${c} off [编号]</code> - 暂停绑定\n<code>${c} del [编号]</code> - 删除绑定\n编号以最新 list 为准；不带编号则操作全部。\n\n限流等待：${Math.max(0, Math.ceil((Math.max(this.state.pausedUntil, this.floodUntil) - Date.now()) / 1000))} 秒\n最近回调：${esc(this.recent)}\n（只显示本次加载后的最近结果，不是历史或到账证明）`); return;
      }
      if (!["on", "off", "unbind"].includes(command)) { await respond(help()); return; }
      if (!entries.length) { await respond(`尚未绑定 Bot。${prompt}`); return; }
      const selector = args.shift();
      let selected = entries;
      if (selector) {
        const index = /^\d+$/.test(selector) ? Number(selector) - 1 : -1;
        if (!Number.isSafeInteger(index) || index < 0 || index >= entries.length || args.length) { await respond(`编号无效，请先发送 <code>${c} list</code> 查看。`); return; }
        selected = [entries[index]];
      }
      const next = this.copy();
      for (const [id] of selected) {
        if (command === "unbind") delete next.rules[id];
        else { next.rules[id].enabled = command === "on"; if (command === "on") next.rules[id].since = Math.floor(Date.now() / 1000) + 1; }
      }
      await this.save(next);
      await respond(`${command === "unbind" ? "🗑 已删除" : command === "on" ? "🟢 已开启" : "⏸ 已暂停"}：${selected.map(([, r]) => esc(r.label)).join("、")}\n<code>${c} list</code> 查看最新列表。`);
    });
  } };
}
export default new HbPlugin();
