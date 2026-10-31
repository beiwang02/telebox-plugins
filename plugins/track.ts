import { Api } from "teleproto";
import bigInt from "big-integer";
import { getPeerId } from "teleproto/Utils";
import { CustomFile } from "teleproto/client/uploads";
import { Plugin, PluginRuntimeContext } from "@utils/pluginBase";
import { getGlobalClient } from "@utils/runtimeManager";
import { getPrefixes } from "@utils/pluginManager";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { randomUUID } from "node:crypto";

type Media = { file: string; kind: "photo" | "sticker" | "gif" | "file"; mime?: string; caption?: string; filename?: string; audio?: { duration: number; voice?: boolean; title?: string; performer?: string }; alt?: string; animated?: boolean; video?: { w: number; h: number; duration: number } };
type Reply = { replyText: string; media?: never } | { media: Media; replyText?: never };
// Legacy arrays remain intact on disk; only their first item is ever used.
const repliesOf = (r: { replies?: Reply[]; replyText?: string; media?: Media }): Reply[] => r.replies?.length ? [r.replies[0]] : r.replyText !== undefined ? [{ replyText: r.replyText }] : r.media ? [{ media: r.media }] : [];
const replySummary = (r: { replies?: Reply[]; replyText?: string; media?: Media }) => { const first = repliesOf(r)[0]; return first?.media ? mediaType(first.media) : first?.replyText ? `文字：${preview(first.replyText, 100)}` : "未设置回复"; };
type Rule = { id: number; uid: number; username?: string; user: string; label: string; mode: "contains" | "exact" | "any"; text: string; enabled: boolean; intervalSeconds: number; replies?: Reply[]; replyText?: string; media?: Media };
type Group = { next: number; cd: number; rules: Rule[] };
type State = { version: 1 | 2; nextUid: number; groups: Record<string, Group> };
const esc = (s: string) => s.replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]!));
// Bound unescaped Unicode text first, so HTML entities and surrogate pairs stay intact.
const preview = (s: string, n: number) => {
  const chars = Array.from(s);
  return esc(chars.slice(0, n).join("").replace(/\s+/g, " ") + (chars.length > n ? "…" : ""));
};
const mediaType = (m: Media) => m.kind === "photo" ? "照片" : m.kind === "sticker" ? "贴纸" : m.kind === "gif" ? "GIF" : m.audio ? (m.audio.voice ? "语音" : "音频") : m.video || m.mime?.startsWith("video/") ? "视频" : "文件";
const root = path.resolve("assets/track");
const help = () => {
  const c = esc(`${getPrefixes()[0] || "."}track`);
  return `定向自动回复

<code>${c}</code> - 开始设置
关键词匹配：多个关键词以空格分隔，消息包含任一关键词即可触发自动回复。
<code>${c} list</code> - 查看全部回复配置
<code>${c} on 1</code> - 启用配置（支持多个序号）
<code>${c} off 1</code> - 停用配置（支持多个序号）
<code>${c} del 1 4 5</code> - 删除配置（支持多个序号）
<code>${c} cd 1 60</code> - 修改配置1间隔为60秒
<code>${c} cancel</code> - 取消当前设置`;
};
const IDLE_MS = 30000;
type Session = { owner?: Api.Message; token?: string; ids?: Set<number>; destination?: string; destinationLabel?: string; user?: string; label?: string; pendingTarget?: string; mode: "contains" | "exact" | "any"; expires: number; text?: string; replyText?: string; media?: Media };


export class TrackPlugin extends Plugin {
  name = "track";
  description = help;
  ignoreEdited = true;
  listenMessageHandlerIgnoreEdited = true;
  private state: State = { version: 2, nextUid: 1, groups: {} };
  private active = true;
  private loaded = false;
  private tail: Promise<unknown> = Promise.resolve();
  private cooldown = new Map<string, number>();
  private seen = new Map<string, number>();
  private signal?: AbortSignal;
  private sessions = new Map<string, Session>();
  private timers = new Map<string, ReturnType<typeof setTimeout>>();
  private internal = new Set<string>();
  private configPrompts = new Map<string, number>();
  private deleteNotices = new Set<string>();
  private deleteWarned = new Set<string>();
  private management = new Map<string, { id: string; label: string }>();
  private control(msg: Api.Message) { return msg.chatId?.toString(); }
  private privateChat(msg: Api.Message) { return !msg.isGroup && (msg.isPrivate || msg.peerId instanceof Api.PeerUser); }
  private log(kind: string) { console.error(`[track] ${kind}`); }
  private live() { return this.active && !this.signal?.aborted; }
  private serial<T>(fn: () => Promise<T>): Promise<T> {
    const job = this.tail.then(fn);
    this.tail = job.catch(() => undefined);
    return job;
  }
  async setup(context: PluginRuntimeContext) {
    this.signal = context.signal;
    this.active = true;
    this.signal?.addEventListener("abort", () => { for (const gid of this.timers.keys()) this.clearTimer(gid); }, { once: true });
    await this.serial(() => this.load());
  }
  async cleanup() { this.active = false; for (const gid of this.timers.keys()) this.clearTimer(gid); await this.tail; for (const gid of this.sessions.keys()) await this.dropSession(gid); }
  private async load() {
    if (this.loaded) return;
    await fs.mkdir(path.join(root, "media"), { recursive: true, mode: 0o700 });
    let readCompleted = false;
    try {
      const raw = await fs.readFile(path.join(root, "rules.json"), "utf8"); readCompleted = true;
      const s = JSON.parse(raw) as State;
      if (![1, 2].includes(s.version) || !s.groups || typeof s.groups !== "object" || Array.isArray(s.groups)) throw Error();
      const uids = new Set<number>();
      if (s.version === 2 && (!Number.isSafeInteger(s.nextUid) || s.nextUid < 1)) throw Error();
      for (const [gid, g] of Object.entries(s.groups)) {
        if ((gid !== "all" && !/^-?\d+$/.test(gid)) || !Number.isSafeInteger(g.next) || g.next < 1 || !Number.isSafeInteger(g.cd) || g.cd < 0 || g.cd > 86400 || !Array.isArray(g.rules)) throw Error();
        const ids = new Set<number>();
        for (const r of g.rules) {
          if (!Number.isSafeInteger(r.id) || r.id < 1 || r.id >= g.next || ids.has(r.id) || !/^\d+$/.test(r.user) || typeof r.label !== "string" || typeof r.text !== "string" || !r.text || !["contains", "exact", "any"].includes(r.mode) || typeof r.enabled !== "boolean" || !Number.isSafeInteger(r.intervalSeconds) || r.intervalSeconds < 0 || r.intervalSeconds > 86400) throw Error();
          ids.add(r.id);
          if (s.version === 2) {
            if (!Number.isSafeInteger(r.uid) || r.uid < 1 || r.uid >= s.nextUid || uids.has(r.uid)) throw Error();
            uids.add(r.uid);
          }
          if (r.username !== undefined && (typeof r.username !== "string" || !/^[a-zA-Z0-9_]{1,32}$/.test(r.username))) throw Error();
          if (r.replies !== undefined) {
            if (!Array.isArray(r.replies) || !r.replies.length || r.replies.length > 10) throw Error();
            for (const x of r.replies) {
              if (!x || typeof x !== "object" || (x.replyText !== undefined) === (x.media !== undefined)) throw Error();
              if (x.media !== undefined && (!x.media || typeof x.media !== "object" || Array.isArray(x.media))) throw Error();
              if (x.replyText !== undefined && (typeof x.replyText !== "string" || !x.replyText.trim() || x.replyText.length > 4096)) throw Error();
              if (x.media && (!/^media\/[a-f0-9-]+\.(jpg|webp|tgs|webm|gif|mp4|bin)$/.test(x.media.file) || !["photo", "sticker", "gif", "file"].includes(x.media.kind) || (x.media.mime !== undefined && (typeof x.media.mime !== "string" || !/^[a-zA-Z0-9.+-]+\/[a-zA-Z0-9.+-]+$/.test(x.media.mime))))) throw Error();
            }
          }
          if (r.replyText !== undefined && (typeof r.replyText !== "string" || !r.replyText || r.replyText.length > 4096)) throw Error();
          if (r.media?.mime !== undefined && (typeof r.media.mime !== "string" || !/^[a-zA-Z0-9.+-]+\/[a-zA-Z0-9.+-]+$/.test(r.media.mime))) throw Error();
          if (r.media && (!/^media\/[a-f0-9-]+\.(jpg|webp|tgs|webm|gif|mp4|bin)$/.test(r.media.file) || !["photo", "sticker", "gif", "file"].includes(r.media.kind))) throw Error();
        }
      }
      if (s.version === 1) {
        s.version = 2; s.nextUid = 1;
        const entries = Object.entries(s.groups).sort(([a], [b]) => a === "all" ? -1 : b === "all" ? 1 : a.localeCompare(b));
        for (const [, g] of entries) for (const r of [...g.rules].sort((a, b) => a.id - b.id)) {
          r.uid = s.nextUid++;
          r.username = this.labelUsername(r.label);
        }
        // Commit migration only after validating every legacy rule; failure leaves disk unchanged.
        await this.save(s);
      } else this.state = s;
    } catch (e) {
      if (readCompleted || (e as NodeJS.ErrnoException).code !== "ENOENT") { this.log("state-load-failed"); throw Error("状态文件无效，拒绝覆盖"); }
    }
    this.loaded = true;
  }
  private async save(next: State) {
    const tmp = path.join(root, `.${randomUUID()}.tmp`);
    try {
      await fs.writeFile(tmp, JSON.stringify(next, null, 2), { mode: 0o600, flag: "wx" });
      await fs.rename(tmp, path.join(root, "rules.json"));
      this.state = JSON.parse(JSON.stringify(next)) as State;
    } finally { await fs.unlink(tmp).catch(() => undefined); }
  }
  private group(msg: Api.Message): string | undefined {
    return msg.isGroup && msg.chatId ? msg.chatId.toString() : undefined;
  }
  private async user(msg: Api.Message): Promise<{ id: string; label: string } | undefined> {
    if (!(msg.fromId instanceof Api.PeerUser) || !msg.senderId) return;
    const sender = await msg.getSender();
    if (!(sender instanceof Api.User) || sender.self || sender.id.toString() !== msg.senderId.toString()) return;
    return { id: sender.id.toString(), label: [sender.firstName, sender.lastName].filter(Boolean).join(" ") + (sender.username ? ` (@${sender.username})` : "") || "未命名用户" };
  }
  private classify(msg: Api.Message): { meta: Omit<Media, "file">; ext: string } {
    if (msg.media instanceof Api.MessageMediaPhoto && msg.media.photo instanceof Api.Photo) return { meta: { kind: "photo" }, ext: "jpg" };
    if (!(msg.media instanceof Api.MessageMediaDocument) || !(msg.media.document instanceof Api.Document)) throw Error("请发送文字或支持的媒体");
    const d = msg.media.document;
    const sticker = d.attributes.find(a => a instanceof Api.DocumentAttributeSticker) as Api.DocumentAttributeSticker | undefined;
    const animated = d.attributes.some(a => a instanceof Api.DocumentAttributeAnimated);
    const v = d.attributes.find(a => a instanceof Api.DocumentAttributeVideo) as Api.DocumentAttributeVideo | undefined;
    const video = v ? { w: v.w, h: v.h, duration: v.duration } : undefined;
    if (sticker && ["image/webp", "application/x-tgsticker", "video/webm"].includes(d.mimeType)) return { meta: { kind: "sticker", mime: d.mimeType, alt: sticker.alt, video }, ext: ({ "image/webp": "webp", "application/x-tgsticker": "tgs", "video/webm": "webm" } as Record<string, string>)[d.mimeType] };
    if (d.mimeType === "image/gif" || (animated && d.mimeType === "video/mp4")) return { meta: { kind: "gif", mime: d.mimeType, animated: true, video }, ext: d.mimeType === "image/gif" ? "gif" : "mp4" };
    const audio = d.attributes.find(a => a instanceof Api.DocumentAttributeAudio) as Api.DocumentAttributeAudio | undefined;
    const filename = d.attributes.find(a => a instanceof Api.DocumentAttributeFilename) as Api.DocumentAttributeFilename | undefined;
    return { meta: { kind: "file", mime: d.mimeType, video, filename: filename?.fileName, audio: audio ? { duration: audio.duration, voice: audio.voice, title: audio.title, performer: audio.performer } : undefined }, ext: "bin" };
  }
  private attributes(m: Media): Api.TypeDocumentAttribute[] {
    const attrs: Api.TypeDocumentAttribute[] = [];
    if (m.filename) attrs.push(new Api.DocumentAttributeFilename({ fileName: m.filename }));
    if (m.audio) attrs.push(new Api.DocumentAttributeAudio(m.audio));
    if (m.kind === "sticker") attrs.push(new Api.DocumentAttributeSticker({ alt: m.alt || "", stickerset: new Api.InputStickerSetEmpty() }));
    if (m.animated) attrs.push(new Api.DocumentAttributeAnimated());
    if (m.video) attrs.push(new Api.DocumentAttributeVideo({ ...m.video, supportsStreaming: true }));
    return attrs;
  }
  private isCommand(msg: Api.Message) {
    return getPrefixes().some(p => (msg.message || "").startsWith(p));
  }
  private remember(chat: string, sent: Api.Message | Api.Message[] | undefined) {
    for (const item of Array.isArray(sent) ? sent : sent ? [sent] : []) this.internal.add(`${chat}:${item.id}`);
    while (this.internal.size > 1000) this.internal.delete(this.internal.values().next().value!);
  }
  private markSeen(key: string, ttl: number) {
    const now = Date.now();
    for (const [k, expiry] of this.seen) if (expiry <= now) this.seen.delete(k);
    if (this.seen.has(key)) return false;
    this.seen.set(key, now + ttl);
    while (this.seen.size > 10000) this.seen.delete(this.seen.keys().next().value!);
    return true;
  }
  private async deleteOwn(msg: Api.Message, ids: number[]) {
    if (!msg.out || !ids.length || !this.control(msg)) return;
    try {
      const client = await getGlobalClient();
      await client!.deleteMessages(msg.chatId!, ids, { revoke: true });
    } catch {
      this.log("config-delete-failed");
      const control = this.control(msg)!;
      if (!this.deleteWarned.has(control)) { this.deleteWarned.add(control); this.deleteNotices.add(control); }
    }
  }
  private clearTimer(control: string) {
    const timer = this.timers.get(control);
    if (timer !== undefined) clearTimeout(timer);
    this.timers.delete(control);
  }
  private pause(control: string) {
    this.clearTimer(control);
    const session = this.sessions.get(control);
    if (session) session.expires = 0; // Accepted input is processing, not idle.
  }
  private arm(control: string, session: Session) {
    this.clearTimer(control);
    if (!this.live() || this.sessions.get(control) !== session) return;
    if (!session.expires) session.expires = Date.now() + IDLE_MS;
    const expires = session.expires, token = session.token;
    const timer = setTimeout(() => {
      void this.serial(async () => {
        const current = this.sessions.get(control);
        if (!this.live() || current !== session || current.token !== token || current.expires !== expires) return;
        if (Date.now() < expires) { this.arm(control, session); return; }
        await this.dropSession(control);
      }).catch(() => this.log("timeout-cleanup-failed"));
    }, Math.max(0, expires - Date.now()));
    timer.unref?.();
    this.timers.set(control, timer);
  }
  private async accepted(msg: Api.Message, configuration = false) {
    const control = this.control(msg)!;
    const session = configuration ? this.sessions.get(control) : undefined;
    if (session) { this.pause(control); session.ids?.add(msg.id); }
    await this.deleteOwn(msg, [msg.id]);
  }
  private async prompt(msg: Api.Message, text: string) {
    try {
      const control = this.control(msg)!;
      const previous = this.configPrompts.get(control);
      if (previous !== undefined) { this.configPrompts.delete(control); await this.deleteOwn(msg, [previous]); }
      const session = this.sessions.get(control);
      if (session) text += `
30秒内未输入将取消；发送 <code>${esc(`${getPrefixes()[0] || "."}track cancel`)}</code> 可随时取消。`;
      if (this.deleteNotices.delete(control)) text += "\n部分配置消息未能删除，不影响设置。";
      const client = await getGlobalClient();
      // Inputs may already be deleted: do not reply to a deleted message ID.
      const sent = await client!.sendMessage(msg.chatId!, { message: text, parseMode: "html" });
      if (sent) {
        this.internal.add(`${control}:${sent.id}`);
        if (session) { this.configPrompts.set(control, sent.id); session.ids?.add(sent.id); }
      }
      if (this.internal.size > 1000) this.internal.delete(this.internal.values().next().value!);
    }
    catch { this.log("prompt-failed"); }
    finally {
      const control = this.control(msg)!;
      const session = this.sessions.get(control);
      if (session) this.arm(control, session);
    }
  }
  private labelUsername(label: string) { return label.match(/[（(]@([a-zA-Z0-9_]{1,32})[）)]\s*$/)?.[1]; }
  private async managementReceipt(cmd: string, r: Rule) {
    const action = cmd === "del" ? "已删除" : cmd === "off" ? "已停用" : cmd === "on" ? (r.enabled ? "已启用" : "未启用") : "已修改";
    return `${action}配置${r.uid}${cmd === "cd" ? `间隔为${r.intervalSeconds}秒` : ""}（${esc(await this.identity(r, { left: 0 }))}）`;
  }
  private identities = new Map<string, { username?: string; expires: number }>();
  private async identity(r: Rule, budget: { left: number }): Promise<string> {
    let username = r.username || this.labelUsername(r.label);
    if (!username) {
      let cached = this.identities.get(r.user);
      if ((!cached || cached.expires <= Date.now()) && budget.left > 0) {
        budget.left--;
        try {
          const client = await getGlobalClient(); const e = await client!.getEntity(r.user);
          cached = { username: e instanceof Api.User && e.id.toString() === r.user ? e.username : undefined, expires: Date.now() + 3600000 };
        } catch { cached = { expires: Date.now() + 3600000 }; }
        this.identities.set(r.user, cached);
        if (this.identities.size > 1000) this.identities.delete(this.identities.keys().next().value!);
      }
      username = cached?.username;
    }
    const name = (r.label || "未命名用户").replace(/\s*[（(]@[a-zA-Z0-9_]{1,32}[）)]\s*$/, "");
    return `${name}（${username ? "@" + username : r.user}）`;
  }
  private async list(msg: Api.Message, inGroup?: string) {
    const entries = Object.entries(this.state.groups).filter(([, g]) => g.rules.length);
    const prefix = esc(getPrefixes()[0] || ".");
    if (!entries.length) {
      const client = await getGlobalClient();
      const sent = await client!.sendMessage(msg.chatId!, { message: `暂无回复配置，发送${prefix}track开始设置`, parseMode: "html" });
      this.remember(this.control(msg)!, sent); return;
    }
    const client = await getGlobalClient();
    const names = new Map<string, string>();
    const remember = (e: unknown) => {
      if (!(e instanceof Api.Chat || (e instanceof Api.Channel && e.megagroup))) return;
      names.set(getPeerId(e), e.title + (e instanceof Api.Channel && e.username ? ` (@${e.username})` : ""));
    };
    if (inGroup) { try { remember(await msg.getChat()); } catch { /* resolve below */ } }
    else for (const e of await this.accessibleGroups()) remember(e);
    const label = async (id: string) => {
      if (id === "all") return "所有群";
      if (!names.has(id)) {
        try { const e = await client!.getEntity(id); if (e instanceof Api.Chat || (e instanceof Api.Channel && e.megagroup)) { if (getPeerId(e) === id) remember(e); } } catch { /* inaccessible groups use known names */ }
      }
      if (names.has(id)) return names.get(id)!;
      for (const c of this.management.values()) if (c.id === id && c.label && c.label !== id) return c.label;
      for (const s of this.sessions.values()) if (s.destination === id && s.destinationLabel && s.destinationLabel !== id) return s.destinationLabel;
      return "指定群";
    };
    // Bound raw previews before escaping; every block is well below Telegram's 4096 HTML limit.
    const rules = entries.flatMap(([, g]) => g.rules);
    const enabledCount = rules.filter(r => r.enabled && repliesOf(r).length > 0).length;
    const lines: string[] = ["全部回复配置", "同一用户的群专属配置优先。", `共${rules.length}条 · 启用${enabledCount} · 停用${rules.length - enabledCount}`];
    entries.sort(([a], [b]) => a === "all" ? -1 : b === "all" ? 1 : a.localeCompare(b));
    const identityBudget = { left: 10 };
    for (const [id, g] of entries) {
      lines.push(`\n<b>${this.scopeLabel(id, await label(id))}</b>`);
      for (const r of [...g.rules].sort((a, b) => a.uid - b.uid)) {
        const enabled = r.enabled && repliesOf(r).length > 0;
        const mode = r.mode === "any" ? "任意消息" : `${r.mode === "exact" ? "精确匹配" : "包含"}：${preview(r.text, 100)}`;
        const reply = replySummary(r);
        lines.push(`${r.uid}. ${preview(await this.identity(r, identityBudget), 140)} · ${enabled ? "启用" : "停用"}\n${mode}\n回复：${reply}\n间隔：${r.intervalSeconds}秒`);
      }
    }
    let chunk = "";
    const send = async () => {
      if (!chunk || !this.live()) return;
      const sent = await client!.sendMessage(msg.chatId!, { message: chunk, parseMode: "html" });
      this.remember(this.control(msg)!, sent);
    };
    for (const line of lines) {
      if (chunk.length + line.length + 1 > 3900) { await send(); chunk = ""; }
      chunk += (chunk ? "\n" : "") + line;
    }
    await send();
  }
  private async target(msg: Api.Message, arg: string, gid: string) {
    if (arg) {
      const numeric = /^[1-9]\d*$/.test(arg);
      if (!numeric && !/^@[a-zA-Z0-9_]{1,32}$/.test(arg)) throw Error("请输入目标@用户名、用户ID，或回复目标消息");
      // Telegram user IDs are positive signed-long values. Never round through Number
      // or infer chat/channel peers from a bare numeric target.
      if (numeric && bigInt(arg).greater("9223372036854775807")) throw Error("请输入目标@用户名、用户ID，或回复目标消息");
      const client = await getGlobalClient();
      let u: unknown;
      try { u = await client!.getEntity(numeric ? new Api.PeerUser({ userId: bigInt(arg) }) : arg); }
      catch { throw Error(numeric ? "无法识别此用户ID，请改用@用户名或回复目标消息" : "请输入目标@用户名、用户ID，或回复目标消息"); }
      if (numeric && !u) throw Error("无法识别此用户ID，请改用@用户名或回复目标消息");
      if (!(u instanceof Api.User) || u.self || (numeric && u.id.toString() !== arg)) throw Error("目标必须是真实用户且不是自己");
      return { id: u.id.toString(), label: [u.firstName, u.lastName].filter(Boolean).join(" ") + (u.username ? ` (@${u.username})` : "") || (numeric ? `用户ID：${u.id.toString()}` : arg) };
    }
    const reply = await msg.getReplyMessage();
    if (!reply || (this.group(msg) ? this.group(reply) !== gid : this.control(reply) !== this.control(msg))) throw Error("请输入目标@用户名、用户ID，或回复目标消息");
    const u = await this.user(reply);
    if (!u) throw Error("目标必须是真实用户且不是自己");
    return u;
  }
  private scopeLabel(id: string, label?: string) {
    return id === "all" ? "所有群" : `${preview(label || "指定群", 100)}（群ID：${esc(id)}）`;
  }
  private async targetPrompt(msg: Api.Message, session: Session) {
    const label = this.scopeLabel(session.destination!, session.destinationLabel);
    await this.prompt(msg, `已指定追踪用户:${esc(session.label || "未命名用户")}\n范围:${label}\n请输入追踪内容（发送 * 表示任意消息）`);
  }
  private async accessibleGroups() {
    const client = await getGlobalClient();
    try {
      const dialogs = await client!.getDialogs({ limit: 100 });
      return dialogs.map(d => d.entity).filter((e): e is Api.Chat | Api.Channel =>
        (e instanceof Api.Chat && !e.left && !e.deactivated) || (e instanceof Api.Channel && !!e.megagroup && !e.left));
    } catch { return []; }
  }
  private async selectGroup(value: string): Promise<{ id: string; label: string }> {
    const client = await getGlobalClient();
    let input = value.trim();
    if (input === "所有群") return { id: "all", label: "所有群（不含私聊/频道）" };
    const choices = await this.accessibleGroups();
    const link = input.match(/^(?:https?:\/\/)?(?:t\.me|telegram\.me)\/([a-zA-Z0-9_]+)\/?$/);
    if (link) input = `@${link[1]}`;
    const named = choices.filter(e => e.title === input || getPeerId(e) === input);
    if (named.length > 1) throw Error("请选择唯一群@名或群ID（群标题重名）");
    let entity: unknown = named[0];
    if (!entity) {
      if (!/^@[a-zA-Z0-9_]{1,32}$/.test(input) && !/^-\d+$/.test(input)) throw Error("请输入群@名、公开群链接或带负号群ID");
      try { entity = await client!.getEntity(input); } catch { throw Error("请选择可访问群：无法解析或没有访问权限"); }
    }
    if (!((entity instanceof Api.Chat && !entity.left && !entity.deactivated) || (entity instanceof Api.Channel && !!entity.megagroup && !entity.left))) throw Error("请选择可访问的群，不支持广播频道或私聊");
    return { id: getPeerId(entity), label: entity.title };
  }
  private async selectScope(msg: Api.Message, value: string): Promise<{ id: string; label: string }> {
    if (value.trim() !== "当前群") return this.selectGroup(value);
    const current = this.group(msg);
    if (current) {
      let label = "当前群";
      try { const chat = await msg.getChat(); if (chat instanceof Api.Chat || chat instanceof Api.Channel) label = chat.title; } catch { /* entity may not be cached */ }
      return { id: current, label };
    }
    const context = this.management.get(this.control(msg)!);
    if (context && context.id !== "all") return context;
    throw Error("请选择生效范围：私聊没有当前群，请发送 所有群，或粘贴群名、群用户名（@xxx）、群ID（带负号，如 -100…）或公开群链接");
  }
  private async scopePrompt(msg: Api.Message) {
    if (this.group(msg)) {
      await this.prompt(msg, "请选择生效范围：当前群 / 所有群（也可直接粘贴其他群名、群用户名（@xxx）、群ID（带负号，如 -100…）或公开群链接）");
    } else {
      const context = this.management.get(this.control(msg)!);
      await this.prompt(msg, "发送 所有群，或粘贴群名、群用户名（@xxx）、群ID（带负号，如 -100…）或公开群链接" + (context && context.id !== "all" ? `\n发送 当前群 指已选管理群：${this.scopeLabel(context.id, context.label)}` : ""));
    }
  }
  cmdHandlers = { track: async (msg: Api.Message) => {
    if (!msg.out || !this.live() || msg.editDate) return;
    await this.serial(async () => {
      try {
        await this.load();
        const control = this.control(msg), inGroup = this.group(msg);
        if (!control || (!inGroup && !this.privateChat(msg))) return;
        let gid = inGroup || this.management.get(control)?.id;
        const prefix = getPrefixes().find(p => (msg.message || "").startsWith(p));
        if (!prefix) return;
        const raw = (msg.message || "").slice(prefix.length);
        const match = raw.match(/^track(?:\s+([\s\S]*)|(edit|add)([1-9]\d*))?$/);
        if (!match) return;
        const parts = (match[1] || (match[2] ? `${match[2]} ${match[3]}` : "")).trim().split(/\s+/).filter(Boolean);
        let cmd = parts[0] || "start", arg = parts.slice(1).join(" ");
        if (cmd.startsWith("@") || /^[+-]?\d+$/.test(cmd)) { arg = parts.join(" "); cmd = "start"; }
        if (cmd === "help") { await this.prompt(msg, help()); return; }
        if (cmd === "cancel") { await this.dropSession(control); await this.accepted(msg); await this.prompt(msg, "已取消配置"); return; }
        if (["done", "add", "edit"].includes(cmd) || /^(edit|add)[1-9]\d*$/.test(cmd)) {
          await this.prompt(msg, `未提供此子命令，请使用 ${esc(getPrefixes()[0] || ".")}help track`); return;
        }
        if (cmd === "group") {
          if (inGroup) throw Error("请在私聊使用 group 选择管理群");
          const selected = await this.selectScope(msg, arg);
          await this.dropSession(control);
          this.management.set(control, selected);
          await this.accepted(msg); await this.prompt(msg, `当前管理群：${this.scopeLabel(selected.id, selected.label)}`); return;
        }
        if (cmd === "start" || cmd === "exact") {
          if (!inGroup) {
            if (arg && !/^@[a-zA-Z0-9_]{1,32}$/.test(arg) && !(/^[1-9]\d*$/.test(arg) && bigInt(arg).lesserOrEquals("9223372036854775807"))) throw Error("请输入目标@用户名、用户ID，或回复目标消息");
            await this.dropSession(control);
            this.sessions.set(control, { pendingTarget: arg || undefined, mode: cmd === "exact" ? "exact" : "contains", owner: msg, token: randomUUID(), ids: new Set<number>(), expires: 0 });
            await this.accepted(msg, true); await this.scopePrompt(msg); return;
          }
          const u = await this.target(msg, arg, inGroup);
          await this.dropSession(control);
          const session: Session = { user: u.id, label: u.label, mode: cmd === "exact" ? "exact" : "contains", owner: msg, token: randomUUID(), ids: new Set<number>(), expires: 0 };
          this.sessions.set(control, session);
          await this.accepted(msg, true); await this.scopePrompt(msg); return;
        }
        if (cmd === "list") { await this.list(msg, inGroup); return; }
        // Stable UIDs are instance-wide; validate the entire batch before one atomic save.
        const numbered = ["del", "on", "off", "cd"].includes(cmd) && !!arg && !arg.startsWith("@");
        const numericParts = arg.split(/\s+/);
        if (numbered && (cmd !== "cd" || numericParts.length > 1)) {
          const tokens = cmd === "cd" ? numericParts.slice(0, 1) : numericParts;
          if ((cmd === "cd" && numericParts.length !== 2) || tokens.some(token => !/^[1-9]\d*$/.test(token) || !Number.isSafeInteger(Number(token)))) throw Error("配置序号无效，请发送 .track list 查看列表");
          const ids = [...new Set(tokens.map(Number))];
          const selected = ids.map(uid => {
            const found = Object.entries(this.state.groups).flatMap(([scope, g]) => g.rules.filter(r => r.uid === uid).map(r => ({ scope, r })));
            if (!found.length) throw Error(`配置${uid}不存在，可能已删除；发送${getPrefixes()[0] || "."}track list查看现有配置`);
            if (found.length !== 1) throw Error("配置序号重复，拒绝操作，请发送 .track list 查看列表");
            return found[0];
          });
          if (cmd === "cd" && (!/^\d+$/.test(numericParts[1]) || !Number.isSafeInteger(Number(numericParts[1])) || Number(numericParts[1]) > 86400)) throw Error("冷却范围0–86400整数秒");
          const next: State = JSON.parse(JSON.stringify(this.state));
          const updated = selected.map(({ scope, r: original }) => {
            const g = next.groups[scope], r = g.rules.find(r => r.uid === original.uid)!;
            if (cmd === "cd") r.intervalSeconds = Number(numericParts[1]);
            else if (cmd === "del") g.rules = g.rules.filter(x => x.uid !== r.uid);
            else r.enabled = cmd === "on" && repliesOf(r).length > 0;
            return r;
          });
          if (!this.live()) return;
          await this.save(next);
          if (cmd === "cd" || cmd === "del") for (const { scope, r } of selected) this.cooldown.delete(`${scope}:${r.user}`);
          const receipt = ids.length === 1 ? await this.managementReceipt(cmd, updated[0]) : `${cmd === "del" ? "已删除" : cmd === "on" ? "已启用" : "已停用"}配置${ids.join("、")}`;
          await this.accepted(msg); await this.prompt(msg, receipt); return;
        }
        if (cmd === "cd" && /^\d+$/.test(arg) && Number(arg) <= 86400 && !await msg.getReplyMessage()) throw Error("请回复目标消息或使用.track cd 配置序号 秒数");
        if (!gid) { await this.prompt(msg, `请先发送 ${esc(getPrefixes()[0] || ".")}track group @群用户名 或带负号的群ID，选择当前管理群`); return; }
        const next: State = JSON.parse(JSON.stringify(this.state));
        const g = next.groups[gid] ||= { next: 1, cd: 30, rules: [] };
        let cooldownKey: string | undefined;
        let managedRule: Rule | undefined;
        if (cmd === "cd") {
          const args = arg.split(/\s+/);
          const username = args[0]?.startsWith("@") ? args.shift()! : "";
          const seconds = args.join(" ");
          if (!/^\d+$/.test(seconds) || !Number.isSafeInteger(Number(seconds)) || Number(seconds) > 86400) throw Error("冷却范围0–86400整数秒");
          const reply = await msg.getReplyMessage();
          if (username || reply) {
            const u = await this.target(msg, username, gid);
            const matches = g.rules.filter(r => r.user === u.id);
            if (matches.length > 1) throw Error("该用户有多条配置，请用.track list查看编号后操作");
            const r = matches[0];
            if (!r) throw Error("本群没有该目标规则");
            managedRule = r;
            r.intervalSeconds = Number(seconds);
            cooldownKey = `${gid}:${u.id}`;
          } else throw Error("请回复目标消息或使用.track cd 配置序号 秒数");
        } else if (["del", "on", "off"].includes(cmd)) {
          const u = await this.target(msg, arg, gid);
          const matches = g.rules.filter(r => r.user === u.id);
          if (matches.length > 1) throw Error("该用户有多条配置，请用.track list查看编号后操作");
          const r = matches[0];
          if (!r) throw Error("本群没有该目标规则");
          managedRule = r;
          if (cmd === "del") { g.rules = g.rules.filter(x => x !== r); cooldownKey = `${gid}:${r.user}`; }
          else r.enabled = cmd === "on" && repliesOf(r).length > 0;
        } else throw Error(`未知子命令，请使用 ${getPrefixes()[0] || "."}help track`);
        if (!this.live()) return;
        await this.save(next);
        if (cooldownKey) this.cooldown.delete(cooldownKey);
        await this.accepted(msg); await this.prompt(msg, await this.managementReceipt(cmd, managedRule!));
      } catch (e) {
        this.log("command-failed");
        const text = e instanceof Error && /^(配置(?:序号|[1-9]\d*不存在)|未找到配置序号|请指定|请回复|目标必须|冷却范围|本群没有|未知子命令|请选择|请输入群|请输入目标|无法识别此用户ID|请在私聊|该用户有多条配置)/.test(e.message) ? e.message : "操作失败，未保存更改";
        await this.prompt(msg, text);
      }
    });
  } };
  private async dropSession(gid: string) {
    const old = this.sessions.get(gid);
    this.clearTimer(gid);
    this.sessions.delete(gid);
    const previous = this.configPrompts.get(gid);
    this.configPrompts.delete(gid);
    if (old?.owner) {
      const ids = new Set(old.ids || []);
      if (previous !== undefined) ids.add(previous);
      await this.deleteOwn(old.owner, [...ids]);
    }
    if (old?.media) await fs.unlink(path.join(root, old.media.file)).catch(() => undefined);
  }
  private async configure(msg: Api.Message, gid: string) {
    const session = this.sessions.get(gid);
    if (!session) return;
    if (session.expires && Date.now() >= session.expires) { await this.dropSession(gid); return; }
    if (!session.destination) {
      try {
        const selected = await this.selectScope(msg, msg.message || "");
        if (session.pendingTarget) {
          const u = await this.target(msg, session.pendingTarget, selected.id);
          session.user = u.id; session.label = u.label;
        }
        session.destination = selected.id; session.destinationLabel = selected.label;
        this.management.set(gid, selected);
        await this.accepted(msg, true);
        if (session.user) await this.targetPrompt(msg, session);
        else await this.prompt(msg, `已选择生效群：${this.scopeLabel(selected.id, selected.label)}\n请输入目标@用户名、用户ID，或回复目标消息`);
      } catch (e) { await this.prompt(msg, e instanceof Error && /^(请选择|请输入群|请输入目标|无法识别此用户ID|目标必须|请指定)/.test(e.message) ? e.message : "群或目标验证失败，请重新输入"); }
      return;
    }
    if (!session.user) {
      try {
        const u = await this.target(msg, (msg.message || "").trim(), session.destination);
        session.user = u.id; session.label = u.label;
        await this.accepted(msg, true);
        await this.targetPrompt(msg, session);
      } catch (e) { await this.prompt(msg, e instanceof Error && /^(无法识别此用户ID|目标必须)/.test(e.message) ? e.message : "请输入目标@用户名、用户ID，或回复目标消息"); }
      return;
    }
    if (session.text === undefined) {
      if (msg.media || !msg.message?.trim() || msg.message.length > 1000) { await this.prompt(msg, "请输入1–1000字符纯文字追踪内容"); return; }
      session.text = msg.message;
      session.mode = session.text === "*" ? "any" : session.mode === "exact" ? "exact" : "contains";
      await this.accepted(msg, true); await this.prompt(msg, "请发送要自动回复的内容（文字、贴纸、GIF、照片、视频、语音、音频或文件）"); return;
    }
    if (session.replyText !== undefined || session.media !== undefined) {
      const value = msg.message || "";
      if (msg.media || !/^\d+$/.test(value) || !Number.isSafeInteger(Number(value)) || Number(value) > 86400) { await this.prompt(msg, "请输入0–86400的整数秒数字（0表示不限）"); return; }
      try { await this.commitSession(msg, gid, session, Number(value)); }
      catch { await this.dropSession(gid); this.log("configure-failed"); await this.prompt(msg, "配置未保存，旧规则不变；请重新开始"); }
      return;
    }
    let newFile: string | undefined;
    try {
      let reply: Reply;
      if (!msg.media) {
        if (!msg.message?.trim() || msg.message.length > 4096) throw Error();
        reply = { replyText: msg.message };
      } else {
        if (msg.groupedId) throw Error();
        const { meta, ext } = this.classify(msg);
        if (msg.media instanceof Api.MessageMediaDocument && msg.media.document instanceof Api.Document && Number(msg.media.document.size) > 32 * 1024 * 1024) throw Error();
        this.pause(gid);
        const client = await getGlobalClient(); const buffer = await client!.downloadMedia(msg.media!, {});
        if (!Buffer.isBuffer(buffer) || !buffer.length || buffer.length > 32 * 1024 * 1024) throw Error();
        const file = `media/${randomUUID()}.${ext}`; newFile = path.join(root, file);
        await fs.writeFile(newFile, buffer, { mode: 0o600, flag: "wx" });
        if (!this.live()) throw Error();
        reply = { media: { ...meta, file, caption: msg.message || undefined } };
      }
      session.replyText = reply.replyText; session.media = reply.media; newFile = undefined;
      await this.accepted(msg, true); await this.prompt(msg, "请输入发送间隔（0–86400整数秒，0表示不限）");
    } catch {
      if (newFile) await fs.unlink(newFile).catch(() => undefined);
      this.log("configure-failed");
      await this.prompt(msg, "内容未保存，旧规则不变；请发送文字或单条媒体（最多32MiB，不支持相册/投票/联系人/位置）");
    }
  }
  private async commitSession(msg: Api.Message, control: string, s: Session, seconds: number) {
    const next: State = JSON.parse(JSON.stringify(this.state));
    const g = next.groups[s.destination!] ||= { next: 1, cd: 30, rules: [] };
    if ((s.replyText === undefined && !s.media) || g.rules.length >= 100 || next.nextUid >= Number.MAX_SAFE_INTEGER || g.next >= Number.MAX_SAFE_INTEGER) throw Error();
    const r: Rule = { id: g.next++, uid: next.nextUid++, username: this.labelUsername(s.label!), user: s.user!, label: s.label!, mode: s.mode, text: s.text!, enabled: true, intervalSeconds: seconds, ...(s.media ? { media: s.media } : { replyText: s.replyText! }) };
    g.rules.push(r);
    if (!this.live()) throw Error();
    this.pause(control); await this.save(next);
    // The persisted rule owns its media only after the atomic commit.
    s.media = undefined; s.replyText = undefined;
    await this.accepted(msg, true); await this.dropSession(control);
    const trigger = r.mode === "any" ? "任意消息" : `${r.mode === "exact" ? "完全匹配" : "包含"} ${preview(r.text, 100)}`;
    await this.prompt(msg, `已新增配置#${r.uid}，已开启自动回复\n配置：${r.uid}\n范围：${this.scopeLabel(s.destination!, s.destinationLabel)}\n用户：${preview(r.label, 140)}\n触发：${trigger}\n回复：${replySummary(r)}\n发送间隔：${r.intervalSeconds}秒`);
  }

  listenMessageHandler = async (msg: Api.Message, options?: { isEdited?: boolean }) => {
    if (!this.live() || options?.isEdited || msg.editDate) return;
    const receivedAt = Date.now();
    const gid = this.group(msg);
    const control = this.control(msg);
    if (!control || (!gid && !this.privateChat(msg)) || (msg.out && this.isCommand(msg))) return;
    await this.serial(async () => {
      try {
        if (!this.live()) return;
        await this.load();
        if (msg.out) {
          const key = `${control}:${msg.id}`;
          if (this.internal.has(key)) return;
          if (!this.markSeen(key, 300000)) return;
          await this.configure(msg, control); return;
        }
        if (!gid || !(msg.fromId instanceof Api.PeerUser)) return;
        if (!msg.senderId) return;
        const uid = msg.senderId.toString();
        const specific = this.state.groups[gid]?.rules.filter(r => r.user === uid);
        const scope = specific?.length ? gid : "all";
        const rules = specific?.length ? specific : this.state.groups.all?.rules || [];
        const text = msg.message || "";
        // contains 模式下，触发词用空格分隔为多个关键词，命中任意一个即可回复。
        const r = [...rules].sort((a,b) => Number(a.mode === "any") - Number(b.mode === "any") || a.uid - b.uid).find(r => r.enabled && repliesOf(r).length > 0 && r.user === uid && (r.mode === "any" || (r.mode === "exact" ? text === r.text : r.text.trim().split(/\s+/).filter(Boolean).some(keyword => text.includes(keyword)))));
        if (!r || !repliesOf(r).length) return;
        const key = `${gid}:${msg.id}`, now = Date.now();
        if (!this.markSeen(key, 24 * 3600 * 1000)) return;
        if (receivedAt < (this.cooldown.get(`${scope}:${uid}`) || 0)) return;
        if (!await this.user(msg) || !this.live()) return;
        // Reserve cooldown even on failure: no retries and no error-induced storm.
        this.cooldown.set(`${scope}:${uid}`, now + r.intervalSeconds * 1000);
        const client = await getGlobalClient();
        if (!this.live()) return;
        const selected = repliesOf(r)[0];
        if (selected.replyText !== undefined) {
          const sent = await client!.sendMessage(msg.chatId!, { message: selected.replyText, replyTo: msg.id, parseMode: false });
          this.remember(gid, sent); return;
        }
        if (!selected.media) return;
        const local = path.join(root, selected.media.file);
        let file: string | Api.InputMediaUploadedDocument = local;
        if (selected.media.kind !== "photo") {
          const stat = await fs.stat(local);
          const uploaded = await client!.uploadFile({ file: new CustomFile(path.basename(local), stat.size, local), workers: 1 });
          const ext = path.extname(local).slice(1);
          const mime = ({ webp: "image/webp", tgs: "application/x-tgsticker", webm: "video/webm", gif: "image/gif", mp4: "video/mp4" } as Record<string,string>)[ext];
          file = new Api.InputMediaUploadedDocument({ file: uploaded, mimeType: selected.media.mime || mime || "application/octet-stream", attributes: this.attributes(selected.media), forceFile: selected.media.kind === "file" && !selected.media.video && !selected.media.audio });
        }
        if (!this.live()) return;
        const sent = await client!.sendFile(msg.chatId!, { file, caption: selected.media.caption, parseMode: false, replyTo: msg.id, forceDocument: selected.media.kind !== "photo", workers: 1 });
        this.remember(gid, sent);
      } catch { this.log("send-or-listen-failed"); }
    });
  };
}
export default new TrackPlugin();
