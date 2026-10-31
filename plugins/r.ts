import { Plugin } from "@utils/pluginBase";
import { getPrefixes } from "@utils/pluginManager";
import { safeGetReplyMessage } from "@utils/safeGetMessages";
import { getGlobalClient } from "@utils/runtimeManager";

const htmlEscape = (text: string): string =>
  String(text || "").replace(/[&<>"']/g, (m) =>
    (({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#x27;" } as any)[m] || m)
  );

class RepeatPlugin extends Plugin {
  name = "复读";
  description: string = "复读消息";

  cmdHandlers: Record<string, (msg: any) => Promise<void>> = {
    r: async (msg: any) => {
      const replied = await safeGetReplyMessage(msg);
      if (!replied) {
        await msg.edit({ text: "💡 请回复一条消息后使用" });
        return;
      }

      try {
        const client = await getGlobalClient();
        if (!client) {
          await msg.edit({ text: "❌ 无法连接" });
          return;
        }

        // 直接转发被回复的消息内容
        await client.sendMessage(msg.peerId, {
          message: replied.message || " ",
        });

        // 删除原命令
        await msg.delete({ revoke: true });
      } catch (e: any) {
        await msg.edit({ text: `❌ 复读失败: ${htmlEscape(e?.message || "")}` });
      }
    },
  };
}

export default new RepeatPlugin();
