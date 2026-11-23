// 留言本 + 文件上传下载 后端（Cloudflare Worker + KV）
// KV 键：msg:<倒序时间>-<随机>  → JSON 留言
//        file:<id>              → 文件二进制，metadata 存 {name,size,type,time}
// 删除需要请求头 X-Admin-Key 等于密钥 ADMIN_KEY（在 Cloudflare 后台设置）

const MAX_FILE = 20 * 1024 * 1024; // 20MB（KV 单值上限 25MB）
const MAX_MSG = 1000;

const json = (data, status = 200) =>
  new Response(JSON.stringify(data), { status, headers: { 'content-type': 'application/json; charset=utf-8' } });
const rid = () => Math.random().toString(36).slice(2, 10);
const revTs = () => String(9999999999999 - Date.now()).padStart(13, '0');
const isAdmin = (req, env) => env.ADMIN_KEY && req.headers.get('x-admin-key') === env.ADMIN_KEY;

async function listAll(kv, prefix, limit = 200) {
  const r = await kv.list({ prefix, limit });
  return r.keys;
}

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const p = url.pathname;
    if (!p.startsWith('/api/')) return env.ASSETS.fetch(req);
    if (!env.KV) return json({ error: '未绑定 KV（检查 wrangler.jsonc 的 kv_namespaces）' }, 500);

    try {
      // ---------- 校验管理密钥 ----------
      if (p === '/api/admin') {
        if (!env.ADMIN_KEY) return json({ ok: false, error: '服务器未设置 ADMIN_KEY（Worker → 设置 → 变量和机密，类型选“密钥/Secret”）' }, 500);
        return isAdmin(req, env) ? json({ ok: true }) : json({ ok: false, error: '管理密钥错误' }, 403);
      }

      // ---------- 留言 ----------
      if (p === '/api/messages' && req.method === 'GET') {
        const keys = await listAll(env.KV, 'msg:', 100);
        const msgs = await Promise.all(keys.map(async k => ({ id: k.name, ...(await env.KV.get(k.name, 'json')) })));
        const admin = isAdmin(req, env);
        return json(msgs.filter(m => m.text && (!m.private || admin)));
      }
      if (p === '/api/messages' && req.method === 'POST') {
        const b = await req.json().catch(() => ({}));
        const name = String(b.name || '匿名').trim().slice(0, 30) || '匿名';
        const text = String(b.text || '').trim().slice(0, MAX_MSG);
        if (!text) return json({ error: '内容不能为空' }, 400);
        const key = `msg:${revTs()}-${rid()}`;
        await env.KV.put(key, JSON.stringify({ name, text, time: Date.now(), private: !!b.private }));
        return json({ ok: true, id: key });
      }
      if (p.startsWith('/api/messages/') && req.method === 'DELETE') {
        if (!isAdmin(req, env)) return json({ error: '管理密钥错误' }, 403);
        await env.KV.delete(decodeURIComponent(p.slice('/api/messages/'.length)));
        return json({ ok: true });
      }

      // ---------- 文件 ----------
      if (p === '/api/files' && req.method === 'GET') {
        const keys = await listAll(env.KV, 'file:', 500);
        const files = keys.map(k => ({ id: k.name.slice(5), ...(k.metadata || {}) }));
        files.sort((a, b) => (b.time || 0) - (a.time || 0));
        return json(files);
      }
      if (p === '/api/files' && req.method === 'POST') {
        const form = await req.formData();
        const f = form.get('file');
        if (!f || typeof f === 'string') return json({ error: '没有文件' }, 400);
        if (f.size > MAX_FILE) return json({ error: '文件超过 20MB' }, 413);
        const id = Date.now().toString(36) + rid();
        const meta = { name: f.name.slice(0, 200), size: f.size, type: f.type || 'application/octet-stream', time: Date.now() };
        await env.KV.put('file:' + id, await f.arrayBuffer(), { metadata: meta });
        return json({ ok: true, id, ...meta });
      }
      const m = p.match(/^\/api\/files\/([\w]+)$/);
      if (m && req.method === 'GET') {
        const { value, metadata } = await env.KV.getWithMetadata('file:' + m[1], 'stream');
        if (!value) return json({ error: '文件不存在' }, 404);
        const name = (metadata && metadata.name) || m[1];
        return new Response(value, { headers: {
          'content-type': (metadata && metadata.type) || 'application/octet-stream',
          'content-disposition': `attachment; filename*=UTF-8''${encodeURIComponent(name)}`,
        } });
      }
      if (m && req.method === 'DELETE') {
        if (!isAdmin(req, env)) return json({ error: '管理密钥错误' }, 403);
        await env.KV.delete('file:' + m[1]);
        return json({ ok: true });
      }

      return json({ error: 'Not found' }, 404);
    } catch (e) {
      return json({ error: e.message }, 500);
    }
  },
};
