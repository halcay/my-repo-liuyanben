// 留言本 + 文件上传下载 后端（Cloudflare Worker + KV）
// KV 键：msg:<倒序时间>-<随机>  → JSON 留言
//        file:<id>              → 文件二进制，metadata 存 {name,size,type,time}
// 删除需要请求头 X-Admin-Key 等于密钥 ADMIN_KEY（在 Cloudflare 后台设置）

const MAX_FILE = 20 * 1024 * 1024;    // KV：20MB（KV 单值上限 25MB）
const MAX_FILE_R2 = 95 * 1024 * 1024; // R2：95MB（Workers 免费版请求体上限 100MB）
const MAX_BIG = 5 * 1024 * 1024 * 1024; // 分片上传：单个文件最大 5GB（只开放给管理员，防止被人刷爆存储）
const MAX_MSG = 1000;

const MIME = {
  mp4: 'video/mp4', m4v: 'video/mp4', webm: 'video/webm', ogv: 'video/ogg', mov: 'video/quicktime',
  mp3: 'audio/mpeg', m4a: 'audio/mp4', aac: 'audio/aac', wav: 'audio/wav', ogg: 'audio/ogg', flac: 'audio/flac',
  png: 'image/png', jpg: 'image/jpeg', jpeg: 'image/jpeg', gif: 'image/gif', webp: 'image/webp', avif: 'image/avif', bmp: 'image/bmp',
  pdf: 'application/pdf', txt: 'text/plain', log: 'text/plain', md: 'text/plain', csv: 'text/plain',
};
const mimeByName = n => MIME[(String(n || '').split('.').pop() || '').toLowerCase()];

const json = (data, status = 200) =>
  new Response(JSON.stringify(data), { status, headers: { 'content-type': 'application/json; charset=utf-8' } });
const rid = () => Math.random().toString(36).slice(2, 10);
const revTs = () => String(9999999999999 - Date.now()).padStart(13, '0');
const isAdmin = (req, env) => env.ADMIN_KEY && req.headers.get('x-admin-key') === env.ADMIN_KEY;

// 文件夹名：去掉斜杠和首尾空格，最长 40 字；空字符串 = 根目录
const cleanFolder = v => String(v || '').replace(/[\\/]/g, '').trim().slice(0, 40);

// 列出全部文件（带 folder 字段）
async function listFiles(env, useR2) {
  const out = [];
  if (useR2) {
    let cursor;
    do {
      const r = await env.R2.list({ limit: 1000, cursor, include: ['customMetadata'] });
      for (const o of r.objects) out.push({ id: o.key, name: o.customMetadata?.name || o.key, size: o.size,
        type: o.customMetadata?.type, folder: o.customMetadata?.folder || '',
        time: Number(o.customMetadata?.time) || o.uploaded.getTime() });
      cursor = r.truncated ? r.cursor : null;
    } while (cursor && out.length < 10000);
  } else {
    const keys = await listAll(env.KV, 'file:', 1000);
    for (const k of keys) out.push({ id: k.name.slice(5), folder: '', ...(k.metadata || {}) });
  }
  return out;
}

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
      // 绑定了 R2（env.R2）就存 R2，单文件上限 95MB（免费版请求体上限 100MB）；否则退回 KV，上限 20MB
      const useR2 = !!env.R2;
      const maxFile = useR2 ? MAX_FILE_R2 : MAX_FILE;
      if (p === '/api/config' && req.method === 'GET') {
        return json({ storage: useR2 ? 'R2' : 'KV', maxFile, maxBig: useR2 ? MAX_BIG : 0 });
      }
      // ---------- 大文件分片上传（仅 R2、仅管理员）----------
      // 1) start 建立上传  2) part 逐片上传（每片 < 95MB）  3) complete 合并  /  abort 取消
      if (p.startsWith('/api/upload/')) {
        if (!useR2) return json({ error: '大文件上传需要绑定 R2' }, 400);
        if (!isAdmin(req, env)) return json({ error: '大文件只有管理员可以上传，请先点“管理”输入密钥' }, 403);
        if (p === '/api/upload/start' && req.method === 'POST') {
          const b = await req.json().catch(() => ({}));
          const size = Number(b.size) || 0;
          if (size > MAX_BIG) return json({ error: `文件超过 ${MAX_BIG / 1073741824}GB` }, 413);
          const id = Date.now().toString(36) + rid();
          const folder = cleanFolder(b.folder);
          const type = String(b.type || 'application/octet-stream').slice(0, 100);
          const name = String(b.name || id).split(/[\\/]/).pop().slice(0, 200);
          const mpu = await env.R2.createMultipartUpload(id, { httpMetadata: { contentType: type },
            customMetadata: { name, type, time: String(Date.now()), folder } });
          return json({ id, uploadId: mpu.uploadId, folder });
        }
        const id = url.searchParams.get('id') || '', uploadId = url.searchParams.get('uploadId') || '';
        if (!/^\w+$/.test(id) || !uploadId) return json({ error: '参数错误' }, 400);
        const mpu = env.R2.resumeMultipartUpload(id, uploadId);
        if (p === '/api/upload/part' && req.method === 'PUT') {
          const n = Number(url.searchParams.get('part'));
          if (!(n >= 1 && n <= 10000) || !req.body) return json({ error: '分片参数错误' }, 400);
          const part = await mpu.uploadPart(n, req.body);
          return json(part);
        }
        if (p === '/api/upload/complete' && req.method === 'POST') {
          const b = await req.json().catch(() => ({}));
          const parts = (b.parts || []).map(x => ({ partNumber: Number(x.partNumber), etag: String(x.etag) }));
          if (!parts.length) return json({ error: '没有分片' }, 400);
          const o = await mpu.complete(parts);
          const folder = cleanFolder(b.folder);
          if (folder && !(await env.KV.get('dir:' + folder))) await env.KV.put('dir:' + folder, '1');
          return json({ ok: true, id, size: o.size });
        }
        if (p === '/api/upload/abort' && req.method === 'POST') {
          await mpu.abort().catch(() => {});
          return json({ ok: true });
        }
        return json({ error: 'Not found' }, 404);
      }

      if (p === '/api/files' && req.method === 'GET') {
        const folder = cleanFolder(url.searchParams.get('folder'));
        const files = (await listFiles(env, useR2)).filter(f => (f.folder || '') === folder);
        files.sort((a, b) => (b.time || 0) - (a.time || 0));
        return json(files);
      }

      // ---------- 文件夹 ----------
      if (p === '/api/folders' && req.method === 'GET') {
        const files = await listFiles(env, useR2);
        const map = {};
        for (const k of await listAll(env.KV, 'dir:', 1000)) map[k.name.slice(4)] = { count: 0, size: 0 };
        for (const f of files) if (f.folder) {
          const d = map[f.folder] || (map[f.folder] = { count: 0, size: 0 });
          d.count++; d.size += f.size || 0;
        }
        const list = Object.entries(map).map(([name, d]) => ({ name, ...d }));
        list.sort((a, b) => a.name.localeCompare(b.name, 'zh', { numeric: false }));
        return json(list);
      }
      if (p === '/api/folders' && req.method === 'POST') {
        const b = await req.json().catch(() => ({}));
        const name = cleanFolder(b.name);
        if (!name) return json({ error: '文件夹名不能为空' }, 400);
        await env.KV.put('dir:' + name, '1');
        return json({ ok: true, name });
      }
      if (p.startsWith('/api/folders/') && req.method === 'DELETE') {
        if (!isAdmin(req, env)) return json({ error: '管理密钥错误' }, 403);
        const name = cleanFolder(decodeURIComponent(p.slice('/api/folders/'.length)));
        if ((await listFiles(env, useR2)).some(f => f.folder === name)) return json({ error: '文件夹不为空，请先删除里面的文件' }, 400);
        await env.KV.delete('dir:' + name);
        return json({ ok: true });
      }
      if (p === '/api/files' && req.method === 'POST') {
        const form = await req.formData();
        const f = form.get('file');
        if (!f || typeof f === 'string') return json({ error: '没有文件' }, 400);
        if (f.size > maxFile) return json({ error: `文件超过 ${maxFile / 1048576}MB` }, 413);
        const id = Date.now().toString(36) + rid();
        const folder = cleanFolder(form.get('folder'));
        const meta = { name: f.name.split(/[\\/]/).pop().slice(0, 200), size: f.size, type: f.type || mimeByName(f.name) || 'application/octet-stream', time: Date.now(), folder };
        if (useR2) {
          await env.R2.put(id, f, { httpMetadata: { contentType: meta.type },
            customMetadata: { name: meta.name, type: meta.type, time: String(meta.time), folder } });
        } else {
          await env.KV.put('file:' + id, await f.arrayBuffer(), { metadata: meta });
        }
        // 记录文件夹（已存在就不重复写，节省 KV 写入次数）
        if (folder && !(await env.KV.get('dir:' + folder))) await env.KV.put('dir:' + folder, '1');
        return json({ ok: true, id, ...meta });
      }
      // 文件 ID 允许任意 R2 键（例如在 Cloudflare 后台直接上传的 “xxx.webm”、带中文/横杠的名字）
      const m0 = p.match(/^\/api\/files\/(.+)$/);
      let m = null;
      if (m0) { try { m = [m0[0], decodeURIComponent(m0[1])]; } catch (e) { return json({ error: '文件地址错误' }, 400); } }
      if (m && req.method === 'GET') {
        // ?view=1：在浏览器里直接查看/播放；否则下载
        const view = url.searchParams.get('view') === '1';
        let body, name, type, status = 200;
        const extra = {};
        if (useR2) {
          // 支持 Range 请求，视频可以拖动进度条
          const o = await env.R2.get(m[1], { range: req.headers });
          if (!o) return json({ error: '文件不存在' }, 404);
          body = o.body; name = o.customMetadata?.name; type = o.customMetadata?.type;
          extra['accept-ranges'] = 'bytes';
          if (o.range && req.headers.has('range')) {
            const off = o.range.offset ?? 0;
            const len = o.range.length ?? (o.size - off);
            status = 206;
            extra['content-range'] = `bytes ${off}-${off + len - 1}/${o.size}`;
            extra['content-length'] = String(len);
          } else {
            extra['content-length'] = String(o.size);
          }
        } else {
          const { value, metadata } = await env.KV.getWithMetadata('file:' + m[1], 'stream');
          if (!value) return json({ error: '文件不存在' }, 404);
          body = value; name = metadata?.name; type = metadata?.type;
        }
        // 上传时浏览器没给类型（常见于 webm/mkv/m4a 等），按扩展名补上，否则 nosniff 下浏览器拒绝播放
        if (!type || /^application\/octet-stream/i.test(type)) type = mimeByName(name) || 'application/octet-stream';
        // 安全：只允许图片、音视频、PDF、纯文本直接打开；网页、SVG 等一律下载，防止上传恶意网页
        const safe = /^(image\/(png|jpe?g|gif|webp|avif|bmp)|video\/|audio\/|application\/pdf$|text\/plain)/i.test(type);
        const inline = view && safe;
        if (inline && /^text\//i.test(type)) type = 'text/plain; charset=utf-8';
        return new Response(body, { status, headers: {
          ...extra,
          'content-type': type,
          'x-content-type-options': 'nosniff',
          ...(inline ? { 'content-security-policy': 'sandbox' } : {}),
          'content-disposition': `${inline ? 'inline' : 'attachment'}; filename*=UTF-8''${encodeURIComponent(name || m[1])}`,
        } });
      }
      if (m && req.method === 'DELETE') {
        if (!isAdmin(req, env)) return json({ error: '管理密钥错误' }, 403);
        if (useR2) await env.R2.delete(m[1]); else await env.KV.delete('file:' + m[1]);
        return json({ ok: true });
      }

      return json({ error: 'Not found' }, 404);
    } catch (e) {
      return json({ error: e.message }, 500);
    }
  },
};
