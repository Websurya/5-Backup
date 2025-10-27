import os from 'os';
import express from 'express';
import multer from 'multer';
import unzipper from 'unzipper';
import { XMLParser } from 'fast-xml-parser';
import imageSize from 'image-size';
import path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import cors from 'cors';
import * as cheerio from 'cheerio';

const app = express();
app.use(cors({ origin: true }));
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
app.use(express.static(path.join(__dirname, '../web')));
app.options('*', cors());

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 100 * 1024 * 1024 }
});

const DEFAULT_PIXEL_THRESHOLD = 5_600_000;
const IMG_EXT_REGEX = /\.(png|jpe?g|gif)$/i;

const normalize = (p) => p.replace(/\\/g, '/');
const safeJoin = (...segs) => normalize(path.posix.join(...segs));

function stripQueryAndHash(ref) {
  return ref.split('#')[0].split('?')[0];
}
function decodeRef(ref) {
  try { return decodeURI(ref); } catch { return ref; }
}

function resolveRef(baseFilePath, href, contentRoot = '') {
  if (!href) return '';
  if (/^([a-z]+:)?\/\//i.test(href) || href.startsWith('data:')) return '';
  let clean = stripQueryAndHash(href.trim());
  clean = decodeRef(clean);
  if (clean.startsWith('/')) clean = clean.slice(1);
  const baseDir = normalize(baseFilePath).replace(/[^/]+$/, '');
  const abs = normalize(path.posix.normalize(safeJoin(baseDir, clean)));
  if (contentRoot) {
    const relToRoot = path.posix.relative(normalize(contentRoot), abs);
    if (relToRoot.startsWith('..')) return '';
  }
  return abs;
}

function getContentRootFromContainerXml(xml) {
  try {
    const parser = new XMLParser({ ignoreAttributes: false });
    const doc = parser.parse(xml);
    const rootfiles = doc?.container?.rootfiles;
    let fullPath = '';
    if (Array.isArray(rootfiles?.rootfile)) {
      fullPath = rootfiles.rootfile[0]?.['@_full-path'] || '';
    } else {
      fullPath = rootfiles?.rootfile?.['@_full-path'] || '';
    }
    if (!fullPath) return '';
    return normalize(fullPath.replace(/[^/]+$/, ''));
  } catch {
    return '';
  }
}

function getManifestImagesFromOpf(xml) {
  try {
    const parser = new XMLParser({ ignoreAttributes: false });
    const doc = parser.parse(xml);
    const manifest = doc?.package?.manifest?.item;
    const items = Array.isArray(manifest) ? manifest : (manifest ? [manifest] : []);
    const hrefs = new Set();
    for (const it of items) {
      const mt = it?.['@_media-type'] || '';
      const href = it?.['@_href'] || '';
      if (mt.startsWith('image/') && href) hrefs.add(normalize(href));
    }
    return hrefs;
  } catch { return new Set(); }
}

async function unzipToMap(buffer) {
  const directory = await unzipper.Open.buffer(buffer);
  const files = new Map();
  for (const f of directory.files) {
    if (f.type !== 'File') continue;
    const p = normalize(f.path);
    const buf = await f.buffer();
    files.set(p, { path: p, buffer: buf });
  }
  return files;
}

function listCandidateImages(files, options) {
  const exts = ['.png', '.jpg', '.jpeg', '.gif'];
  const isImageFile = (p) => exts.some(e => p.toLowerCase().endsWith(e));
  const inImagesFolder = (p) => p.toLowerCase().split('/').includes('images');
  const out = [];
  for (const p of files.keys()) {
    if (!isImageFile(p)) continue;
    if (!options.searchAllImages && !inImagesFolder(p)) continue;
    out.push(p);
  }
  return out;
}

function extractUrlsFromCss(cssText) {
  const urls = [];
  const re = /url\(\s*(["']?)([^)"']+)\1\s*\)/gi;
  let m; while ((m = re.exec(cssText)) !== null) { urls.push(m[2]); }
  return urls;
}

function collectReferences({ files, contentRoot, options }) {
  const usedAbs = new Set();
  const docExts = ['.xhtml'];
  if (options.includeHtml) docExts.push('.html', '.htm');
  const isDoc = (p) => docExts.some(e => p.toLowerCase().endsWith(e));
  const isCss = (p) => p.toLowerCase().endsWith('.css');
  const isSvg = (p) => p.toLowerCase().endsWith('.svg');

  for (const [p, obj] of files.entries()) {
    if (isDoc(p)) {
      const html = obj.buffer.toString('utf8');
      const $ = cheerio.load(html, { xmlMode: p.toLowerCase().endsWith('.xhtml') });
      $('img[src]').each((_, el) => {
        const href = $(el).attr('src');
        const abs = resolveRef(p, href, contentRoot); if (abs) usedAbs.add(abs);
      });
      $('[srcset]').each((_, el) => {
        const srcset = ($(el).attr('srcset') || '').split(',');
        for (const part of srcset) {
          const href = part.trim().split(' ')[0];
          const abs = resolveRef(p, href, contentRoot); if (abs) usedAbs.add(abs);
        }
      });
      $('[style]').each((_, el) => {
        const st = $(el).attr('style') || '';
        for (const u of extractUrlsFromCss(st)) {
          const abs = resolveRef(p, u, contentRoot); if (abs) usedAbs.add(abs);
        }
      });
      if (options.includeSvg) {
        $('image').each((_, el) => {
          const href = $(el).attr('href') || $(el).attr('xlink:href');
          const abs = resolveRef(p, href, contentRoot); if (abs) usedAbs.add(abs);
        });
      }
    }
    if (options.includeCss && isCss(p)) {
      const css = obj.buffer.toString('utf8');
      for (const u of extractUrlsFromCss(css)) {
        const abs = resolveRef(p, u, contentRoot); if (abs) usedAbs.add(abs);
      }
    }
    if (options.includeSvg && isSvg(p)) {
      const xml = obj.buffer.toString('utf8');
      try {
        const $ = cheerio.load(xml, { xmlMode: true });
        $('image').each((_, el) => {
          const href = $(el).attr('href') || $(el).attr('xlink:href');
          const abs = resolveRef(p, href, contentRoot); if (abs) usedAbs.add(abs);
        });
        $('style').each((_, el) => {
          const css = $(el).text();
          for (const u of extractUrlsFromCss(css)) {
            const abs = resolveRef(p, u, contentRoot); if (abs) usedAbs.add(abs);
          }
        });
      } catch { /* ignore malformed SVG */ }
    }
  }
  return usedAbs;
}

function buildFullPath(internalPath, epubBasePath) {
  return epubBasePath ? path.join(epubBasePath, internalPath) : internalPath;
}

app.post('/api/validate', upload.single('epub'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'Missing file field "epub"' });

    const q = req.query || {};
    const options = {
      includeHtml: q.includeHtml === 'true',
      includeCss: q.includeCss === 'true',
      includeSvg: q.includeSvg === 'true',
      searchAllImages: q.searchAllImages === 'true',
      followOpfManifestOnly: q.followOpfManifestOnly === 'true',
      pixelThreshold: Number(q.pixelThreshold || DEFAULT_PIXEL_THRESHOLD) || DEFAULT_PIXEL_THRESHOLD
    };
    const epubBasePath = q.epubBasePath ? q.epubBasePath.replace(/[\\\/]$/, '') : '';

    const files = await unzipToMap(req.file.buffer);

    const container = files.get('META-INF/container.xml');
    let contentRoot = '';
    if (container) {
      contentRoot = getContentRootFromContainerXml(container.buffer.toString('utf8'));
    }

    let manifestAbs = new Set();
    if (options.followOpfManifestOnly && contentRoot) {
      for (const p of files.keys()) {
        if (p.startsWith(contentRoot) && p.toLowerCase().endsWith('.opf')) {
          const xml = files.get(p).buffer.toString('utf8');
          const hrefs = getManifestImagesFromOpf(xml);
          manifestAbs = new Set([...hrefs].map(h => normalize(path.posix.join(normalize(contentRoot), normalize(h)))));
          break;
        }
      }
    }

    const candidateList = listCandidateImages(files, options);
    const candidatesAbs = [];
    for (const p of candidateList) {
      if (options.followOpfManifestOnly && !manifestAbs.has(p)) continue;
      candidatesAbs.push(p);
    }

    const usedAbs = collectReferences({ files, contentRoot, options });

    const unreferenced = [];
    const oversized = [];

    for (const abs of candidatesAbs) {
      if (!usedAbs.has(abs)) {
        unreferenced.push({ file: path.posix.basename(abs), internalPath: abs, fullPath: buildFullPath(abs, epubBasePath) });
      }
      if (IMG_EXT_REGEX.test(abs)) {
        try {
          const dim = imageSize(files.get(abs).buffer);
          if (dim?.width && dim?.height && (dim.width * dim.height >= options.pixelThreshold)) {
            const px = dim.width * dim.height;
            oversized.push({
              file: path.posix.basename(abs),
              internalPath: abs,
              fullPath: buildFullPath(abs, epubBasePath),
              width: dim.width,
              height: dim.height,
              px
            });
          }
        } catch {}
      }
    }

    let report = '';
    report += `**Unused Images**\n`;
    if (unreferenced.length) {
      unreferenced.forEach((it, i) => { report += `${i + 1}. ${it.file} - ${it.fullPath}\n`; });
    } else {
      report += 'None\n';
    }
    report += `\n**Images Exceeding Pixel Limit (>= ${options.pixelThreshold.toLocaleString()} pixels)**\n`;
    if (oversized.length) {
      oversized.forEach((it, i) => {
        report += `${i + 1}. ${it.file}: ${it.width}x${it.height} = ${it.px} pixels - ${it.fullPath}\n`;
      });
    } else {
      report += 'None\n';
    }

    res.json({ unreferenced, oversized, reportText: report, contentRoot, epubBasePath });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e?.message || 'Server error' });
  }
});

app.get('/health', (_, res) => res.json({ ok: true }));

function getLocalIPv4s() {
  const ifaces = os.networkInterfaces();
  const addrs = [];
  for (const name of Object.keys(ifaces)) {
    for (const iface of ifaces[name] || []) {
      if (iface && iface.family === 'IPv4' && !iface.internal) {
        addrs.push(iface.address);
      }
    }
  }
  return addrs;
}

if (!process.env.AWS_LAMBDA_FUNCTION_NAME) {
  const host = process.env.HOST || '0.0.0.0';
  const port = Number(process.env.PORT) || 6000;
  app.listen(port, host, () => {
    const publicUrl = process.env.PUBLIC_BASE_URL?.replace(/\/$/, '');
    if (publicUrl) {
      console.log(`Listening on ${publicUrl}`);
      console.log(`Health:   ${publicUrl}/health`);
      console.log(`Validate: ${publicUrl}/api/validate`);
      return;
    }
    const ips = getLocalIPv4s();
    if (ips.length) {
      console.log('Listening on:');
      for (const ip of ips) console.log(`  → http://${ip}:${port}`);
      console.log(`(Also reachable on 127.0.0.1 and ::1 if local firewall allows)`);
    } else {
      console.log(`Listening on http://127.0.0.1:${port}`);
    }
  });
}

export default app;
