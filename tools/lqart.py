"""Listening Queue articles: blog posts read aloud in full, or as an audio summary.

Run by the "Listening Queue articles" scheduled task, which downloads the queue's database into DIR
(ArtifactData out_dir dumps: DIR/config/*.json, DIR/feed/art.json, DIR/state/*.json, DIR/deep/*.json) and then:

  python3 -I lqart.py feeds DIR
      Looks up newly added blogs (DIR/config/blogSubs.json), drops removed ones, reads every blog's feed and writes
      DIR/out/blogs.json (the new config/blogs document) and DIR/out/art.json (the new feed/art document).
  python3 -I lqart.py prep DIR
      Picks the posts that need audio: posts set aside in the queue (state later) with the version asked for in
      config/artMode (f = read in full, s = summary, b = both), plus requests from the player's drop-box sheet
      (DIR/loop.json rows "s-<id>" and "f-<id>"). Fetches each post's text into DIR/art/<id>.json, writes
      DIR/chunks/<id>.txt and DIR/jobs.json for the ones that need a summary, DIR/need.json, and DIR/sbatch.json
      (state / artMode writes for player requests).
  python3 -I lqart.py finalize DIR
      Reads DIR/sum/<id>.json (a JSON array of summary paragraphs) and writes DIR/artbatch_<n>.json: the
      ArtifactData batch writes for deep/<id> and atext/<id>.

deep/<id> for a post: {src: 'art', at, p: [[secs, text], ...] (summary), ft: paragraph count of the full text
(the text itself is atext/<id> = {t: [...]}), err / serr / ferr}. The audio publisher (lqdeep.py speak) then adds
sa/ss/sd for the summary and fa/fd for the full read-aloud.
"""
import datetime as dt, email.utils, glob, hashlib, html, json, os, re, sys, time, urllib.error, urllib.parse, urllib.request
import xml.etree.ElementTree as ET
from html.parser import HTMLParser

NOW = dt.datetime.now(dt.timezone.utc)
ISO = lambda d: d.strftime('%Y-%m-%dT%H:%M:%SZ')
DAYS = 14           # posts newer than this are listed
PER_BLOG = 10       # at most this many posts per blog
KEEP_DAYS = 21      # a listed post stays this long even after it leaves the blog's feed
WPM = 165           # read-aloud speed, for the estimated length
MIN_WORDS = 120     # fewer words than this is only a teaser, not the post
MAX_WORDS = 9000    # longer posts are read up to here
SUM_WORDS = 650     # one summary paragraph per this many words
DONE = {'done', 'listened', 'removed'}
UA = {'User-Agent': 'Mozilla/5.0 (compatible; ListeningQueue/1.0; feed reader)', 'Accept': '*/*'}


def load(path, default=None):
    try:
        d = json.load(open(path))
        return d.get('data', d) if isinstance(d, dict) and set(d) >= {'data'} and len(d) <= 3 else d
    except Exception:
        return default


def version(path):
    try:
        d = json.load(open(path))
        return d.get('version') if isinstance(d, dict) else None
    except Exception:
        return None


def norm(s):
    return str(s or '').strip().lower()


def pdate(s):
    s = str(s or '').strip()
    if not s:
        return None
    try:
        d = dt.datetime.fromisoformat(s.replace('Z', '+00:00'))
    except Exception:
        try:
            d = email.utils.parsedate_to_datetime(s)
        except Exception:
            return None
    return d.replace(tzinfo=dt.timezone.utc) if d.tzinfo is None else d.astimezone(dt.timezone.utc)


def get(url, limit=4 << 20):
    req = urllib.request.Request(url, headers=UA)
    for attempt in (1, 2):          # one more try after a dropped connection
        try:
            with urllib.request.urlopen(req, timeout=40) as r:
                raw = r.read(limit)
                return raw, r.geturl(), (r.headers.get_content_charset() or 'utf-8')
        except urllib.error.HTTPError:
            raise
        except Exception:
            if attempt == 2:
                raise
            time.sleep(3)


def text_of(raw, cs='utf-8'):
    try:
        return raw.decode(cs, 'replace')
    except Exception:
        return raw.decode('utf-8', 'replace')


# ---------- HTML to paragraphs ----------
SKIP = {'script', 'style', 'noscript', 'nav', 'footer', 'aside', 'form', 'figure', 'figcaption', 'svg', 'button',
        'iframe', 'select', 'template', 'head', 'header', 'sup', 'audio', 'video', 'table'}
BLOCK = {'p', 'li', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'blockquote', 'pre', 'div', 'section', 'article', 'main',
         'ul', 'ol', 'br', 'hr', 'td', 'tr', 'dd', 'dt'}
VOID = {'br', 'hr', 'img', 'input', 'meta', 'link', 'source', 'wbr', 'area', 'base', 'col', 'embed', 'track'}
JUNK = re.compile(r'(comment|share|social|related|sidebar|subscribe|newsletter|promo|advert|footer|byline|author-bio|'
                  r'breadcrumb|cookie|popup|modal|paywall|signup|sign-up|tags?-list|post-meta|wp-block-buttons)', re.I)


class Paras(HTMLParser):
    """Collects readable paragraphs. With want_article, only text inside the page's <article> (or <main>) counts
    when the page has one."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.skip, self.stack, self.buf = 0, [], []
        self.out = []            # (in_article, text)
        self.art = 0

    def flush(self):
        t = re.sub(r'\s+', ' ', ''.join(self.buf)).strip()
        self.buf = []
        if t:
            self.out.append((self.art > 0, t))

    def handle_starttag(self, tag, attrs):
        if tag in VOID:
            if tag in ('br', 'hr'):
                self.flush()
            return
        a = dict(attrs)
        junk = tag in SKIP or bool(JUNK.search((a.get('class') or '') + ' ' + (a.get('id') or ''))) and tag in ('div', 'section', 'ul', 'p', 'span')
        if tag in BLOCK:
            self.flush()
        self.stack.append((tag, junk, tag in ('article', 'main')))
        if junk:
            self.skip += 1
        if tag in ('article', 'main'):
            self.art += 1

    def handle_endtag(self, tag):
        if tag in VOID:
            return
        for k in range(len(self.stack) - 1, -1, -1):
            if self.stack[k][0] == tag:
                if tag in BLOCK:
                    self.flush()
                for t, junk, art in self.stack[k:]:
                    self.skip -= junk
                    self.art -= art
                del self.stack[k:]
                break

    def handle_data(self, d):
        if not self.skip:
            self.buf.append(d)


BOILER = re.compile(r'^(share( this)?|subscribe|sign up|read more|continue reading|leave a (comment|reply)|related( posts)?|'
                    r'tags?:|posted (in|on)|filed under|click here|photo( credit)?:|image:|advertisement|loading|'
                    r'thanks for reading|this post is for paid subscribers|listen to this (article|post)).{0,80}$', re.I)


def clean(t):
    t = html.unescape(t)
    t = re.sub(r'https?://\S+', '', t)
    t = re.sub(r'\[\s*\d+\s*\]|\[(edit|citation needed)\]', '', t)
    t = t.replace(' ', ' ').replace('​', '')
    return re.sub(r'\s+', ' ', t).strip()


def paragraphs(markup, want_article=False):
    p = Paras()
    try:
        p.feed(markup)
        p.close()
    except Exception:
        pass
    p.flush()
    rows = p.out
    if want_article and sum(len(t.split()) for a, t in rows if a) >= MIN_WORDS:
        rows = [r for r in rows if r[0]]
    out = []
    for _, t in rows:
        t = clean(t)
        if len(t) < 2 or BOILER.match(t) or (out and t == out[-1]):
            continue
        out.append(t)
    return out


def words(paras):
    return sum(len(x.split()) for x in paras)


def page_text(url):
    raw, _, cs = get(url)
    paras = paragraphs(text_of(raw, cs), want_article=True)
    # Outside an <article>, menus and link lists come through as short lines: keep real sentences and headings between them.
    keep = [x for x in paras if len(x.split()) >= 8 or (x[-1:] in '.!?:"”' and len(x.split()) >= 3)]
    return keep if words(keep) >= 0.6 * words(paras) or words(keep) >= MIN_WORDS else paras


# ---------- feeds ----------
def local(tag):
    return tag.rsplit('}', 1)[-1].lower()


def child(el, *names):
    for c in el:
        if local(c.tag) in names:
            return c
    return None


def parse_feed(raw):
    """Returns (title, site link, [entries]); each entry {title, link, pub, html}."""
    root = ET.fromstring(raw)
    chan = root if local(root.tag) == 'feed' else child(root, 'channel')
    chan = root if chan is None else chan
    title = (getattr(child(chan, 'title'), 'text', '') or '').strip()
    site = ''
    for c in chan:
        if local(c.tag) == 'link':
            href = c.get('href') or (c.text or '').strip()
            if href and c.get('rel', 'alternate') == 'alternate' and not site:
                site = href
    items = [c for c in root.iter() if local(c.tag) in ('item', 'entry')]
    out = []
    for it in items:
        t = (getattr(child(it, 'title'), 'text', '') or '').strip()
        link = ''
        for c in it:
            if local(c.tag) == 'link':
                href = c.get('href') or (c.text or '').strip()
                if href and c.get('rel', 'alternate') == 'alternate' and not link:
                    link = href
        if not link:
            g = child(it, 'guid', 'id')
            if g is not None and (g.text or '').strip().startswith('http'):
                link = g.text.strip()
        pub = None
        for n in ('pubdate', 'published', 'date', 'updated'):
            c = child(it, n)
            if c is not None and pdate(c.text):
                pub = pdate(c.text)
                break
        body = ''
        for c in it:
            if local(c.tag) in ('encoded', 'content') and (c.text or '').strip():
                body = c.text
            elif local(c.tag) == 'content' and len(c):       # atom xhtml content
                body = ET.tostring(c, encoding='unicode')
        summ = child(it, 'description', 'summary')
        summ = (summ.text or '') if summ is not None else ''
        has_audio = any(local(c.tag) == 'enclosure' and str(c.get('type', '')).startswith('audio') for c in it)
        if t and link:
            out.append({'title': html.unescape(re.sub(r'\s+', ' ', t)), 'link': link.strip(), 'pub': pub, 'html': body or '',
                        'summary': summ, 'audio': has_audio})
    return html.unescape(title), site, out


def looks_like_feed(raw):
    head = raw[:600].lstrip().lower()
    return head.startswith(b'<?xml') or b'<rss' in head or b'<feed' in head or b'<rdf' in head


class Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.found, self.title, self._t = [], '', False

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == 'link' and 'alternate' in (a.get('rel') or '').lower() and re.search(r'(rss|atom)\+xml', a.get('type') or '', re.I) and a.get('href'):
            self.found.append((a['href'], a.get('title') or ''))
        self._t = tag == 'title' and not self.title

    def handle_data(self, d):
        if self._t:
            self.title += d

    def handle_endtag(self, tag):
        self._t = False


def resolve(q):
    """Finds the feed for what Ryan typed (a blog address or a feed link). Returns {name, feed, page}."""
    q = q.strip()
    if ' ' in q or '.' not in q:
        raise ValueError('needs the blog\'s web address, not its name')
    url = q if re.match(r'^https?://', q, re.I) else 'https://' + q
    raw, final, cs = get(url)
    if looks_like_feed(raw):
        title, site, _ = parse_feed(raw)
        return {'name': title or urllib.parse.urlparse(final).netloc, 'feed': final, 'page': site or ''}
    lk = Links()
    try:
        lk.feed(text_of(raw, cs))
    except Exception:
        pass
    cands = [urllib.parse.urljoin(final, h) for h, t in sorted(lk.found, key=lambda x: 'comment' in (x[0] + x[1]).lower())]
    base = final if final.endswith('/') else final + '/'
    root = '{0.scheme}://{0.netloc}/'.format(urllib.parse.urlparse(final))
    for b in dict.fromkeys([base, root]):
        cands += [b + s for s in ('feed', 'feed/', 'rss', 'rss.xml', 'atom.xml', 'index.xml', 'feed.xml', 'blog/feed', 'blog/rss.xml', 'feeds/posts/default')]
    for c in dict.fromkeys(cands):
        try:
            fr, ffinal, _ = get(c)
            if looks_like_feed(fr):
                title, site, ents = parse_feed(fr)
                if ents:
                    return {'name': title or lk.title.strip() or urllib.parse.urlparse(final).netloc, 'feed': ffinal, 'page': site or final}
        except Exception:
            continue
    raise ValueError('no feed found at that address')


def aid(link):
    return 'b-' + hashlib.sha1(link.strip().encode()).hexdigest()[:14]


def feeds(D):
    os.makedirs(f'{D}/out', exist_ok=True)
    cur = load(f'{D}/config/blogs.json', {}) or {}
    subs = load(f'{D}/config/blogSubs.json', {}) or {}
    blogs = [b for b in (cur.get('list') or []) if isinstance(b, dict) and b.get('feed')]
    resolved, errors = dict(cur.get('resolved') or {}), {}
    removed = {}
    for r in subs.get('remove') or []:
        if isinstance(r, dict) and r.get('name'):
            removed[norm(r['name'])] = max(removed.get(norm(r['name']), ''), r.get('at') or '')
    blogs = [b for b in blogs if not (removed.get(norm(b['name']), '') > (b.get('at') or ''))]
    fresh = set()
    for a in subs.get('add') or []:
        q = (a.get('q') or '').strip() if isinstance(a, dict) else ''
        if not q:
            continue
        name = resolved.get(q)
        if name and any(norm(b['name']) == norm(name) for b in blogs):
            continue
        if name and removed.get(norm(name), '') > (a.get('at') or ''):
            continue                      # added, then removed: stays removed
        try:
            b = resolve(q)
            dup = next((x for x in blogs if norm(x['feed']) == norm(b['feed']) or norm(x['name']) == norm(b['name'])), None)
            if not dup:
                b['at'] = ISO(NOW)
                blogs.append(b)
                fresh.add(norm(b['name']))
            resolved[q] = (dup or b)['name']
        except Exception as e:
            errors[q] = str(e)[:140]
    old = {it['id']: it for it in (load(f'{D}/feed/art.json', {}) or {}).get('items', []) if isinstance(it, dict) and it.get('id')}
    items, problems = {}, {}
    for b in blogs:
        try:
            raw, _, _ = get(b['feed'])
            _, _, ents = parse_feed(raw)
        except Exception as e:
            problems[b['name']] = str(e)[:140]
            for it in old.values():       # feed unreachable: keep what was listed
                if norm(it.get('ch')) == norm(b['name']):
                    items[it['id']] = it
            continue
        ents = [e for e in ents if e['pub']]
        ents.sort(key=lambda e: e['pub'], reverse=True)
        recent = [e for e in ents if (NOW - e['pub']).days < DAYS][:PER_BLOG]
        if norm(b['name']) in fresh and len(recent) < 3:
            recent = ents[:3]
        for e in recent:
            paras = paragraphs(e['html']) if e['html'] else []
            n = words(paras)
            desc = clean(' '.join(paragraphs(e['summary'])[:3])) if e['summary'] else ''
            if len(desc) < 30:
                desc = clean(' '.join(paras[:3]))
            it = {'id': aid(e['link']), 'title': e['title'][:200], 'ch': b['name'], 'pub': ISO(e['pub']), 'url': e['link'],
                  'desc': desc[:260] + ('…' if len(desc) > 260 else ''), 'art': 1}
            if n >= MIN_WORDS:
                it['words'] = n
                it['dur'] = round(min(n, MAX_WORDS) / WPM * 60)
            items[it['id']] = it
    live = {norm(b['name']) for b in blogs}
    for i, it in old.items():             # posts that left the feed stay a while
        p = pdate(it.get('pub'))
        if i not in items and norm(it.get('ch')) in live and p and (NOW - p).days < KEEP_DAYS:
            items[i] = it
    out = sorted(items.values(), key=lambda x: x.get('pub') or '')
    json.dump({'items': out, 'updated': ISO(NOW)}, open(f'{D}/out/art.json', 'w'))
    json.dump({'list': blogs, 'resolved': resolved, 'errors': errors, 'problems': problems, 'at': ISO(NOW)}, open(f'{D}/out/blogs.json', 'w'))
    print(f"blogs: {len(blogs)} ({len(fresh)} new) | posts listed: {len(out)} | lookups failed: {errors or 'none'} | feeds unreachable: {problems or 'none'}")


# ---------- which posts need audio ----------
def post_text(it, blogs):
    """The post's own text: from the blog's feed when it carries the whole post, else from the post's page."""
    best = []
    b = next((x for x in blogs if norm(x.get('name')) == norm(it.get('ch'))), None)
    if b:
        try:
            raw, _, _ = get(b['feed'])
            for e in parse_feed(raw)[2]:
                if aid(e['link']) == it['id'] and e['html']:
                    best = paragraphs(e['html'])
        except Exception:
            pass
    teaser = words(best) < 350 or bool(best and re.search(r'(\[…\]|\[\.\.\.\]|…|read more|continue reading)\W*$', best[-1], re.I))
    if teaser:
        try:
            pg = page_text(it['url'])
            if words(pg) > words(best) * 1.2:
                best = pg
        except Exception as e:
            if not best:
                raise RuntimeError(f'could not open the post ({str(e)[:80]})')
    if best and norm(best[0]) == norm(it.get('title')):
        best = best[1:]
    out, n = [], 0
    for x in best:
        if n >= MAX_WORDS:
            break
        out.append(x)
        n += len(x.split())
    return out


def prep(D):
    for d in ('art', 'chunks', 'sum'):
        os.makedirs(f'{D}/{d}', exist_ok=True)
    feedp = f'{D}/out/art.json' if os.path.exists(f'{D}/out/art.json') else f'{D}/feed/art.json'
    items = {it['id']: it for it in (load(feedp, {}) or {}).get('items', [])}
    blogs = (load(f'{D}/out/blogs.json') or load(f'{D}/config/blogs.json', {}) or {}).get('list') or []
    modes = dict((load(f'{D}/config/artMode.json', {}) or {}).get('ids') or {})
    st = {}
    for p in glob.glob(f'{D}/state/b-*.json'):
        v = load(p, {}) or {}
        st[os.path.basename(p)[:-5]] = (v.get('s'), pdate(v.get('at')))
    # Requests from the player: "s-<id>" (summarize instead) and "f-<id>" (read in full instead), last 3 days.
    lj = load(f'{D}/loop.json', {}) or {}
    rows = lj.get('values', []) if isinstance(lj, dict) else (lj if isinstance(lj, list) else [])
    asked = {}
    for row in rows:
        cells = [str(c or '').strip() for c in (row or [])]
        reqs = [(c[0], c[2:]) for c in cells if re.match(r'^[sf]-b-[0-9a-f]{6,}$', c)]
        if not reqs:
            continue
        when = None
        for c in cells:
            for fmt in ('%m/%d/%Y %H:%M:%S', '%Y-%m-%d %H:%M:%S'):
                try:
                    when = dt.datetime.strptime(c, fmt).replace(tzinfo=dt.timezone.utc)
                    break
                except Exception:
                    pass
            if when:
                break
        if when and (NOW - when).total_seconds() > 4 * 86400:
            continue
        for kind, iid in reqs:
            asked.setdefault(iid, set()).add(kind)
    swrites, mode_changed = [], False
    for iid, kinds in sorted(asked.items()):
        s = st.get(iid, (None, None))[0]
        if iid not in items or s in DONE:
            continue
        have = modes.get(iid, '')
        want = 'b' if (kinds | set(have if have != 'b' else 'sf')) >= {'s', 'f'} else next(iter(kinds))
        if want != have:
            modes[iid] = want
            mode_changed = True
        doc = load(f'{D}/deep/{iid}.json', {}) or {}
        missing = ('s' in kinds and not doc.get('p')) or ('f' in kinds and not doc.get('ft'))
        if s != 'later' and missing:      # back to "being made" in the queue (also when it was already finished)
            w = {'op': 'set', 'collection': 'state', 'doc_id': iid, 'data': {'s': 'later', 'at': NOW.strftime('%Y-%m-%dT%H:%M:%S.000Z')}}
            v = version(f'{D}/state/{iid}.json')
            if v:
                w['if_version'] = v
            swrites.append(w)
            st[iid] = ('later', NOW)
    if mode_changed:
        w = {'op': 'set', 'collection': 'config', 'doc_id': 'artMode', 'data': {'ids': modes, 'at': ISO(NOW)}}
        v = version(f'{D}/config/artMode.json')
        if v:
            w['if_version'] = v
        swrites.append(w)
    json.dump(swrites, open(f'{D}/sbatch.json', 'w'))
    need, jobs = [], []
    for iid, it in items.items():
        s, at = st.get(iid, (None, None))
        if s != 'later' or not at or (NOW - at).days >= 7:
            continue
        mode = modes.get(iid) or 's'
        doc = load(f'{D}/deep/{iid}.json', {}) or {}
        dat = pdate(doc.get('at'))
        if doc.get('err') and dat and (NOW - dat).total_seconds() < 6 * 3600:
            continue                      # just failed: tried again later, for 3 days
        if doc.get('err') and at and (NOW - at).days >= 3:
            continue
        ns = mode in ('s', 'b') and not doc.get('p')
        nf = mode in ('f', 'b') and not doc.get('ft')
        if not (ns or nf):
            continue
        n = {'id': iid, 'title': it.get('title', ''), 'ch': it.get('ch', ''), 's': ns, 'f': nf}
        try:
            paras = post_text(it, blogs)
            if words(paras) < MIN_WORDS:
                raise RuntimeError(f'only a preview is available ({words(paras)} words); the full post may be for subscribers')
            json.dump({'paras': paras, 'words': words(paras)}, open(f'{D}/art/{iid}.json', 'w'))
            n['words'] = words(paras)
            if ns:
                k = max(1, min(8, round(words(paras) / SUM_WORDS)))
                open(f'{D}/chunks/{iid}.txt', 'w').write(f"TITLE: {n['title']}\nSOURCE: {n['ch']} (a written blog post)\nPARAGRAPHS WANTED: {k}\n\n" + '\n\n'.join(paras))
                jobs.append({'id': iid, 'file': f'{D}/chunks/{iid}.txt', 'out': f'{D}/sum/{iid}.json', 'paragraphs': k, 'words': words(paras)})
        except Exception as e:
            open(f'{D}/art/{iid}.err', 'w').write(str(e)[:200])
        need.append(n)
    json.dump(need, open(f'{D}/need.json', 'w'), indent=1)
    json.dump(jobs, open(f'{D}/jobs.json', 'w'), indent=1)
    bad = sum(1 for n in need if os.path.exists(f"{D}/art/{n['id']}.err"))
    print(f"posts needing audio: {len(need)} ({sum(1 for n in need if n['f'])} in full, {len(jobs)} to summarize, {bad} could not be read) | "
          f"player requests: {len(asked)} | state batch: {len(swrites)} writes in {D}/sbatch.json")
    for j in jobs:
        print(f"  summarize {j['id']}: {j['words']} words -> {j['paragraphs']} paragraphs, read {j['file']}, write {j['out']}")


def finalize(D):
    need = load(f'{D}/need.json', []) or []
    writes, okf, oks, errs = [], 0, 0, []
    now = ISO(NOW)
    for n in need:
        iid = n['id']
        dp = f'{D}/deep/{iid}.json'
        old = load(dp, {}) or {}
        ver = version(dp) or (1 if os.path.exists(dp) else None)
        data = {}
        errp = f'{D}/art/{iid}.err'
        if os.path.exists(errp):
            reason = open(errp).read().strip()[:160]
            errs.append(f'{iid}: {reason}')
            if old.get('p') or old.get('ft'):
                data = {('ferr' if n['f'] else 'serr'): reason}
            else:
                data = {'err': reason, 'at': now, 'src': 'art'}
        else:
            art = load(f'{D}/art/{iid}.json', {}) or {}
            paras = art.get('paras') or []
            est = round(min(art.get('words') or 0, MAX_WORDS) / WPM * 60)
            if n['f'] and paras:
                intro = f"{n['title'].rstrip('.')}. From {n['ch']}."
                writes.append({'op': 'set', 'collection': 'atext', 'doc_id': iid, 'data': {'t': [intro] + paras, 'at': now}})
                data.update({'ft': len(paras) + 1, 'fw': art.get('words') or 0, 'ferr': ''})
                okf += 1
            if n['s']:
                try:
                    sm = json.load(open(f'{D}/sum/{iid}.json'))
                    if isinstance(sm, dict):
                        sm = sm.get('paragraphs') or sm.get('p') or []
                    sm = [re.sub(r'\s+', ' ', str(x)).strip() for x in sm if str(x).strip()]
                    if not sm:
                        raise ValueError('empty')
                    data.update({'p': [[round(est * j / len(sm)), x] for j, x in enumerate(sm)], 'serr': ''})
                    oks += 1
                except Exception as e:
                    errs.append(f'{iid}: no summary written ({str(e)[:60]})')
            if data:
                data.update({'at': now, 'src': 'art', 'err': '', 'dur': est})
        if not data:
            continue
        w = {'op': 'update' if ver else 'set', 'collection': 'deep', 'doc_id': iid, 'data': data}
        if ver:
            w['if_version'] = ver
        writes.append(w)
    files = []
    for b in range(0, len(writes), 30):
        f = f'{D}/artbatch_{b // 30}.json'
        json.dump(writes[b:b + 30], open(f, 'w'))
        files.append(f)
    print(f'full texts saved: {okf} | summaries saved: {oks} | problems: {errs or "none"} | batch files: {files or "none"}')


if __name__ == '__main__':
    cmd, D = sys.argv[1], sys.argv[2].rstrip('/')
    {'feeds': feeds, 'prep': prep, 'finalize': finalize}[cmd](D)
