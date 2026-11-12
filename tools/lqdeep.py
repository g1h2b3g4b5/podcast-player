"""Listening Queue read-aloud summaries: the mechanical half of the overnight run.

The scheduled run downloads the queue's database into DIR (ArtifactData out_dir dumps: DIR/feed/*.json,
DIR/config/deep.json, DIR/state/*.json, DIR/deep/*.json), then:

  python3 -I lqdeep.py prep DIR
      Picks the items to summarize: every item set aside with Audio summary (state later, last 2 days), plus items from a source switched on in config/deep, still in the queue
      (not removed or finished), no deep/<id> document yet, released within the last 7 days.
      Writes DIR/need.json and DIR/tx_start.js (YouTube ids, for the browser step); prints a summary.
  python3 -I lqdeep.py audio DIR          (run in the background; it can take a while)
      Installs faster-whisper on first use, downloads each podcast episode and transcribes it.
      Writes DIR/tx/<id>.json = [[startSecs, text], ...] or DIR/tx/<id>.err. Progress in DIR/audio.log;
      DIR/audio.done appears when finished.
  python3 -I lqdeep.py yt DIR RESULT_FILE
      Turns the saved browser result (window.__txres between @@BEGIN and @@END) into DIR/tx/<id>.json.
  python3 -I lqdeep.py chunks DIR
      Splits every transcript into parts of about 7 minutes (one paragraph each) and writes
      DIR/chunks/<id>.txt for the summarizer, plus DIR/jobs.json listing them.
  python3 -I lqdeep.py finalize DIR
      Reads DIR/sum/<id>.json (a JSON array of paragraph strings, one per part) and prints the
      ArtifactData batch files to send: DIR/batch_<n>.json. Items whose transcript failed get {err}.

Segmented shows (DIR/config/seg.json: podcasts switched to "Segments on" in the queue's Auto-sort tab): prep also
picks their new episodes (last 2 days). chunks writes those transcripts one line per few seconds with its time, and
the summarizer returns {"segments": [{start, end, title, paragraphs}]}: the stretches of the show between ad
breaks, each with its own summary.  python3 -I lqdeep.py segcheck DIR ID  says whether that answer is usable.
finalize stores them as deep/<id> = {p: [[startSecs, text], ...], sg: [[startSecs, endSecs, firstParagraph, title], ...]}
so the player can switch between the original and the summary segment by segment.
"""
import datetime as dt, glob, json, os, re, subprocess, sys, time, urllib.request

NOW = dt.datetime.now(dt.timezone.utc)
ISO = lambda d: d.strftime('%Y-%m-%dT%H:%M:%SZ')
PART = 420          # seconds of episode per summary paragraph (7 minutes; between the 5 and 10 asked for)
MAXDAYS = 7
SEGDAYS = 2        # segmented shows: only episodes released in the last 2 days are picked automatically
MINSEG = 20        # a segment shorter than this many seconds is dropped
SHORT = 300        # 5 minutes or less: one short paragraph of about three sentences
DONE = {'done', 'fin', 'listened', 'removed'}


def load(path, default=None):
    try:
        d = json.load(open(path))
        return d.get('data', d) if isinstance(d, dict) and set(d) >= {'data'} and len(d) <= 3 else d
    except Exception:
        return default


def norm(s):
    return str(s or '').strip().lower()


def pdate(s):
    try:
        return dt.datetime.fromisoformat(str(s).replace('Z', '+00:00'))
    except Exception:
        return None


def log(D, msg):
    with open(f'{D}/audio.log', 'a') as f:
        f.write(time.strftime('%H:%M:%S ') + msg + '\n')


def prep(D):
    keys = (load(f'{D}/config/deep.json', {}) or {}).get('keys', {}) or {}
    on = set(keys)
    segkeys = (load(f'{D}/config/seg.json', {}) or {}).get('keys', {}) or {}
    segon = {k for k, v in segkeys.items() if isinstance(v, dict) and v.get('on')}
    have, retry = set(), {}
    for p in glob.glob(f'{D}/deep/*.json'):
        iid = os.path.basename(p)[:-5]
        raw = load(p, {}) or {}
        doc = raw if isinstance(raw, dict) else {}
        at = pdate(doc.get('at'))
        # A video YouTube had no transcript for gets one more try a day later (captions for long streams appear late).
        if (iid.startswith('y-') and doc.get('err') == 'no transcript available' and not doc.get('retried')
                and at and (NOW - at).total_seconds() >= 20 * 3600):
            rv = json.load(open(p)) if os.path.exists(p) else {}
            retry[iid] = (rv.get('version') if isinstance(rv, dict) else None) or 1   # err docs are written once
            continue
        have.add(iid)
    st, asked = {}, set()
    for p in glob.glob(f'{D}/state/*.json'):
        v = load(p, {}) or {}
        iid = os.path.basename(p)[:-5]
        st[iid] = v.get('s')
        at = pdate(v.get('at'))
        if v.get('s') == 'later' and at and (NOW - at).days < 2:
            asked.add(iid)   # tapped "Audio summary" in the queue: summarize whatever its source or age
    # "Summarize instead" in the player: rows "s-<id>" in the player's drop-box sheet (saved by the task as
    # DIR/loop.json). Those count as asked too, and their state is set to 'later' so the queue shows them under
    # Audio summaries in Playlist (never To Sort). Rows older than 2 days are ignored.
    sreq, swrites = set(), []
    lj = load(f'{D}/loop.json', {}) or {}
    rows = lj.get('values', []) if isinstance(lj, dict) else (lj if isinstance(lj, list) else [])
    for row in rows:
        cells = [str(c or '').strip() for c in (row or [])]
        ids = [c[2:] for c in cells if re.match(r'^s-[py]-[A-Za-z0-9_-]{6,}$', c)]
        if not ids:
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
        if when and (NOW - when).total_seconds() > 2 * 86400 + 86400:   # +1 day slack for the sheet's own time zone
            continue
        sreq.update(ids)
    for iid in sorted(sreq):
        if st.get(iid) in DONE or iid in have:
            continue
        asked.add(iid)
        if st.get(iid) != 'later':
            sp = f'{D}/state/{iid}.json'
            rv = json.load(open(sp)) if os.path.exists(sp) else None
            w = {'op': 'set', 'collection': 'state', 'doc_id': iid, 'data': {'s': 'later', 'at': NOW.strftime('%Y-%m-%dT%H:%M:%S.000Z')}}
            if isinstance(rv, dict) and rv.get('version'):
                w['if_version'] = rv['version']
            swrites.append(w)
    json.dump(swrites, open(f'{D}/sbatch.json', 'w'))
    if swrites:
        print(f'player asked for {len(swrites)} summaries; state batch: {D}/sbatch.json')
    need = []
    for k in ('podcasts', 'daily', 'wl'):
        for it in (load(f'{D}/feed/{k}.json', {}) or {}).get('items', []):
            if it['id'] in have or st.get(it['id']) in DONE:
                continue
            pub = pdate(it.get('pub'))
            # Segmented show: its episodes are split at the ad breaks, each segment with its own summary.
            isseg = k == 'podcasts' and bool(it.get('audio')) and norm(it.get('ch')) in segon
            if it['id'] not in asked and it['id'] not in retry:
                if not (isseg and pub and (NOW - pub).total_seconds() <= SEGDAYS * 86400):
                    if norm(it.get('ch')) not in on:
                        continue
                    if pub and (NOW - pub).days > MAXDAYS:
                        continue
            if it.get('vid'):
                need.append({'id': it['id'], 'kind': 'yt', 'vid': it['vid'], 'title': it.get('title', ''), 'ch': it.get('ch', ''), 'dur': it.get('dur', 0)})
            elif it.get('audio'):
                need.append({'id': it['id'], 'kind': 'audio', 'audio': it['audio'], 'title': it.get('title', ''), 'ch': it.get('ch', ''), 'dur': it.get('dur', 0)})
            else:   # nothing to transcribe: report it as failed instead of leaving it waiting
                need.append({'id': it['id'], 'kind': 'none', 'title': it.get('title', ''), 'ch': it.get('ch', ''), 'dur': it.get('dur', 0)})
            if it['id'] in retry:
                need[-1]['retry'] = retry[it['id']]
            if isseg and need[-1]['kind'] == 'audio':
                need[-1]['seg'] = True
    json.dump(need, open(f'{D}/need.json', 'w'), indent=1)
    vids = [n['vid'] for n in need if n['kind'] == 'yt']
    open(f'{D}/tx_start.js', 'w').write('window.__runTx(%s)' % json.dumps(vids))
    hrs = sum(float(n['dur'] or 0) for n in need if n['kind'] == 'audio') / 3600
    pods = sum(1 for n in need if n['kind'] == 'audio')
    nolink = sum(1 for n in need if n['kind'] == 'none')
    print(f"segmented shows on: {len(segon)} ({sum(1 for n in need if n.get('seg'))} episodes to split into segments)")
    print(f"sources on: {len(on)} | asked for: {len(asked)} | retries: {sum(1 for n in need if n.get('retry'))} | to summarize: {len(need)} ({pods} podcast episodes, about {hrs:.1f} h of audio; {len(vids)} videos; {nolink} with no audio link, marked failed)")


def ensure_whisper(D):
    try:
        import faster_whisper  # noqa
        return
    except Exception:
        pass
    log(D, 'installing faster-whisper')
    subprocess.run([sys.executable, '-m', 'pip', 'install', '-q', '--break-system-packages', 'faster-whisper', 'av<14'], check=True)


def audio(D):
    os.makedirs(f'{D}/tx', exist_ok=True)
    open(f'{D}/audio.log', 'w').close()
    try:
        need = [n for n in json.load(open(f'{D}/need.json')) if n['kind'] == 'audio']
        if need:
            ensure_whisper(D)
            from faster_whisper import WhisperModel
            model = WhisperModel(os.environ.get('LQ_MODEL', 'base.en'), device='cpu', compute_type='int8', cpu_threads=os.cpu_count() or 2)
            log(D, f'model ready; {len(need)} episodes')
        for n in need:
            out = f"{D}/tx/{n['id']}.json"
            if os.path.exists(out):
                continue
            mp3 = f"{D}/a_{n['id']}.mp3"
            t0 = time.time()
            try:
                req = urllib.request.Request(n['audio'], headers={'User-Agent': 'Mozilla/5.0 (podcast player)'})
                with urllib.request.urlopen(req, timeout=120) as r, open(mp3, 'wb') as f:
                    while True:
                        b = r.read(1 << 20)
                        if not b:
                            break
                        f.write(b)
                segs, info = model.transcribe(mp3, language='en', vad_filter=True, beam_size=1, condition_on_previous_text=False)
                rows = [[round(s.start, 1), s.text.strip()] for s in segs if s.text.strip()]
                if not rows:
                    raise RuntimeError('no speech found')
                json.dump(rows, open(out, 'w'))
                open(f"{D}/tx/{n['id']}.dur", 'w').write(str(round(info.duration or 0, 1)))
                log(D, f"done {n['id']} ({info.duration / 60:.0f} min audio in {(time.time() - t0) / 60:.1f} min)")
            except Exception as e:
                open(f"{D}/tx/{n['id']}.err", 'w').write(str(e)[:200])
                log(D, f"FAILED {n['id']}: {str(e)[:200]}")
            finally:
                if os.path.exists(mp3):
                    os.remove(mp3)
    except Exception as e:
        log(D, f'STOPPED: {str(e)[:300]}')
    open(f'{D}/audio.done', 'w').write(time.strftime('%H:%M:%S'))


def extract(path):
    raw = open(path).read()
    try:
        arr = json.loads(raw)
    except Exception:
        arr = raw
    texts = [p.get('text', '') for p in arr] if isinstance(arr, list) else [arr if isinstance(arr, str) else json.dumps(arr)]
    out = {}
    for t in texts:
        for m in re.finditer(r'@@BEGIN', t):
            end = t.find('@@END', m.end())
            body = t[m.end():end]
            try:
                out.update(json.loads(body))
            except Exception:
                out.update(json.loads(json.loads('"' + body + '"')))
    return out


def yt(D, f):
    os.makedirs(f'{D}/tx', exist_ok=True)
    need = {n['vid']: n for n in json.load(open(f'{D}/need.json')) if n['kind'] == 'yt'}
    data = extract(f)
    ok = 0
    for vid, n in need.items():
        r = data.get(vid)
        if isinstance(r, dict) and r.get('segs'):
            rows = []
            for ts, t in r['segs']:
                secs = sum(int(x) * 60 ** i for i, x in enumerate(reversed(ts.split(':')))) if ts else 0
                rows.append([secs, t])
            json.dump(rows, open(f"{D}/tx/{n['id']}.json", 'w')); ok += 1
        else:
            open(f"{D}/tx/{n['id']}.err", 'w').write((r or {}).get('err', 'no transcript') if isinstance(r, dict) else 'no transcript')
    print(f'videos: {ok} transcripts, {len(need) - ok} without')


def fmt(t):
    t = int(t); h, m, s = t // 3600, t % 3600 // 60, t % 60
    return f'{h}:{m:02d}:{s:02d}' if h else f'{m}:{s:02d}'


def tsec(v):
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().strip('[]')
    if not re.fullmatch(r'\d+(:\d{1,2}){0,2}(\.\d+)?', s):
        raise ValueError(f'bad time {v!r}')
    out = 0.0
    for part in s.split(':'):
        out = out * 60 + float(part)
    return out


def seg_len(D, iid, rows, n):
    """Length of the episode in seconds: the audio's own length when known, else the last transcript line."""
    last = max((r[0] for r in rows), default=0)
    try:
        real = float(open(f'{D}/tx/{iid}.dur').read())
    except Exception:
        real = 0
    return max(real, last + 5) if real else max(float(n.get('dur') or 0), last + 5)


def read_seg(D, iid, n):
    """Reads the summarizer's answer for a segmented episode. Returns [(start, end, title, [paragraphs]), ...],
    cleaned so segments are in order, inside the episode and never overlap. Raises ValueError when unusable."""
    rows = json.load(open(f'{D}/tx/{iid}.json'))
    total = seg_len(D, iid, rows, n)
    last = max((r[0] for r in rows), default=0)
    obj = json.load(open(f'{D}/sum/{iid}.json'))
    raw = obj.get('segments') if isinstance(obj, dict) else obj
    if not isinstance(raw, list) or not raw or not all(isinstance(x, dict) for x in raw):
        raise ValueError('no "segments" list')
    segs = []
    for x in raw:
        paras = x.get('paragraphs', x.get('summary'))
        paras = [paras] if isinstance(paras, str) else (paras or [])
        paras = [str(q).strip() for q in paras if str(q).strip()]
        if not paras:
            continue
        st, en = tsec(x.get('start')), tsec(x.get('end'))
        segs.append([st, en, re.sub(r'\s+', ' ', str(x.get('title') or '')).strip()[:60], paras])
    segs.sort(key=lambda q: q[0])
    out, prev = [], 0.0
    for st, en, title, paras in segs:
        st = max(st, prev)
        if en >= last - 1:          # the last content line: run to the end of the audio
            en = total
        en = min(en, total)
        if en - st < MINSEG:
            continue
        out.append((round(st, 1), round(en, 1), title or f'Segment {len(out) + 1}', paras))
        prev = en
    if not out:
        raise ValueError('no usable segments')
    covered = sum(en - st for st, en, _, _ in out)
    if covered < 0.3 * total:
        raise ValueError(f'segments cover only {covered / 60:.0f} of {total / 60:.0f} minutes')
    return out


def segcheck(D, iid):
    need = {n['id']: n for n in json.load(open(f'{D}/need.json'))}
    try:
        segs = read_seg(D, iid, need.get(iid, {}))
        bad = [w for w in ('sponsor', 'advertis', 'promo', 'brought to you by', 'discount code')
               if any(w in q.lower() for _, _, t, ps in segs for q in ps + [t])]
        if bad:
            print(f'BAD: the summaries mention ads ({", ".join(bad)})')
            return
        print(f'OK: {len(segs)} segments, ' + ', '.join(f'{fmt(st)}-{fmt(en)}' for st, en, _, _ in segs))
    except Exception as e:
        print(f'BAD: {str(e)[:200]}')


def chunks(D):
    os.makedirs(f'{D}/chunks', exist_ok=True)
    need = {n['id']: n for n in json.load(open(f'{D}/need.json'))}
    jobs = []
    for p in sorted(glob.glob(f'{D}/tx/*.json')):
        iid = os.path.basename(p)[:-5]
        n = need.get(iid)
        if not n:
            continue
        rows = json.load(open(p))
        end = max((r[0] for r in rows), default=0)
        if n.get('seg'):
            body = [f"TITLE: {n['title']}\nSOURCE: {n['ch']}\nLENGTH: {fmt(seg_len(D, iid, rows, n))}\n"]
            body += [f'[{fmt(t)}] {x}' for t, x in rows]
            open(f'{D}/chunks/{iid}.txt', 'w').write('\n'.join(body))
            jobs.append({'id': iid, 'seg': True, 'file': f'{D}/chunks/{iid}.txt', 'out': f'{D}/sum/{iid}.json',
                         'chars': sum(len(x) for _, x in rows), 'minutes': round(end / 60)})
            continue
        k = max(1, round(max(end, n.get('dur') or 0) / PART))
        size = max(end, 1) / k
        parts = [[] for _ in range(k)]
        for t, x in rows:
            parts[min(k - 1, int(t // size))].append((t, x))
        parts = [q for q in parts if q]
        starts = [q[0][0] for q in parts]
        body = [f"TITLE: {n['title']}\nSOURCE: {n['ch']}\nPARTS: {len(parts)}\n"]
        for j, q in enumerate(parts):
            body.append(f"\n## PART {j + 1} (starts {fmt(q[0][0])})\n" + ' '.join(x for _, x in q))
        open(f'{D}/chunks/{iid}.txt', 'w').write('\n'.join(body))
        json.dump(starts, open(f'{D}/chunks/{iid}.starts', 'w'))
        jobs.append({'id': iid, 'parts': len(parts), 'file': f'{D}/chunks/{iid}.txt', 'out': f'{D}/sum/{iid}.json', 'chars': sum(len(x) for _, x in rows), 'short': max(end, n.get('dur') or 0) <= SHORT})
    os.makedirs(f'{D}/sum', exist_ok=True)
    json.dump(jobs, open(f'{D}/jobs.json', 'w'), indent=1)
    print(f'{len(jobs)} transcripts ready to summarize:', ', '.join(f"{j['id']} (segments)" if j.get('seg') else f"{j['id']} ({j['parts']} parts)" for j in jobs))


def finalize(D):
    need = json.load(open(f'{D}/need.json'))
    writes, okn, errs = [], 0, []
    now = ISO(NOW)
    for n in need:
        iid = n['id']
        sp, stp = f'{D}/sum/{iid}.json', f'{D}/chunks/{iid}.starts'
        doc = None
        if n.get('seg') and os.path.exists(sp):
            try:
                p, sg = [], []
                for st, en, title, paras in read_seg(D, iid, n):
                    sg.append([st, en, len(p), title])
                    p += [[round(st + (en - st) * j / len(paras)), x] for j, x in enumerate(paras)]
                doc = {'p': p, 'sg': sg, 'at': now, 'src': n['kind'], 'dur': n.get('dur') or 0}
                okn += 1
            except Exception as e:
                # Segments unusable: keep whatever paragraphs there are as an ordinary summary.
                try:
                    obj = json.load(open(sp))
                    raw = obj.get('segments') if isinstance(obj, dict) else obj
                    paras = []
                    for x in raw or []:
                        q = x.get('paragraphs', x.get('summary')) if isinstance(x, dict) else x
                        paras += [q] if isinstance(q, str) else list(q or [])
                    paras = [str(x).strip() for x in paras if str(x).strip()]
                    rows = json.load(open(f'{D}/tx/{iid}.json'))
                    end = max((r[0] for r in rows), default=0)
                    if paras:
                        doc = {'p': [[round(end * j / len(paras)), x] for j, x in enumerate(paras)], 'at': now, 'src': n['kind'], 'dur': n.get('dur') or 0}
                        okn += 1
                except Exception:
                    pass
                errs.append(f'{iid}: segments unusable ({str(e)[:80]}), ' + ('saved as a plain summary' if doc else 'no summary'))
        elif os.path.exists(sp) and os.path.exists(stp):
            try:
                paras = json.load(open(sp))
                if isinstance(paras, dict):
                    paras = paras.get('paragraphs') or paras.get('p') or []
                starts = json.load(open(stp))
                paras = [str(x).strip() for x in paras if str(x).strip()]
                if paras:
                    if len(paras) != len(starts):   # spread the start times evenly if the count drifted
                        starts = [starts[min(len(starts) - 1, round(j * len(starts) / len(paras)))] for j in range(len(paras))]
                    doc = {'p': [[round(t), x] for t, x in zip(starts, paras)], 'at': now, 'src': n['kind'], 'dur': n.get('dur') or 0}
                    okn += 1
            except Exception as e:
                errs.append(f'{iid}: bad summary ({e})')
        if doc is None and n['kind'] == 'none':
            doc = {'err': 'no audio link', 'at': now}
            errs.append(f'{iid}: no audio link')
        if doc is None and os.path.exists(f'{D}/tx/{iid}.err'):
            reason = open(f'{D}/tx/{iid}.err').read().strip()[:120]
            # Retry on later nights unless it plainly has no transcript; give up after 2 days.
            pub = None
            for k in ('podcasts', 'daily', 'wl'):
                for it in (load(f'{D}/feed/{k}.json', {}) or {}).get('items', []):
                    if it['id'] == iid:
                        pub = pdate(it.get('pub'))
            # Reasons the Mac transcriber settles for good (nothing to transcribe) are shown as they are.
            settled = ('not yet released', 'no speech found', 'too long to transcribe', 'still streaming')
            if 'no transcript' in reason:
                doc = {'err': 'no transcript available', 'at': now}
            elif any(k in reason for k in settled):
                doc = {'err': reason, 'at': now}
            elif pub and (NOW - pub).days >= 2:
                doc = {'err': 'could not be transcribed: ' + reason, 'at': now}
            errs.append(f'{iid}: {reason}')
        if doc is not None:
            w = {'op': 'set', 'collection': 'deep', 'doc_id': iid, 'data': doc}
            if n.get('retry'):
                w['if_version'] = n['retry']
                if doc.get('err'):
                    doc['retried'] = True
            writes.append(w)
    files = []
    for b in range(0, len(writes), 40):
        f = f'{D}/batch_{b // 40}.json'
        json.dump(writes[b:b + 40], open(f, 'w'))
        files.append(f)
    print(f'summaries written: {okn} | problems: {errs or "none"} | batch files: {files or "none"}')


VOICE_URL = 'https://github.com/rhasspy/piper/releases/download/v0.0.2/voice-en-us-lessac-medium.tar.gz'
GAP = 0.7           # seconds of silence between paragraphs


def ensure_voice():
    vd = os.path.expanduser('~/.cache/lqvoice')
    onnx = f'{vd}/en-us-lessac-medium.onnx'
    try:
        import piper  # noqa
    except Exception:
        subprocess.run([sys.executable, '-m', 'pip', 'install', '-q', '--break-system-packages', 'piper-tts'], check=True)
    if not os.path.exists(onnx):
        os.makedirs(vd, exist_ok=True)
        subprocess.run(f'curl -sL {VOICE_URL} | tar xz -C {vd}', shell=True, check=True)
    from piper import PiperVoice
    return PiperVoice.load(onnx)


def say_one(voice, iid, paras, outdir):
    """Reads the paragraphs into outdir/<id>.mp3; returns (paragraph start seconds, total seconds)."""
    import wave, hashlib, tempfile
    tmp = tempfile.mkdtemp()
    starts, t, wavs, rate = [], 0.0, [], 22050
    for j, x in enumerate(paras):
        w = f'{tmp}/{j}.wav'
        with wave.open(w, 'wb') as f:
            voice.synthesize_wav(x, f)
        with wave.open(w) as f:
            rate = f.getframerate(); d = f.getnframes() / rate
        starts.append(round(t, 2)); wavs.append(w); t += d + GAP
    lst = f'{tmp}/list.txt'
    gap = f'{tmp}/gap.wav'
    subprocess.run(['ffmpeg', '-loglevel', 'error', '-y', '-f', 'lavfi', '-i', f'anullsrc=r={rate}:cl=mono', '-t', str(GAP), gap], check=True)
    with open(lst, 'w') as f:
        for j, w in enumerate(wavs):
            f.write(f"file '{w}'\n")
            if j < len(wavs) - 1:
                f.write(f"file '{gap}'\n")
    os.makedirs(outdir, exist_ok=True)
    subprocess.run(['ffmpeg', '-loglevel', 'error', '-y', '-f', 'concat', '-safe', '0', '-i', lst, '-ac', '1', '-c:a', 'libmp3lame', '-b:a', '48k', f'{outdir}/{iid}.mp3'], check=True)
    h = hashlib.sha1(' '.join(paras).encode()).hexdigest()[:8]
    return starts, round(t - GAP, 1), h


def speak(D):
    """Turns summaries into real audio (DIR/aout/<id>.mp3, published in the player's repo under a/).
    New summaries: adds sa/ss/sd to the docs in DIR/batch_*.json. Older summaries in DIR/deep without audio:
    writes update batches DIR/abatch_*.json (pinned to the versions downloaded)."""
    out = f'{D}/aout'
    voice = None
    done = 0
    for bf in sorted(glob.glob(f'{D}/batch_*.json')):
        writes = json.load(open(bf))
        for w in writes:
            p = (w.get('data') or {}).get('p')
            if not p:
                continue
            voice = voice or ensure_voice()
            ss, sd, h = say_one(voice, w['doc_id'], [x for _, x in p], out)
            w['data'].update({'sa': f"a/{w['doc_id']}.mp3?v={h}", 'ss': ss, 'sd': sd})
            done += 1
        json.dump(writes, open(bf, 'w'))
    ups = []
    for path in sorted(glob.glob(f'{D}/deep/*.json')):
        raw = json.load(open(path))
        doc = raw.get('data', raw) if isinstance(raw, dict) else {}
        ver = (raw.get('version') if isinstance(raw, dict) else None) or 1   # deep docs are written once, so version 1 unless audio was added
        iid = os.path.basename(path)[:-5]
        if not doc.get('p') or doc.get('sa'):
            continue
        voice = voice or ensure_voice()
        ss, sd, h = say_one(voice, iid, [x for _, x in doc['p']], out)
        ups.append({'op': 'update', 'collection': 'deep', 'doc_id': iid, 'if_version': ver,
                    'data': {'sa': f'a/{iid}.mp3?v={h}', 'ss': ss, 'sd': sd}})
        done += 1
    files = []
    for b in range(0, len(ups), 40):
        f = f'{D}/abatch_{b // 40}.json'
        json.dump(ups[b:b + 40], open(f, 'w'))
        files.append(f)
    print(f'audio made: {done} | update batch files: {files or "none"} | audio folder: {out}')


def publish(D, repo):
    """Copies DIR/aout/*.mp3 into REPO/a/ and removes audio for items that no longer have a summary."""
    import shutil
    dst = f'{repo}/a'
    os.makedirs(dst, exist_ok=True)
    keep = {os.path.basename(p)[:-5] for p in glob.glob(f'{D}/deep/*.json')}
    for bf in glob.glob(f'{D}/batch_*.json'):
        keep |= {w['doc_id'] for w in json.load(open(bf))}
    n = 0
    for p in glob.glob(f'{D}/aout/*.mp3'):
        shutil.copy(p, dst); n += 1
    gone = 0
    for p in glob.glob(f'{dst}/*.mp3'):
        if os.path.basename(p)[:-4] not in keep:
            os.remove(p); gone += 1
    # Next to each audio file, a small a/<id>.json {sm, sa, ss} lets the player turn an episode it asked to have
    # summarized ("Summarize instead") into its summary without a new Play all. Recent failures get {err}.
    docs = {}
    for path in glob.glob(f'{D}/deep/*.json'):
        raw = json.load(open(path))
        docs[os.path.basename(path)[:-5]] = dict(raw.get('data', raw) if isinstance(raw, dict) else {})
    for bf in glob.glob(f'{D}/batch_*.json') + glob.glob(f'{D}/abatch_*.json'):
        for w in json.load(open(bf)):
            docs.setdefault(w['doc_id'], {}).update(w.get('data') or {})
    js = 0
    for iid, doc in docs.items():
        out, at = None, pdate(doc.get('at'))
        if doc.get('p') and doc.get('sa') and os.path.exists(f'{dst}/{iid}.mp3'):
            out = {'sm': doc['p'], 'sa': doc['sa'], 'ss': doc.get('ss') or []}
            if doc.get('sg'):
                out['sg'] = doc['sg']
        elif doc.get('err') and not doc.get('p') and at and (NOW - at).days < 3:
            out = {'err': str(doc['err'])[:200]}
        if out is not None:
            new = json.dumps(out, separators=(',', ':'))
            jp = f'{dst}/{iid}.json'
            if not os.path.exists(jp) or open(jp).read() != new:
                open(jp, 'w').write(new); js += 1
    for p in glob.glob(f'{dst}/*.json'):
        if os.path.basename(p)[:-5] not in docs:
            os.remove(p); gone += 1
    print(f'copied {n}, wrote {js} summary files, removed {gone} old')


if __name__ == '__main__':
    cmd, D = sys.argv[1], sys.argv[2].rstrip('/')
    {'prep': lambda: prep(D), 'audio': lambda: audio(D), 'yt': lambda: yt(D, sys.argv[3]),
     'chunks': lambda: chunks(D), 'finalize': lambda: finalize(D), 'segcheck': lambda: segcheck(D, sys.argv[3]),
     'speak': lambda: speak(D), 'publish': lambda: publish(D, sys.argv[3])}[cmd]()
