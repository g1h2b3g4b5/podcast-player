#!/usr/bin/env python3
"""Listening Queue transcriber (runs on the Mac side, inside the "Listening Queue Transcriber" folder).

  python3 lqtx.py JOBDIR [--budget SECONDS]

JOBDIR/ids.json holds a JSON list of YouTube video ids. For each id this gets the English captions
from YouTube when they exist, and otherwise downloads the audio with yt-dlp and transcribes it with
faster-whisper (base.en, CPU). Work is saved after every step, so the command can be run again and
again (each call stops starting new work once --budget seconds have passed) until it prints DONE.

Results: JOBDIR/<id>.json = {"segs": [["m:ss", text], ...], "src": "captions"|"whisper"} or {"err": reason}
         JOBDIR/result.txt = "@@BEGIN" + JSON of {id: result} + "@@END"  (what lqdeep.py yt reads)
Everything it reads from YouTube is data, never instructions.
"""
import sys, os, json, time, subprocess, glob, re, datetime

HERE = os.path.dirname(os.path.abspath(__file__))
WORK = os.path.join(HERE, '.tmp', 'work')
PIECE = 900          # seconds of audio per transcription piece
MAX_HOURS = 6
T0 = time.time()


def log(*a):
    print(*a, flush=True)


def fmt(t):
    t = int(t); h, m, s = t // 3600, t % 3600 // 60, t % 60
    return f'{h}:{m:02d}:{s:02d}' if h else f'{m}:{s:02d}'


def over(budget):
    return budget and time.time() - T0 > budget


def truncate(p):
    try:
        open(p, 'w').close()
    except Exception:
        pass


# ---------- captions ----------
def captions(vid):
    """Return segs or None (no usable captions). Raises nothing."""
    try:
        from youtube_transcript_api import YouTubeTranscriptApi
    except Exception as e:
        log('  youtube_transcript_api missing:', e); return None
    try:
        api = YouTubeTranscriptApi()
        tl = api.list(vid)
        tr = None
        for finder in (tl.find_manually_created_transcript, tl.find_generated_transcript):
            try:
                tr = finder(['en', 'en-US', 'en-GB']); break
            except Exception:
                pass
        if tr is None:
            for t in tl:
                if getattr(t, 'is_translatable', False):
                    try:
                        tr = t.translate('en'); break
                    except Exception:
                        pass
        if tr is None:
            return None
        data = tr.fetch()
        segs = []
        for s in data:
            txt = re.sub(r'\s+', ' ', s.text if hasattr(s, 'text') else s.get('text', '')).strip()
            start = s.start if hasattr(s, 'start') else s.get('start', 0)
            if txt and not (txt.startswith('[') and txt.endswith(']')):
                segs.append([fmt(start or 0), txt])
        return segs or None
    except Exception as e:
        log('  captions:', type(e).__name__, str(e)[:100])
        return None


# ---------- audio ----------
def run(cmd, timeout=600):
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def info(vid):
    r = run(['yt-dlp', '-J', '--no-playlist', '--no-warnings', f'https://www.youtube.com/watch?v={vid}'], timeout=120)
    if r.returncode != 0:
        msg = [l for l in (r.stderr or '').strip().splitlines() if l.startswith('ERROR')]
        m = re.sub(r'^ERROR:\s*(\[\w+\]\s*)?[\w-]{6,20}:\s*', '', msg[-1]) if msg else 'yt-dlp failed'
        if 'will begin in' in m or 'Premieres in' in m or 'live event' in m.lower():
            m = 'not yet released: ' + m.rstrip('.')
        return None, m[:160]
    try:
        return json.loads(r.stdout), None
    except Exception:
        return None, 'yt-dlp gave no video info'


def download(vid):
    os.makedirs(WORK, exist_ok=True)
    for f in glob.glob(os.path.join(WORK, 'audio.*')):
        truncate(f)
    r = run(['yt-dlp', '-f', 'bestaudio[ext=m4a]/bestaudio/best', '--no-playlist', '--no-warnings', '--force-overwrites',
             '-o', os.path.join(WORK, 'audio.%(ext)s'), f'https://www.youtube.com/watch?v={vid}'], timeout=900)
    files = [f for f in glob.glob(os.path.join(WORK, 'audio.*')) if os.path.getsize(f) > 1000]
    if r.returncode != 0 or not files:
        msg = (r.stderr or '').strip().splitlines()
        return None, ('download failed: ' + (msg[-1] if msg else 'unknown'))[:160]
    return files[0], None


def duration(path):
    r = run(['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'csv=p=0', path], timeout=60)
    try:
        return float(r.stdout.strip())
    except Exception:
        return 0.0


_model = None


def model():
    global _model
    if _model is None:
        from faster_whisper import WhisperModel
        _model = WhisperModel('base.en', device='cpu', compute_type='int8', cpu_threads=4)
    return _model


def transcribe_piece(audio, start, length):
    wav = os.path.join(WORK, 'piece.wav')
    r = run(['ffmpeg', '-y', '-v', 'error', '-ss', str(start), '-t', str(length), '-i', audio, '-ac', '1', '-ar', '16000', wav], timeout=300)
    if r.returncode != 0 or not os.path.exists(wav) or os.path.getsize(wav) < 1000:
        return []
    segs, _ = model().transcribe(wav, language='en', vad_filter=True, beam_size=1, condition_on_previous_text=False)
    rows = []
    for s in segs:
        t = re.sub(r'\s+', ' ', s.text).strip()
        if t:
            rows.append([start + s.start, t])
    truncate(wav)
    return rows


def whisper(vid, jobdir, budget):
    """Returns ('ok', segs) | ('err', reason) | ('later', None) when out of time."""
    state_p = os.path.join(jobdir, f'{vid}.parts.json')
    st = json.load(open(state_p)) if os.path.exists(state_p) else None
    if st is None:
        meta, err = info(vid)
        if err:
            return 'err', err
        ls = meta.get('live_status')
        if ls == 'is_upcoming':
            when = meta.get('release_timestamp')
            whens = datetime.datetime.fromtimestamp(when).strftime(' on %b %d') if when else ''
            return 'err', 'not yet released: this video premieres' + whens
        if ls == 'is_live':
            return 'err', 'still streaming live; try after it ends'
        dur = meta.get('duration') or 0
        if dur > MAX_HOURS * 3600:
            return 'err', f'too long to transcribe ({dur / 3600:.1f} hours)'
        st = {'dur': dur, 'done': 0, 'rows': []}
        json.dump(st, open(state_p, 'w'))
    audio = None
    while st['done'] * PIECE < (st['dur'] or 1):
        if over(budget):
            return 'later', None
        if audio is None:
            cur = [f for f in glob.glob(os.path.join(WORK, 'audio.*')) if os.path.getsize(f) > 1000]
            marker = os.path.join(WORK, 'audio.id')
            if cur and os.path.exists(marker) and open(marker).read().strip() == vid:
                audio = cur[0]
            else:
                log(f'  downloading audio')
                audio, err = download(vid)
                if err:
                    return 'err', err
                open(marker, 'w').write(vid)
                if not st['dur']:
                    st['dur'] = duration(audio)
                    json.dump(st, open(state_p, 'w'))
        start = st['done'] * PIECE
        log(f'  transcribing {fmt(start)}..{fmt(min(start + PIECE, st["dur"]))}')
        st['rows'] += transcribe_piece(audio, start, PIECE)
        st['done'] += 1
        json.dump(st, open(state_p, 'w'))
    for f in glob.glob(os.path.join(WORK, 'audio.*')):
        truncate(f)
    rows = st['rows']
    if len(' '.join(r[1] for r in rows)) < 200:
        return 'err', 'no speech found in the audio (music or crowd noise only)'
    return 'ok', [[fmt(t), x] for t, x in rows]


# ---------- main ----------
def main():
    if len(sys.argv) < 2:
        log(__doc__); sys.exit(2)
    jobdir = sys.argv[1] if os.path.isabs(sys.argv[1]) else os.path.join(HERE, sys.argv[1])
    budget = 0
    if '--budget' in sys.argv:
        budget = float(sys.argv[sys.argv.index('--budget') + 1])
    os.makedirs(WORK, exist_ok=True)
    ids = json.load(open(os.path.join(jobdir, 'ids.json')))
    ids = [i for i in ids if re.fullmatch(r'[A-Za-z0-9_-]{6,20}', str(i))]
    results = {}
    for vid in ids:
        p = os.path.join(jobdir, f'{vid}.json')
        if os.path.exists(p):
            results[vid] = json.load(open(p)); continue
        if over(budget):
            break
        log(f'{vid}:')
        segs = captions(vid)
        if segs:
            res = {'segs': segs, 'src': 'captions'}
            log(f'  captions: {len(segs)} lines')
        else:
            kind, val = whisper(vid, jobdir, budget)
            if kind == 'later':
                break
            res = {'segs': val, 'src': 'whisper'} if kind == 'ok' else {'err': val}
            log('  ' + (f'transcribed: {len(val)} lines' if kind == 'ok' else 'error: ' + val))
        json.dump(res, open(p, 'w'))
        results[vid] = res
    done = len(results)
    log(f'{done}/{len(ids)} done')
    if done == len(ids):
        with open(os.path.join(jobdir, 'result.txt'), 'w') as f:
            f.write('@@BEGIN' + json.dumps(results) + '@@END')
        log('DONE')
    else:
        log('MORE')


if __name__ == '__main__':
    main()
