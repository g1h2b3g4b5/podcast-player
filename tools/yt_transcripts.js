// Works with the browser pane hidden: pure fetch, no iframe, no DOM. Run in any youtube.com tab.
// window.__getTx(videoId) -> {segs:[[ts,text],...]} or {err}. window.__runTx(ids) fetches 3 at a time into window.__txres; poll window.__txdone.
// Uses the same get_panel call the page's own "Show transcript" button makes; params is a protobuf wrapping the video id.
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
window.__txParams=v=>{const b=[0xaa,0x09,0x0f,0x0a,0x0b,...new TextEncoder().encode(v),0x18,0x02];return btoa(String.fromCharCode(...b));};
window.__getTx=async function(v){
 try{
  const r=await fetch('/youtubei/v1/get_panel?prettyPrint=false',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({context:ytcfg.data_.INNERTUBE_CONTEXT,panelId:'PAmodern_transcript_view',params:window.__txParams(v)})});
  if(!r.ok) return {err:'http '+r.status};
  const s=await r.text();const segs=[];
  const re=/"transcriptSegmentViewModel":\{"simpleText":"((?:[^"\\]|\\.)*)"[^}]*?"timestamp":"([^"]+)"/g;let m;
  while((m=re.exec(s)))segs.push([m[2],JSON.parse('"'+m[1]+'"').replace(/\s+/g,' ').trim()]);
  return segs.length?{segs}:{err:'no transcript'};
 }catch(e){return {err:String(e)}}
};
window.__runTx=function(ids){window.__txres={};window.__txdone=false;let k=0;const w=async()=>{while(k<ids.length){const v=ids[k++];window.__txres[v]=await window.__getTx(v);await sleep(300)}};Promise.all([w(),w(),w()]).then(()=>{window.__txdone=true});return ids.length};
