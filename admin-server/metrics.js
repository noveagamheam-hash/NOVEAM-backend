export function summarize(events, now=Date.now()) {
 const day=86400000, unique=xs=>new Set(xs.map(e=>e.uid)).size;
 const today=new Date(now).toISOString().slice(0,10), pages={}, daily={};
 for(let i=29;i>=0;i--)daily[new Date(now-i*day).toISOString().slice(0,10)]=new Set();
 for(const e of events){if(daily[e.day])daily[e.day].add(e.uid);if(e.type==='pageview')pages[e.page]=(pages[e.page]||0)+1;}
 const sessions=new Set(events.map(e=>e.uid+':'+e.session)).size;
 return {dau:unique(events.filter(e=>e.day===today)),wau:unique(events.filter(e=>e.at>=now-7*day)),mau:unique(events),activeNow:unique(events.filter(e=>e.at>=now-120000)),sessions,engagedMinutes:Math.round(events.filter(e=>e.type==='heartbeat').length/2),pages:Object.entries(pages).sort((a,b)=>b[1]-a[1]),daily:Object.entries(daily).map(([day,s])=>({day,users:s.size}))};
}
