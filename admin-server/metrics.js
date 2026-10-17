export function summarize(events, now=Date.now()) {
 const day=86400000, unique=xs=>new Set(xs.map(e=>e.uid)).size;
 const today=new Date(now).toISOString().slice(0,10), pages={}, daily={};
 for(let i=29;i>=0;i--)daily[new Date(now-i*day).toISOString().slice(0,10)]=new Set();
 for(const e of events){if(daily[e.day])daily[e.day].add(e.uid);if(e.type==='pageview')pages[e.page]=(pages[e.page]||0)+1;}
 const sessions=new Set(events.map(e=>e.uid+':'+e.session)).size;
 return {dau:unique(events.filter(e=>e.day===today)),wau:unique(events.filter(e=>e.at>=now-7*day)),mau:unique(events),activeNow:unique(events.filter(e=>e.at>=now-120000)),sessions,engagedMinutes:Math.round(events.filter(e=>e.type==='heartbeat').length/2),pages:Object.entries(pages).sort((a,b)=>b[1]-a[1]),daily:Object.entries(daily).map(([day,s])=>({day,users:s.size}))};
}

export function summarizeDateRange(events, startDate, endDate) {
  const DAY = 86400000;
  const start = Date.parse(startDate + 'T00:00:00.000Z');
  const end = Date.parse(endDate + 'T00:00:00.000Z');
  const endExclusive = end + DAY;

  const filtered = events.filter(e =>
    Number.isFinite(e.at) &&
    e.at >= start &&
    e.at < endExclusive
  );

  const unique = items =>
    new Set(items.map(e => e.uid)).size;

  const daily = {};
  const pages = {};

  for (let time = start; time < endExclusive; time += DAY) {
    daily[new Date(time).toISOString().slice(0, 10)] = new Set();
  }

  for (const e of filtered) {
    const date = new Date(e.at).toISOString().slice(0, 10);

    if (daily[date]) daily[date].add(e.uid);

    if (e.type === 'pageview') {
      const page = e.page || 'Unknown';
      pages[page] = (pages[page] || 0) + 1;
    }
  }

  const sessions = new Set(
    filtered.map(e => e.uid + ':' + e.session)
  ).size;

  const windowStart = (days) =>
    Math.max(start, endExclusive - days * DAY);

  return {
    // Unique users across the selected range
    uniqueUsers: unique(filtered),

    // Activity on the final selected day
    dau: unique(filtered.filter(e => e.at >= end)),

    // Trailing windows ending on the final selected day
    wau: unique(filtered.filter(e =>
      e.at >= windowStart(7)
    )),

    mau: unique(filtered.filter(e =>
      e.at >= windowStart(30)
    )),

    sessions,

    engagedMinutes: Math.round(
      filtered.filter(e => e.type === 'heartbeat').length / 2
    ),

    pages: Object.entries(pages)
      .sort((a, b) => b[1] - a[1]),

    daily: Object.entries(daily)
      .map(([day, users]) => ({
        day,
        users: users.size
      }))
  };
}
