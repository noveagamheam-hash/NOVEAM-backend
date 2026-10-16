// ADD to existing server.js AFTER the admin authorization middleware and BEFORE error handler.
// Do NOT replace your existing server.js. Existing auth, ADMIN_UIDS, Firestore and routes remain intact.
// User directory: read-only, paginated, and protected by existing Firebase UID admin check.
app.get('/api/insights/admin/users', async (req,res,next)=>{
  try {
    const limit=Math.min(100,Math.max(1,Number.parseInt(req.query.limit,10)||50));
    const pageToken=typeof req.query.pageToken==='string'&&req.query.pageToken.length<=2048?req.query.pageToken:undefined;
    const result=await auth.listUsers(limit,pageToken);
    res.json({users:result.users.map(u=>({uid:u.uid,email:u.email||null,displayName:u.displayName||null,disabled:u.disabled,role:admins.has(u.uid)?'Administrator':'Normal User',createdAt:u.metadata.creationTime||null})),nextPageToken:result.pageToken||null});
  }catch(err){next(err);}
});
// Geo aggregates must come from FastAPI's *private*, admin-authorized aggregate endpoint.
// Never send user IP addresses or DB credentials to browsers. Fail closed when unconfigured.
app.get('/api/insights/admin/geography',async(req,res,next)=>{
  try {
    const base=(process.env.NOVEAM_GEO_INTERNAL_URL||'').replace(/\/$/,'');
    const secret=process.env.NOVEAM_GEO_SERVICE_TOKEN||'';
    if(!base||!secret)return res.status(503).json({error:'Geographic analytics not configured'});
    const controller=new AbortController();const timer=setTimeout(()=>controller.abort(),5000);
    let upstream;
    try{upstream=await fetch(base+'/internal/admin/geography',{headers:{Authorization:'Bearer '+secret},signal:controller.signal});}
    finally{clearTimeout(timer);}
    if(!upstream.ok)return res.status(502).json({error:'Geographic analytics service unavailable'});
    const result=await upstream.json();
    // Only aggregated non-identifying statistics from the trusted FastAPI service.
    res.json(result);
  }catch(err){next(err);}
});
