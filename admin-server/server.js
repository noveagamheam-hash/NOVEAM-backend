
import express from 'express';
import rateLimit from 'express-rate-limit';
import cors from 'cors';

import {
  initializeApp,
  applicationDefault
} from 'firebase-admin/app';

import { getAuth } from 'firebase-admin/auth';
import { getFirestore } from 'firebase-admin/firestore';
import {
  summarize,
  summarizeDateRange
} from './metrics.js';

// ===================================================
// NOVEAM ADMIN SERVER — CONFIGURATION
// ===================================================

const admins = new Set(
  (process.env.ADMIN_UIDS || '')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean)
);

if (!admins.size) {
  throw Error(
    'Set ADMIN_UIDS before starting. Access is closed by default.'
  );
}

initializeApp({
  credential: applicationDefault(),
  projectId: process.env.FIREBASE_PROJECT_ID
});

// Preserve your existing named Firestore database.
const db = getFirestore('noveagamheam');
const auth = getAuth();
const app = express();

// ===================================================
// EXPRESS AND RENDER CONFIGURATION
// ===================================================

app.set('trust proxy', 1);

app.use(cors({
  origin: [
    'https://noveam.in',
    'https://www.noveam.in'
  ],
  methods: ['GET', 'POST', 'PATCH', 'OPTIONS'],
  allowedHeaders: [
    'Content-Type',
    'Authorization'
  ]
}));

app.disable('x-powered-by');
app.use(express.json({ limit: '12kb' }));

// ===================================================
// HEALTH CHECK
// ===================================================

app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    service: 'NOVEAM Admin Server'
  });
});

// ===================================================
// RATE LIMITING
// ===================================================

app.use(
  '/api/insights',
  rateLimit({
    windowMs: 60000,
    limit: 180
  })
);

// ===================================================
// FIREBASE AUTHENTICATION
// ===================================================

app.use('/api/insights', async (req, res, next) => {
  try {
    const token = req.headers.authorization
      ?.match(/^Bearer (.+)$/)?.[1];

    if (!token) {
      return res.status(401).json({
        error: 'Sign in required'
      });
    }

    req.user = await auth.verifyIdToken(token, true);
    next();

  } catch (error) {
    res.status(401).json({
      error: 'Session expired or invalid'
    });
  }
});

// ===================================================
// ADMINISTRATOR AUTHORIZATION
// ===================================================

app.use('/api/insights/admin', (req, res, next) => {
  res.set('Cache-Control', 'no-store');

  if (!admins.has(req.user.uid)) {
    return res.status(403).json({
      error: 'Admin access required'
    });
  }

  next();
});

const clean = (value, maxLength) =>
  typeof value === 'string'
    ? value.slice(0, maxLength)
    : '';

// ===================================================
// EXISTING EVENT TRACKING
// ===================================================

app.post('/api/insights/event', async (req, res, next) => {
  try {
    const b = req.body || {};

    if (
      !['pageview', 'heartbeat', 'feature'].includes(b.type) ||
      !/^[-\w]{8,80}$/.test(b.session || '')
    ) {
      return res.status(400).json({
        error: 'Invalid event'
      });
    }

    const at = Date.now();

    await db.collection('noveamEvents').add({
      uid: req.user.uid,
      type: b.type,
      session: b.session,
      page: clean(b.page, 150).split('?')[0],
      at,
      day: new Date(at).toISOString().slice(0, 10),
      expiresAt: new Date(at + 35 * 86400000)
    });

    res.sendStatus(204);

  } catch (error) {
    next(error);
  }
});

// ===================================================
// EXISTING FEEDBACK SUBMISSION
// ===================================================

app.post(
  '/api/insights/feedback',
  rateLimit({
    windowMs: 3600000,
    limit: 10
  }),
  async (req, res, next) => {
    try {
      const b = req.body || {};

      if (
        !['Bug', 'Suggestion', 'Feature request', 'General']
          .includes(b.category) ||
        !Number.isInteger(b.rating) ||
        b.rating < 1 ||
        b.rating > 5 ||
        typeof b.message !== 'string' ||
        b.message.trim().length < 5 ||
        b.message.length > 2000
      ) {
        return res.status(400).json({
          error:
            'Choose a category, rating and a message of 5–2000 characters.'
        });
      }

      await db.collection('noveamFeedback').add({
        uid: req.user.uid,
        category: b.category,
        rating: b.rating,
        message: b.message.trim(),
        page: clean(b.page, 150).split('?')[0],
        status: 'New',
        at: Date.now()
      });

      res.status(201).json({ ok: true });

    } catch (error) {
      next(error);
    }
  }
);

// ===================================================
// EXISTING ADMIN OVERVIEW
// ===================================================

app.get(
  '/api/insights/admin/overview',
  async (req, res, next) => {
    try {
      const since = Date.now() - 30 * 86400000;

      const [ev, fb] = await Promise.all([
        db.collection('noveamEvents')
          .where('at', '>=', since)
          .get(),

        db.collection('noveamFeedback')
          .orderBy('at', 'desc')
          .limit(500)
          .get()
      ]);

      let count = 0;
      let token;

      do {
        const batch = await auth.listUsers(1000, token);
        count += batch.users.length;
        token = batch.pageToken;
      } while (token);

      res.json({
        registeredUsers: count,
        ...summarize(ev.docs.map(d => d.data())),
        feedback: fb.docs.map(d => ({
          id: d.id,
          ...d.data()
        })),
        updatedAt: Date.now()
      });

    } catch (error) {
      next(error);
    }
  }
);

// ===================================================
// EXISTING ADMIN FEEDBACK STATUS UPDATE
// ===================================================

app.patch(
  '/api/insights/admin/feedback/:id',
  async (req, res, next) => {
    try {
      if (
        !['New', 'Reviewing', 'Planned', 'Resolved']
          .includes(req.body?.status)
      ) {
        return res.status(400).json({
          error: 'Invalid status'
        });
      }

      const ref = db.collection('noveamFeedback')
        .doc(req.params.id);

      if (!(await ref.get()).exists) {
        return res.sendStatus(404);
      }

      await ref.update({
        status: req.body.status
      });

      res.json({ ok: true });

    } catch (error) {
      next(error);
    }
  }
);

// ===================================================
// NEW FEATURE 1 — FIREBASE USER DIRECTORY
// ===================================================

app.get(
  '/api/insights/admin/users',
  async (req, res, next) => {
    try {
      const limit = Math.min(
        100,
        Math.max(
          1,
          Number.parseInt(req.query.limit, 10) || 50
        )
      );

      const pageToken =
        typeof req.query.pageToken === 'string' &&
        req.query.pageToken.length <= 2048
          ? req.query.pageToken
          : undefined;

      const result = await auth.listUsers(
        limit,
        pageToken
      );

      
res.json({
  users: result.users.map(u => ({
    uid: u.uid,
    email: u.email || null,
    displayName: u.displayName || null,
    disabled: u.disabled,

    role: admins.has(u.uid)
      ? 'Administrator'
      : 'Normal User',

    emailVerified: u.emailVerified,

    providers: u.providerData.map(p => p.providerId),

    createdAt: u.metadata.creationTime || null,
    lastSignIn: u.metadata.lastSignInTime || null
  })),

  nextPageToken: result.pageToken || null
});


    } catch (error) {
      next(error);
    }
  }
);

// ===================================================
// PRIVATE SERVICE HELPER
// ===================================================

// Only use HTTPS for private service requests.
// Do not expose these service tokens in frontend JS.

async function fetchPrivateAnalytics(base, token, path) {
  if (!base || !token) {
    return null;
  }

  const url = new URL(
    path,
    base.replace(/\/+$/, '') + '/'
  );

  if (url.protocol !== 'https:') {
    throw Error('Private analytics requires HTTPS');
  }

  const controller = new AbortController();

  const timer = setTimeout(
    () => controller.abort(),
    5000
  );

  try {
    const response = await fetch(url, {
      headers: {
        Authorization: 'Bearer ' + token
      },
      signal: controller.signal
    });

    if (!response.ok) {
      throw Error(
        'Private analytics returned HTTP ' +
        response.status
      );
    }

    return await response.json();

  } finally {
    clearTimeout(timer);
  }
}

// ===================================================
// NEW FEATURE 2 — GEOGRAPHIC ANALYTICS
// ===================================================

app.get(
  '/api/insights/admin/geography',
  async (req, res, next) => {
    try {
      const base =
        process.env.NOVEAM_GEO_INTERNAL_URL || '';

      const token =
        process.env.NOVEAM_GEO_SERVICE_TOKEN || '';

      if (!base || !token) {
        return res.status(503).json({
          status: 'unavailable',
          error: 'Geographic analytics not configured'
        });
      }

      const result = await fetchPrivateAnalytics(
        base,
        token,
        '/internal/admin/geography'
      );

      // Return only aggregated statistics.
      // The private FastAPI endpoint must not
      // return raw IPs or individual locations.

      res.json(result);

    } catch (error) {
      console.error(
        'Geographic analytics failed:',
        error.message
      );

      res.status(503).json({
        status: 'unavailable',
        error: 'Geographic analytics unavailable'
      });
    }
  }
);

// ===================================================
// NEW FEATURE 3 — REGISTRATION SOURCE COUNTS
// ===================================================

app.get(
  '/api/insights/admin/registration-sources',
  async (req, res, next) => {
    try {
      const result = {
        sqlite: {
          normalUsers: null
        },
        postgresql: {
          normalUsers: null
        },
        firebase: {
          adminUsers: null
        }
      };

      // Count existing Firebase administrator UIDs.

      let adminCount = 0;

      try {
        for (const uid of admins) {
          try {
            await auth.getUser(uid);
            adminCount++;

          } catch (error) {
            if (error.code !== 'auth/user-not-found') {
              throw error;
            }
          }
        }

        result.firebase.adminUsers = adminCount;

      } catch (error) {
        console.error(
          'Firebase admin count failed:',
          error.message
        );
      }

      // Read SQLite and PostgreSQL counts
      // from the authenticated FastAPI service.

      const base =
        process.env.NOVEAM_STATS_INTERNAL_URL || '';

      const token =
        process.env.NOVEAM_STATS_SERVICE_TOKEN || '';

      if (base && token) {
        try {
          const stats = await fetchPrivateAnalytics(
            base,
            token,
            '/internal/admin/registration-sources'
          );

          const sqliteCount =
            stats?.sqlite?.normalUsers;

          const postgresCount =
            stats?.postgresql?.normalUsers;

          if (
            Number.isSafeInteger(sqliteCount) &&
            sqliteCount >= 0
          ) {
            result.sqlite.normalUsers =
              sqliteCount;
          }

          if (
            Number.isSafeInteger(postgresCount) &&
            postgresCount >= 0
          ) {
            result.postgresql.normalUsers =
              postgresCount;
          }

        } catch (error) {
          console.error(
            'Database statistics unavailable:',
            error.message
          );
        }
      }

      res.json(result);

    } catch (error) {
      next(error);
    }
  }
);
/* NOVEAM — Additional Admin Analytics
   Add before the centralized error handler.
   Read-only: no database modifications.
*/

app.get('/api/insights/admin/analytics-summary', async (req, res) => {
  // Validate the selected analytics date range.
  const today = new Date().toISOString().slice(0, 10);

  const startDate = req.query.startDate;
  const endDate = req.query.endDate;

  const validDate = value => {
    if (typeof value !== 'string' ||
        !/^\d{4}-\d{2}-\d{2}$/.test(value)) {
      return false;
    }

    const parsed = new Date(value + 'T00:00:00.000Z');

    return !Number.isNaN(parsed.getTime()) &&
      parsed.toISOString().slice(0, 10) === value;
  };

  if (!validDate(startDate) ||
      !validDate(endDate) ||
      startDate > endDate ||
      endDate > today) {
    return res.status(400).json({
      error: 'Select a valid date range ending today or earlier.'
    });
  }

  const rangeStart =
    Date.parse(startDate + 'T00:00:00.000Z');

  const rangeEnd =
    Date.parse(endDate + 'T00:00:00.000Z') + 86400000;

  const result = {
    updatedAt: Date.now(),
    users: {
      registered: null,
      newLast7Days: null,
      disabled: null,
      administrators: null
    },
    engagement: {
      dau: null,
      wau: null,
      mau: null,
      sessions: null,
      activeNow: null,
      mostVisitedPages: [],
      daily: []
    },
    feedback: {
      total: null,
      new: null,
      reviewing: null,
      planned: null,
      resolved: null
    },
    sources: {
      firebase: 'unavailable',
      firestore: 'unavailable',
      sqlite: 'unavailable',
      postgresql: 'unavailable'
    },
    futureFeatures: {
      ai: 'not_enabled',
      web3: 'not_enabled',
      monetization: 'not_enabled'
    }
  };

  // Firebase user statistics
  try {
    const allUsers = [];
    let token;

    do {
      const batch = await auth.listUsers(1000, token);
      allUsers.push(...batch.users);
      token = batch.pageToken;
    } while (token);

    const weekAgo = Date.now() - 7 * 86400000;

    result.users.registered = allUsers.length;
    result.users.disabled =
      allUsers.filter(u => u.disabled).length;
    result.users.administrators =
      allUsers.filter(u => admins.has(u.uid)).length;
    result.users.newLast7Days =
      allUsers.filter(u =>
        Date.parse(u.metadata.creationTime) >= weekAgo
      ).length;

    result.sources.firebase = 'available';
  } catch (err) {
    console.error('Firebase analytics:', err.message);
  }

  // Firestore engagement and feedback
  try {
    
    const [events, feedback] = await Promise.all([
     
db.collection('noveamEvents')
  .where('at', '>=', rangeStart)
  .where('at', '<', rangeEnd)
  .get(),

      db.collection('noveamFeedback')
        .limit(500).get()
    ]);

    const metrics = summarizeDateRange(
  events.docs.map(d => d.data()),
  startDate,
  endDate
);
    );

    Object.assign(result.engagement, {
      dau: metrics.dau,
      wau: metrics.wau,
      mau: metrics.mau,
      sessions: metrics.sessions,
      activeNow: metrics.activeNow,
      mostVisitedPages: metrics.pages,
      daily: metrics.daily
    });

    const items = feedback.docs.map(d => d.data());

    result.feedback = {
      total: items.length,
      new: items.filter(x => x.status === 'New').length,
      reviewing: items.filter(
        x => x.status === 'Reviewing'
      ).length,
      planned: items.filter(
        x => x.status === 'Planned'
      ).length,
      resolved: items.filter(
        x => x.status === 'Resolved'
      ).length
    };

    result.sources.firestore = 'available';
  } catch (err) {
    console.error('Firestore analytics:', err.message);
  }

  res.set('Cache-Control', 'no-store');
  res.json(result);
});

// ===================================================
// CENTRALIZED ERROR HANDLER
// ===================================================

app.use((err, req, res, next) => {
  console.error('NOVEAM Admin API error:', {
    method: req.method,
    path: req.path,
    code: err.code,
    name: err.name,
    message: err.message,
    stack: err.stack
  });

  res.status(500).json({
    error: 'Service unavailable. Check backend configuration.'
  });
});

// ===================================================
// START RENDER SERVER
// ===================================================

const PORT = Number(process.env.PORT || 8080);

app.listen(PORT, '0.0.0.0', () => {
  console.log(
    `NOVEAM Admin Server running on port ${PORT}`
  );
});
