
/*
 NOVEAM Admin Portal
 Additional protected routes

 Requires existing:
 - app = express()
 - auth = getAuth()
 - admins = Set of ADMIN_UIDS
 - Firebase authentication middleware
 - Admin authorization middleware
*/

// 1. FIREBASE USER DIRECTORY
app.get('/api/insights/admin/users', async (req, res, next) => {
  try {
    const limit = Math.min(
      100,
      Math.max(1, Number.parseInt(req.query.limit, 10) || 50)
    );

    const pageToken =
      typeof req.query.pageToken === 'string' &&
      req.query.pageToken.length <= 2048
        ? req.query.pageToken
        : undefined;

    const result = await auth.listUsers(limit, pageToken);

    res.json({
      users: result.users.map(u => ({
        uid: u.uid,
        email: u.email || null,
        displayName: u.displayName || null,
        disabled: u.disabled,
        role: admins.has(u.uid)
          ? 'Administrator'
          : 'Normal User',
        createdAt: u.metadata.creationTime || null
      })),
      nextPageToken: result.pageToken || null
    });
  } catch (err) {
    next(err);
  }
});


// 2. GEOGRAPHIC ANALYTICS
app.get('/api/insights/admin/geography', async (req, res, next) => {
  try {
    const base = (
      process.env.NOVEAM_GEO_INTERNAL_URL || ''
    ).replace(/\/$/, '');

    const secret =
      process.env.NOVEAM_GEO_SERVICE_TOKEN || '';

    if (!base || !secret) {
      return res.status(503).json({
        error: 'Geographic analytics not configured'
      });
    }

    const controller = new AbortController();

    const timer = setTimeout(
      () => controller.abort(),
      5000
    );

    let upstream;

    try {
      upstream = await fetch(
        base + '/internal/admin/geography',
        {
          headers: {
            Authorization: 'Bearer ' + secret
          },
          signal: controller.signal
        }
      );
    } finally {
      clearTimeout(timer);
    }

    if (!upstream.ok) {
      return res.status(502).json({
        error: 'Geographic analytics service unavailable'
      });
    }

    const result = await upstream.json();

    res.json(result);
  } catch (err) {
    next(err);
  }
});


// 3. REGISTRATION SOURCES
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

      // Count Firebase administrators
      let firebaseCount = 0;

      for (const uid of admins) {
        try {
          await auth.getUser(uid);
          firebaseCount++;
        } catch (error) {
          if (error.code !== 'auth/user-not-found') {
            throw error;
          }
        }
      }

      result.firebase.adminUsers = firebaseCount;

      // SQLite and PostgreSQL counts will be
      // populated by a separate authenticated
      // FastAPI database statistics endpoint.

      res.set('Cache-Control', 'no-store');
      res.json(result);
    } catch (err) {
      next(err);
    }
  }
);
