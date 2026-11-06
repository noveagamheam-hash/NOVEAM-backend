"""Social activity recipients. Called inside the action's database transaction."""


def notify_many(conn, notify, actor, recipients, kind, title, target_type, target_id):
    for uid in sorted({int(uid) for uid in recipients if uid is not None} - {int(actor)}):
        blocked = conn.execute('''SELECT 1 FROM conversation_controls WHERE blocked=1
            AND ((user_id=%s AND peer_user_id=%s) OR (user_id=%s AND peer_user_id=%s))''',
            (uid, actor, actor, uid)).fetchone()
        if not blocked:
            notify(conn, uid, actor, kind, title, '', target_type, target_id)


def connected_users(conn, actor):
    rows = conn.execute('''SELECT CASE WHEN sender_user_id=%s THEN receiver_user_id ELSE sender_user_id END AS user_id
        FROM connection_requests WHERE status='ACCEPTED' AND (sender_user_id=%s OR receiver_user_id=%s)''',
        (actor, actor, actor)).fetchall()
    return {row['user_id'] for row in rows}


def activity_event(conn, notify, event, actor, entity_id, parent_id=None):
    recipients, kind, title, target = set(), event.upper(), '', ''
    if event == 'post':
        post = conn.execute('SELECT author_user_id,visibility FROM posts WHERE id=%s', (entity_id,)).fetchone()
        if not post or post['visibility'] not in ('PUBLIC', 'CONNECTIONS'):
            return
        recipients = connected_users(conn, actor)
        if post['visibility'] == 'PUBLIC':
            recipients |= {row['follower_user_id'] for row in conn.execute('SELECT follower_user_id FROM follows WHERE followed_user_id=%s', (actor,)).fetchall()}
        kind, title, target = 'NEW_POST', 'Shared a new post', 'post'
    elif event == 'channel_message':
        channel = conn.execute('SELECT space_id FROM communication_channels WHERE id=%s', (parent_id,)).fetchone()
        if not channel:
            return
        recipients = {r['user_id'] for r in conn.execute('SELECT user_id FROM communication_space_members WHERE space_id=%s', (channel['space_id'],)).fetchall()}
        kind, title, target, entity_id = 'CHANNEL_MESSAGE', 'New group message', 'channel', parent_id
    elif event in ('meeting_message', 'meeting_end'):
        recipients = {r['user_id'] for r in conn.execute("SELECT user_id FROM communication_meeting_participants WHERE meeting_id=%s AND state='joined'", (parent_id,)).fetchall()}
        kind, title, target, entity_id = ('MEETING_MESSAGE' if event == 'meeting_message' else 'MEETING_ENDED'), ('New meeting message' if event == 'meeting_message' else 'Meeting ended'), 'meeting', parent_id
    elif event == 'meeting_created':
        meeting = conn.execute('SELECT space_id FROM communication_meetings WHERE id=%s', (entity_id,)).fetchone()
        if not meeting or not meeting['space_id']:
            return  # Personal meetings are private until the host invites someone.
        recipients = {r['user_id'] for r in conn.execute('SELECT user_id FROM communication_space_members WHERE space_id=%s', (meeting['space_id'],)).fetchall()}
        kind, title, target = 'MEETING_CREATED', 'A group meeting was created', 'meeting'
    elif event == 'meeting_admitted':
        member = conn.execute("SELECT 1 FROM communication_meeting_participants WHERE meeting_id=%s AND user_id=%s AND state='joined'", (parent_id, entity_id)).fetchone()
        if not member:
            return
        recipients, kind, title, target, entity_id = {entity_id}, 'MEETING_ADMITTED', 'You were admitted to a meeting', 'meeting', parent_id
    elif event == 'meeting_waiting':
        meeting = conn.execute('SELECT host_user_id FROM communication_meetings WHERE id=%s', (parent_id,)).fetchone()
        recipients = {meeting['host_user_id']} if meeting else set()
        kind, title, target, entity_id = 'MEETING_JOIN_REQUEST', 'Someone is waiting to join your meeting', 'meeting', parent_id
    elif event == 'page_post':
        recipients = {r['user_id'] for r in conn.execute('SELECT user_id FROM organization_page_followers WHERE page_id=%s', (parent_id,)).fetchall()}
        kind, title, target = 'PAGE_POST', 'A page you follow shared a post', 'page_post'
    elif event in ('page_comment', 'page_like'):
        row = conn.execute('SELECT author_user_id FROM page_posts WHERE id=%s', (parent_id,)).fetchone()
        recipients = {row['author_user_id']} if row else set()
        kind, title, target, entity_id = ('PAGE_COMMENT' if event == 'page_comment' else 'PAGE_LIKE'), ('Commented on your page post' if event == 'page_comment' else 'Liked your page post'), 'page_post', parent_id
    elif event == 'group_post':
        recipients = {r['user_id'] for r in conn.execute("SELECT user_id FROM community_group_members WHERE group_id=%s AND status='active'", (parent_id,)).fetchall()}
        kind, title, target = 'GROUP_POST', 'New post in your group', 'group_post'
    elif event == 'group_comment':
        row = conn.execute('''SELECT p.author_user_id FROM community_group_posts p JOIN community_group_members m
            ON m.group_id=p.group_id AND m.user_id=p.author_user_id WHERE p.id=%s AND m.status='active' ''', (parent_id,)).fetchone()
        recipients = {row['author_user_id']} if row else set()
        kind, title, target, entity_id = 'GROUP_COMMENT', 'Commented on your group post', 'group_post', parent_id
    elif event == 'event_invite':
        recipients, kind, title, target, entity_id = {entity_id}, 'EVENT_INVITE', 'You were invited to an event', 'event', parent_id
    if title:
        notify_many(conn, notify, actor, recipients, kind, title, target, entity_id)


def activity_visible(conn, row):
    """Recheck access after a queue delay or membership/visibility change."""
    kind, uid, target = row['type'], row['user_id'], row.get('target_id')
    if kind == 'NEW_POST':
        post = conn.execute('SELECT author_user_id,visibility FROM posts WHERE id=%s', (target,)).fetchone()
        if not post or post['visibility'] not in ('PUBLIC', 'CONNECTIONS'):
            return False
        if uid in connected_users(conn, post['author_user_id']):
            return True
        return post['visibility'] == 'PUBLIC' and bool(conn.execute('SELECT 1 FROM follows WHERE followed_user_id=%s AND follower_user_id=%s', (post['author_user_id'], uid)).fetchone())
    if kind == 'CHANNEL_MESSAGE':
        return bool(conn.execute('''SELECT 1 FROM communication_channels c JOIN communication_space_members s
            ON s.space_id=c.space_id WHERE c.id=%s AND s.user_id=%s''', (target, uid)).fetchone())
    if kind in ('MEETING_MESSAGE', 'MEETING_ADMITTED', 'MEETING_ENDED'):
        return bool(conn.execute("SELECT 1 FROM communication_meeting_participants WHERE meeting_id=%s AND user_id=%s AND state='joined'", (target, uid)).fetchone())
    if kind == 'MEETING_CREATED':
        return bool(conn.execute('''SELECT 1 FROM communication_meetings m JOIN communication_space_members s
            ON s.space_id=m.space_id WHERE m.id=%s AND s.user_id=%s''', (target, uid)).fetchone())
    if kind in ('GROUP_POST', 'GROUP_COMMENT'):
        return bool(conn.execute('''SELECT 1 FROM community_group_posts p JOIN community_group_members m
            ON m.group_id=p.group_id WHERE p.id=%s AND m.user_id=%s AND m.status='active' ''', (target, uid)).fetchone())
    if kind == 'PAGE_POST':
        return bool(conn.execute('''SELECT 1 FROM page_posts p JOIN organization_page_followers f
            ON f.page_id=p.page_id WHERE p.id=%s AND f.user_id=%s''', (target, uid)).fetchone())
    if kind in ('PAGE_LIKE', 'PAGE_COMMENT'):
        return bool(conn.execute('SELECT 1 FROM page_posts WHERE id=%s AND author_user_id=%s', (target, uid)).fetchone())
    if kind == 'EVENT_INVITE':
        return bool(conn.execute("SELECT 1 FROM event_attendees WHERE event_id=%s AND user_id=%s AND rsvp='invited'", (target, uid)).fetchone())
    return True
