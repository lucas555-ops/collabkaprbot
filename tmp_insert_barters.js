// --- Barters: Filters + Public view + Inbox (v0.9.2) ---

function fmtMs(ms) {
  const m = Math.max(0, Math.floor(ms / 60000));
  const h = Math.floor(m / 60);
  const mm = m % 60;
  if (h <= 0) return `${mm}м`;
  return `${h}ч ${mm}м`;
}

function bxMark(on) {
  return on ? '✅ ' : '';
}

function bxFiltersKb(wsId, page, f) {
  const kb = new InlineKeyboard();

  // Category
  kb.text(bxMark(f.category == null) + 'Все', `a:bx_fcat|ws:${wsId}|v:all|p:${page}`)
    .text(bxMark(f.category === 'cosmetics') + '💄', `a:bx_fcat|ws:${wsId}|v:cosmetics|p:${page}`)
    .text(bxMark(f.category === 'skincare') + '🧴', `a:bx_fcat|ws:${wsId}|v:skincare|p:${page}`)
    .row()
    .text(bxMark(f.category === 'accessories') + '🎀', `a:bx_fcat|ws:${wsId}|v:accessories|p:${page}`)
    .text(bxMark(f.category === 'other') + '✨', `a:bx_fcat|ws:${wsId}|v:other|p:${page}`)
    .row();

  // Offer type
  kb.text(bxMark(f.offerType == null) + 'Все', `a:bx_ftype|ws:${wsId}|v:all|p:${page}`)
    .text(bxMark(f.offerType === 'ad') + '📣', `a:bx_ftype|ws:${wsId}|v:ad|p:${page}`)
    .text(bxMark(f.offerType === 'review') + '🎥', `a:bx_ftype|ws:${wsId}|v:review|p:${page}`)
    .row()
    .text(bxMark(f.offerType === 'giveaway') + '🎁', `a:bx_ftype|ws:${wsId}|v:giveaway|p:${page}`)
    .text(bxMark(f.offerType === 'other') + '✍️', `a:bx_ftype|ws:${wsId}|v:other|p:${page}`)
    .row();

  // Compensation
  kb.text(bxMark(f.compensationType == null) + 'Все', `a:bx_fcomp|ws:${wsId}|v:all|p:${page}`)
    .text(bxMark(f.compensationType === 'barter') + '🤝', `a:bx_fcomp|ws:${wsId}|v:barter|p:${page}`)
    .text(bxMark(f.compensationType === 'cert') + '🎟', `a:bx_fcomp|ws:${wsId}|v:cert|p:${page}`)
    .row()
    .text(bxMark(f.compensationType === 'rub') + '💸', `a:bx_fcomp|ws:${wsId}|v:rub|p:${page}`)
    .text(bxMark(f.compensationType === 'mixed') + '🔁', `a:bx_fcomp|ws:${wsId}|v:mixed|p:${page}`)
    .row();

  kb.text('🔄 Сброс', `a:bx_freset|ws:${wsId}|p:${page}`)
    .row()
    .text('⬅️ Назад', `a:bx_feed|ws:${wsId}|p:${page}`);

  return kb;
}

async function renderBxFilters(ctx, ownerUserId, wsId, page) {
  const ws = await db.getWorkspace(ownerUserId, wsId);
  if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
  if (!ws.network_enabled) return renderBxOpen(ctx, ownerUserId, wsId);

  const f = await getBxFilter(ctx.from.id, wsId);
  const text = `🎛 <b>Фильтры ленты</b>

${escapeHtml(bxFilterSummary(f))}`;
  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: bxFiltersKb(wsId, page ?? 0, f) });
}

async function renderBxPublicOffer(ctx, ownerUserId, wsId, offerId, backPage) {
  const ws = await db.getWorkspace(ownerUserId, wsId);
  if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
  if (!ws.network_enabled) return renderBxOpen(ctx, ownerUserId, wsId);

  const o = await db.getBarterOfferPublic(offerId);
  if (!o || String(o.status).toUpperCase() !== 'ACTIVE') return ctx.answerCallbackQuery({ text: 'Оффер недоступен.' });

  const ch = o.channel_username ? `@${o.channel_username}` : (o.ws_title || 'канал');
  const contact = (o.contact || '').trim();
  const contactLine = contact ? `\nКонтакт: <b>${escapeHtml(contact)}</b>` : '';

  const text = `🔎 <b>Оффер #${o.id}</b>

${escapeHtml(bxCategoryLabel(o.category))} · ${escapeHtml(bxTypeLabel(o.offer_type))} · ${escapeHtml(bxCompLabel(o.compensation_type))}
Канал: <b>${escapeHtml(ch)}</b>${contactLine}

<b>${escapeHtml(o.title)}</b>
${escapeHtml(o.description || '—')}`;

  const kb = new InlineKeyboard();
  if (o.owner_user_id && o.owner_user_id !== ownerUserId) {
    kb.text('💬 Написать', `a:bx_thread_new|ws:${wsId}|o:${o.id}|p:${backPage ?? 0}`).row();
  } else {
    kb.text('🛠 Управлять', `a:bx_view|ws:${wsId}|o:${o.id}|back:my`).row();
  }
  kb.text('⬅️ Назад', `a:bx_feed|ws:${wsId}|p:${backPage ?? 0}`);

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
}

async function renderBxInbox(ctx, ownerUserId, wsId, page = 0) {
  const ws = await db.getWorkspace(ownerUserId, wsId);
  if (!ws) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });
  if (!ws.network_enabled) return renderBxOpen(ctx, ownerUserId, wsId);

  const limit = CFG.BARTER_INBOX_PAGE_SIZE;
  const offset = page * limit;
  const rows = await db.listBarterThreadsForUser(ownerUserId, limit, offset);

  const kb = new InlineKeyboard();
  for (const t of rows) {
    const who = t.other_tg_username ? '@' + t.other_tg_username : ('user#' + t.other_user_id);
    kb.text(`#${t.offer_id} · ${t.offer_title} · ${who}`, `a:bx_thread|ws:${wsId}|i:${t.id}|p:${page}`).row();
  }
  kb.text('⬅️ Назад', `a:bx_open|ws:${wsId}`);

  const text = `📨 <b>Inbox</b>

${rows.length ? 'Открой диалог:' : 'Пока пусто. Открой оффер в ленте и нажми “💬 Написать”.'}`;
  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
}

async function renderBxThread(ctx, ownerUserId, wsId, threadId, backPage = 0) {
  const th = await db.getBarterThreadForUser(threadId, ownerUserId);
  if (!th) return ctx.answerCallbackQuery({ text: 'Нет доступа.' });

  const msgs = await db.listBarterMessages(threadId, 12);
  const lines = msgs.slice().reverse().map(m => {
    const who = m.sender_tg_username ? '@' + m.sender_tg_username : ('user#' + m.sender_user_id);
    return `<b>${escapeHtml(who)}</b>: ${escapeHtml(m.body)}`;
  });

  const other = th.other_tg_username ? '@' + th.other_tg_username : ('user#' + th.other_user_id);
  const text = `💬 <b>Диалог</b>
Оффер: <b>#${th.offer_id}</b> · ${escapeHtml(th.offer_title)}
Собеседник: <b>${escapeHtml(other)}</b>

${lines.length ? lines.join('\n') : 'Пока сообщений нет.'}`;

  const kb = new InlineKeyboard()
    .text('✍️ Написать', `a:bx_thread_write|ws:${wsId}|i:${threadId}`)
    .row()
    .text('⬅️ Inbox', `a:bx_inbox|ws:${wsId}|p:${backPage}`);

  await ctx.editMessageText(text, { parse_mode: 'HTML', reply_markup: kb });
}
