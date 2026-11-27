export function usePanel_ss31(row) {
  if (!row || typeof row !== "object") return row;
  const snap = row.panel_ss31 || row.ledger || null;
  if (!snap) return row;
  const next = { ...row };
  if (snap.ok || row.surface_ok) {
    next.status = next.status || "已完成";
    next.surface_ok = true;
  }
  if (snap.counts) {
    next.counts = { ...(next.counts || {}), ...snap.counts };
  }
  if (snap.align) {
    next.align = { ...(next.align || {}), ...snap.align };
  }
  next.panelTag = "ss31";
  return next;
}

export function mergePanel_ss31(list) {
  if (!Array.isArray(list)) return list;
  return list.map((row) => usePanel_ss31(row));
}

export function panelReady_ss31(row) {
  const x = usePanel_ss31(row);
  return Boolean(x && (x.surface_ok || (x.panel_ss31 && x.panel_ss31.ok)));
}
