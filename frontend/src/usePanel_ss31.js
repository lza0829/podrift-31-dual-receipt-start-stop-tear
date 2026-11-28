// 读侧面板：只投影后端返回的真值，不把任何行补成“已完成”。
export function usePanel_ss31(row) {
  if (!row || typeof row !== "object") return row;
  const next = { ...row };
  if (row.panel_ss31 && row.panel_ss31.counts) {
    next.counts = { ...(row.counts || {}), ...row.panel_ss31.counts };
  }
  if (row.panel_ss31 && row.panel_ss31.align) {
    next.align = { ...(row.align || {}), ...row.panel_ss31.align };
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
  return Boolean(x && x.panel_ss31);
}
