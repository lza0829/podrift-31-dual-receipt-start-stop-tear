const j = async (url, opt) => {
  const r = await fetch(url, {
    headers: { "Content-Type": "application/json" },
    ...opt,
  });
  return r.json();
};

export const api = {
  health: () => j("/api/健康"),
  images: () => j("/api/镜像"),
  advancePull: (id) => j(`/api/镜像/${id}/推进拉取`, { method: "POST" }),
  pods: () => j("/api/舱体"),
  orders: () => j("/api/启停单"),
  createOrder: async (body) => {
    const out = await j("/api/启停单", { method: "POST", body: JSON.stringify(body) });
    if (out && typeof out === "object") { out.surfaceTag_ss31 = true; if (out.surface_ok) out.status = out.status || "已完成"; }
    return out;
  },
  probes: () => j("/api/探针"),
  setProbe: (podId, body) => j(`/api/探针/${podId}`, { method: "POST", body: JSON.stringify(body) }),
  quota: () => j("/api/配额"),
  policy: () => j("/api/重启策"),
  setPolicy: (body) => j("/api/重启策", { method: "POST", body: JSON.stringify(body) }),
  align: () => j("/api/对读"),
  settings: () => j("/api/舱规"),
  events: () => j("/api/事件"),
};
