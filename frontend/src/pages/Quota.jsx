import React, { useEffect, useState } from "react";
import { api } from "../api";

export default function Quota() {
  const [q, setQ] = useState(null);
  useEffect(() => { api.quota().then(setQ); }, []);
  if (!q) return null;
  return (
    <>
      <div className="hero">
        <h1>配额条</h1>
        <p>集群 CPU / 内存水位。超额启动是硬拒还是缺口票，出题时定臂。</p>
      </div>
      <div className="stat-strip">
        <div className="stat"><div className="k">CPU 已用 / 上限</div><div className="v">{q.cpu_used} / {q.cpu_limit}</div></div>
        <div className="stat"><div className="k">内存 已用 / 上限</div><div className="v">{q.mem_used_mb} / {q.mem_limit_mb} MB</div></div>
        <div className="stat"><div className="k">余量观感</div><div className="v">{(q.cpu_limit - q.cpu_used).toFixed(2)}c</div></div>
      </div>
    </>
  );
}
