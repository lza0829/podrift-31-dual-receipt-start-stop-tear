import React, { useEffect, useState } from "react";
import { api } from "../api";

export default function Policy() {
  const [p, setP] = useState(null);
  const load = () => api.policy().then(setP);
  useEffect(() => { load(); }, []);
  if (!p) return null;
  return (
    <>
      <div className="hero">
        <h1>重启策</h1>
        <p>失败重启 / 永不 / 总是。与启停单、探针灯争用时谁先落库由题面定臂。</p>
      </div>
      <div className="panel">
        <p>当前模式：<strong>{p.mode}</strong> · 退避 {p.backoff_sec}s</p>
        <div className="row-actions">
          {["失败重启", "永不", "总是"].map((m) => (
            <button key={m} className="btn" onClick={() => api.setPolicy({ mode: m }).then(load)}>{m}</button>
          ))}
        </div>
      </div>
    </>
  );
}
