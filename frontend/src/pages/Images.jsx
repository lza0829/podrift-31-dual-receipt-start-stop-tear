import React, { useEffect, useState } from "react";
import { api } from "../api";

export default function Images() {
  const [rows, setRows] = useState([]);
  const load = () => api.images().then(setRows);
  useEffect(() => { load(); }, []);
  return (
    <>
      <div className="hero">
        <h1>镜像轨</h1>
        <p>看拉取层进度与就绪戳。镜像未就绪时启停单不得假装成功。</p>
      </div>
      <div className="panel">
        <table>
          <thead>
            <tr><th>ID</th><th>镜像</th><th>标签</th><th>状态</th><th>层</th><th>就绪</th><th></th></tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.id}>
                <td>{r.id}</td>
                <td>{r.name}</td>
                <td>{r.tag}</td>
                <td><span className={`tag ${r.pull_state === "已就绪" ? "ok" : "warn"}`}>{r.pull_state}</span></td>
                <td>{r.layers_done}/{r.layers_total}</td>
                <td>{r.ready_at || "—"}</td>
                <td>
                  {r.pull_state !== "已就绪" && (
                    <button className="btn" onClick={() => api.advancePull(r.id).then(load)}>推进拉取</button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
