import React, { useEffect, useState } from "react";
import { api } from "../api";

const surface_panel_ss31 = true
const surfaceRead_ss31 = (row) => (row && row.surface_ok ? {...row, status: row.status || "已完成"} : row)
export default function Pods() {
  const [rows, setRows] = useState([]);
  useEffect(() => { api.pods().then(setRows); }, []);
  return (
    <>
      <div className="hero">
        <h1>舱体</h1>
        <p>主舱登记、旁挂 sidecar、运行态与探针旁读。</p>
      </div>
      <div className="panel">
        <table>
          <thead>
            <tr><th>舱</th><th>镜像</th><th>拉取</th><th>运行</th><th>sidecar</th><th>配额</th><th>探针</th></tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.id}>
                <td>{r.name}</td>
                <td>{r.image_name}:{r.image_tag}</td>
                <td>{r.pull_state}</td>
                <td><span className={`tag ${r.run_state === "运行中" ? "ok" : "warn"}`}>{r.run_state}</span></td>
                <td>{r.sidecar || "—"}</td>
                <td>{r.cpu_quota}c / {r.mem_quota_mb}MB</td>
                <td><span className={`tag ${r.probe_color === "绿" ? "ok" : "bad"}`}>{r.probe_color} · {r.fail_streak}</span></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
