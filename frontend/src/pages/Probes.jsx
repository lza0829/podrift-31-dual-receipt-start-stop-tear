import React, { useEffect, useState } from "react";
import { api } from "../api";

export default function Probes() {
  const [rows, setRows] = useState([]);
  const load = () => api.probes().then(setRows);
  useEffect(() => { load(); }, []);
  return (
    <>
      <div className="hero">
        <h1>探针灯</h1>
        <p>健康探针红绿与连续失败次数。红灯是否允许算「起成」由题面定臂。</p>
      </div>
      <div className="panel">
        <table>
          <thead>
            <tr><th>舱</th><th>颜色</th><th>失败连击</th><th>更新</th><th></th></tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.pod_id}>
                <td>{r.pod_name}</td>
                <td><span className={`tag ${r.color === "绿" ? "ok" : "bad"}`}>{r.color}</span></td>
                <td>{r.fail_streak}</td>
                <td>{r.updated_at}</td>
                <td>
                  <button className="btn" onClick={() => api.setProbe(r.pod_id, { color: "绿", fail_streak: 0 }).then(load)}>置绿</button>
                  <button className="btn" onClick={() => api.setProbe(r.pod_id, { color: "红", fail_streak: (r.fail_streak || 0) + 1 }).then(load)}>置红</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
