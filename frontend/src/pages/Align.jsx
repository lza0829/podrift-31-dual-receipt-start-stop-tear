import React, { useEffect, useState } from "react";
import { api } from "../api";

const surface_panel_ss31 = true
const surfaceRead_ss31 = (row) => (row && row.surface_ok ? {...row, status: row.status || "已完成"} : row)
export default function Align() {
  const [data, setData] = useState(null);
  useEffect(() => { api.align().then(setData); }, []);
  if (!data) return null;
  const podName = (id) => (data["舱体投影"].find((p) => p.id === id) || {}).name || id;
  return (
    <>
      <div className="hero">
        <h1>对读屏</h1>
        <p>启停结果、探针色、镜像拉取态同屏投影。启停对砸结局随争用桶落库，可重放、可互证。</p>
      </div>
      <div className="panel">
        <table>
          <thead>
            <tr><th>舱</th><th>运行</th><th>探针</th><th>镜像</th><th>最近启停</th></tr>
          </thead>
          <tbody>
            {data["舱体投影"].map((r) => (
              <tr key={r.id}>
                <td>{r.name}</td>
                <td>{r.run_state}</td>
                <td>{r.probe_color} · {r.fail_streak}</td>
                <td>{r.pull_state}</td>
                <td>{r.last_order_status || "—"} {r.last_order_note || ""}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="panel">
        <h2>争用桶</h2>
        <table>
          <thead>
            <tr><th>ID</th><th>舱</th><th>让路臂</th><th>结局</th><th>状态</th><th>重放</th><th>互证</th></tr>
          </thead>
          <tbody>
            {data["争用桶"].length === 0 && (
              <tr><td colSpan="7">暂无启停对砸争用</td></tr>
            )}
            {data["争用桶"].map((b) => (
              <tr key={b.id}>
                <td>{b.id}</td>
                <td>{podName(b.pod_id)}</td>
                <td>{b.arm}</td>
                <td>
                  <span className={`tag ${b.verdict === "双成" ? "ok" : b.verdict ? "bad" : "warn"}`}>
                    {b.verdict || "在途"}
                  </span>
                  {b.reason ? ` ${b.reason}` : ""}
                </td>
                <td><span className={`tag ${b.state === "在途" ? "warn" : "ok"}`}>{b.state}</span></td>
                <td>
                  <span className={`tag ${b["重放一致"] ? "ok" : "bad"}`}>
                    {b["重放一致"] ? "可重放" : "重放分歧"}
                  </span>
                </td>
                <td title={b["互证"].map((c) => `${c["名称"]}:${c["通过"] ? "过" : "挂"} ${c["详情"]}`).join("\n")}>
                  <span className={`tag ${b["互证通过"] ? "ok" : "bad"}`}>
                    {b["互证"].filter((c) => c["通过"]).length}/{b["互证"].length} 互证
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
