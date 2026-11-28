import React, { useEffect, useState } from "react";
import { api } from "../api";

export default function Align() {
  const [data, setData] = useState(null);
  useEffect(() => { api.align().then(setData); }, []);
  if (!data) return null;
  const podName = (id) => (data["舱体投影"].find((p) => p.id === id) || {}).name || id;
  const fourSide = (b) => (b["互证"] || []).find((c) => c["名称"] === "四侧证");
  return (
    <>
      <div className="hero">
        <h1>对读屏</h1>
        <p>启停结果、探针色、镜像拉取态同屏投影。双完成必须四侧同屏可核：锁旁读 ∪ 锁水位桶 ∪ 启停回执对 ∪ 审计；缺侧则退回后到那笔，不由面板灯号宣称。</p>
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
            <tr><th>ID</th><th>舱</th><th>让路臂</th><th>结局</th><th>状态</th><th>重放</th><th>四侧</th><th>互证</th></tr>
          </thead>
          <tbody>
            {data["争用桶"].length === 0 && (
              <tr><td colSpan="8">暂无启停对砸争用</td></tr>
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
                <td title={(fourSide(b) || {}).详情 || ""}>
                  <span className={`tag ${fourSide(b)?.通过 ? "ok" : "bad"}`}>
                    {fourSide(b)?.通过 ? "四侧齐" : "四侧缺"}
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
